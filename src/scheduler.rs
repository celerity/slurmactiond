use crate::config::{Config, TargetId};
use crate::{github, slurm};
use anyhow::{bail, Context};
use log::{debug, error, info, warn};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use thiserror::Error;

fn match_target<'c>(config: &'c Config, labels: &[String]) -> Option<&'c TargetId> {
    let unmatched_labels: Vec<_> = (labels.iter())
        // GitHub only includes the "self-hosted" label if the set of labels is otherwise empty.
        // We don't require the user to list "self-hosted" in the config.
        .filter(|l| !(*l == "self-hosted" || config.runner.registration.labels.contains(*l)))
        .collect();
    let closest_matching_target = (config.targets.iter())
        .filter(|(_, p)| unmatched_labels.iter().all(|l| p.runner_labels.contains(l)))
        .min_by_key(|(_, p)| p.runner_labels.len()); // min: closest match
    if let Some((id, _)) = closest_matching_target {
        debug!("matched runner labels {:?} to target {}", labels, id.0);
        Some(id)
    } else {
        None
    }
}

#[derive(Debug, PartialEq, Eq)]
enum JobState {
    Queueing,
    Queued,
    InProgress(slurm::JobId),
}

struct RunnerState {
    slurm_job: slurm::JobId,
}

struct SchedulerState {
    jobs: HashMap<github::WorkflowJobId, JobState>,
    runners: HashMap<String, RunnerState>,
}

#[derive(Clone)]
pub struct Scheduler {
    state: Arc<Mutex<SchedulerState>>,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    InvalidState(String),
    #[error("{0}")]
    Failed(#[from] anyhow::Error),
}

impl Scheduler {
    pub fn new() -> Scheduler {
        Scheduler {
            state: Arc::new(Mutex::new(SchedulerState {
                jobs: HashMap::new(),
                runners: HashMap::new(),
            })),
        }
    }

    fn with_state<R, F: FnOnce(&mut SchedulerState) -> R>(&self, f: F) -> R {
        f(&mut self.state.lock().expect("Poisoned Mutex"))
    }

    async fn complete_runner_job(&self, runner: slurm::RunnerJob) -> anyhow::Result<()> {
        let mut runner = runner
            .attach()
            .await
            .with_context(|| "Attaching to SLURM job")?;
        let metadata = runner.take_metadata();

        self.with_state(|state| {
            match state.runners.entry(metadata.runner_name.clone()) {
                Entry::Occupied(_) => bail!("Runner {metadata} already known to scheduler"),
                Entry::Vacant(entry) => entry.insert(RunnerState {
                    slurm_job: metadata.slurm_job,
                }),
            };
            Ok(())
        })?;

        let result = runner.wait().await.with_context(|| "Waiting for SLURM job");

        // remove runner independent of job success
        self.with_state(|state| {
            (state.runners)
                .remove(&metadata.runner_name)
                .expect("Runner has not been removed from scheduler state")
        });

        info!("SLURM job exited with status {}", result?);

        Ok(())
    }

    pub fn job_enqueued<'c>(
        &self,
        job_id: github::WorkflowJobId,
        labels: &[String],
        config_path: &Path,
        config: &Config,
    ) -> Result<(), Error> {
        self.with_state(|state| match state.jobs.entry(job_id) {
            Entry::Occupied(_) => Err(Error::InvalidState(format!(
                "Job {job_id} is already enqueued"
            ))),
            Entry::Vacant(entry) => {
                entry.insert(JobState::Queueing);
                Ok(())
            }
        })?;

        if let Some(target) = match_target(&config, &labels) {
            debug!("Matched runner labels {labels:?} to target {}", job_id.0);
            info!("Launching SLURM job for workflow job {job_id}");
            let runner_job = slurm::RunnerJob::spawn(config_path, config, target)
                .with_context(|| "Submitting job to SLURM")?;

            let handle = self.clone();
            actix_web::rt::spawn(async move {
                if let Err(e) = handle.complete_runner_job(runner_job).await {
                    error!("{e:#}");
                }
            });

            let replaced = self.with_state(|state| state.jobs.insert(job_id, JobState::Queued));
            assert_eq!(replaced, Some(JobState::Queueing));
        } else {
            debug!("Runner labels {labels:?} do not match any target");
        }

        Ok(())
    }

    pub fn job_processing(
        &self,
        job_id: github::WorkflowJobId,
        runner_name: &str,
    ) -> Result<(), Error> {
        self.with_state(|state| {
            let runner = state.runners.get(runner_name);
            let job = state.jobs.get_mut(&job_id);
            match (runner, job) {
                (Some(runner_state), Some(job_state @ JobState::Queued)) => {
                    info!(
                        "Workflow job {job_id} picked up by runner {runner_name} (SLURM #{})",
                        runner_state.slurm_job
                    );
                    *job_state = JobState::InProgress(runner_state.slurm_job);
                    Ok(())
                }
                (Some(_runner_state), Some(_job_state /* not Queued */)) => {
                    Err(Error::InvalidState(format!("Job {job_id} not queued")))
                }
                (None, Some(_job_state)) => {
                    // There is no runner, but the job has started running. This can mean:
                    //   - The job was picked up by a foreign runner - TODO how to detect this?
                    //   - The webhook arrived faster than the IPC message with the metadata.
                    //     this should never happen, ideally we TODO panic in this case.
                    warn!(
                        "Job {job_id} appears to have been picked up by a foreign runner, {}",
                        "removing it from tracking"
                    );
                    state.jobs.remove(&job_id);
                    // TODO now we have a runner scheduled with nothing to do
                    Ok(())
                }
                (Some(runner_state), None) => {
                    // Our runner has picked up a foreign job. TODO spawn another one
                    warn!(
                        "Runner {runner_name} (SLURM #{}) has picked up foreign job {job_id}",
                        runner_state.slurm_job
                    );
                    Ok(())
                }
                (None, None) => {
                    // Foreign job was picked up by a foreign runner - this is expected.
                    Ok(())
                }
            }
        })
    }

    pub fn job_completed(&self, job_id: github::WorkflowJobId) {
        // It's acceptable for job_id to be unknown in case it's from a foreign job
        self.with_state(|state| state.jobs.remove(&job_id));
    }
}
