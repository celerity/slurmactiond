use crate::config::{Config, TargetId};
use crate::{github, slurm};
use anyhow::Context as _;
use log::{debug, error, info, warn};
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
    target: TargetId,
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

    async fn supervise_runner_job(
        &self,
        runner: slurm::RunnerJob,
        spawn_target: TargetId,
    ) -> anyhow::Result<()> {
        let (metadata, runner) = runner
            .attach()
            .await
            .with_context(|| "Attaching to SLURM job")?;

        let previous_state = {
            let key = metadata.runner_name.clone();
            let value = RunnerState {
                slurm_job: metadata.slurm_job,
                target: spawn_target,
            };
            self.with_state(|state| state.runners.insert(key, value))
        };
        assert!(
            previous_state.is_none(),
            "Runner already connected to scheduler"
        );

        let result = runner.wait().await.with_context(|| "Waiting for SLURM job");

        // remove runner independent of job success
        self.with_state(|state| state.runners.remove(&metadata.runner_name))
            .expect("Runner has not been removed from scheduler state");

        info!("SLURM job exited with status {}", result?);

        Ok(())
    }

    fn schedule(
        &self,
        target: &TargetId,
        config_path: &Path,
        config: &Config,
    ) -> anyhow::Result<()> {
        let runner_job = slurm::RunnerJob::spawn(config_path, config, &target)
            .with_context(|| "Submitting job to SLURM")?;
        let handle = self.clone();
        let spawn_target = target.clone();
        actix_web::rt::spawn(async move {
            if let Err(e) = handle.supervise_runner_job(runner_job, spawn_target).await {
                error!("{e:#}");
            }
        });
        Ok(())
    }

    pub fn job_enqueued<'c>(
        &self,
        job_id: github::WorkflowJobId,
        labels: &[String],
        config_path: &Path,
        config: &Config,
    ) -> Result<(), Error> {
        let queued_before = self.with_state(|state| state.jobs.insert(job_id, JobState::Queueing));
        if queued_before.is_some() {
            return Err(Error::InvalidState(format!(
                "Job {job_id} is already queued"
            )));
        }

        if let Some(target) = match_target(&config, &labels) {
            debug!("Matched runner labels {labels:?} to target {}", job_id.0);

            info!("Launching SLURM job for workflow job {job_id}");
            self.schedule(target, config_path, config)?;

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
        config_path: &Path,
        config: &Config,
    ) -> Result<(), Error> {
        enum Transition {
            RunnerPickedUpQueuedJob {
                runner_slurm_job: slurm::JobId,
            },
            RunnerStoleForeignJob {
                runner_slurm_job: slurm::JobId,
                respawn_target: TargetId,
            },
            ForeignRunnerStoleQueuedJob,
            ForeignRunnerPickedUpForeignJob,
            JobNotQueued,
        }

        let transition = self.with_state(|state| {
            let runner_state = state.runners.get(runner_name);
            let scheduler_job_state = state.jobs.get_mut(&job_id);
            match (runner_state, scheduler_job_state) {
                (Some(runner_state), Some(job_state @ JobState::Queued)) => {
                    *job_state = JobState::InProgress(runner_state.slurm_job);
                    Transition::RunnerPickedUpQueuedJob {
                        runner_slurm_job: runner_state.slurm_job,
                    }
                }
                (Some(_), Some(JobState::Queueing | JobState::InProgress(_))) => {
                    Transition::JobNotQueued
                }
                (None, Some(_)) => {
                    state.jobs.remove(&job_id);
                    Transition::ForeignRunnerStoleQueuedJob
                }
                (Some(runner_state), None) => Transition::RunnerStoleForeignJob {
                    runner_slurm_job: runner_state.slurm_job,
                    respawn_target: runner_state.target.clone(),
                },
                (None, None) => Transition::ForeignRunnerPickedUpForeignJob,
            }
        });

        match transition {
            Transition::RunnerPickedUpQueuedJob { runner_slurm_job } => {
                info!(
                    "Workflow job {job_id} picked up by runner {runner_name} (SLURM #{})",
                    runner_slurm_job
                );
                Ok(())
            }
            Transition::RunnerStoleForeignJob {
                runner_slurm_job,
                respawn_target,
            } => {
                warn!(
                    "Runner {runner_name} (SLURM #{}) has picked up foreign job {job_id}",
                    runner_slurm_job
                );
                info!("Launching a replacement SLURM job for workflow job {job_id}");
                self.schedule(&respawn_target, config_path, config)
                    .map_err(Error::Failed)
            }
            Transition::ForeignRunnerStoleQueuedJob => {
                // TODO detect when job pickup happens before we receive runner metadata via IPC
                //  (which should never happen in practice)
                warn!("Job {job_id} was picked up by a foreign runner, removed it from tracking");
                // TODO now we have a runner scheduled with nothing to do
                Ok(())
            }
            Transition::ForeignRunnerPickedUpForeignJob => {
                // This is expected.
                Ok(())
            }
            Transition::JobNotQueued => {
                Err(Error::InvalidState(format!("Job {job_id} not queued")))
            }
        }
    }

    pub fn job_completed(&self, job_id: github::WorkflowJobId) {
        // It's acceptable for job_id to be unknown in case it's from a foreign job
        self.with_state(|state| state.jobs.remove(&job_id));
    }
}
