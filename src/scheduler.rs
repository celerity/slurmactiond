use crate::config::{Config, TargetId};
use crate::ipc::RunnerMetadata;
use crate::{github, slurm};
use anyhow::Context as _;
use log::{debug, error, info, warn};
use serde::Serialize;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
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

#[derive(Copy, Clone, Serialize, Debug, PartialEq, Eq, Hash)]
#[serde(transparent)]
pub struct InternalRunnerId(pub u32);

impl Display for InternalRunnerId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "R{}", self.0)
    }
}

#[derive(Clone, Serialize, Debug, PartialEq, Eq)]
pub enum WorkflowJobState {
    Pending,
    InProgress(InternalRunnerId),
}

#[derive(Clone, Serialize)]
pub enum RunnerState {
    Queued,
    Active(RunnerMetadata),
}

#[derive(Clone, Serialize)]
pub struct RunnerInfo {
    pub target: TargetId,
    pub state: RunnerState,
}

struct SchedulerState {
    jobs: HashMap<github::WorkflowJobId, WorkflowJobState>,
    runners: HashMap<InternalRunnerId, RunnerInfo>,
    rids_by_name: HashMap<String, InternalRunnerId>,
    next_runner_id: InternalRunnerId,
}

#[derive(Clone, Serialize)]
pub struct SchedulerStateSnapshot {
    pub jobs: HashMap<github::WorkflowJobId, WorkflowJobState>,
    pub runners: HashMap<InternalRunnerId, RunnerInfo>,
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
                rids_by_name: HashMap::new(),
                next_runner_id: InternalRunnerId(0),
            })),
        }
    }

    fn with_state<R, F: FnOnce(&mut SchedulerState) -> R>(&self, f: F) -> R {
        f(&mut self.state.lock().expect("Poisoned Mutex"))
    }

    async fn supervise_runner_job(
        &self,
        runner_id: InternalRunnerId,
        slurm_runner: slurm::RunnerJob,
    ) -> anyhow::Result<()> {
        let (metadata, slurm_runner) = slurm_runner
            .attach()
            .await
            .with_context(|| "Attaching to SLURM job")?;

        self.with_state(|state| {
            let info = state
                .runners
                .get_mut(&runner_id)
                .expect("Unknown internal runner id");
            assert!(matches!(info.state, RunnerState::Queued));
            info.state = RunnerState::Active(metadata.clone());

            let existing_by_name = state
                .rids_by_name
                .insert(metadata.runner_name.clone(), runner_id);
            assert!(existing_by_name.is_none());
        });

        let result = slurm_runner
            .wait()
            .await
            .with_context(|| "Waiting for SLURM job");

        // remove runner independent of job success
        self.with_state(|state| {
            (state.rids_by_name)
                .remove(&metadata.runner_name)
                .expect("Runner name vanished");
            state
                .runners
                .remove(&runner_id)
                .expect("Runner id vanished");
        });

        info!("SLURM job exited with status {}", result?);

        Ok(())
    }

    fn spawn_new_runner(
        &self,
        target: &TargetId,
        config_path: &Path,
        config: &Config,
    ) -> anyhow::Result<()> {
        let runner_info = RunnerInfo {
            target: target.clone(),
            state: RunnerState::Queued,
        };
        let runner_id = self.with_state(|state| {
            let runner_id = state.next_runner_id;
            state.next_runner_id.0 += 1;
            state.runners.insert(runner_id, runner_info);
            runner_id
        });

        let slurm_runner = slurm::RunnerJob::spawn(config_path, config, &target)
            .with_context(|| "Submitting job to SLURM")?;

        let handle = self.clone();
        actix_web::rt::spawn(async move {
            if let Err(e) = handle.supervise_runner_job(runner_id, slurm_runner).await {
                error!("{e:#}");
            }
        }); // TODO manage JoinHandle in RunnerInfo

        Ok(())
    }

    pub fn job_enqueued<'c>(
        &self,
        job_id: github::WorkflowJobId,
        labels: &[String],
        config_path: &Path,
        config: &Config,
    ) -> Result<(), Error> {
        if let Some(target) = match_target(&config, &labels) {
            debug!("Matched runner labels {labels:?} to target {}", job_id.0);

            self.with_state(|state| {
                // like state.try_insert(), but stable
                match state.jobs.entry(job_id) {
                    Entry::Occupied(_) => Err(Error::InvalidState(format!(
                        "Workflow job {job_id} is already queued"
                    ))),
                    Entry::Vacant(entry) => {
                        entry.insert(WorkflowJobState::Pending);
                        Ok(())
                    }
                }
            })?;

            info!("Launching SLURM job for workflow job {job_id}");
            self.spawn_new_runner(target, config_path, config)?;
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
            RunnerPickedUpQueuedJob(InternalRunnerId),
            RunnerStoleForeignJob {
                runner_id: InternalRunnerId,
                respawn_target: TargetId,
            },
            ForeignRunnerStoleQueuedJob,
            ForeignRunnerPickedUpForeignJob,
            JobNotPending,
        }

        let transition = self.with_state(|state| {
            let active_runner = (state.rids_by_name.get(runner_name))
                .map(|rid| (rid, state.runners.get(rid).unwrap()))
                .filter(|(_, info)| matches!(info.state, RunnerState::Active(_)));

            let workflow_job_state = state.jobs.get_mut(&job_id);
            match (active_runner, workflow_job_state) {
                (Some((runner_id, _)), Some(job_state @ WorkflowJobState::Pending)) => {
                    *job_state = WorkflowJobState::InProgress(*runner_id);
                    Transition::RunnerPickedUpQueuedJob(*runner_id)
                }
                (Some(..), Some(WorkflowJobState::InProgress(_))) => Transition::JobNotPending,
                (None, Some(_)) => {
                    state.jobs.remove(&job_id);
                    Transition::ForeignRunnerStoleQueuedJob
                }
                (Some((runner_id, runner_info)), None) => Transition::RunnerStoleForeignJob {
                    runner_id: *runner_id,
                    respawn_target: runner_info.target.clone(),
                },
                (None, None) => Transition::ForeignRunnerPickedUpForeignJob,
            }
        });

        match transition {
            Transition::RunnerPickedUpQueuedJob(runner_id) => {
                info!("Workflow job {job_id} picked up by runner {runner_id} ({runner_name})");
                Ok(())
            }
            Transition::RunnerStoleForeignJob {
                runner_id,
                respawn_target,
            } => {
                warn!(
                    "Runner {runner_id} ({runner_name}) has picked up foreign workflow job {}",
                    job_id
                );
                info!("Launching a replacement SLURM job for workflow job {job_id}");
                self.spawn_new_runner(&respawn_target, config_path, config)
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
            Transition::JobNotPending => {
                Err(Error::InvalidState(format!("Job {job_id} not pending")))
            }
        }
    }

    pub fn job_completed(&self, job_id: github::WorkflowJobId) {
        // It's acceptable for job_id to be unknown in case it's from a foreign job
        self.with_state(|state| state.jobs.remove(&job_id));
    }

    pub fn snapshot_state(&self) -> SchedulerStateSnapshot {
        self.with_state(|state| SchedulerStateSnapshot {
            jobs: state.jobs.clone(),
            runners: state.runners.clone(),
        })
    }
}
