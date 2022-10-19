use crate::config::{ConfigFile, TargetConfig, TargetId};
use crate::ipc::RunnerMetadata;
use crate::util;
use crate::{github, slurm};
use anyhow::Context as _;
use log::{debug, error, info, warn};
use serde::Serialize;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::{Arc, Mutex};
use thiserror::Error;

fn match_targets<'t, T>(
    job_labels: &[String],
    runner_labels: &[String],
    targets: &'t T,
) -> Vec<TargetId>
where
    &'t T: IntoIterator<Item = (&'t TargetId, &'t TargetConfig)>,
{
    // GitHub only includes the "self-hosted" label if the set of labels is otherwise empty.
    // We don't require the user to list "self-hosted" in the config.
    let unmatched_labels: Vec<_> = (job_labels.iter())
        .filter(|l| !(*l == "self-hosted" || runner_labels.contains(*l)))
        .map(String::clone)
        .collect();
    let mut matching_target_distances: Vec<_> = targets
        .into_iter()
        .filter(|(_, t)| unmatched_labels.iter().all(|l| t.runner_labels.contains(l)))
        .map(|(id, t)| (id, t.runner_labels.len()))
        .collect();
    matching_target_distances.sort_by_key(|(_, len)| *len);
    matching_target_distances
        .iter()
        .map(|(id, _)| (*id).clone())
        .collect()
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
    Pending(Vec<TargetId>),
    InProgress(InternalRunnerId),
    InProgressOnForeignRunner(String),
}

#[derive(Clone, Serialize, Debug, PartialEq, Eq)]
pub struct WorkflowJobInfo {
    pub name: String,
    pub url: String,
    pub state: WorkflowJobState,
}

#[derive(Clone, Serialize)]
pub enum RunnerState {
    Queued,
    Waiting,
    Running(github::WorkflowJobId),
}

#[derive(Clone, Serialize)]
pub struct RunnerInfo {
    pub target: TargetId,
    pub metadata: Option<RunnerMetadata>,
    pub state: RunnerState,
}

struct SchedulerState {
    jobs: HashMap<github::WorkflowJobId, WorkflowJobInfo>,
    runners: HashMap<InternalRunnerId, RunnerInfo>,
    rids_by_name: HashMap<String, InternalRunnerId>,
    next_runner_id: InternalRunnerId,
}

#[derive(Clone, Serialize)]
pub struct SchedulerStateSnapshot {
    pub jobs: HashMap<github::WorkflowJobId, WorkflowJobInfo>,
    pub runners: HashMap<InternalRunnerId, RunnerInfo>,
}

#[derive(Clone)]
pub struct Scheduler {
    config_file: Arc<ConfigFile>,
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
    pub fn new(config_file: ConfigFile) -> Scheduler {
        Scheduler {
            config_file: Arc::new(config_file),
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
            info.metadata = Some(metadata.clone());
            info.state = RunnerState::Waiting;

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

    fn spawn_new_runner(&self, target: &TargetId) -> anyhow::Result<()> {
        let runner_info = RunnerInfo {
            target: target.clone(),
            metadata: None,
            state: RunnerState::Queued,
        };
        let runner_id = self.with_state(|state| {
            let runner_id = state.next_runner_id;
            state.next_runner_id.0 += 1;
            state.runners.insert(runner_id, runner_info);
            runner_id
        });

        let slurm_runner = slurm::RunnerJob::spawn(&self.config_file, &target)
            .with_context(|| "Submitting job to SLURM")?;

        let handle = self.clone();
        actix_web::rt::spawn(async move {
            if let Err(e) = handle.supervise_runner_job(runner_id, slurm_runner).await {
                error!("{e:#}");
            }
        }); // TODO manage JoinHandle in RunnerInfo

        Ok(())
    }

    // TODO what do we actually signal with a result here? If scheduling fails, does it place the
    //  scheduler in some permanent invalid state, will we try again next time, and can we find a
    //  way to avoid endlessly re-trying a failing operation?
    fn schedule(&self) -> anyhow::Result<()> {
        let (mut pending_job_targets, mut queued_runner_targets) = self.with_state(|state| {
            let pending_job_targets: Vec<_> = (state.jobs)
                .iter()
                .filter_map(|(_, info)| match &info.state {
                    WorkflowJobState::Pending(targets) => Some(targets.clone()),
                    _ => None,
                })
                .collect();

            let mut queued_runner_targets = HashMap::new();
            for (_, info) in &state.runners {
                if let RunnerState::Queued = info.state {
                    util::increment_or_insert(&mut queued_runner_targets, &info.target);
                }
            }
            (pending_job_targets, queued_runner_targets)
        });

        pending_job_targets.retain(|job_targets| {
            job_targets
                .iter()
                .find(|t| util::decrement_or_remove(&mut queued_runner_targets, t))
                .is_some()
        });

        for targets in &pending_job_targets {
            self.spawn_new_runner(&targets[0])?;
        }

        Ok(())
    }

    pub fn job_enqueued<'c>(
        &self,
        job_id: github::WorkflowJobId,
        name: &str,
        url: &str,
        labels: &[String],
    ) -> Result<(), Error> {
        let targets = match_targets(
            labels,
            &self.config_file.config.runner.registration.labels,
            &self.config_file.config.targets,
        );
        if !targets.is_empty() {
            debug!("Matched labels {labels:?} of job {job_id} to targets {targets:?}");

            self.with_state(|state| {
                // like state.try_insert(), but stable
                match state.jobs.entry(job_id) {
                    Entry::Occupied(_) => Err(Error::InvalidState(format!(
                        "Workflow job {job_id} is already queued"
                    ))),
                    Entry::Vacant(entry) => {
                        entry.insert(WorkflowJobInfo {
                            name: name.to_owned(),
                            url: url.to_owned(),
                            state: WorkflowJobState::Pending(targets),
                        });
                        Ok(())
                    }
                }
            })?;

            self.schedule()?;
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
        enum Transition {
            RunnerPickedUpPendingJob(InternalRunnerId),
            RunnerStoleForeignJob(InternalRunnerId),
            ForeignRunnerStolePendingJob,
            JobNotPending,
            RunnerNotWaiting(InternalRunnerId),
            ForeignRunnerPickedUpForeignJob,
        }

        let transition = self.with_state(|state| {
            let runner_and_state = (state.rids_by_name.get(runner_name))
                .map(|rid| (rid, &mut state.runners.get_mut(rid).unwrap().state));
            let workflow_job_state = state.jobs.get_mut(&job_id).map(|info| &mut info.state);
            match (runner_and_state, workflow_job_state) {
                (
                    Some((runner_id, runner_state @ RunnerState::Waiting)),
                    Some(job_state @ WorkflowJobState::Pending(_)),
                ) => {
                    *runner_state = RunnerState::Running(job_id);
                    *job_state = WorkflowJobState::InProgress(*runner_id);
                    Transition::RunnerPickedUpPendingJob(*runner_id)
                }
                (Some((runner_id, runner_state @ RunnerState::Waiting)), None) => {
                    *runner_state = RunnerState::Running(job_id);
                    Transition::RunnerStoleForeignJob(*runner_id)
                }
                (None, Some(job_state @ WorkflowJobState::Pending(_))) => {
                    *job_state =
                        WorkflowJobState::InProgressOnForeignRunner(runner_name.to_owned());
                    Transition::ForeignRunnerStolePendingJob
                }
                (Some((runner_id, _)), _) => Transition::RunnerNotWaiting(*runner_id),
                (_, Some(_)) => Transition::JobNotPending,
                (None, None) => Transition::ForeignRunnerPickedUpForeignJob,
            }
        });

        match transition {
            Transition::RunnerPickedUpPendingJob(runner_id) => {
                info!("Workflow job {job_id} picked up by runner {runner_id} ({runner_name})");
                // Github might not assign jobs to runners optimally: If job 1 can run on runners
                // A and B, and job 2 can run on runner B only, Github might still assign job 1
                // to runner B, leaving job 2 without an eligible runner.
                self.schedule()?;
                Ok(())
            }
            Transition::RunnerStoleForeignJob(runner_id) => {
                warn!(
                    "Runner {runner_id} ({runner_name}) has picked up foreign workflow job {}",
                    job_id
                );
                // There is most likely a missing runner now.
                self.schedule()?;
                Ok(())
            }
            Transition::ForeignRunnerStolePendingJob => {
                // TODO detect when job pickup happens before we receive runner metadata via IPC
                //  (which should never happen in practice)
                warn!("Job {job_id} was picked up by foreign runner {runner_name}");
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
            Transition::RunnerNotWaiting(runner_id) => Err(Error::InvalidState(format!(
                "Runner {runner_id} ({runner_name}) is not waiting for a job"
            ))),
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
