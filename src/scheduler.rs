use crate::github;
use crate::ipc::RunnerMetadata;
use crate::util;
use log::{debug, error, info, warn};
use serde::Serialize;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use thiserror::Error;

pub type Label = String;

util::literal_types! {
    pub struct TargetId(pub String);

    #[derive(Copy)]
    pub struct RunnerId(pub u32);
}

#[derive(Clone, Serialize, Debug, PartialEq, Eq)]
pub enum WorkflowJobState {
    Pending(Vec<TargetId>),
    InProgress(RunnerId),
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
    runners: HashMap<RunnerId, RunnerInfo>,
    rids_by_name: HashMap<String, RunnerId>,
    next_runner_id: RunnerId,
}

#[derive(Clone, Serialize)]
pub struct SchedulerStateSnapshot {
    pub jobs: HashMap<github::WorkflowJobId, WorkflowJobInfo>,
    pub runners: HashMap<RunnerId, RunnerInfo>,
}

pub trait Executor {
    fn spawn_runner(&self, target: &TargetId, scheduler: &Arc<Scheduler>) -> anyhow::Result<()>;
}

pub struct Scheduler {
    executor: Box<dyn Executor + Send + Sync>,
    runner_labels: Vec<Label>,
    targets_with_labels: Vec<(TargetId, Vec<Label>)>,
    state: Mutex<SchedulerState>,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    InvalidState(String),
    #[error("{0}")]
    Failed(#[from] anyhow::Error),
}

impl Scheduler {
    pub fn new(
        executor: Box<dyn Executor + Send + Sync>,
        runner_labels: Vec<Label>,
        targets_with_labels: Vec<(TargetId, Vec<Label>)>,
    ) -> Self {
        Scheduler {
            executor,
            runner_labels,
            targets_with_labels,
            state: Mutex::new(SchedulerState {
                jobs: HashMap::new(),
                runners: HashMap::new(),
                rids_by_name: HashMap::new(),
                next_runner_id: RunnerId(0),
            }),
        }
    }

    fn with_state<R, F: FnOnce(&mut SchedulerState) -> R>(&self, f: F) -> R {
        f(&mut self.state.lock().expect("Poisoned Mutex"))
    }

    pub fn create_runner(&self, target: TargetId) -> RunnerId {
        let runner_info = RunnerInfo {
            target,
            metadata: None,
            state: RunnerState::Queued,
        };
        self.with_state(|state| {
            let runner_id = state.next_runner_id;
            state.next_runner_id.0 += 1;
            state.runners.insert(runner_id, runner_info);
            runner_id
        })
    }

    pub fn runner_connected(self: &Arc<Self>, runner_id: RunnerId, metadata: RunnerMetadata) {
        let runner_name = metadata.runner_name.clone();

        let target = self.with_state(|state| {
            let info = state
                .runners
                .get_mut(&runner_id)
                .expect("Unknown internal runner id");
            assert!(matches!(info.state, RunnerState::Queued));

            info.metadata = Some(metadata);
            info.state = RunnerState::Waiting;

            let existing_by_name = state.rids_by_name.insert(runner_name.clone(), runner_id);
            assert!(existing_by_name.is_none());

            info.target.clone()
        });

        info!("Runner {runner_id} ({runner_name}) connected for target {target}");
    }

    pub fn runner_disconnected(self: &Arc<Self>, runner_id: RunnerId) {
        // remove runner independent of job success
        let (runner_name, runner_state) = self.with_state(|state| {
            let info = (state.runners)
                .remove(&runner_id)
                .expect("Runner id vanished");
            let runner_name = info.metadata.expect("Runner had no metadata").runner_name;
            (state.rids_by_name)
                .remove(&runner_name)
                .expect("Runner name vanished");
            (runner_name, info.state)
        });

        match runner_state {
            RunnerState::Queued | RunnerState::Waiting => {
                warn!(
                    "Runner {runner_id} ({runner_name}) disconnected {detail}",
                    detail = concat!(
                        "without having picked up a job. This might be due to a ",
                        "timeout or a foreign runner picking up our jobs."
                    )
                );
                self.schedule().unwrap_or_else(|e| error!("{e:#}"))
            }
            RunnerState::Running(_) => info!("Runner {runner_id} ({runner_name}) disconnected"),
        }
    }

    // TODO what do we actually signal with a result here? If scheduling fails, does it place the
    //  scheduler in some permanent invalid state, will we try again next time, and can we find a
    //  way to avoid endlessly re-trying a failing operation?
    fn schedule(self: &Arc<Self>) -> anyhow::Result<()> {
        let (mut pending_job_targets, mut unassigned_runner_targets) = self.with_state(|state| {
            let pending_job_targets: Vec<_> = (state.jobs)
                .iter()
                .filter_map(|(_, info)| match &info.state {
                    WorkflowJobState::Pending(targets) => Some(targets.clone()),
                    _ => None,
                })
                .collect();

            let mut unassigned_runner_targets = HashMap::new();
            for (_, info) in &state.runners {
                if let RunnerState::Queued | RunnerState::Waiting = info.state {
                    util::increment_or_insert(&mut unassigned_runner_targets, &info.target);
                }
            }
            (pending_job_targets, unassigned_runner_targets)
        });

        pending_job_targets.retain(|job_targets| {
            job_targets
                .iter()
                .find(|t| util::decrement_or_remove(&mut unassigned_runner_targets, t))
                .is_none()
        });

        for targets in &pending_job_targets {
            info!("Spawning runner for target {}", targets[0]);
            self.executor.spawn_runner(&targets[0], self)?;
        }

        Ok(())
    }

    fn match_targets(&self, job_labels: &[Label]) -> Vec<TargetId> {
        // GitHub only includes the "self-hosted" label if the set of labels is otherwise empty.
        // We don't require the user to list "self-hosted" in the config.
        let unmatched_labels: Vec<_> = (job_labels.iter())
            .filter(|jl| !(*jl == "self-hosted" || self.runner_labels.contains(*jl)))
            .collect();
        let mut matching_target_distances: Vec<_> = self
            .targets_with_labels
            .iter()
            .filter(|(_, tls)| unmatched_labels.iter().all(|jl| tls.contains(jl)))
            .map(|(t, tls)| (t, tls.len()))
            .collect();
        matching_target_distances.sort_by_key(|(_, len)| *len);
        matching_target_distances
            .into_iter()
            .map(|(id, _)| id.clone())
            .collect()
    }

    pub fn job_enqueued<'c>(
        self: &Arc<Self>,
        job_id: github::WorkflowJobId,
        name: &str,
        url: &str,
        labels: &[String],
    ) -> Result<(), Error> {
        let targets = self.match_targets(labels);
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

            info!("Job {job_id} enqueued");
            self.schedule()?;
        } else {
            debug!("Runner labels {labels:?} do not match any target");
        }

        Ok(())
    }

    pub fn job_processing(
        self: &Arc<Self>,
        job_id: github::WorkflowJobId,
        runner_name: &str,
    ) -> Result<(), Error> {
        enum Transition {
            RunnerPickedUpPendingJob(RunnerId),
            RunnerStoleForeignJob(RunnerId),
            ForeignRunnerStolePendingJob,
            JobNotPending,
            RunnerNotWaiting(RunnerId),
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

    pub fn job_completed(self: &Arc<Self>, job_id: github::WorkflowJobId) {
        // It's acceptable for job_id to be unknown in case it's from a foreign job
        if self
            .with_state(|state| state.jobs.remove(&job_id))
            .is_some()
        {
            info!("Job {job_id} completed")
        }
    }

    pub fn snapshot_state(&self) -> SchedulerStateSnapshot {
        self.with_state(|state| SchedulerStateSnapshot {
            jobs: state.jobs.clone(),
            runners: state.runners.clone(),
        })
    }
}

#[cfg(test)]
use crate::slurm;

#[cfg(test)]
#[derive(Default, Clone)]
struct MockExecutor {
    runners: Arc<Mutex<Vec<(RunnerId, TargetId)>>>,
}

#[cfg(test)]
impl MockExecutor {
    fn take_runners(&self) -> Vec<(RunnerId, TargetId)> {
        std::mem::take(&mut self.runners.lock().unwrap())
    }
}

#[cfg(test)]
impl Executor for MockExecutor {
    fn spawn_runner(&self, target: &TargetId, scheduler: &Arc<Scheduler>) -> anyhow::Result<()> {
        let rid = scheduler.create_runner(target.clone());
        self.runners.lock().unwrap().push((rid, target.clone()));
        Ok(())
    }
}

#[test]
fn test_scheduler() {
    env_logger::init();

    let runner_labels = vec!["base-label-1".to_owned(), "base-label-2".to_owned()];
    let targets_with_labels = vec![
        (
            TargetId("target-a".to_owned()),
            vec!["a-label-1".to_owned()],
        ),
        (
            TargetId("target-b".to_owned()),
            vec!["b-label-1".to_owned(), "b-label-2".to_owned()],
        ),
    ];
    let executor = MockExecutor::default();
    let scheduler = Arc::new(Scheduler::new(
        Box::new(executor.clone()),
        runner_labels,
        targets_with_labels,
    ));

    scheduler
        .job_enqueued(
            github::WorkflowJobId(1),
            "job 1",
            "http://job1",
            &["base-label-1".to_owned(), "a-label-1".to_owned()],
        )
        .unwrap();

    {
        let state = scheduler.snapshot_state();
        assert_eq!(state.jobs.len(), 1);
        assert_eq!(state.runners.len(), 1);
    }

    {
        let runners = executor.take_runners();
        assert_eq!(runners.len(), 1);
        assert_eq!(runners[0].1 .0, "target-a".to_owned());
        let runner_id = runners[0].0;

        scheduler.runner_connected(
            runner_id,
            RunnerMetadata {
                slurm_job: slurm::JobId(0),
                runner_name: "runner-a-0".to_string(),
                host_name: "host-a".to_string(),
                concurrent_id: 0,
            },
        );

        // our runner picks up a foreign job, causing a reschedule
        scheduler
            .job_processing(github::WorkflowJobId(2), "runner-a-0")
            .unwrap();
        scheduler.job_completed(github::WorkflowJobId(2));
        scheduler.runner_disconnected(runner_id);
    }

    {
        let state = scheduler.snapshot_state();
        assert_eq!(state.jobs.len(), 1);
        assert_eq!(state.runners.len(), 1);
    }

    {
        let runners = executor.take_runners();
        assert_eq!(runners.len(), 1);
        assert_eq!(runners[0].1 .0, "target-a".to_owned());
        let runner_id = runners[0].0;

        scheduler.runner_connected(
            runner_id,
            RunnerMetadata {
                slurm_job: slurm::JobId(0),
                runner_name: "runner-a-1".to_string(),
                host_name: "host-a".to_string(),
                concurrent_id: 0,
            },
        );

        // the re-scheduled runner now picks up our job
        scheduler
            .job_processing(github::WorkflowJobId(1), "runner-a-1")
            .unwrap();
        scheduler.job_completed(github::WorkflowJobId(1));
        scheduler.runner_disconnected(runner_id);
    }

    {
        let state = scheduler.snapshot_state();
        assert!(state.jobs.is_empty());
        assert!(state.runners.is_empty());
    }

    scheduler
        .job_enqueued(
            github::WorkflowJobId(3),
            "job 3",
            "http://job3",
            &["base-label-1".to_owned(), "b-label-1".to_owned()],
        )
        .unwrap();

    {
        let runners = executor.take_runners();
        assert_eq!(runners.len(), 1);
        assert_eq!(runners[0].1 .0, "target-b".to_owned());
        let runner_id = runners[0].0;

        scheduler.runner_connected(
            runner_id,
            RunnerMetadata {
                slurm_job: slurm::JobId(0),
                runner_name: "runner-b-0".to_string(),
                host_name: "host-b".to_string(),
                concurrent_id: 0,
            },
        );

        // a foreign runner picks up our job
        scheduler
            .job_processing(github::WorkflowJobId(3), "foreign-runner-0")
            .unwrap();
        scheduler.job_completed(github::WorkflowJobId(3));
        scheduler.runner_disconnected(runner_id); // due to timeout
    }

    {
        let state = scheduler.snapshot_state();
        assert!(state.jobs.is_empty());
        assert!(state.runners.is_empty());
    }

    scheduler
        .job_enqueued(
            github::WorkflowJobId(4),
            "job 4",
            "http://job4",
            &["base-label-1".to_owned(), "b-label-1".to_owned()],
        )
        .unwrap();

    {
        let runners = executor.take_runners();
        assert_eq!(runners.len(), 1);
        assert_eq!(runners[0].1 .0, "target-b".to_owned());
        let runner_id = runners[0].0;

        scheduler.runner_connected(
            runner_id,
            RunnerMetadata {
                slurm_job: slurm::JobId(0),
                runner_name: "runner-b-0".to_string(),
                host_name: "host-b".to_string(),
                concurrent_id: 0,
            },
        );

        // spurious runner timeout - requires a reschedule
        scheduler.runner_disconnected(runner_id);
    }

    {
        let state = scheduler.snapshot_state();
        assert_eq!(state.jobs.len(), 1);
        assert_eq!(state.runners.len(), 1);
    }

    {
        let runners = executor.take_runners();
        assert_eq!(runners.len(), 1);
        assert_eq!(runners[0].1 .0, "target-b".to_owned());
        let runner_id = runners[0].0;

        scheduler.runner_connected(
            runner_id,
            RunnerMetadata {
                slurm_job: slurm::JobId(0),
                runner_name: "runner-b-2".to_string(),
                host_name: "host-b".to_string(),
                concurrent_id: 0,
            },
        );

        // the re-scheduled runner now picks up our job
        scheduler
            .job_processing(github::WorkflowJobId(4), "runner-b-2")
            .unwrap();
        scheduler.job_completed(github::WorkflowJobId(4));
        scheduler.runner_disconnected(runner_id);
    }

    {
        let state = scheduler.snapshot_state();
        assert!(state.jobs.is_empty());
        assert!(state.runners.is_empty());
    }
}
