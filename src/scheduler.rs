use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use log::{debug, error, info, warn};
use serde::Serialize;
use thiserror::Error;

use crate::github;
use crate::ipc::RunnerMetadata;
#[cfg(test)]
use crate::slurm;
use crate::util;

pub type Label = String;

util::literal_types! {
    pub struct TargetId(pub String);

    #[derive(Copy)]
    pub struct RunnerId(pub u32);
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AssignedRunner {
    Scheduled(RunnerId),
    Foreign(String),
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowJobState {
    Pending(Vec<TargetId>),
    InProgress(AssignedRunner),
}

#[derive(Debug, Copy, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowJobTermination {
    CompletedWithSuccess,
    CompletedWithFailure,
    Cancelled,
    Skipped,
    Failed,
    TimedOut,
}

#[derive(Clone, Serialize)]
pub struct WorkflowJobInfo {
    pub id: github::WorkflowJobId,
    pub name: String,
    pub url: String,
}

#[derive(Clone, Serialize)]
pub struct ActiveWorkflowJob {
    pub info: WorkflowJobInfo,
    pub state: WorkflowJobState,
    #[serde(serialize_with = "util::time_as_iso8601")]
    pub state_changed_at: SystemTime,
}

#[derive(Clone, Serialize)]
pub struct TerminatedWorkflowJob {
    pub info: WorkflowJobInfo,
    pub assignment: Option<AssignedRunner>,
    pub termination: WorkflowJobTermination,
    #[serde(serialize_with = "util::time_as_iso8601")]
    pub terminated_at: SystemTime,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunnerState {
    Queued,
    Starting,
    Listening,
    Running(github::WorkflowJobId),
}

#[derive(Copy, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunnerTermination {
    Completed(github::WorkflowJobId),
    ListeningTimeout,
    Failed,
}

#[derive(Clone, Serialize)]
pub struct RunnerInfo {
    pub id: RunnerId,
    pub target: TargetId,
    pub metadata: Option<RunnerMetadata>,
}

#[derive(Clone, Serialize)]
pub struct ActiveRunner {
    pub info: RunnerInfo,
    pub state: RunnerState,
    #[serde(serialize_with = "util::time_as_iso8601")]
    pub state_changed_at: SystemTime,
}

#[derive(Clone, Serialize)]
pub struct TerminatedRunner {
    pub info: RunnerInfo,
    pub termination: RunnerTermination,
    #[serde(serialize_with = "util::time_as_iso8601")]
    pub terminated_at: SystemTime,
}

struct SchedulerState {
    active_jobs: HashMap<github::WorkflowJobId, ActiveWorkflowJob>,
    active_runners: HashMap<RunnerId, ActiveRunner>,
    active_rids_by_name: HashMap<String, RunnerId>,
    next_runner_id: RunnerId,
    job_history: VecDeque<TerminatedWorkflowJob>,
    full_job_history_len: usize,
    runner_history: VecDeque<TerminatedRunner>,
    full_runner_history_len: usize,
}

#[derive(Clone, Serialize)]
pub struct SchedulerStateSnapshot {
    pub active_jobs: Vec<ActiveWorkflowJob>,
    pub active_runners: Vec<ActiveRunner>,
    pub job_history: Vec<TerminatedWorkflowJob>,
    pub full_job_history_len: usize,
    pub runner_history: Vec<TerminatedRunner>,
    pub full_runner_history_len: usize,
}

pub trait Executor {
    fn spawn_runner(&self, target: &TargetId, scheduler: &Arc<Scheduler>) -> anyhow::Result<()>;
}

type TargetPriority = i64;

pub struct Target {
    pub id: TargetId,
    pub runner_labels: Vec<Label>,
    pub priority: TargetPriority,
}

pub struct Scheduler {
    executor: Box<dyn Executor + Send + Sync>,
    runner_labels: Vec<Label>,
    targets: Vec<Target>,
    history_len: usize,
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
        targets: Vec<Target>,
        history_len: usize,
    ) -> Self {
        let mut job_history = VecDeque::new();
        job_history.reserve(history_len);
        let mut runner_history = VecDeque::new();
        runner_history.reserve(history_len);

        Scheduler {
            executor,
            runner_labels,
            targets,
            history_len,
            state: Mutex::new(SchedulerState {
                active_jobs: HashMap::new(),
                active_runners: HashMap::new(),
                active_rids_by_name: HashMap::new(),
                next_runner_id: RunnerId(0),
                job_history,
                full_job_history_len: 0,
                runner_history,
                full_runner_history_len: 0,
            }),
        }
    }

    fn with_state<R, F: FnOnce(&mut SchedulerState) -> R>(&self, f: F) -> R {
        f(&mut self.state.lock().expect("Poisoned Mutex"))
    }

    pub fn create_runner(&self, target: TargetId) -> RunnerId {
        let state_changed_at = SystemTime::now();
        self.with_state(|sched| {
            let id = sched.next_runner_id;
            sched.next_runner_id.0 += 1;
            let runner = ActiveRunner {
                info: RunnerInfo {
                    id,
                    target,
                    metadata: None,
                },
                state: RunnerState::Queued,
                state_changed_at,
            };
            sched.active_runners.insert(id, runner);
            id
        })
    }

    pub fn runner_connected(
        self: &Arc<Self>,
        runner_id: RunnerId,
        metadata: RunnerMetadata,
    ) -> Result<(), Error> {
        let runner_name = metadata.runner_name.clone();
        let state_change_at = SystemTime::now();

        let target = self.with_state(|sched| {
            let runner = sched
                .active_runners
                .get_mut(&runner_id)
                .expect("Unknown internal runner id");
            assert!(matches!(runner.state, RunnerState::Queued));

            let existing_by_name = sched
                .active_rids_by_name
                .insert(runner_name.clone(), runner_id);
            if existing_by_name.is_some() {
                return Err(Error::InvalidState(format!(
                    "Runner with name {runner_name} already exists"
                )));
            }

            runner.info.metadata = Some(metadata);
            runner.state = RunnerState::Starting;
            runner.state_changed_at = state_change_at;

            Ok(runner.info.target.clone())
        })?;

        info!("Runner {runner_id} ({runner_name}) connected for target {target}");
        Ok(())
    }

    pub fn runner_listening(self: &Arc<Self>, runner_id: RunnerId) {
        let state_change_at = SystemTime::now();
        self.with_state(|sched| {
            let runner = sched
                .active_runners
                .get_mut(&runner_id)
                .expect("Unknown internal runner id");
            if let RunnerState::Starting = runner.state {
                runner.state = RunnerState::Listening;
                runner.state_changed_at = state_change_at;
            }
        });
    }

    pub fn runner_disconnected(
        self: &Arc<Self>,
        runner_id: RunnerId,
        result: anyhow::Result<bool /* exited successfully */>,
    ) {
        let terminated_at = SystemTime::now();
        // remove runner independent of job success
        let (runner_name, runner_termination) = self.with_state(|sched| {
            let runner = (sched.active_runners)
                .remove(&runner_id)
                .expect("Runner id vanished");

            let runner_name = runner.info.metadata.as_ref().map(|m| m.runner_name.clone());
            if let Some(runner_name) = &runner_name {
                (sched.active_rids_by_name)
                    .remove(runner_name)
                    .expect("Runner name vanished");
            }

            if sched.runner_history.len() >= self.history_len {
                sched.runner_history.pop_back();
            }
            let termination = match (runner.state, &result) {
                (RunnerState::Listening, Ok(_)) => RunnerTermination::ListeningTimeout,
                (RunnerState::Running(job), Ok(true)) => RunnerTermination::Completed(job),
                _ => RunnerTermination::Failed,
            };
            sched.runner_history.push_front(TerminatedRunner {
                info: runner.info,
                termination,
                terminated_at,
            });
            sched.full_runner_history_len += 1;

            (runner_name, termination)
        });

        let runner_log_ref = match runner_name {
            Some(runner_name) => format!("Runner {runner_id} ({runner_name})"),
            None => format!("Runner {runner_id}"),
        };
        match runner_termination {
            RunnerTermination::Completed(job) => {
                info!("{runner_log_ref} completed job {job}")
            }
            RunnerTermination::ListeningTimeout => {
                warn!(
                    "{runner_log_ref} timed out waiting for a job. {}",
                    "This might be due to a a foreign runner picking up our jobs."
                );
                self.schedule().unwrap_or_else(|e| error!("{e:#}"));
            }
            RunnerTermination::Failed => {
                let message = match result {
                    Ok(status) => format!("(prematurely) exited with status {status}"),
                    Err(e) => format!("{e:#}"),
                };
                error!("{runner_log_ref} failed: {message}");
                self.schedule().unwrap_or_else(|e| error!("{e:#}"));
            }
        }
    }

    // TODO what do we actually signal with a result here? If scheduling fails, does it place the
    //  scheduler in some permanent invalid state, will we try again next time, and can we find a
    //  way to avoid endlessly re-trying a failing operation?
    fn schedule(self: &Arc<Self>) -> anyhow::Result<()> {
        let (mut pending_job_targets, mut unassigned_runner_targets) = self.with_state(|sched| {
            let pending_job_targets: Vec<_> = (sched.active_jobs)
                .iter()
                .filter_map(|(_, job)| match &job.state {
                    WorkflowJobState::Pending(targets) => Some(targets.clone()),
                    _ => None,
                })
                .collect();

            let mut unassigned_runner_targets = HashMap::new();
            for (_, runner) in &sched.active_runners {
                if let RunnerState::Queued | RunnerState::Starting | RunnerState::Listening =
                    runner.state
                {
                    util::increment_or_insert(&mut unassigned_runner_targets, &runner.info.target);
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
        let mut matching_targets: Vec<_> = (self.targets.iter())
            .filter(|t| {
                unmatched_labels
                    .iter()
                    .all(|jl| t.runner_labels.contains(jl))
            })
            .collect();
        // sort first by priority ascending, then by remaining runner label count descending
        matching_targets.sort_by_key(|t| (t.priority, usize::MAX - t.runner_labels.len()));
        (matching_targets.iter()).map(|t| t.id.clone()).collect()
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

            let state_changed_at = SystemTime::now();
            self.with_state(|sched| {
                // like state.try_insert(), but stable
                match sched.active_jobs.entry(job_id) {
                    Entry::Occupied(_) => Err(Error::InvalidState(format!(
                        "Workflow job {job_id} is already queued"
                    ))),
                    Entry::Vacant(entry) => {
                        entry.insert(ActiveWorkflowJob {
                            info: WorkflowJobInfo {
                                id: job_id,
                                name: name.to_owned(),
                                url: url.to_owned(),
                            },
                            state: WorkflowJobState::Pending(targets),
                            state_changed_at,
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
            RunnerNotConnected(RunnerId),
            ForeignRunnerPickedUpForeignJob,
        }

        let transition = self.with_state(|sched| {
            use {RunnerState as Rs, WorkflowJobState as Wjs};
            let runner_and_state = (sched.active_rids_by_name.get(runner_name))
                .map(|rid| (rid, &mut sched.active_runners.get_mut(rid).unwrap().state));
            let workflow_job_state = sched.active_jobs.get_mut(&job_id).map(|job| &mut job.state);
            match (runner_and_state, workflow_job_state) {
                (
                    Some((runner_id, runner_state @ (Rs::Starting | Rs::Listening))),
                    Some(job_state @ Wjs::Pending(_)),
                ) => {
                    *runner_state = RunnerState::Running(job_id);
                    *job_state = Wjs::InProgress(AssignedRunner::Scheduled(*runner_id));
                    Transition::RunnerPickedUpPendingJob(*runner_id)
                }
                (Some((runner_id, runner_state @ (Rs::Starting | Rs::Listening))), None) => {
                    *runner_state = Rs::Running(job_id);
                    Transition::RunnerStoleForeignJob(*runner_id)
                }
                (None, Some(job_state @ Wjs::Pending(_))) => {
                    *job_state = Wjs::InProgress(AssignedRunner::Foreign(runner_name.to_owned()));
                    Transition::ForeignRunnerStolePendingJob
                }
                (Some((runner_id, _)), _) => Transition::RunnerNotConnected(*runner_id),
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
            Transition::RunnerNotConnected(runner_id) => Err(Error::InvalidState(format!(
                "Runner {runner_id} ({runner_name}) is not connected"
            ))),
        }
    }

    pub fn job_terminated(
        self: &Arc<Self>,
        job_id: github::WorkflowJobId,
        termination: WorkflowJobTermination,
    ) -> Result<(), Error> {
        enum Transition {
            TrackedJobTerminated,
            UntrackedJobTerminated,
            TrackedJobNotInProgress,
        }

        let terminated_at = SystemTime::now();
        let transition = self.with_state(|sched| {
            let terminated_job = match sched.active_jobs.remove(&job_id) {
                Some(ActiveWorkflowJob {
                    info,
                    state: WorkflowJobState::InProgress(assignment),
                    ..
                }) => TerminatedWorkflowJob {
                    info,
                    assignment: Some(assignment),
                    termination,
                    terminated_at,
                },
                Some(_) => return Transition::TrackedJobNotInProgress,
                // It's acceptable for job_id to be unknown in case it's from a foreign job
                None => return Transition::UntrackedJobTerminated,
            };

            if sched.job_history.len() >= self.history_len {
                sched.job_history.pop_back();
            }
            sched.job_history.push_front(terminated_job);
            sched.full_job_history_len += 1;
            Transition::TrackedJobTerminated
        });

        match transition {
            Transition::TrackedJobTerminated => Ok(info!("Job {job_id} {termination:?}")),
            Transition::UntrackedJobTerminated => Ok(()),
            Transition::TrackedJobNotInProgress => {
                Err(Error::InvalidState(format!("Job {job_id} not in progress")))
            }
        }
    }

    pub fn snapshot_state(&self) -> SchedulerStateSnapshot {
        self.with_state(|sched| SchedulerStateSnapshot {
            active_jobs: sched.active_jobs.values().map(Clone::clone).collect(),
            active_runners: sched.active_runners.values().map(Clone::clone).collect(),
            job_history: sched.job_history.iter().map(Clone::clone).collect(),
            full_job_history_len: sched.full_job_history_len,
            runner_history: sched.runner_history.iter().map(Clone::clone).collect(),
            full_runner_history_len: sched.full_runner_history_len,
        })
    }
}

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
    let targets = vec![
        Target {
            id: TargetId("target-a".to_owned()),
            runner_labels: vec!["a-label-1".to_owned()],
            priority: 0,
        },
        Target {
            id: TargetId("target-b".to_owned()),
            runner_labels: vec!["b-label-1".to_owned(), "b-label-2".to_owned()],
            priority: 0,
        },
    ];
    let executor = MockExecutor::default();
    let scheduler = Arc::new(Scheduler::new(
        Box::new(executor.clone()),
        runner_labels,
        targets,
        2, // history_len
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
        assert_eq!(state.active_jobs.len(), 1);
        assert_eq!(state.active_runners.len(), 1);
    }

    {
        let runners = executor.take_runners();
        assert_eq!(runners.len(), 1);
        assert_eq!(runners[0].1 .0, "target-a".to_owned());
        let runner_id = runners[0].0;

        scheduler
            .runner_connected(
                runner_id,
                RunnerMetadata {
                    slurm_job: slurm::JobId(0),
                    runner_name: "runner-a-0".to_string(),
                    host_name: "host-a".to_string(),
                    concurrent_id: 0,
                },
            )
            .unwrap();

        scheduler.runner_listening(runner_id);

        // our runner picks up a foreign job, causing a reschedule
        scheduler
            .job_processing(github::WorkflowJobId(2), "runner-a-0")
            .unwrap();
        scheduler
            .job_terminated(
                github::WorkflowJobId(2),
                WorkflowJobTermination::CompletedWithSuccess,
            )
            .unwrap();
        scheduler.runner_disconnected(runner_id, Ok(true));
    }

    {
        let state = scheduler.snapshot_state();
        assert_eq!(state.active_jobs.len(), 1);
        assert_eq!(state.active_runners.len(), 1);
    }

    {
        let runners = executor.take_runners();
        assert_eq!(runners.len(), 1);
        assert_eq!(runners[0].1 .0, "target-a".to_owned());
        let runner_id = runners[0].0;

        scheduler
            .runner_connected(
                runner_id,
                RunnerMetadata {
                    slurm_job: slurm::JobId(0),
                    runner_name: "runner-a-1".to_string(),
                    host_name: "host-a".to_string(),
                    concurrent_id: 0,
                },
            )
            .unwrap();

        scheduler.runner_listening(runner_id);

        // the re-scheduled runner now picks up our job
        scheduler
            .job_processing(github::WorkflowJobId(1), "runner-a-1")
            .unwrap();
        scheduler
            .job_terminated(
                github::WorkflowJobId(1),
                WorkflowJobTermination::CompletedWithSuccess,
            )
            .unwrap();
        scheduler.runner_disconnected(runner_id, Ok(true));
    }

    {
        let state = scheduler.snapshot_state();
        assert!(state.active_jobs.is_empty());
        assert!(state.active_runners.is_empty());
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

        scheduler
            .runner_connected(
                runner_id,
                RunnerMetadata {
                    slurm_job: slurm::JobId(0),
                    runner_name: "runner-b-0".to_string(),
                    host_name: "host-b".to_string(),
                    concurrent_id: 0,
                },
            )
            .unwrap();

        scheduler.runner_listening(runner_id);

        // a foreign runner picks up our job
        scheduler
            .job_processing(github::WorkflowJobId(3), "foreign-runner-0")
            .unwrap();
        scheduler
            .job_terminated(
                github::WorkflowJobId(3),
                WorkflowJobTermination::CompletedWithSuccess,
            )
            .unwrap();
        scheduler.runner_disconnected(runner_id, Ok(false)); // due to timeout
    }

    {
        let state = scheduler.snapshot_state();
        assert!(state.active_jobs.is_empty());
        assert!(state.active_runners.is_empty());
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

        scheduler
            .runner_connected(
                runner_id,
                RunnerMetadata {
                    slurm_job: slurm::JobId(0),
                    runner_name: "runner-b-0".to_string(),
                    host_name: "host-b".to_string(),
                    concurrent_id: 0,
                },
            )
            .unwrap();

        // spurious runner timeout - requires a reschedule
        scheduler.runner_disconnected(runner_id, Ok(false));
    }

    {
        let state = scheduler.snapshot_state();
        assert_eq!(state.active_jobs.len(), 1);
        assert_eq!(state.active_runners.len(), 1);
    }

    {
        let runners = executor.take_runners();
        assert_eq!(runners.len(), 1);
        assert_eq!(runners[0].1 .0, "target-b".to_owned());
        let runner_id = runners[0].0;

        scheduler
            .runner_connected(
                runner_id,
                RunnerMetadata {
                    slurm_job: slurm::JobId(0),
                    runner_name: "runner-b-2".to_string(),
                    host_name: "host-b".to_string(),
                    concurrent_id: 0,
                },
            )
            .unwrap();

        scheduler.runner_listening(runner_id);

        // the re-scheduled runner now picks up our job
        scheduler
            .job_processing(github::WorkflowJobId(4), "runner-b-2")
            .unwrap();
        scheduler
            .job_terminated(
                github::WorkflowJobId(4),
                WorkflowJobTermination::CompletedWithSuccess,
            )
            .unwrap();
        scheduler.runner_disconnected(runner_id, Ok(true));
    }

    {
        let state = scheduler.snapshot_state();
        assert!(state.active_jobs.is_empty());
        assert!(state.active_runners.is_empty());
    }
}
