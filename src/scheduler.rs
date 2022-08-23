use crate::config::{Config, TargetId};
use crate::{github, slurm};
use anyhow::Context;
use log::{debug, error, info};
use std::collections::HashMap;
use std::path::Path;
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

enum JobStatus {
    Queued,
    InProgress(slurm::JobId),
}

pub struct Scheduler {
    jobs: HashMap<github::WorkflowJobId, JobStatus>,
    runners: HashMap<String, slurm::JobId>,
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
            jobs: HashMap::new(),
            runners: HashMap::new(),
        }
    }

    pub fn job_enqueued<'c>(
        &mut self,
        job_id: github::WorkflowJobId,
        labels: &[String],
        config_path: &Path,
        config: &Config,
    ) -> Result<(), Error> {
        if self.jobs.contains_key(&job_id) {
            return Err(Error::InvalidState(format!(
                "Job {job_id} is already enqueued"
            )));
        }
        if let Some(target) = match_target(&config, &labels) {
            debug!("Matched runner labels {labels:?} to target {}", job_id.0);
            info!("Launching SLURM job for workflow job {job_id}");
            let runner = slurm::RunnerJob::spawn(config_path, config, target)
                .with_context(|| "Submitting job to SLURM")?;
            actix_web::rt::spawn(async move {
                match runner.join().await {
                    Ok(status) => info!("SLURM job exited with status {status}"),
                    Err(e) => error!("Running SLURM job: {e:#}"),
                }
                // TODO Scheduler should be notified when srun returns
            });
            let _replaced = self.jobs.insert(job_id, JobStatus::Queued);
            debug_assert!(_replaced.is_none());
        } else {
            debug!("Runner labels {labels:?} do not match any target");
        }
        Ok(())
    }

    pub fn job_processing(
        &mut self,
        job_id: github::WorkflowJobId,
        runner_name: &str,
    ) -> Result<(), Error> {
        if let Some(slurm_job) = self.runners.get(runner_name) {
            if let Some(status @ JobStatus::Queued) = self.jobs.get_mut(&job_id) {
                info!(
                    "Workflow job {job_id} picked up by runner {runner_name} (SLURM #{slurm_job})"
                );
                *status = JobStatus::InProgress(*slurm_job);
                Ok(())
            } else {
                // TODO our runner might have picked up a "foreign" job! Spawn another one.
                Err(Error::InvalidState(format!("Job {job_id} not queued")))
            }
        } else {
            // TODO a foreign runner has taken "our" job - remove from tracking
            debug!("Runner {runner_name} has not been spawned by us, ignoring");
            Ok(())
        }
    }

    pub fn job_completed(&mut self, job_id: github::WorkflowJobId, runner_name: &str) {
        // It's acceptable for job_id to be unknown in case it's from a foreign job
        if let Some(_) = self.jobs.remove(&job_id) {
            // TODO runner removal should actually happen on runner process exit
            self.runners.remove(runner_name);
        }
    }
}
