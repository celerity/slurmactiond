use std::io;
use std::num::ParseIntError;
use std::process::{ExitStatus, Stdio};

use derive_more::{Display, From, FromStr};
use log::{debug, warn};
use serde::{Deserialize, Serialize};
use tokio::process::{Child, Command};

use crate::config::{Config, TargetId};
use crate::json_log;
use crate::util::{self, ChildStream, ChildStreamMux, ExitError, OutputExt};

#[derive(Copy, Clone, PartialEq, Eq, Hash, Display, Debug, Serialize, Deserialize, FromStr)]
#[display(fmt = "{}", _0)]
#[serde(transparent)]
pub struct JobId(pub u64);

const JOB_ID_VAR: &str = "SLURM_JOB_ID";

#[derive(Debug, Display)]
pub enum JobIdReadError {
    #[display(fmt = "Environment variable {JOB_ID_VAR} not set")]
    Unset,
    #[display(fmt = "Could not parse {JOB_ID_VAR}: {}", _0)]
    Parse(ParseIntError),
}

pub fn current_job() -> Result<JobId, JobIdReadError> {
    let id_string = std::env::vars()
        .find(|(k, _)| k == JOB_ID_VAR)
        .ok_or(JobIdReadError::Unset)?
        .1;
    let id_int = id_string.parse().map_err(|e| JobIdReadError::Parse(e))?;
    Ok(JobId(id_int))
}

impl std::error::Error for JobIdReadError {}

#[derive(Debug, Display, From)]
pub enum Error {
    #[display(fmt = "{}", _0)]
    Io(io::Error),
    #[display(fmt = "{}", _0)]
    ChildProcess(ExitError),
    #[display(fmt = "Cannot parse output: {}", _0)]
    Parse(Box<dyn std::error::Error + Send + Sync>),
}

pub struct RunnerJob {
    name: String,
    child: Child,
}

impl RunnerJob {
    pub async fn spawn(config: &Config, target: &TargetId) -> Result<RunnerJob, Error> {
        let name = format!("{}-{}", &config.slurm.job_name, &target.0);

        let executable = std::env::current_exe()?
            .as_os_str()
            .to_string_lossy()
            .into_owned();

        let mut args = Vec::new();
        args.extend_from_slice(&config.slurm.srun_options);
        args.extend_from_slice(&config.targets[target].srun_options);
        args.push("-J".to_string());
        args.push(name.clone());
        args.push(executable);
        args.push("runner".to_owned());
        args.push(target.0.to_owned());

        let srun = config.slurm.srun.as_deref().unwrap_or("srun");
        debug!("Starting {srun} {}", args.join(" "));

        let child = Command::new(srun)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;

        Ok(RunnerJob { name, child })
    }

    pub async fn join(self) -> io::Result<ExitStatus> {
        let RunnerJob { name, mut child } = self;

        let mut output = ChildStreamMux::new(child.stdout.take(), child.stderr.take());
        while let Some((stream, line)) = output.next_line().await? {
            match stream {
                ChildStream::Stdout => json_log::parse_and_log(&name, &line),
                ChildStream::Stderr => warn!("{}", line),
            }
        }

        child.wait().await
    }
}

pub async fn active_jobs(config: &Config) -> Result<Vec<JobId>, Error> {
    let squeue = config.slurm.squeue.as_deref().unwrap_or("squeue");
    let uid = util::getuid().to_string();
    let output = Command::new(squeue)
        .args(&[
            "-h",
            "-o",
            "%A",
            "-t",
            "CF,CG,PD,R,SO,ST,S,SE,SI,RS,RQ,RF",
            "-u",
            &uid,
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .await?
        .successful()?;
    String::from_utf8(output.stdout)
        .map_err(|e| Error::Parse(Box::new(e)))?
        .lines()
        .map(|l| l.parse().map_err(|e| Error::Parse(Box::new(e))))
        .collect()
}

pub fn job_has_terminated(job: JobId, active_jobs: &[JobId]) -> bool {
    // this test races with the enqueueing of new jobs, so we have to conservatively assume that
    // a job newer than all active jobs has not terminated yet.
    !active_jobs.contains(&job) && active_jobs.iter().any(|a| a.0 > job.0)
}
