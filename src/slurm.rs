use std::ffi::OsString;
use std::fmt::{Display, Formatter};
use std::num::ParseIntError;
use std::path::Path;
use std::process::{ExitStatus, Stdio};
use std::str::FromStr;

use anyhow::Context as _;
use log::{debug, error, warn};
use nix::unistd::getuid;
use serde::{Deserialize, Serialize};
use tokio::process::{Child, Command};

use crate::config::{Config, TargetId};
use crate::ipc;
use crate::util::{ChildStream, ChildStreamMux, ResultSuccessExt as _};

#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JobId(pub u64);

impl Display for JobId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for JobId {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse().map(JobId)
    }
}

const JOB_ID_VAR: &str = "SLURM_JOB_ID";

pub fn current_job() -> anyhow::Result<JobId> {
    let id_string = std::env::vars()
        .find(|(k, _)| k == JOB_ID_VAR)
        .ok_or_else(|| anyhow::anyhow!("Environment variable {JOB_ID_VAR} not set"))?
        .1;
    let id_int = id_string
        .parse()
        .with_context(|| format!("Cannot parse {JOB_ID_VAR}"))?;
    Ok(JobId(id_int))
}

pub struct RunnerJobState {
    child: Child,
    output: ChildStreamMux,
}

impl RunnerJobState {
    async fn next_message(&mut self) -> anyhow::Result<Option<ipc::Envelope>> {
        while let Some((stream, line)) = self
            .output
            .next_line()
            .await
            .with_context(|| "Error reading output of srun")?
        {
            match stream {
                ChildStream::Stdout => return Ok(Some(ipc::parse(&line)?)),
                ChildStream::Stderr => warn!("{}", line),
            }
        }
        Ok(None)
    }

    async fn wait(&mut self) -> anyhow::Result<ExitStatus> {
        return (self.child.wait().await)
            .with_context(|| "Error waiting for srun execution to complete");
    }
}

#[must_use]
pub struct RunnerJob {
    state: RunnerJobState,
}

#[must_use]
pub struct AttachedRunnerJob {
    metadata: Option<ipc::RunnerMetadata>,
    state: RunnerJobState,
}

impl RunnerJob {
    pub fn spawn(
        config_path: &Path,
        config: &Config,
        target: &TargetId,
    ) -> anyhow::Result<RunnerJob> {
        let name = format!("{}-{}", &config.slurm.job_name, &target.0);
        let executable = std::env::current_exe()
            .with_context(|| "Cannot determine current executable name")?
            .as_os_str()
            .to_owned();

        let mut args: Vec<OsString> = Vec::new();
        args.extend(config.slurm.srun_options.iter().map(OsString::from));
        args.extend(
            config.targets[target]
                .srun_options
                .iter()
                .map(OsString::from),
        );
        args.push("-n1".into());
        args.push("-J".into());
        args.push(name.as_str().into());
        args.push(executable);
        args.push("-c".into());
        args.push(config_path.into());
        args.push("runner".into());
        args.push(target.0.as_str().into());

        if log::log_enabled!(log::Level::Debug) {
            let mut cl = vec![config.slurm.srun.to_string_lossy().to_owned()];
            for a in &args {
                cl.push(a.to_string_lossy().to_owned());
            }
            debug!("Starting {}", cl.join(" "));
        }

        let mut child = Command::new(&config.slurm.srun)
            .args(args)
            .envs(config.slurm.srun_env.iter())
            .envs(config.targets[target].srun_env.iter())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| "Failed to execute srun")?;

        let output = ChildStreamMux::new(child.stdout.take(), child.stderr.take());

        Ok(RunnerJob {
            state: RunnerJobState {
                child,
                output,
            },
        })
    }

    pub async fn attach(mut self) -> anyhow::Result<AttachedRunnerJob> {
        loop {
            match self.state.next_message().await? {
                Some(ipc::Envelope::Log(entry)) => entry.log(),
                Some(ipc::Envelope::Metadata(metadata)) => {
                    return Ok(AttachedRunnerJob {
                        state: self.state,
                        metadata: Some(metadata),
                    })
                }
                None => {
                    if let Err(e) = self.state.wait().await {
                        error!("{e:#}");
                    }
                    anyhow::bail!("Child process closed stdout before sending metadata")
                }
            }
        }
    }
}

impl AttachedRunnerJob {
    pub fn take_metadata(&mut self) -> ipc::RunnerMetadata {
        self.metadata.take().expect("AttachedRunnerJob metadata already taken")
    }

    pub async fn wait(mut self) -> anyhow::Result<ExitStatus> {
        loop {
            match self.state.next_message().await? {
                Some(ipc::Envelope::Log(entry)) => entry.log(),
                Some(ipc::Envelope::Metadata(_)) => {
                    if let Err(e) = self.state.child.wait().await {
                        error!("Waiting on child process after it closed stdout early: {e:#}");
                    }
                    anyhow::bail!("Child process closed stdout before sending metadata")
                }
                None => return self.state.wait().await,
            }
        }
    }
}

pub async fn active_jobs(config: &Config) -> anyhow::Result<Vec<JobId>> {
    let uid = getuid().to_string();
    let output = Command::new(&config.slurm.squeue)
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
        .await
        .and_successful()?;
    String::from_utf8(output.stdout)
        .with_context(|| "Decoding the output of squeue")?
        .lines()
        .map(|l| l.parse().with_context(|| "Parsing the output of squeue"))
        .collect()
}

pub fn job_has_terminated(job: JobId, active_jobs: &[JobId]) -> bool {
    // this test races with the enqueueing of new jobs, so we have to conservatively assume that
    // a job newer than all active jobs has not terminated yet.
    !active_jobs.contains(&job) && active_jobs.iter().any(|a| a.0 > job.0)
}
