use std::ffi::OsString;
use std::fmt::{Display, Formatter};
use std::num::ParseIntError;
use std::path::Path;
use std::process::{ExitStatus, Stdio};
use std::str::FromStr;

use anyhow::Context as _;
use log::{debug, warn};
use nix::unistd::getuid;
use serde::{Deserialize, Serialize};
use tokio::process::{Child, Command};

use crate::config::{Config, TargetId};
use crate::json_log;
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

pub struct RunnerJob {
    name: String,
    child: Child,
}

impl RunnerJob {
    pub async fn spawn(
        config_path: &Path,
        config: &Config,
        target: &TargetId,
    ) -> anyhow::Result<RunnerJob> {
        let name = format!("{}-{}", &config.slurm.job_name, &target.0);
        let os_name = OsString::from_str(&name)
            .with_context(|| "Runner name is not an OS-compatible string")?;

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
        args.push(OsString::from_str("-J").unwrap());
        args.push(os_name);
        args.push(executable);
        args.push(OsString::from_str("-c").unwrap());
        args.push(config_path.as_os_str().to_owned());
        args.push(OsString::from_str("runner").unwrap());
        args.push(OsString::from_str(&target.0).unwrap());

        if log::log_enabled!(log::Level::Debug) {
            let mut cl = vec![config.slurm.srun.to_string_lossy().to_owned()];
            for a in &args {
                cl.push(a.to_string_lossy().to_owned());
            }
            debug!("Starting {}", cl.join(" "));
        }

        let child = Command::new(&config.slurm.srun)
            .args(args)
            .envs(config.slurm.srun_env.iter())
            .envs(config.targets[target].srun_env.iter())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| "Failed to execute srun")?;

        Ok(RunnerJob { name, child })
    }

    pub async fn join(self) -> anyhow::Result<ExitStatus> {
        let RunnerJob { name, mut child } = self;

        let mut output = ChildStreamMux::new(child.stdout.take(), child.stderr.take());
        while let Some((stream, line)) = output
            .next_line()
            .await
            .with_context(|| "Error reading output of srun")?
        {
            match stream {
                ChildStream::Stdout => json_log::parse_and_log(&name, &line),
                ChildStream::Stderr => warn!("{}", line),
            }
        }

        child
            .wait()
            .await
            .with_context(|| "Error waiting for srun execution to complete")
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
