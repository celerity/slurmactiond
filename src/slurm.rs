use std::io;
use std::num::ParseIntError;
use std::process::{ExitStatus, Stdio};

use derive_more::{Display, From};
use log::{debug, warn};
use tokio::process::{Child, Command};
use serde::{Serialize, Deserialize};

use crate::config::{Config, TargetId};
use crate::json_log;
use crate::util::{ExitError, ChildStream, ChildStreamMux};


#[derive(Copy, Clone, PartialEq, Eq, Hash, Display, Serialize, Deserialize)]
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

pub fn current_job_id() -> Result<JobId, JobIdReadError> {
    let id_string = std::env::vars()
        .find(|(k, _)| k == JOB_ID_VAR)
        .ok_or(JobIdReadError::Unset)?
        .1;
    let id_int = id_string
        .parse()
        .map_err(|e| JobIdReadError::Parse(e))?;
    Ok(JobId(id_int))
}

impl std::error::Error for JobIdReadError {}


#[derive(Debug, Display, From)]
pub enum Error {
    #[display(fmt = "{}", _0)]
    Io(io::Error),
    #[display(fmt = "{}", _0)]
    ChildProcess(ExitError),
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
