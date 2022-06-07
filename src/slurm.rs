use std::io;
use std::process::{ExitStatus, Stdio};

use derive_more::{Display, From};
use lazy_static::lazy_static;
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

lazy_static! {
    pub static ref CURRENT_JOB_ID: Option<JobId> = {
        match std::env::vars().find(|(k, _)| k == JOB_ID_VAR) {
            Some((_, v)) => match v.parse() {
                Ok(id) => return Some(JobId(id)),
                Err(e) => warn!("Could not parse {JOB_ID_VAR}: {e}"),
            },
            None => warn!("Environment variable {JOB_ID_VAR} not set"),
        }
        None
    };
}

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
