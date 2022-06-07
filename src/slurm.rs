use std::io;
use std::process::{ExitStatus, Stdio};

use derive_more::{Display, From};
use log::debug;
use tokio::io::BufReader;
use tokio::process::{Child, Command};

use crate::config::{Config, TargetId};
use crate::json_log;
use crate::util::ExitError;

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

        Ok(RunnerJob{name, child})
    }

    pub async fn join(self) -> Result<ExitStatus, Error> {
        use tokio::io::AsyncBufReadExt;

        let RunnerJob{name, mut child} = self;

        let stdout = child.stdout.take().unwrap();
        let mut lines = BufReader::new(stdout).lines();
        while let Some(line) = lines.next_line().await? {
            json_log::parse_and_log(&name, &line);
        }

        let status = child.wait().await?;
        Ok(status)
    }
}
