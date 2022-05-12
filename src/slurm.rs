use std::borrow::Cow;
use std::io;
use std::process::Stdio;

use derive_more::{Display, From};
use log::{debug, LevelFilter};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

use crate::config::{Config, TargetId};
use crate::util::{ExitError, OutputExt};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct JobId(pub u64);

fn writeln_batch_opts(f: &mut impl io::Write, opts: &[String]) -> io::Result<()> {
    if !opts.is_empty() {
        write!(f, "#SBATCH")?;
        for o in opts {
            write!(f, " {}", sh_escape(o))?;
        }
        writeln!(f)?;
    }
    Ok(())
}

fn sh_escape<'s>(s: impl Into<Cow<'s, str>>) -> Cow<'s, str> {
    shell_escape::unix::escape(s.into())
}

#[derive(Debug, Display, From)]
pub enum SlurmError {
    #[display(fmt = "{}", _0)]
    Io(io::Error),
    #[display(fmt = "{}", _0)]
    ChildProcess(ExitError),
}

impl SlurmError {
    pub fn from_other(message: String) -> Self {
        SlurmError::Io(io::Error::new(io::ErrorKind::Other, message))
    }
}

pub async fn batch_submit(config: &Config, target: &TargetId) -> Result<JobId, SlurmError> {
    use std::io::Write;

    let sh_job_name = sh_escape(&config.slurm.job_name);
    let sh_target_id = sh_escape(&target.0);
    let sh_executable = std::env::current_exe()?
        .as_os_str()
        .to_str()
        .map(|slice| sh_escape(slice).into_owned())
        .ok_or(SlurmError::from_other(
            "process executable path is not in UTF-8".to_owned(),
        ))?;

    let mut script: Vec<u8> = Vec::new();
    writeln!(script, "#!/bin/bash")?;
    writeln_batch_opts(&mut script, &config.slurm.sbatch_options)?;
    writeln_batch_opts(&mut script, &config.targets[target].sbatch_options)?;
    writeln!(script, "#SBATCH -J {sh_job_name}-{sh_target_id}")?;
    writeln!(script, "#SBATCH --parsable")?;
    writeln!(script)?;
    writeln!(script, "exec {sh_executable} {sh_target_id} $SLURM_JOB_ID")?;

    let sbatch = config.slurm.sbatch.as_deref().unwrap_or("sbatch");
    if log::max_level() >= LevelFilter::Debug {
        debug!("Submtting to {sbatch}:\n{}", String::from_utf8_lossy(&script));
    }

    let mut child = Command::new(sbatch)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    child.stdin.as_mut().unwrap().write(&script).await?;
    let output = child.wait_with_output().await?.successful()?;

    let stdout = String::from_utf8(output.stdout)
        .map_err(|e| SlurmError::from_other(format!("cannot decode sbatch output: {e}")))?;
    let job_id = stdout
        .trim()
        .parse()
        .map_err(|e| SlurmError::from_other(format!("cannot parse sbatch output: {e}")))?;
    Ok(JobId(job_id))
}
