use std::borrow::Cow;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::io;
use std::process::{Command, Stdio};

use derive_more::{Display, From};
use log::debug;

use crate::config::{Config, PartitionId};
use crate::github::{Asset, RunnerRegistrationToken};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct JobId(pub u64);

struct BatchScript<'c> {
    config: &'c Config,
    tarball: &'c Asset,
    partition: &'c PartitionId,
    token: &'c RunnerRegistrationToken,
}

fn write_batch_opts<S: Display>(f: &mut Formatter<'_>, opts: &[S]) -> fmt::Result {
    if !opts.is_empty() {
        write!(f, "\n#SBATCH")?;
        for o in opts {
            write!(f, " {}", o)?;
        }
    }
    Ok(())
}

fn sh_escape<'s>(s: impl Into<Cow<'s, str>>) -> Cow<'s, str> {
    shell_escape::unix::escape(s.into())
}

impl Display for BatchScript<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let config = self.config;
        let partition = &config.partitions[self.partition];

        write!(f, "#!/bin/bash")?;
        write_batch_opts(f, &config.slurm.sbatch_options)?;
        write_batch_opts(f, &partition.sbatch_options)?;

        let base_labels = config.runner.registration.labels.iter().map(AsRef::as_ref);
        let partition_labels = partition.runner_labels.iter().map(AsRef::as_ref);
        let all_labels: Vec<&str> = base_labels.chain(partition_labels).collect();
        assert!(!all_labels.iter().any(|l| l.contains(',')));

        write!(
            f,
            r#"
#SBATCH -J {job_name}-{part_name}
#SBATCH --parsable

set -e -o pipefail -o noclobber

mkdir -p {work_dir}
JOB_DIR={work_dir}/$SLURM_JOB_ID
mkdir "$JOB_DIR"
cleanup() {{ cd; rm -rf "$JOB_DIR"; }}
trap cleanup EXIT

cd "$JOB_DIR"
if ! [ -f {tarball_name} ]; then
    curl -SsfL {tarball_url} -o {tarball_name}
fi
tar xf {tarball_name}

./config.sh \
    --unattended \
    --url https://github.com/{entity} \
    --token {token} \
    --name {runner_name}-{part_name}-"$SLURM_JOB_ID" \
    --labels {labels} \"#,
            job_name = sh_escape(&config.slurm.job_name),
            work_dir = sh_escape(&config.runner.work_dir),
            tarball_url = sh_escape(&self.tarball.url),
            tarball_name = sh_escape(&self.tarball.name),
            entity = sh_escape(&config.github.entity.to_string()),
            token = sh_escape(&self.token.0),
            runner_name = sh_escape(&config.runner.registration.name),
            part_name = sh_escape(&self.partition.0),
            labels = sh_escape(all_labels.join(",")),
        )?;

        write!(
            f,
            r#"
    --ephemeral
./run.sh
"#
        )
    }
}

#[derive(Debug, Display, From)]
pub enum SlurmError {
    #[display(fmt = "I/O error: {}", _0)]
    IoError(io::Error),
    #[display(
    fmt = "Child process exited with status {}, error message:\n{}",
    status,
    message
    )]
    ErrorExit { status: i32, message: String },
}

impl SlurmError {
    pub fn from_other(message: String) -> Self {
        SlurmError::IoError(io::Error::new(io::ErrorKind::Other, message))
    }
}

pub fn batch_submit(
    config: &Config,
    tarball: &Asset,
    partition: &PartitionId,
    token: &RunnerRegistrationToken,
) -> Result<JobId, SlurmError> {
    use std::io::Write;

    let sbatch = config.slurm.sbatch.as_deref().unwrap_or("sbatch");
    let child = Command::new(sbatch)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let script = BatchScript {
        config,
        tarball,
        partition,
        token,
    };
    debug!("{script}");
    child
        .stdin
        .as_ref()
        .unwrap()
        .write_all(script.to_string().as_bytes())?;
    let output = child.wait_with_output()?;
    if output.status.success() {
        let stdout = String::from_utf8(output.stdout)
            .map_err(|e| SlurmError::from_other(format!("cannot decode sbatch output: {e}")))?;
        let job_id = stdout
            .trim()
            .parse()
            .map_err(|e| SlurmError::from_other(format!("cannot parse sbatch output: {e}")))?;
        return Ok(JobId(job_id));
    }
    let stderr = String::from_utf8(output.stderr)
        .map_err(|e| SlurmError::from_other(format!("cannot decode sbatch error: {e}")))?;
    Err(SlurmError::ErrorExit {
        status: output.status.code().unwrap(),
        message: stderr,
    })
}
