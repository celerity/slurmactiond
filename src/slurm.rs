use std::borrow::Cow;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::io;
use std::process::{Command, Stdio};
use std::str::from_utf8;

use crate::config::{Config, PartitionId};
use crate::github::{Asset, RunnerRegistrationToken};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct JobId(pub u64);

fn into_io_result<T, E: Display>(r: Result<T, E>, cause: &str) -> io::Result<T> {
    r.map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{cause}: {e}")))
}

fn sh_escape<'s>(s: impl Into<Cow<'s, str>>) -> Cow<'s, str> {
    shell_escape::unix::escape(s.into())
}

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

pub fn batch_submit(
    config: &Config,
    tarball: &Asset,
    partition: &PartitionId,
    token: &RunnerRegistrationToken,
) -> io::Result<JobId> {
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
    }
        .to_string();
    child.stdin.as_ref().unwrap().write_all(script.as_bytes())?;
    let output = child.wait_with_output()?;
    if output.status.success() {
        let stdout = into_io_result(from_utf8(&output.stdout), "cannot decode sbatch output")?;
        let job_id = into_io_result(
            stdout.trim().parse(),
            "cannot interpret sbatch output as job id",
        )?;
        return Ok(JobId(job_id));
    }
    let stderr = into_io_result(
        from_utf8(&output.stderr),
        "cannot decode sbatch error message",
    )?;
    into_io_result(
        Err(stderr),
        &format!(
            "sbatch returned exit code {}",
            output.status.code().unwrap()
        ),
    )
}
