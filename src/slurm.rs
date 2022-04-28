use crate::config::{ActionRunnerConfig, MapConfig, SlurmConfig};
use crate::Config;
use std::borrow::Cow;
use std::fmt;
use std::fmt::{Display, Formatter, Write as _};
use std::io::{self, Read, Write as _};
use std::iter::Map;
use std::process::{Command, Stdio};
use std::str::from_utf8;

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct JobId(pub u32);

fn into_io_result<T, E: Display>(r: Result<T, E>) -> io::Result<T> {
    r.map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{e}")))
}

fn sh_escape<'s>(s: impl Into<Cow<'s, str>>) -> Cow<'s, str> {
    shell_escape::unix::escape(s.into())
}

pub struct BatchScript<'cfg> {
    pub slurm: &'cfg SlurmConfig,
    pub runner: &'cfg ActionRunnerConfig,
    pub mapping: &'cfg MapConfig,
    pub runner_seq: u64,
    pub concurrent_id: u64,
}

impl Display for BatchScript<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "#!/bin/bash")?;
        for opt_group in [&self.slurm.sbatch_args, &self.mapping.sbatch_args] {
            if !opt_group.is_empty() {
                let escaped_opts: Vec<_> = opt_group.iter().map(sh_escape).collect();
                write!(f, "\n#SBATCH {}", escaped_opts.join(" "))?;
            }
        }

        let base_labels = self.runner.base_labels.iter().map(AsRef::as_ref);
        let mapping_labels = self.mapping.runner_labels.iter().map(AsRef::as_ref);
        let all_labels: Vec<&str> = base_labels.chain(mapping_labels).collect();
        assert!(!all_labels.iter().any(|l| l.contains(',')));

        write!(
            f,
            r#"
#SBATCH -J {name}

set -e -o pipefail -o noclobber
CONCURRENT_DIR="$HOME/slurmactiond/concurrent/{concurrent_id}"
if ! [ -d "$CONCURRENT_DIR" ]; then
    mkdir -p "$CONCURRENT_DIR"
    cd "$CONCURRENT_DIR"
    tar xf {tarball}
fi
cd "$CONCURRENT_DIR"
./config.sh \
    --unattend \
    --url {url} \
    --token {token} \
    --name {name} \
    --labels {labels} \"#,
            tarball = sh_escape(&self.runner.tarball),
            url = sh_escape(&self.runner.repository_url),
            token = sh_escape(&self.runner.registration_token),
            name = sh_escape(format!("{}-{}", &self.runner.name_prefix, &self.runner_seq)),
            labels = sh_escape(all_labels.join(",")),
            concurrent_id = self.concurrent_id,
        )?;

        if let Some(group) = &self.runner.group {
            write!(
                f,
                r#"
    --runner-group {} \"#,
                sh_escape(group)
            )?;
        }

        write!(
            f,
            r#"
    --ephemeral
./run.sh
"#
        )
    }
}

pub fn batch_submit(script: BatchScript) -> io::Result<JobId> {
    let child = Command::new("sbatch")
        .args(&["--parsable"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    child
        .stdin
        .as_ref()
        .unwrap()
        .write_all(script.to_string().as_bytes())?;
    let output = child.wait_with_output()?;
    if output.status.success() {
        let stdout = into_io_result(from_utf8(&output.stdout))?;
        let job_id = into_io_result(stdout.parse())?;
        return Ok(JobId(job_id));
    }
    let stderr = into_io_result(from_utf8(&output.stderr))?;
    into_io_result(Err(stderr))
}
