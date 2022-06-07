use std::io;
use std::process::{ExitStatus, Stdio};

use derive_more::{Display, From};
use log::debug;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, BufReader, Lines};
use tokio::process::{Child, ChildStderr, ChildStdout, Command};
use tokio::select;

use crate::config::{Config, TargetId};
use crate::util::ExitError;

#[derive(Debug, Display, From)]
pub enum Error {
    #[display(fmt = "{}", _0)]
    Io(io::Error),
    #[display(fmt = "{}", _0)]
    ChildProcess(ExitError),
}

enum ChildStream {
    Stdout,
    Stderr,
}

struct ChildStreamMux {
    stdout: Option<Lines<BufReader<ChildStdout>>>,
    stderr: Option<Lines<BufReader<ChildStderr>>>,
}

async fn maybe_next_line<R>(stream: &mut Option<Lines<R>>) -> io::Result<Option<String>>
where
    R: AsyncBufRead + Unpin,
{
    if let Some(s) = stream {
        let line = s.next_line().await?;
        if line.is_none() {
            *stream = None;
        }
        Ok(line)
    } else {
        Ok(None)
    }
}

impl ChildStreamMux {
    fn new(stdout: Option<ChildStdout>, stderr: Option<ChildStderr>) -> Self {
        ChildStreamMux {
            stdout: stdout.map(|out| BufReader::new(out).lines()),
            stderr: stderr.map(|err| BufReader::new(err).lines()),
        }
    }

    async fn next_line(&mut self) -> io::Result<Option<(ChildStream, String)>> {
        loop {
            let (stream, line) = match (&mut self.stdout, &mut self.stderr) {
                (out @ Some(_), err @ Some(_)) => select! {
                    o = maybe_next_line(out) => (ChildStream::Stdout, o?),
                    e = maybe_next_line(err) => (ChildStream::Stderr, e?),
                },
                (out @ Some(_), None) => (ChildStream::Stdout, maybe_next_line(out).await?),
                (None, err @ Some(_)) => (ChildStream::Stderr, maybe_next_line(err).await?),
                (None, None) => return Ok(None),
            };
            if let Some(l) = line {
                return Ok(Some((stream, l)));
            }
        }
    }
}

pub struct Job(Child);

impl Job {
    pub async fn spawn(config: &Config, target: &TargetId) -> Result<Job, Error> {
        let executable = std::env::current_exe()?
            .as_os_str()
            .to_string_lossy()
            .into_owned();

        let mut args = Vec::new();
        args.extend_from_slice(&config.slurm.srun_options);
        args.extend_from_slice(&config.targets[target].srun_options);
        args.push("-J".to_string());
        args.push(format!("{}-{}", &config.slurm.job_name, &target.0));
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
        Ok(Job(child))
    }

    pub async fn join(&mut self) -> Result<ExitStatus, Error> {
        let Job(child) = self;
        let mut output = ChildStreamMux::new(child.stdout.take(), child.stderr.take());
        while let Some((stream, line)) = output.next_line().await? {
            let label = match stream {
                ChildStream::Stdout => "srun",
                ChildStream::Stderr => "srun[err]",
            };
            debug!("{label}: {line}");
        }

        let status = child.wait().await?;
        Ok(status)
    }
}
