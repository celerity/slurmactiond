use log::{debug, warn};
use std::fmt::{self, Display, Formatter};
use std::io;
use std::process::{ExitStatus, Output, Stdio};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, BufReader, Lines};
use tokio::process::{ChildStderr, ChildStdout, Command};
use tokio::select;

#[derive(Debug)]
pub struct ExitError {
    status: i32,
    stderr: String,
}

impl Display for ExitError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Child process exited with status {}", self.status)?;
        if !self.stderr.is_empty() {
            write!(f, ":\n{}", self.stderr)?;
        }
        Ok(())
    }
}

impl std::error::Error for ExitError {}

pub trait OutputExt {
    fn successful(self) -> Result<Output, ExitError>;
}

impl OutputExt for Output {
    fn successful(self) -> Result<Output, ExitError> {
        if self.status.success() {
            Ok(self)
        } else {
            Err(ExitError {
                status: self.status.code().expect("process exit code not set"),
                stderr: String::from_utf8_lossy(&self.stderr).into_owned(),
            })
        }
    }
}

pub trait ExitStatusExt {
    fn successful(self) -> Result<(), ExitError>;
}

impl ExitStatusExt for ExitStatus {
    fn successful(self) -> Result<(), ExitError> {
        if self.success() {
            Ok(())
        } else {
            Err(ExitError {
                status: self.code().expect("process exit code not set"),
                stderr: String::new(),
            })
        }
    }
}

pub enum ChildStream {
    Stdout,
    Stderr,
}

pub struct ChildStreamMux {
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
    pub fn new(stdout: Option<ChildStdout>, stderr: Option<ChildStderr>) -> Self {
        ChildStreamMux {
            stdout: stdout.map(|out| BufReader::new(out).lines()),
            stderr: stderr.map(|err| BufReader::new(err).lines()),
        }
    }

    pub async fn next_line(&mut self) -> io::Result<Option<(ChildStream, String)>> {
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

pub async fn run_and_log_output(tag: &str, command: &mut Command) -> io::Result<ExitStatus> {
    let mut child = command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let mut output = ChildStreamMux::new(child.stdout.take(), child.stderr.take());
    while let Some((stream, line)) = output.next_line().await? {
        match stream {
            ChildStream::Stdout => debug!("{tag}: {line}"),
            ChildStream::Stderr => warn!("{tag}: {line}"),
        }
    }
    child.wait().await
}

pub fn getuid() -> libc::uid_t {
    unsafe { libc::getuid() }
}
