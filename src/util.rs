use log::{debug, error, info, warn};
use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::io;
use std::process::{ExitStatus, Output, Stdio};
use std::time::Duration;
use thiserror::Error;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, BufReader, Lines};
use tokio::process::{Child, ChildStderr, ChildStdout, Command};
use tokio::select;

#[derive(Error, Debug)]
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

pub trait SuccessExt {
    type Item;
    fn successful(self) -> Result<Self::Item, ExitError>;
}

impl SuccessExt for Output {
    type Item = Self;
    fn successful(self) -> Result<Self::Item, ExitError> {
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

impl SuccessExt for ExitStatus {
    type Item = ();
    fn successful(self) -> Result<Self::Item, ExitError> {
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

pub trait ResultSuccessExt {
    type Item;
    fn and_successful(self) -> anyhow::Result<Self::Item>;
}

impl<T, E> ResultSuccessExt for Result<T, E>
where
    T: SuccessExt,
    anyhow::Error: From<E>,
{
    type Item = <T as SuccessExt>::Item;
    fn and_successful(self) -> anyhow::Result<Self::Item> {
        Ok(self?.successful()?)
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
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

pub struct ChildOutputStream {
    pub child: Child,
    pub output_stream: ChildStreamMux,
}

pub fn log_child_output(stream: ChildStream, tag: &str, line: &str) {
    match stream {
        ChildStream::Stdout => debug!("{tag}: {line}"),
        ChildStream::Stderr => warn!("{tag}: {line}"),
    }
}

impl ChildOutputStream {
    pub async fn log_output(mut self, tag: &'static str) -> io::Result<ExitStatus> {
        while let Some((stream, line)) = self.output_stream.next_line().await? {
            log_child_output(stream, tag, &line);
        }
        self.child.wait().await
    }
}

pub trait CommandOutputStreamExt {
    fn output_stream(&mut self) -> io::Result<ChildOutputStream>;
}

impl CommandOutputStreamExt for Command {
    fn output_stream(&mut self) -> io::Result<ChildOutputStream> {
        let mut child = self
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let output_stream = ChildStreamMux::new(child.stdout.take(), child.stderr.take());
        Ok(ChildOutputStream {
            child,
            output_stream,
        })
    }
}

pub fn getuid() -> libc::uid_t {
    unsafe { libc::getuid() }
}

pub async fn async_retry_after<F, T, E>(
    delay: Duration,
    max_attempts: u32,
    mut f: impl FnMut() -> F,
) -> Result<T, anyhow::Error>
where
    F: Future<Output = Result<T, E>>,
    anyhow::Error: From<E>,
    E: Display,
{
    let mut attempt = 0u32;
    loop {
        match f().await {
            Ok(item) => return Ok(item),
            Err(e) => {
                error!("{}", e);
                attempt += 1;
                if attempt >= max_attempts {
                    return Err(anyhow::Error::from(e)
                        .context(format!("Giving up after {attempt} attempts")));
                }
            }
        }
        tokio::time::sleep(delay).await;
        info!("Retrying ({}/{})", attempt, max_attempts - 1);
    }
}
