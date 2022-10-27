use log::{debug, info, warn};
use std::collections::HashMap;
use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::hash::Hash;
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
                warn!("{}", e);
                attempt += 1;
                if attempt >= max_attempts {
                    return Err(anyhow::Error::from(e)
                        .context(format!("Giving up after {attempt} attempts")));
                }
            }
        }
        tokio::time::sleep(delay).await;
        info!("Retrying ({}/{})", attempt + 1, max_attempts);
    }
}

// Serde custom deserializer: Process an empty string literal as Option<String>::None
// From https://github.com/serde-rs/serde/issues/1425#issuecomment-462282398
pub fn empty_string_as_none<'de, D, T>(de: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    use serde::de::{Deserialize, IntoDeserializer};

    let opt = Option::<String>::deserialize(de)?;
    let opt = opt.as_ref().map(String::as_str);
    match opt {
        None | Some("") => Ok(None),
        Some(s) => T::deserialize(s.into_deserializer()).map(Some),
    }
}

// Concurrently map a function FnMut(I) -> Result<IntoIterator<T>, E> over an an IntoIterator<I>
// and return the collected unordered Result<Vec<T>, E>.
pub async fn async_map_unordered_and_flatten<I, J, M, F, T, E, V>(
    into_iter: I,
    map_fn: M,
) -> Result<Vec<T>, E>
where
    I: IntoIterator<Item = J>,
    M: FnMut(J) -> F,
    F: Future<Output = Result<V, E>>,
    V: IntoIterator<Item = T>,
{
    use futures_util::StreamExt as _;

    const CONCURRENCY: usize = 4;
    let mut vec = Vec::new();
    let mut stream = futures_util::stream::iter(into_iter)
        .map(map_fn)
        .buffer_unordered(CONCURRENCY);
    while let (Some(head), tail) = stream.into_future().await {
        vec.extend(head?.into_iter());
        stream = tail;
    }
    Ok(vec)
}

pub fn increment_or_insert<K: Clone + PartialEq + Eq + Hash>(map: &mut HashMap<K, usize>, key: &K) {
    match map.get_mut(key) {
        Some(count) => *count += 1,
        None => drop(map.insert(key.clone(), 1)),
    }
}

pub fn decrement_or_remove<K: PartialEq + Eq + Hash>(map: &mut HashMap<K, usize>, key: &K) -> bool {
    match map.get_mut(key) {
        None => false,
        Some(1) => {
            map.remove(key);
            true
        }
        Some(count) => {
            *count -= 1;
            true
        }
    }
}

macro_rules! literal_types {
    ($($(#[$attrs:meta])* $vis:vis struct $id:ident(pub $ty:ty);)*) => ($(
        #[derive(Clone, Debug, PartialEq, Eq, Hash)]
        #[derive(::serde::Serialize, ::serde::Deserialize)]
        #[serde(transparent)]
        $(#[$attrs])*
        $vis struct $id(pub $ty);

        impl ::std::ops::Deref for $id {
            type Target = $ty;
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl ::std::convert::From<$ty> for $id {
            fn from(v: $ty) -> Self {
                Self(v)
            }
        }

        impl ::std::str::FromStr for $id {
            type Err = <$ty as ::std::str::FromStr>::Err;
            fn from_str(s: &str) -> ::std::result::Result<Self, Self::Err> {
                Ok($id(<$ty>::from_str(s)?))
            }
        }

        impl ::std::fmt::Display for $id {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    )*);
}

pub(crate) use literal_types;
