use std::fmt::{self, Display, Formatter};
use std::process::Output;

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
