use std::fmt::Display;
use std::io::{self, Read, Write};
use std::process::{Command, Stdio};
use std::str::from_utf8;

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct JobId(pub u32);

fn into_io_result<T, E: Display>(r: Result<T, E>) -> io::Result<T> {
    r.map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{e}")))
}

pub fn batch_submit(script: &str) -> io::Result<JobId> {
    let child = Command::new("sbatch")
        .args(&["--parsable"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    child.stdin.as_ref().unwrap().write_all(script.as_bytes())?;
    let output = child.wait_with_output()?;
    if output.status.success() {
        let stdout = into_io_result(from_utf8(&output.stdout))?;
        let job_id = into_io_result(stdout.parse())?;
        return Ok(JobId(job_id));
    }
    let stderr = into_io_result(from_utf8(&output.stderr))?;
    into_io_result(Err(stderr))
}
