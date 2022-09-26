use crate::slurm;
use log::log;
use log::{Level, Record};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::io::{Result, Write};

#[derive(Debug, Serialize, Deserialize)]
pub struct RunnerMetadata {
    pub slurm_job: slurm::JobId,
    pub runner_name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LogEntry {
    level: Level,
    message: String,
    job: Option<slurm::JobId>,
}

impl LogEntry {
    pub fn log(&self) {
        match self.job {
            Some(job) => log!(self.level, "SLURM #{job}: {}", self.message),
            None => log!(self.level, "{}", self.message),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Envelope {
    Metadata(RunnerMetadata),
    Log(LogEntry),
}

pub struct LogFormatter {
    job: Option<slurm::JobId>,
}

impl LogFormatter {
    pub fn new() -> LogFormatter {
        LogFormatter {
            // Can't log here in case current_job_id() fails because we're in the process of
            // setting up the logger!
            job: slurm::current_job().ok(),
        }
    }

    pub fn format<W: Write>(self: &Self, w: &mut W, record: &Record) -> Result<()> {
        let envelope = Envelope::Log(LogEntry {
            level: record.metadata().level(),
            message: record.args().to_string(),
            job: self.job,
        });
        serde_json::to_writer(w as &mut _, &envelope)?;
        w.write(b"\n")?;
        Ok(())
    }
}

impl Display for RunnerMetadata {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} (SLURM #{})", self.runner_name, self.slurm_job)
    }
}

pub fn parse(json: &str) -> anyhow::Result<Envelope> {
    let envelope = serde_json::from_str(json)?;
    Ok(envelope)
}

pub fn send(metadata: RunnerMetadata) -> anyhow::Result<()> {
    let envelope = Envelope::Metadata(metadata);
    let mut stdout = std::io::stdout();
    serde_json::to_writer(&mut stdout, &envelope)?;
    stdout.write(b"\n")?;
    Ok(())
}
