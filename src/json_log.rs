use crate::slurm;
use log::{log, warn};
use log::{Level, Record};
use serde::{Deserialize, Serialize};
use std::io::{Result, Write};

#[derive(Serialize, Deserialize)]
struct Envelope {
    level: Level,
    message: String,
    job: Option<slurm::JobId>,
}

pub struct Formatter {
    job: Option<slurm::JobId>,
}

impl Formatter {
    pub fn new() -> Formatter {
        Formatter {
            // Can't log here in case current_job_id() fails because we're in the process of
            // setting up the logger!
            job: slurm::current_job().ok(),
        }
    }

    pub fn format<W: Write>(self: &Self, w: &mut W, record: &Record) -> Result<()> {
        let envelope = Envelope {
            level: record.metadata().level(),
            message: record.args().to_string(),
            job: self.job,
        };
        serde_json::to_writer(w as &mut _, &envelope)?;
        w.write(b"\n")?;
        Ok(())
    }
}

pub fn parse_and_log(tag: &str, line: &str) {
    match serde_json::from_str(line) {
        Ok(Envelope {
            level,
            message,
            job: Some(job),
        }) => log!(level, "SLURM #{job}: {message}"),
        Ok(Envelope {
            level,
            message,
            job: None,
        }) => log!(level, "{message}"),
        Err(_) => warn!("{tag}: {line}"),
    }
}
