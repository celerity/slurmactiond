use log::{Level, Record};
use std::io::{Result, Write};
use log::{log, warn};
use serde::{Serialize, Deserialize};
use crate::slurm;

#[derive(Serialize, Deserialize)]
struct Envelope {
    level: Level,
    message: String,
    job: Option<slurm::JobId>
}

pub fn format<W: Write>(w: &mut W, record: &Record) -> Result<()> {
    let envelope = Envelope {
        level: record.metadata().level(),
        message: record.args().to_string(),
        job: *slurm::CURRENT_JOB_ID,
    };
    serde_json::to_writer(w as &mut _, &envelope)?;
    w.write(b"\n")?;
    Ok(())
}

pub fn parse_and_log(tag: &str, line: &str) {
    match serde_json::from_str(line) {
        Ok(Envelope{level, message, job: Some(job)}) => log!(level, "SLURM #{job}: {message}"),
        Ok(Envelope{level, message, job: None}) => log!(level, "{message}"),
        Err(_) => warn!("{tag}: {line}"),
    }
}
