use log::{Level, Record};
use std::io::{Result, Write};
use log::{log, warn};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
struct Envelope {
    level: Level,
    message: String,
}

pub fn format<W: Write>(w: &mut W, record: &Record) -> Result<()> {
    let envelope = Envelope {
        level: record.metadata().level(),
        message: record.args().to_string(),
    };
    serde_json::to_writer(w, &envelope)?;
    Ok(())
}

pub fn parse_and_log(tag: &str, line: &str) {
    match serde_json::from_str(line) {
        Ok(Envelope{level, message}) => log!(level, "{tag}: {message}"),
        Err(_) => warn!("{tag}: {line}"),
    }
}
