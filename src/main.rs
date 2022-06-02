use std::fs;
use std::path::Path;

use log::error;

use crate::config::TargetId;
use clap::{Parser, Subcommand};
use config::Config;

mod config;
mod github;
mod runner;
mod slurm;
mod util;
mod webhook;

#[derive(Subcommand, Debug)]
enum Command {
    Server,
    Runner { target: TargetId },
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
#[clap(propagate_version = true)]
struct Arguments {
    #[clap(subcommand)]
    command: Option<Command>,
}

fn main_inner() -> Result<(), String> {
    let cfg_path = Path::new("config.toml");
    let cfg = fs::read(cfg_path)
        .and_then(|bytes| Ok(toml::from_slice(&bytes)?))
        .map_err(|e| format!("Reading config file {p}: {e}", p = cfg_path.display()))?;

    match Arguments::parse().command.unwrap_or(Command::Server) {
        Command::Server => {
            webhook::main(cfg).map_err(|e| format!("Serving webhook over HTTP: {e}"))
        }
        Command::Runner { target } => {
            let seq = std::env::vars()
                .find(|(k, _)| k == "SLURM_JOB_ID")
                .ok_or_else(|| "Environment variable SLURM_JOB_ID not set".to_owned())?
                .1
                .parse()
                .map_err(|e| format!("Could not parse SLURM_JOB_ID: {e}"))?;
            runner::run(cfg, target, seq).map_err(|e| format!("Starting runner: {e}"))
        }
    }
}

fn main() {
    let env = env_logger::Env::default().default_filter_or("info");
    env_logger::Builder::from_env(env).init();

    if let Err(e) = main_inner() {
        error!("{e}");
        std::process::exit(1);
    }
}
