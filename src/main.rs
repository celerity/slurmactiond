use std::fs;
use std::path::Path;

use clap::{Parser, Subcommand};
use log::error;

use config::Config;

use crate::config::TargetId;

mod config;
mod github;
mod runner;
mod slurm;
mod util;
mod webhook;
mod json_log;

#[derive(Subcommand, Debug)]
enum Command {
    Server,
    Runner { target: TargetId },
}

fn configure_logger(cmd: &Command) {
    use log::LevelFilter;
    use env_logger::{Env, Builder, Target::Stdout};

    let env = Env::default().default_filter_or("info");
    let mut builder = Builder::from_env(env);
    builder.filter_module("h2", LevelFilter::Info);
    if let Command::Runner { .. } = cmd {
        builder.format(json_log::format);
        builder.target(Stdout);
    }
    builder.init();
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
#[clap(propagate_version = true)]
struct Arguments {
    #[clap(subcommand)]
    command: Option<Command>,
}

fn main_inner(cmd: Command) -> Result<(), String> {
    let cfg_path = Path::new("config.toml");
    let cfg = fs::read(cfg_path)
        .and_then(|bytes| Ok(toml::from_slice(&bytes)?))
        .map_err(|e| format!("Reading config file {p}: {e}", p = cfg_path.display()))?;

    match cmd {
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
    let cmd = Arguments::parse().command.unwrap_or(Command::Server);

    configure_logger(&cmd);

    if let Err(e) = main_inner(cmd) {
        error!("{e}");
        std::process::exit(1);
    }
}
