use std::fs;
use std::path::{Path, PathBuf};

use atty::Stream;
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

    match cmd {
        Command::Runner { .. } => {
            builder.format(json_log::format);
            builder.target(Stdout);
        },
        Command::Server => {
            builder.format_target(log::max_level() >= log::Level::Debug);
            if !atty::is(Stream::Stdout) && !atty::is(Stream::Stderr) {
                // probably running as a service to systemd, which does its own timestamps
                builder.format_timestamp(None);
            }
        },
    }

    builder.filter_module("h2", LevelFilter::Info);
    builder.init();
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
#[clap(propagate_version = true)]
struct Arguments {
    #[clap(subcommand)]
    command: Option<Command>,
    #[clap(short, long, value_parser, default_value = "slurmactiond.toml")]
    config_file: PathBuf,
}

fn main_inner(command: Command, config_file: &Path) -> Result<(), String> {
    let cfg = fs::read(config_file)
        .and_then(|bytes| Ok(toml::from_slice(&bytes)?))
        .map_err(|e| format!("Reading config file {p}: {e}", p = config_file.display()))?;

    match command {
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
    let Arguments { command, config_file } = Arguments::parse();
    let command = command.unwrap_or(Command::Server);

    configure_logger(&command);

    if let Err(e) = main_inner(command, &config_file) {
        error!("{e}");
        std::process::exit(1);
    }
}
