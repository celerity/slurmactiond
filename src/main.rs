extern crate core;

use anyhow::Context as _;
use std::io::{stderr, stdout};
use std::os::unix::io::AsRawFd as _;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use log::error;
use nix::unistd::isatty;

use config::Config;

use crate::config::TargetId;

mod config;
mod file_io;
mod github;
mod ipc;
mod runner;
mod scheduler;
mod slurm;
mod util;
mod webhook;

#[derive(Subcommand, Debug)]
enum Command {
    Server,
    Runner { target: TargetId },
}

fn configure_logger(cmd: &Command) {
    use env_logger::{Builder, Env, Target::Stdout};
    use log::LevelFilter;

    let env = Env::default().default_filter_or("info");
    let mut builder = Builder::from_env(env);

    match cmd {
        Command::Runner { .. } => {
            let formatter = ipc::LogFormatter::new();
            builder.format(move |f, r| formatter.format(f, r));
            builder.target(Stdout);
        }
        Command::Server => {
            builder.format_target(log::max_level() >= log::Level::Debug);
            if !isatty(stdout().as_raw_fd()).unwrap_or(false)
                && !isatty(stderr().as_raw_fd()).unwrap_or(false)
            {
                // probably running as a service to systemd, which does its own timestamps
                builder.format_timestamp(None);
            }
        }
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

fn main_inner() -> anyhow::Result<()> {
    let args = Arguments::parse();

    let command = args.command.unwrap_or(Command::Server);
    configure_logger(&command);

    let config_file = (args.config_file.canonicalize()).with_context(|| {
        format!(
            "Cannot resolve config path `{}`",
            args.config_file.display()
        )
    })?;

    match command {
        Command::Server => {
            webhook::main(&config_file).with_context(|| "Error while serving webhook over HTTP")
        }
        Command::Runner { target } => {
            let job_id = slurm::current_job()
                .with_context(|| "Failed to determine the SLURM job id of this process")?;
            runner::run(&config_file, target, job_id)
        }
    }
}

fn main() {
    if let Err(e) = main_inner() {
        error!("{e:#}");
        std::process::exit(1);
    }
}
