use std::fs;
use std::path::Path;

use log::error;

use config::Config;

mod config;
mod github;
mod slurm;
mod webhook;

fn main_inner() -> Result<(), String> {
    let cfg_path = Path::new("config.toml");
    let cfg = fs::read(cfg_path)
        .and_then(|bytes| Ok(toml::from_slice(&bytes)?))
        .map_err(|e| format!("Reading config file {p}: {e}", p = cfg_path.display()))?;
    webhook::main(cfg).map_err(|e| format!("Serving webhook over HTTP: {e}"))
}

fn main() {
    let env = env_logger::Env::default().default_filter_or("info");
    env_logger::Builder::from_env(env).init();

    if let Err(e) = main_inner() {
        error!("{e}");
        std::process::exit(1);
    }
}
