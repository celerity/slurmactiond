mod restapi;
mod slurm;
mod config;

use config::Config;

fn main() -> std::io::Result<()> {
    let cfg = toml::from_slice(&std::fs::read("config.example.toml")?)?;
    restapi::main(cfg)
}
