use config::Config;

mod github;
mod restapi;
mod slurm;
mod config;

fn main() -> std::io::Result<()> {
    env_logger::init();
    let cfg = toml::from_slice(&std::fs::read("config.toml")?)?;
    restapi::main(cfg)
}
