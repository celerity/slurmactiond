mod restapi;
mod slurm;
mod config;

use config::Config;

fn main() -> std::io::Result<()> {
    let cfg: Config = toml::from_slice(&std::fs::read("config.toml")?)?;
    restapi::main()
}
