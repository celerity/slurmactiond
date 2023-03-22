use anyhow::Context;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;

use crate::github;
use crate::scheduler::{Label, TargetId};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SlurmConfig {
    #[serde(default = "SlurmConfig::default_srun")]
    pub srun: PathBuf,
    #[serde(default = "SlurmConfig::default_squeue")]
    pub squeue: PathBuf,
    #[serde(default)]
    pub srun_options: Vec<String>,
    #[serde(default)]
    pub srun_env: HashMap<String, String>,
    pub job_name: String,
}

impl SlurmConfig {
    fn default_srun() -> PathBuf {
        PathBuf::from("srun")
    }

    fn default_squeue() -> PathBuf {
        PathBuf::from("squeue")
    }
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetConfig {
    pub runner_labels: Vec<Label>,
    #[serde(default)]
    pub priority: i64,
    #[serde(default)]
    pub srun_options: Vec<String>,
    #[serde(default)]
    pub srun_env: HashMap<String, String>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GithubConfig {
    pub entity: github::Entity,
    pub api_token: github::ApiToken,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerRegistrationConfig {
    pub name: String,
    #[serde(default)]
    pub labels: Vec<Label>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerConfig {
    pub platform: String,
    pub work_dir: PathBuf,
    pub registration: RunnerRegistrationConfig,
    #[serde(default = "RunnerConfig::default_listen_timeout_s")]
    pub listen_timeout_s: u64,
}

impl RunnerConfig {
    fn default_listen_timeout_s() -> u64 {
        300
    }
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpConfig {
    pub bind: String,
    pub secret: String,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchedulerConfig {
    #[serde(default = "SchedulerConfig::default_history_len")]
    pub history_len: usize,
}

impl SchedulerConfig {
    fn default_history_len() -> usize {
        20
    }
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        SchedulerConfig {
            history_len: Self::default_history_len(),
        }
    }
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub http: HttpConfig,
    #[serde(default)]
    pub scheduler: SchedulerConfig,
    pub github: GithubConfig,
    pub slurm: SlurmConfig,
    #[serde(rename = "action_runner")]
    pub runner: RunnerConfig,
    #[serde(default)]
    pub targets: HashMap<TargetId, TargetConfig>, // TODO move into SchedulerConfig
}

impl Config {}

#[derive(Clone)]
pub struct ConfigFile {
    pub config: Config,
    pub path: PathBuf,
}

impl ConfigFile {
    pub fn read(path: PathBuf) -> anyhow::Result<ConfigFile> {
        let bytes = std::fs::read(&path)
            .with_context(|| format!("Error reading from {}", path.display()))?;
        let config = toml::from_slice(&bytes)
            .with_context(|| format!("Error parsing {} as TOML", path.display()))?;
        Ok(ConfigFile { config, path })
    }
}

#[test]
fn test_load_example_config() {
    let toml_file = include_bytes!("../slurmactiond.example.toml");
    toml::from_slice::<Config>(toml_file).unwrap();
}
