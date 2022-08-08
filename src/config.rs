use std::collections::HashMap;
use std::convert::Infallible;
use std::path::PathBuf;
use std::str::FromStr;

use serde::Deserialize;

use crate::github;

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct SlurmConfig {
    pub srun: Option<String>,
    pub squeue: Option<String>,
    #[serde(default)]
    pub srun_options: Vec<String>,
    #[serde(default)]
    pub srun_env: HashMap<String, String>,
    pub job_name: String,
}

#[derive(Debug, Deserialize, Hash, PartialEq, Eq, Clone)]
#[serde(transparent)]
pub struct TargetId(pub String);

impl FromStr for TargetId {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(TargetId(s.to_owned()))
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetConfig {
    pub runner_labels: Vec<String>,
    #[serde(default)]
    pub srun_options: Vec<String>,
    #[serde(default)]
    pub srun_env: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GithubConfig {
    pub entity: github::Entity,
    pub api_token: github::ApiToken,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerRegistrationConfig {
    pub name: String,
    #[serde(default)]
    pub labels: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerConfig {
    pub platform: String,
    pub work_dir: PathBuf,
    pub registration: RunnerRegistrationConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpConfig {
    pub bind: String,
    pub secret: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub http: HttpConfig,
    pub github: GithubConfig,
    pub slurm: SlurmConfig,
    #[serde(rename = "action_runner")]
    pub runner: RunnerConfig,
    #[serde(default)]
    pub targets: HashMap<TargetId, TargetConfig>,
}

#[test]
fn test_load_example_config() {
    let toml_file = include_bytes!("../slurmactiond.example.toml");
    toml::from_slice::<Config>(toml_file).unwrap();
}
