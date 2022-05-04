use std::collections::HashMap;

use serde::Deserialize;

use crate::github;

#[derive(Debug, Deserialize, Default)]
pub struct SlurmConfig {
    pub sbatch: Option<String>,
    #[serde(default)]
    pub sbatch_options: Vec<String>,
    pub job_name: String,
}

#[derive(Debug, Deserialize, Hash, PartialEq, Eq)]
#[serde(transparent)]
pub struct TargetId(pub String);

#[derive(Debug, Deserialize)]
pub struct TargetConfig {
    pub runner_labels: Vec<String>,
    #[serde(default)]
    pub sbatch_options: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct GithubConfig {
    pub entity: github::Entity,
    pub api_token: github::ApiToken,
}

#[derive(Debug, Deserialize)]
pub struct RunnerRegistrationConfig {
    pub name: String,
    #[serde(default)]
    pub labels: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct RunnerConfig {
    pub platform: String,
    pub work_dir: String,
    pub registration: RunnerRegistrationConfig,
}

#[derive(Debug, Deserialize)]
pub struct HttpConfig {
    pub bind: String,
    pub secret: String,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub http: HttpConfig,
    pub github: GithubConfig,
    pub slurm: SlurmConfig,
    #[serde(rename = "action_runner")]
    pub runner: RunnerConfig,
    #[serde(default)] pub targets: HashMap<TargetId, TargetConfig>,
}

#[test]
fn test_load_example_config() {
    let toml_file = include_bytes!("../config.example.toml");
    toml::from_slice::<Config>(toml_file).unwrap();
}
