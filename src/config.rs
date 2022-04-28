use std::collections::HashMap;

use serde::Deserialize;

#[derive(Debug, Deserialize, Default)]
pub struct SlurmConfig {
    pub sbatch: Option<String>,
    #[serde(default)]
    pub sbatch_options: Vec<String>,
    pub job_name: String,
}

#[derive(Debug, Deserialize, Hash, PartialEq, Eq)]
#[serde(transparent)]
pub struct PartitionId(pub String);

#[derive(Debug, Deserialize)]
pub struct PartitionConfig {
    pub runner_labels: Vec<String>,
    #[serde(default)]
    pub sbatch_options: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct RunnerRegistrationConfig {
    pub url: String,
    pub token: String,
    pub name: String,
    #[serde(default)]
    pub labels: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct RunnerConfig {
    pub tarball: String,
    pub work_dir: String,
    pub registration: RunnerRegistrationConfig,
}

#[derive(Debug, Deserialize)]
pub struct HttpConfig {
    pub bind: String,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub http: HttpConfig,
    pub slurm: SlurmConfig,
    #[serde(rename = "action_runner")]
    pub runner: RunnerConfig,
    pub partitions: HashMap<PartitionId, PartitionConfig>,
}

#[test]
fn test_load_example_config() {
    let toml_file = include_bytes!("../config.example.toml");
    toml::from_slice::<Config>(toml_file).unwrap();
}
