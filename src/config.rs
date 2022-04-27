use serde::Deserialize;

#[derive(Debug, Deserialize, Default)]
pub struct SlurmConfig {
    pub sbatch: Option<String>,
    #[serde(default)] pub sbatch_args: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct MapConfig {
    pub runner_labels: Vec<String>,
    pub sbatch_args: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct ActionRunnerConfig {
    pub installation_path: String,
    pub name_prefix: String,
    pub base_labels: Vec<String>,
    pub repository_url: String,
    pub registration_token: String,
}

#[derive(Debug, Deserialize)]
pub struct HttpConfig {
    pub listen: String,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub slurm: SlurmConfig,
    pub action_runner: ActionRunnerConfig,
    #[serde(default)] pub mappings: Vec<MapConfig>,
    pub http: HttpConfig,
}

#[test]
fn test_load_example_config() {
    let toml_file = include_bytes!("../config.example.toml");
    toml::from_slice::<Config>(toml_file).unwrap();
}