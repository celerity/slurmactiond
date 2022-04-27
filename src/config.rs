use serde::Deserialize;

#[derive(Debug, Deserialize, Default)]
pub struct SlurmConfig {
    sbatch: Option<String>,
    #[serde(default)] sbatch_args: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct MapConfig {
    runner_labels: Vec<String>,
    sbatch_args: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct ActionRunnerConfig {
    installation_path: String,
    name_prefix: String,
    base_labels: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct GithubConfig {
    repository_url: String,
    runner_registration_token: String,
}

#[derive(Debug, Deserialize)]
pub struct HttpConfig {
    listen: String,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    slurm: SlurmConfig,
    action_runner: ActionRunnerConfig,
    #[serde(default)] mappings: Vec<MapConfig>,
    github: GithubConfig,
    http: HttpConfig,
}

#[test]
fn test_load_example_config() {
    let toml_file = include_bytes!("../config.example.toml");
    toml::from_slice::<Config>(toml_file).unwrap();
}