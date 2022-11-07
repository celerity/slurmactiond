use anyhow::{anyhow, Context as _};

use std::ffi::{OsStr, OsString};
use std::path::PathBuf;

fn getenv(env: &(impl PartialEq<OsStr> + ?Sized)) -> Option<OsString> {
    std::env::vars_os()
        .find(|(e, _)| *env == **e)
        .map(|(_, v)| v)
}

fn is_development_mode() -> bool {
    getenv("SLURMACTIOND_DEVELOP")
        .filter(|v| v.len() > 0)
        .is_some()
}

macro_rules! join {
    ($($components:expr),*) => {
        {
            let mut path = std::path::PathBuf::new();
            $(path.push($components);)*
            path
        }
    }
}

pub(crate) use join;

pub fn find_config_path(explicit_path: &Option<PathBuf>) -> anyhow::Result<PathBuf> {
    if let Some(explicit_path) = explicit_path {
        explicit_path
            .canonicalize()
            .with_context(|| format!("Cannot resolve config path `{}`", explicit_path.display()))
    } else {
        let mut config_search_paths = Vec::new();
        if is_development_mode() {
            config_search_paths.push(join!(std::env::current_dir()?, "slurmactiond.toml"));
        }
        if let Some(home) = getenv("HOME") {
            config_search_paths.push(join!(&home, ".config", "slurmactiond.toml"));
        }
        config_search_paths.push(PathBuf::from("/etc/slurmactiond.toml"));
        config_search_paths
            .iter()
            .filter_map(|path| path.canonicalize().ok())
            .next()
            .ok_or_else(|| {
                let paths_list = config_search_paths
                    .iter()
                    .map(|p| p.display().to_string())
                    .collect::<Vec<_>>()
                    .join(" or ");
                anyhow!(
                    "Cannot find configuration in at {paths_list}, {}",
                    "consider specifying -c / --config"
                )
            })
    }
}

pub fn find_resources_path() -> anyhow::Result<PathBuf> {
    if is_development_mode() {
        let mut path = std::env::current_dir()?;
        path.push("res");
        Ok(path)
    } else {
        let mut path = std::env::current_exe() //
            .with_context(|| "Cannot determine current executable name")?;
        assert!(path.pop(), "executable path has a parent directory");
        let path = join!(path, "..", "share", "slurmactiond");
        path.canonicalize().with_context(|| {
            format!(
                "Error resolving slurmactiond resource path {}",
                path.display()
            )
        })
    }
}
