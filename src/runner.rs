use std::ffi::OsStr;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;

use derive_more::{Display, From};
use log::{debug, error, info, warn};

use crate::{Config, github};
use crate::config::TargetId;
use crate::github::DownloadError;
use crate::util::{ExitError, OutputExt};

const LOCK_FILE_NAME: &str = ".lock";

struct WorkDir(PathBuf);

impl WorkDir {
    fn lock(base_dir: &Path) -> io::Result<WorkDir> {
        for concurrent_id in 0u32.. {
            let work_dir = base_dir.join(PathBuf::from(concurrent_id.to_string()));
            fs::create_dir_all(&work_dir)?;
            match fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&work_dir.join(LOCK_FILE_NAME))
            {
                Ok(_) => return Ok(WorkDir(work_dir)),
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => (),
                Err(e) => return Err(e),
            }
        }
        unreachable!(); // not technically, but would require 2^32 existing locked work dirs
    }
}

impl Drop for WorkDir {
    fn drop(&mut self) {
        let lock_file = self.0.join(LOCK_FILE_NAME);
        if let Err(e) = fs::remove_file(&lock_file) {
            error!(
                "Unable to unlock working directory: {}: {e}",
                lock_file.display()
            );
        }
    }
}

async fn find_or_download_runner_tarball(
    url: &str,
    tarball_path: &Path,
) -> Result<(), DownloadError> {
    match fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tarball_path)
    {
        Ok(mut file) => {
            debug!("Downloading tarball from {url}");
            github::download(url, &mut file).await
        }
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists && tarball_path.is_file() => {
            debug!("Tarball exists locally at {}", tarball_path.display());
            Ok(())
        }
        Err(e) => {
            error!("Unable to download {url}: {e}");
            Err(DownloadError::Io(e))
        }
    }
}

async fn update_runner_tarball(
    url: &str,
    tarball_path: &Path,
    expected_size: u64,
) -> Result<(), DownloadError> {
    const RETRIES: u32 = 3;
    for _ in 0..RETRIES {
        find_or_download_runner_tarball(url, tarball_path).await?;
        if tarball_path.metadata()?.len() == expected_size {
            return Ok(());
        } else {
            warn!("{} does not have the expected size", tarball_path.display());
            fs::remove_file(tarball_path)?;
            // continue
        }
    }
    Err(DownloadError::TooManyRetries)
}

#[derive(Debug, Display, From)]
pub enum RunnerError {
    #[display(fmt = "GitHub: {}", _0)]
    GitHub(github::ApiError),
    #[display(fmt = "Runner download: {}", _0)]
    Download(github::DownloadError),
    #[display(fmt = "{}", _0)]
    ChildProcess(ExitError),
    #[display(fmt = "{}", _0)]
    Io(io::Error),
}

impl std::error::Error for RunnerError {}

#[actix_web::main]
pub async fn run(cfg: Config, target: TargetId, runner_seq: u64) -> Result<(), RunnerError> {
    let base_labels = cfg.runner.registration.labels.iter().map(AsRef::as_ref);
    let target_labels = cfg.targets[&target].runner_labels.iter().map(AsRef::as_ref);
    let all_labels: Vec<&str> = base_labels.chain(target_labels).collect();
    assert!(!all_labels.iter().any(|l| l.contains(',')));

    let work_dir = WorkDir::lock(&cfg.runner.work_dir.join(&target.0))?;
    assert!(work_dir.0.join(LOCK_FILE_NAME).is_file());
    info!("Using working directory {}", work_dir.0.display());

    if work_dir.0.join("config.sh").is_file() {
        debug!("Re-using existing Github Actions Runner installation");
    } else {
        info!("Installing Github Actions Runner");
        let tarball = github::locate_runner_tarball(&cfg.runner.platform).await?;
        debug!(
            "Downloading latest release for {} from {}",
            cfg.runner.platform, tarball.name
        );
        let tarball_path = work_dir.0.join(tarball.name);
        update_runner_tarball(&tarball.url, &tarball_path, tarball.size).await?;

        debug!("Unpacking runner tarball {}", tarball_path.display());
        Command::new("tar")
            .current_dir(&work_dir.0)
            .args(&[OsStr::new("xfz"), tarball_path.as_os_str()])
            .output()?
            .successful()?;
    }

    info!(
        "Generating runner registration token for {}",
        cfg.github.entity
    );
    let registration_token =
        github::generate_runner_registration_token(&cfg.github.entity, &cfg.github.api_token)
            .await?;

    let runner_name = format!(
        "{}-{}-{}",
        cfg.runner.registration.name, target.0, runner_seq
    );
    info!("Registering runner {runner_name}");
    let config_output = Command::new("./config.sh")
        .current_dir(&work_dir.0)
        .args(&[
            "--unattended",
            "--url",
            &format!("https://github.com/{}", cfg.github.entity),
            "--token",
            &registration_token.0,
            "--name",
            &runner_name,
            "--labels",
            &all_labels.join(","),
            "--ephemeral",
        ])
        .output()?
        .successful()?;
    debug!(
        "./config.sh output:\n{}",
        String::from_utf8_lossy(&config_output.stdout)
    );

    info!("Starting runner {runner_name}");
    let run_output = Command::new("./run.sh")
        .current_dir(&work_dir.0)
        .output()?
        .successful()?;
    debug!("./run.sh output:\n{}", String::from_utf8_lossy(&run_output.stdout));

    info!("Runner has exited normally");
    Ok(())
}
