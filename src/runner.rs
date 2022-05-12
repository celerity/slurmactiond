use std::ffi::OsStr;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use derive_more::{Display, From};
use log::error;
use std::process::Command;

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
                .open(&base_dir.join(LOCK_FILE_NAME))
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
        if let Err(e) = fs::remove_file(self.0.join(LOCK_FILE_NAME)) {
            let path = self.0.display();
            error!("Unable to unlock working directory {path}: {e}");
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
        Ok(mut file) => github::download(url, &mut file).await,
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists && tarball_path.is_file() => Ok(()),
        Err(e) => Err(DownloadError::Io(e)),
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
        }
        fs::remove_file(tarball_path)?;
        // continue
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

    let registration_token =
        github::generate_runner_registration_token(&cfg.github.entity, &cfg.github.api_token)
            .await?;
    let work_dir = WorkDir::lock(&cfg.runner.work_dir.join(&target.0))?;
    let tarball = github::locate_runner_tarball(&cfg.runner.platform).await?;
    let tarball_path = work_dir.0.join(tarball.name);
    update_runner_tarball(&tarball.url, &tarball_path, tarball.size).await?;

    if !work_dir.0.join("config.sh").is_file() {
        Command::new("tar")
            .current_dir(&work_dir.0)
            .args(&[OsStr::new("xfz"), tarball_path.as_os_str()])
            .output()?
            .successful()?;
    }

    Command::new("./config.sh")
        .current_dir(&work_dir.0)
        .args(&[
            "--unattended",
            "--url",
            &format!("https://github.com/{}", cfg.github.entity),
            "--token",
            &registration_token.0,
            "--name",
            &format!("{}-{}-{}", cfg.runner.registration.name, target.0, runner_seq),
            "--labels",
            &all_labels.join(","),
            "--ephemeral",
        ])
        .output()?
        .successful()?;

    Command::new("./run.sh")
        .current_dir(&work_dir.0)
        .output()?
        .successful()?;

    Ok(())
}
