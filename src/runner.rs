use std::error::Error;
use std::ffi::OsStr;
use std::fs;
use std::io;
use std::path::Path;
use std::process::ExitStatus;
use std::time::Duration;

use derive_more::{Display, From};
use log::{debug, error, info, warn};
use tokio::process::Command;

use crate::config::TargetId;
use crate::file_io::{FileIoError, WorkDir};
use crate::github::RunnerRegistrationToken;
use crate::util::{
    log_child_output, ChildStream, CommandOutputStreamExt, ExitError, ExitStatusExt,
};
use crate::{github, slurm, Config};

async fn find_or_download_runner_tarball(
    url: &str,
    tarball_path: &Path,
) -> Result<(), github::DownloadError> {
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
            Err(github::DownloadError::Io(e))
        }
    }
}

async fn update_runner_tarball(
    url: &str,
    tarball_path: &Path,
    expected_size: u64,
) -> Result<(), github::DownloadError> {
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
    Err(github::DownloadError::TooManyRetries)
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
    Slurm(slurm::Error),
    #[display(fmt = "{}", _0)]
    FileIo(FileIoError),
}

impl Error for RunnerError {}

async fn register_instance(
    work_path: &Path,
    entity: &github::Entity,
    token: &RunnerRegistrationToken,
    name: &str,
    labels: &[&str],
) -> io::Result<ExitStatus> {
    Command::new("./config.sh")
        .current_dir(work_path)
        .args(&[
            "--unattended",
            "--url",
            &format!("https://github.com/{entity}"),
            "--token",
            &token.0,
            "--name",
            &name,
            "--labels",
            &labels.join(","),
            "--ephemeral",
        ])
        .output_stream()?
        .log_output("config.sh")
        .await
}

async fn unregister_instance(
    work_path: &Path,
    token: &RunnerRegistrationToken,
) -> io::Result<ExitStatus> {
    Command::new("./config.sh")
        .current_dir(work_path)
        .args(&["remove", "--token", &token.0])
        .output_stream()?
        .log_output("config.sh")
        .await
}

async fn run_instance(work_path: &Path) -> io::Result<ExitStatus> {
    const LISTEN_TIMEOUT: Duration = Duration::from_secs(30);
    const NUM_RETRIES: u32 = 4;

    let mut retry: u32 = 0;
    loop {
        let mut run = Command::new("./run.sh")
            .current_dir(work_path)
            .output_stream()?;
        loop {
            match tokio::time::timeout(LISTEN_TIMEOUT, run.output_stream.next_line()).await {
                Ok(Ok(Some((stream, line)))) => {
                    log_child_output(stream, "run.sh", &line);
                    if stream == ChildStream::Stdout && line.contains(": Running job:") {
                        return run.log_output("run.sh").await;
                    }
                }
                Ok(Ok(None)) => {
                    error!("runner exited without picking up a job");
                    return run.child.wait().await;
                }
                Ok(Err(io_err)) => {
                    run.child.kill().await?;
                    run.child.wait().await?;
                    return Err(io_err);
                }
                Err(_elapsed)  => {
                    error!("runner timed out waiting for jobs");
                    run.child.kill().await?;
                    let status = run.child.wait().await?;
                    if retry < NUM_RETRIES - 1 {
                        retry += 1;
                        info!("Retrying ({retry}/{NUM_RETRIES})...");
                    } else {
                        // TODO proper error reporting, maybe crate anyhow
                        return Ok(status);
                    }
                }
            }
        }
    }
}

async fn unpack_tar_gz(tarball: &Path, into_dir: &Path) -> io::Result<ExitStatus> {
    Command::new("tar")
        .current_dir(into_dir)
        .args(&[OsStr::new("xfz"), tarball.as_os_str()])
        .output_stream()?
        .log_output("tar")
        .await
}

#[actix_web::main]
pub async fn run(cfg: Config, target: TargetId, job: slurm::JobId) -> Result<(), RunnerError> {
    let base_labels = cfg.runner.registration.labels.iter().map(AsRef::as_ref);
    let target_labels = cfg.targets[&target].runner_labels.iter().map(AsRef::as_ref);
    let all_labels: Vec<&str> = base_labels.chain(target_labels).collect();
    assert!(!all_labels.iter().any(|l| l.contains(',')));

    let active_jobs = slurm::active_jobs(&cfg).await?;
    let work_dir = WorkDir::lock(&cfg.runner.work_dir.join(&target.0), job, &active_jobs)?;

    if log::max_level() >= log::Level::Info {
        let host_name = hostname::get()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|_| "(unknown hostname)".to_string());
        let dir_name = work_dir.path.display();
        info!("Running on {host_name} in {dir_name}");
    }

    if work_dir.path.join("config.sh").is_file() {
        debug!("Re-using existing Github Actions Runner installation");
    } else {
        info!("Installing Github Actions Runner");
        let tarball =
            github::locate_runner_tarball(&cfg.runner.platform, &cfg.github.api_token).await?;
        debug!(
            "Downloading latest release for {} from {}",
            cfg.runner.platform, tarball.name
        );
        let tarball_path = work_dir.path.join(tarball.name);
        update_runner_tarball(&tarball.url, &tarball_path, tarball.size).await?;

        debug!("Unpacking runner tarball {}", tarball_path.display());
        unpack_tar_gz(&tarball_path, &work_dir.path)
            .await
            .map_err(|e| FileIoError::new("Unpacking tarball", "tar", e))?
            .successful()?;
    }

    info!(
        "Generating runner registration token for {}",
        cfg.github.entity
    );
    let registration_token =
        github::generate_runner_registration_token(&cfg.github.entity, &cfg.github.api_token)
            .await?;

    // there might be a left-over runner registration from a task that didn't exit successfully,
    // try unregistering it
    unregister_instance(&work_dir.path, &registration_token)
        .await
        .map_err(|e| FileIoError::new("unregistering previous runner", "config.sh", e))
        // since this is a cleanup task, we don't require a zero exit code
        ?;

    let runner_name = format!("{}-{}-{}", cfg.runner.registration.name, target.0, job);
    info!("Registering runner {runner_name}");
    register_instance(
        &work_dir.path,
        &cfg.github.entity,
        &registration_token,
        &runner_name,
        &all_labels,
    )
    .await
    .map_err(|e| FileIoError::new("registering runner", "config.sh", e))?
    .successful()?;

    info!("Starting runner {runner_name}");
    run_instance(&work_dir.path)
        .await
        .map_err(|e| FileIoError::new("starting runner", "run.sh", e))?
        .successful()?;

    info!("Runner has exited normally");
    Ok(())
}
