use std::ffi::OsStr;
use std::fs;
use std::io;
use std::path::Path;
use std::process::ExitStatus;
use std::time::Duration;

use anyhow::Context as _;
use log::{debug, error, info, warn};
use tokio::process::Command;

use crate::config::TargetId;
use crate::file_io::WorkDir;
use crate::github::RunnerRegistrationToken;
use crate::util::{
    log_child_output, ChildStream, CommandOutputStreamExt as _, ResultSuccessExt as _,
};
use crate::{github, slurm, Config};

async fn find_or_download_runner_tarball(url: &str, tarball_path: &Path) -> anyhow::Result<()> {
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
        Err(e) => Err(e).with_context(|| format!("Unable to download {url}")),
    }
}

async fn update_runner_tarball(
    url: &str,
    tarball_path: &Path,
    expected_size: u64,
) -> anyhow::Result<()> {
    const RETRIES: u32 = 3;
    for _ in 0..RETRIES {
        find_or_download_runner_tarball(url, tarball_path)
            .await
            .with_context(|| format!("Failed to update runner tarball from {url}"))?;
        if tarball_path.metadata()?.len() == expected_size {
            return Ok(());
        } else {
            warn!("{} does not have the expected size", tarball_path.display());
            fs::remove_file(tarball_path).with_context(|| {
                format!(
                    "Error removing outdated or corrupted tarball {}",
                    tarball_path.display()
                )
            })?;
            // continue
        }
    }
    anyhow::bail!("Too many retries")
}

async fn register_instance(
    work_path: &Path,
    entity: &github::Entity,
    token: &RunnerRegistrationToken,
    name: &str,
    labels: &[&str],
) -> anyhow::Result<()> {
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
        .and_successful()
}

async fn unregister_instance(
    work_path: &Path,
    token: &RunnerRegistrationToken,
) -> anyhow::Result<()> {
    Command::new("./config.sh")
        .current_dir(work_path)
        .args(&["remove", "--token", &token.0])
        .output_stream()?
        .log_output("config.sh")
        .await?;
    // since this is a cleanup task, we don't require a zero exit code
    Ok(())
}

async fn wait_child(child: &mut tokio::process::Child) -> anyhow::Result<ExitStatus> {
    child
        .wait()
        .await
        .with_context(|| "Error waiting for subprocess to exit")
}

async fn kill_and_wait_child(child: &mut tokio::process::Child) -> anyhow::Result<()> {
    child
        .kill()
        .await
        .with_context(|| "Unexpected error when trying to kill subprocess")?;
    wait_child(child).await?;
    Ok(())
}

async fn run_instance(work_path: &Path) -> anyhow::Result<()> {
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
                        return run
                            .log_output("run.sh")
                            .await
                            .and_successful()
                            .with_context(|| "Runner finished with non-zero exit code");
                    }
                }
                Ok(Ok(None)) => {
                    wait_child(&mut run.child).await?;
                    anyhow::bail!("runner exited without picking up a job");
                }
                Ok(Err(io_err)) => {
                    kill_and_wait_child(&mut run.child).await?;
                    return Err(From::from(io_err));
                }
                Err(elapsed) => {
                    error!("runner timed out waiting for jobs");
                    kill_and_wait_child(&mut run.child).await?;
                    if retry < NUM_RETRIES - 1 {
                        retry += 1;
                        info!("Retrying ({retry}/{NUM_RETRIES})...");
                    } else {
                        return Err(elapsed)
                            .with_context(|| "Too many retries launching GitHub Actions Runner");
                    }
                }
            }
        }
    }
}

async fn unpack_tar_gz(tarball: &Path, into_dir: &Path) -> anyhow::Result<()> {
    Command::new("tar")
        .current_dir(into_dir)
        .args(&[OsStr::new("xfz"), tarball.as_os_str()])
        .output_stream()?
        .log_output("tar")
        .await
        .and_successful()
        .with_context(|| format!("Unpacking archive {} with tar", tarball.display()))
}

#[actix_web::main]
pub async fn run(cfg: Config, target: TargetId, job: slurm::JobId) -> anyhow::Result<()> {
    let base_labels = cfg.runner.registration.labels.iter().map(AsRef::as_ref);
    let target_labels = cfg.targets[&target].runner_labels.iter().map(AsRef::as_ref);
    let all_labels: Vec<&str> = base_labels.chain(target_labels).collect();
    assert!(!all_labels.iter().any(|l| l.contains(',')));

    let active_jobs = slurm::active_jobs(&cfg)
        .await
        .with_context(|| "Error listing active slurm jobs")?;

    let work_dir = WorkDir::lock(&cfg.runner.work_dir.join(&target.0), job, &active_jobs)
        .with_context(|| "Cannot lock private working directory")?;

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
        let tarball = github::locate_runner_tarball(&cfg.runner.platform, &cfg.github.api_token)
            .await
            .with_context(|| "Error locating latest GitHub Actions Runner release")?;
        debug!(
            "Downloading latest release for {} from {}",
            cfg.runner.platform, tarball.name
        );
        let tarball_path = work_dir.path.join(tarball.name);
        update_runner_tarball(&tarball.url, &tarball_path, tarball.size)
            .await
            .with_context(|| "Error downloading latest GitHub Actions Runner release")?;

        debug!("Unpacking runner tarball {}", tarball_path.display());
        unpack_tar_gz(&tarball_path, &work_dir.path)
            .await
            .with_context(|| "Error installing GitHub Actions Runner")?;
    }

    info!(
        "Generating runner registration token for {}",
        cfg.github.entity
    );
    let registration_token =
        github::generate_runner_registration_token(&cfg.github.entity, &cfg.github.api_token)
            .await
            .with_context(|| "Error generating GitHub Runner Registration Token")?;

    // there might be a left-over runner registration from a task that didn't exit successfully,
    // try unregistering it
    unregister_instance(&work_dir.path, &registration_token)
        .await
        .with_context(|| "Error unregistering previous runner")?;

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
    .with_context(|| "Configuring new Actions Runner")?;

    info!("Starting runner {runner_name}");
    run_instance(&work_dir.path)
        .await
        .with_context(|| "Error while executing Actions Runner")?;

    info!("Runner has exited normally");
    Ok(())
}
