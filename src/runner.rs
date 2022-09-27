use std::ffi::OsStr;
use std::fs;
use std::io;
use std::path::Path;
use std::process::ExitStatus;
use std::time::Duration;

use anyhow::Context as _;
use log::{debug, info, warn};
use nix::sys::signal::{kill, Signal};
use nix::unistd::{gethostname, Pid};
use tokio::process::Command;

use crate::config::{Config, RunnerConfig, TargetId};
use crate::file_io::WorkDir;
use crate::github::RunnerRegistrationToken;
use crate::util::{
    async_retry_after, log_child_output, ChildStream, CommandOutputStreamExt as _,
    ResultSuccessExt as _,
};
use crate::{github, ipc, slurm};

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
            debug!("Tarball exists locally at `{}`", tarball_path.display());
            Ok(())
        }
        Err(e) => Err(e).with_context(|| format!("Unable to download `{url}`")),
    }
}

async fn update_runner_tarball(
    url: &str,
    tarball_path: &Path,
    expected_size: u64,
) -> anyhow::Result<()> {
    find_or_download_runner_tarball(url, tarball_path)
        .await
        .with_context(|| format!("Failed to update runner tarball from `{url}`"))?;
    if tarball_path.metadata()?.len() == expected_size {
        Ok(())
    } else {
        warn!(
            "`{}` does not have the expected size",
            tarball_path.display()
        );
        fs::remove_file(tarball_path).with_context(|| {
            format!(
                "Error removing outdated or corrupted tarball `{}`",
                tarball_path.display()
            )
        })
    }
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
        .await
        // config.sh will print an error message if no runner was registered but will exit with 0
        .and_successful()
}

async fn wait_child(child: &mut tokio::process::Child) -> anyhow::Result<ExitStatus> {
    child
        .wait()
        .await
        .with_context(|| "Error waiting for subprocess to exit")
}

async fn interrupt_child(child: &mut tokio::process::Child) -> anyhow::Result<()> {
    let pid = (child.id())
        .map(|p| Pid::from_raw(p as _))
        .expect("child process already consumed");
    kill(pid, Signal::SIGINT).with_context(|| "Unexpected error when trying to kill subprocess")?;
    wait_child(child).await?;
    Ok(())
}

async fn run_instance(work_path: &Path, runner_cfg: &RunnerConfig) -> anyhow::Result<()> {
    let mut run = Command::new("./run.sh")
        .current_dir(work_path)
        .output_stream()?;

    let listen_timeout = Duration::from_secs(runner_cfg.listen_timeout_s);

    loop {
        // repeat as long as we get output lines, then block within one of the match arms
        match tokio::time::timeout(listen_timeout, run.output_stream.next_line()).await {
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
                anyhow::bail!("Runner exited without picking up a job");
            }
            Ok(Err(io_err)) => {
                interrupt_child(&mut run.child).await?;
                return Err(From::from(io_err));
            }
            Err(elapsed) => {
                return Err(elapsed).with_context(|| "Runner timed out waiting for jobs");
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
        .with_context(|| format!("Unpacking archive `{}` with tar", tarball.display()))
}

#[actix_web::main]
pub async fn run(config_file: &Path, target: TargetId, job: slurm::JobId) -> anyhow::Result<()> {
    const API_ATTEMPTS: u32 = 3;
    const API_COOLDOWN: Duration = Duration::from_secs(30);

    let cfg = Config::read_from_toml_file(config_file)
        .with_context(|| format!("Reading configuration from `{}`", config_file.display()))?;
    let runner_name = format!("{}-{}-{}", cfg.runner.registration.name, target.0, job);

    let metadata = ipc::RunnerMetadata {
        slurm_job: job,
        runner_name: runner_name.clone(),
    };
    ipc::send(metadata).with_context(|| "Error serializing metadata to stdout")?;

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
        let host_name = gethostname()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|_| "(unknown hostname)".to_string());
        info!("Running on {host_name} in `{}`", work_dir.path.display());
    }

    if work_dir.path.join("config.sh").is_file() {
        debug!("Re-using existing Github Actions Runner installation");
    } else {
        info!("Installing Github Actions Runner");
        let tarball = async_retry_after(API_COOLDOWN, API_ATTEMPTS, || {
            github::locate_runner_tarball(&cfg.runner.platform, &cfg.github.api_token)
        })
        .await
        .with_context(|| "Error locating latest GitHub Actions Runner release")?;

        debug!(
            "Downloading latest release for {} from {}",
            cfg.runner.platform, tarball.name
        );
        let tarball_path = work_dir.path.join(tarball.name);
        async_retry_after(API_COOLDOWN, API_ATTEMPTS, || {
            update_runner_tarball(&tarball.url, &tarball_path, tarball.size)
        })
        .await
        .with_context(|| "Error downloading latest GitHub Actions Runner release")?;

        debug!("Unpacking runner tarball `{}`", tarball_path.display());
        unpack_tar_gz(&tarball_path, &work_dir.path)
            .await
            .with_context(|| "Error installing GitHub Actions Runner")?;
    }

    info!(
        "Generating runner registration token for {}",
        cfg.github.entity
    );
    let registration_token = async_retry_after(API_COOLDOWN, API_ATTEMPTS, || {
        github::generate_runner_registration_token(&cfg.github.entity, &cfg.github.api_token)
    })
    .await
    .with_context(|| "Error generating GitHub Runner Registration Token")?;

    // There might be a left-over runner registration from a task that didn't exit successfully,
    // try unregistering it. This can fail if GitHub still views the previously occupying runner
    // as being connected or active in a job.
    info!("Unregistering previous runner if present");
    const UNREGISTER_COOLDOWN: Duration = Duration::from_secs(60);
    const UNREGISTER_ATTEMPTS: u32 = 10;
    async_retry_after(UNREGISTER_COOLDOWN, UNREGISTER_ATTEMPTS, || {
        unregister_instance(&work_dir.path, &registration_token)
    })
    .await
    .with_context(|| "Error unregistering previous runner")?;

    info!("Registering runner {runner_name}");
    const REGISTER_COOLDOWN: Duration = Duration::from_secs(30);
    const REGISTER_ATTEMPTS: u32 = 3;
    async_retry_after(REGISTER_COOLDOWN, REGISTER_ATTEMPTS, || {
        register_instance(
            &work_dir.path,
            &cfg.github.entity,
            &registration_token,
            &runner_name,
            &all_labels,
        )
    })
    .await
    .with_context(|| "Error configuring new Actions Runner")?;

    info!("Starting runner {runner_name}");
    // We can't re-try `./run.sh`, because the runner will perpetually appear connected to GitHub.
    // TODO re-register and, if necessary, re-install the runner if this happens. We would ideally
    //  detect this condition on the headnode and re-spawn the SLURM job altogether if necessary,
    //  because we will also run into the job pick-up timeout if a foreign runner steals "our" job.
    let run_result = run_instance(&work_dir.path, &cfg.runner)
        .await
        .with_context(|| "Error while executing Actions Runner");

    match run_result {
        Ok(_) => info!("Runner has exited normally"),
        Err(_) => {
            // best-effort cleanup to bring GitHub's world view into sync, if this fails, we will
            // re-try before registering another runner from this directory
            let cleanup_result = unregister_instance(&work_dir.path, &registration_token)
                .await
                .with_context(|| "Error unregistering failed runner");
            if let Err(cleanup_err) = cleanup_result {
                warn!("{cleanup_err:#}");
            }
        }
    }
    run_result
}
