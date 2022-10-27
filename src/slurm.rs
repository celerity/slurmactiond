use std::ffi::OsString;
use std::process::{ExitStatus, Stdio};
use std::sync::Arc;

use anyhow::Context as _;
use log::{debug, error, info, warn};
use nix::unistd::getuid;
use tokio::process::{Child, Command};

use crate::config::{Config, ConfigFile};
use crate::ipc;
use crate::ipc::RunnerMetadata;
use crate::scheduler::{self, Scheduler, TargetId};
use crate::util::{self, ChildStream, ChildStreamMux, ResultSuccessExt as _};

util::literal_types! {
    #[derive(Copy)]
    pub struct JobId(pub u64);
}

const JOB_ID_VAR: &str = "SLURM_JOB_ID";

pub fn current_job() -> anyhow::Result<JobId> {
    let id_string = std::env::vars()
        .find(|(k, _)| k == JOB_ID_VAR)
        .ok_or_else(|| anyhow::anyhow!("Environment variable {JOB_ID_VAR} not set"))?
        .1;
    let id_int = id_string
        .parse()
        .with_context(|| format!("Cannot parse {JOB_ID_VAR}"))?;
    Ok(JobId(id_int))
}

pub struct RunnerJobState {
    child: Child,
    output: ChildStreamMux,
}

impl RunnerJobState {
    async fn next_message(&mut self) -> anyhow::Result<Option<ipc::Envelope>> {
        while let Some((stream, line)) = self
            .output
            .next_line()
            .await
            .with_context(|| "Error reading output of srun")?
        {
            match stream {
                ChildStream::Stdout => {
                    return ipc::parse(&line)
                        .map(Some)
                        .with_context(|| "Error parsing IPC output from child process")
                }
                ChildStream::Stderr => warn!("{}", line),
            }
        }
        Ok(None)
    }

    async fn wait(&mut self) -> anyhow::Result<ExitStatus> {
        return (self.child.wait().await)
            .with_context(|| "Error waiting for srun execution to complete");
    }
}

pub struct RunnerJob {
    state: RunnerJobState,
}

#[must_use]
pub struct AttachedRunnerJob {
    state: RunnerJobState,
}

impl RunnerJob {
    pub fn spawn(config_file: &ConfigFile, target: &TargetId) -> anyhow::Result<RunnerJob> {
        let cfg = &config_file.config;

        let name = format!("{}-{}", &cfg.slurm.job_name, &target);
        let executable = std::env::current_exe()
            .with_context(|| "Cannot determine current executable name")?
            .as_os_str()
            .to_owned();

        let mut args: Vec<OsString> = Vec::new();
        args.extend(cfg.slurm.srun_options.iter().map(OsString::from));
        args.extend(cfg.targets[target].srun_options.iter().map(OsString::from));
        args.push("-n1".into());
        args.push("-J".into());
        args.push(name.as_str().into());
        args.push(executable);
        args.push("-c".into());
        args.push(config_file.path.as_os_str().to_owned());
        args.push("runner".into());
        args.push(target.0.as_str().into());

        if log::log_enabled!(log::Level::Debug) {
            let mut cl = vec![cfg.slurm.srun.to_string_lossy().to_owned()];
            for a in &args {
                cl.push(a.to_string_lossy().to_owned());
            }
            debug!("Starting {}", cl.join(" "));
        }

        let mut child = Command::new(&cfg.slurm.srun)
            .args(args)
            .envs(cfg.slurm.srun_env.iter())
            .envs(cfg.targets[target].srun_env.iter())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| "Failed to execute srun")?;

        let output = ChildStreamMux::new(child.stdout.take(), child.stderr.take());

        Ok(RunnerJob {
            state: RunnerJobState { child, output },
        })
    }

    pub async fn attach(mut self) -> anyhow::Result<(RunnerMetadata, AttachedRunnerJob)> {
        loop {
            match self.state.next_message().await? {
                Some(ipc::Envelope::Log(entry)) => entry.log(),
                Some(ipc::Envelope::Metadata(metadata)) => {
                    return Ok((metadata, AttachedRunnerJob { state: self.state }))
                }
                None => {
                    if let Err(e) = self.state.wait().await {
                        error!("{e:#}");
                    }
                    anyhow::bail!("Child process closed stdout before sending metadata")
                }
            }
        }
    }
}

impl AttachedRunnerJob {
    pub async fn wait(mut self) -> anyhow::Result<ExitStatus> {
        loop {
            match self.state.next_message().await? {
                Some(ipc::Envelope::Log(entry)) => entry.log(),
                Some(ipc::Envelope::Metadata(_)) => {
                    if let Err(e) = self.state.child.wait().await {
                        error!("Waiting on child process after it closed stdout early: {e:#}");
                    }
                    anyhow::bail!("Child process closed stdout before sending metadata")
                }
                None => return self.state.wait().await,
            }
        }
    }
}

pub async fn active_jobs(config: &Config) -> anyhow::Result<Vec<JobId>> {
    let uid = getuid().to_string();
    let output = Command::new(&config.slurm.squeue)
        .args(&[
            "-h",
            "-o",
            "%A",
            "-t",
            "CF,CG,PD,R,SO,ST,S,SE,SI,RS,RQ,RF",
            "-u",
            &uid,
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .await
        .and_successful()?;
    String::from_utf8(output.stdout)
        .with_context(|| "Decoding the output of squeue")?
        .lines()
        .map(|l| l.parse().with_context(|| "Parsing the output of squeue"))
        .collect()
}

pub fn job_has_terminated(job: JobId, active_jobs: &[JobId]) -> bool {
    // this test races with the enqueueing of new jobs, so we have to conservatively assume that
    // a job newer than all active jobs has not terminated yet.
    !active_jobs.contains(&job) && active_jobs.iter().any(|a| a.0 > job.0)
}

pub struct SlurmExecutor {
    config_file: ConfigFile,
}

impl SlurmExecutor {
    pub fn new(config_file: ConfigFile) -> Self {
        SlurmExecutor { config_file }
    }

    async fn supervise_runner_job(
        runner_id: scheduler::RunnerId,
        slurm_runner: RunnerJob,
        scheduler: &Arc<Scheduler>,
    ) -> anyhow::Result<()> {
        let (metadata, slurm_runner) = slurm_runner
            .attach()
            .await
            .with_context(|| "Attaching to SLURM job")?;

        scheduler.runner_connected(runner_id, metadata);

        let result = slurm_runner
            .wait()
            .await
            .with_context(|| "Waiting for SLURM job");

        scheduler.runner_disconnected(runner_id);

        info!("SLURM job exited with status {}", result?);

        Ok(())
    }
}

impl scheduler::Executor for SlurmExecutor {
    fn spawn_runner(&self, target: &TargetId, scheduler: &Arc<Scheduler>) -> anyhow::Result<()> {
        let runner_id = scheduler.create_runner(target.clone());

        let slurm_runner = RunnerJob::spawn(&self.config_file, &target)
            .with_context(|| "Submitting job to SLURM")?;

        let scheduler = scheduler.clone();
        actix_web::rt::spawn(async move {
            if let Err(e) = Self::supervise_runner_job(runner_id, slurm_runner, &scheduler).await {
                error!("{e:#}");
            }
        }); // TODO manage JoinHandle in RunnerInfo

        Ok(())
    }
}
