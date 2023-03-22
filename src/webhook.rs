use std::convert::Infallible;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;

use actix_web::dev::ServiceResponse;
use actix_web::error::ParseError;
use actix_web::http::header::{ContentType, Header, HeaderName, HeaderValue, TryIntoHeaderValue};
use actix_web::http::StatusCode;
use actix_web::{web, App, HttpMessage, HttpResponse, HttpServer, ResponseError};
use log::{debug, error};
use serde::Deserialize;
use tera::Tera;

use crate::config::{ConfigFile, GithubConfig, HttpConfig};
use crate::github::{WorkflowJob, WorkflowJobConclusion, WorkflowStatus};
use crate::scheduler::{self, Scheduler, SchedulerStateSnapshot, WorkflowJobTermination};
use crate::slurm::SlurmExecutor;
use crate::{github, paths};

type StaticContent = (&'static str, StatusCode);
type StaticResult = actix_web::Result<StaticContent>;

const NO_CONTENT: StaticContent = ("", StatusCode::NO_CONTENT);

#[derive(Debug)]
struct BadRequest(String);

impl Display for BadRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Bad Request: {}", self.0)
    }
}

impl ResponseError for BadRequest {
    fn status_code(&self) -> StatusCode {
        StatusCode::BAD_REQUEST
    }
}

#[derive(Debug)]
struct InternalServerError;

impl Display for InternalServerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Something went wrong on our end. Please consult the slurmactiond logs."
        )
    }
}

impl ResponseError for InternalServerError {
    fn status_code(&self) -> StatusCode {
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

fn internal_server_error(e: impl Display) -> InternalServerError {
    error!("Internal Server Error: {e:#}");
    InternalServerError
}

#[derive(Deserialize, Debug, PartialEq, Eq)]
struct WorkflowJobPayload {
    action: WorkflowStatus,
    workflow_job: WorkflowJob,
}

async fn workflow_job_event(
    scheduler: &Arc<Scheduler>,
    payload: &WorkflowJobPayload,
) -> StaticResult {
    let WorkflowJobPayload {
        action,
        workflow_job:
            WorkflowJob {
                run_id,
                job_id,
                url: workflow_url,
                name: workflow_name,
                labels: job_labels,
                runner_name,
                conclusion,
                ..
            },
    } = payload;

    debug!("Workflow job {job_id} of run {run_id} ({workflow_name}) is {action:?}");

    let runner_name = runner_name
        .as_ref()
        .ok_or_else(|| BadRequest("workflow_job.runner_name missing".to_owned()));

    let result = match action {
        WorkflowStatus::Queued => {
            scheduler.job_enqueued(*job_id, workflow_name, workflow_url, job_labels)
        }
        WorkflowStatus::InProgress => scheduler.job_processing(*job_id, runner_name?.as_str()),
        WorkflowStatus::Completed => {
            let conclusion = conclusion
                .ok_or_else(|| BadRequest("Missing conclusion to completed job".to_string()))?;
            let termination = match conclusion {
                WorkflowJobConclusion::Success => WorkflowJobTermination::CompletedWithSuccess,
                WorkflowJobConclusion::Failure => WorkflowJobTermination::CompletedWithFailure,
                WorkflowJobConclusion::Cancelled => WorkflowJobTermination::Cancelled,
            };
            scheduler.job_terminated(*job_id, termination)
        }
        WorkflowStatus::Cancelled => {
            scheduler.job_terminated(*job_id, WorkflowJobTermination::Cancelled)
        }
        WorkflowStatus::TimedOut => {
            scheduler.job_terminated(*job_id, WorkflowJobTermination::TimedOut)
        }
        WorkflowStatus::Failure => {
            scheduler.job_terminated(*job_id, WorkflowJobTermination::Failed)
        }
        _ => Ok(()),
    };
    match result {
        Ok(()) => Ok(NO_CONTENT),
        Err(scheduler::Error::InvalidState(s)) => Err(BadRequest(s).into()),
        Err(scheduler::Error::Failed(e)) => Err(internal_server_error(e).into()),
    }
}

#[derive(Debug)]
enum GithubEvent {
    WorkflowJob,
    Other,
}

impl TryIntoHeaderValue for GithubEvent {
    type Error = Infallible;
    fn try_into_value(self) -> Result<HeaderValue, Self::Error> {
        unimplemented!();
    }
}

impl Header for GithubEvent {
    fn name() -> HeaderName {
        HeaderName::from_static("x-github-event")
    }

    fn parse<M: HttpMessage>(msg: &M) -> Result<Self, ParseError> {
        let value = msg.headers().get(Self::name()).ok_or_else(|| {
            debug!("Header {} missing from request", Self::name());
            ParseError::Header
        })?;
        match value.as_bytes() {
            b"workflow_job" => Ok(GithubEvent::WorkflowJob),
            _ => Ok(GithubEvent::Other),
        }
    }
}

#[derive(Debug)]
struct HubSignature256([u8; 32]);

impl TryIntoHeaderValue for HubSignature256 {
    type Error = Infallible;
    fn try_into_value(self) -> Result<HeaderValue, Self::Error> {
        unimplemented!();
    }
}

impl Header for HubSignature256 {
    fn name() -> HeaderName {
        HeaderName::from_static("x-hub-signature-256")
    }

    fn parse<M: HttpMessage>(msg: &M) -> Result<Self, ParseError> {
        use hex::FromHex;
        let value = msg.headers().get(Self::name()).ok_or_else(|| {
            debug!("Header {} missing from request", Self::name());
            ParseError::Header
        })?;
        let lead = b"sha256=";
        if value.as_bytes().starts_with(lead) {
            let hex = &value.as_bytes()[lead.len()..];
            let sha = FromHex::from_hex(hex).map_err(|e| {
                debug!("Cannot decode hash from {} header: {e}", Self::name());
                ParseError::Header
            })?;
            Ok(HubSignature256(sha))
        } else {
            debug!("Value of header {} has unexpected format", Self::name());
            Err(ParseError::Header)
        }
    }
}

#[actix_web::post("/")]
async fn webhook_event(
    event: web::Header<GithubEvent>,
    sig: web::Header<HubSignature256>,
    payload: web::Bytes,
    scheduler: web::Data<Scheduler>,
    http_config: web::Data<HttpConfig>,
) -> StaticResult {
    use hmac::Mac;
    type HmacSha256 = hmac::Hmac<sha2::Sha256>;

    let mut mac = HmacSha256::new_from_slice(http_config.secret.as_bytes()).unwrap();
    mac.update(&payload);
    mac.verify_slice(&sig.0 .0)
        .map_err(|_| BadRequest("HMAC mismatch".to_owned()))?;

    match event.0 {
        GithubEvent::WorkflowJob => {
            let p = serde_json::from_slice(&payload).map_err(|e| BadRequest(format!("{e:#}")))?;
            workflow_job_event(&scheduler, &p).await?;
            Ok(NO_CONTENT)
        }
        GithubEvent::Other => Ok(NO_CONTENT),
    }
}

fn sort_snapshot(state: &mut SchedulerStateSnapshot) {
    (state.active_jobs).sort_by_key(|job| match job.state {
        scheduler::WorkflowJobState::InProgress(scheduler::AssignedRunner::Scheduled(_)) => 0,
        scheduler::WorkflowJobState::InProgress(scheduler::AssignedRunner::Foreign(_)) => 1,
        scheduler::WorkflowJobState::Pending(_) => 2,
    });
    (state.active_runners).sort_by_key(|runner| match runner.state {
        scheduler::RunnerState::Running(_) => 0,
        scheduler::RunnerState::Listening => 1,
        scheduler::RunnerState::Starting => 2,
        scheduler::RunnerState::Queued => 3,
    });
}

#[actix_web::get("/")]
async fn index(scheduler: web::Data<Scheduler>, tera: web::Data<Tera>) -> HttpResponse {
    let mut state = scheduler.snapshot_state();
    sort_snapshot(&mut state);

    let cxt = tera::Context::from_serialize(&state).expect("Error serializing state");
    let html = tera
        .render("index", &cxt)
        .expect("Error rendering index template");

    HttpResponse::build(StatusCode::OK)
        .content_type(ContentType::html())
        .body(html)
}

async fn schedule_all_pending_jobs(github_config: &GithubConfig, scheduler: &Arc<Scheduler>) {
    // We don't intend to fail the entire daemon if the initial querying or scheduling errors out,
    // so we log errors and return ()
    github::list_all_pending_workflow_jobs(&github_config.entity, &github_config.api_token)
        .await
        .unwrap_or_else(|e| {
            error!("Error querying pending workflow jobs from Github: {e:#}");
            Default::default()
        })
        .into_iter()
        .map(|job| {
            if let Err(e) = scheduler.job_enqueued(job.job_id, &job.name, &job.url, &job.labels) {
                error!("Cannot enqueue pre-existing job: {e:#}");
            }
        })
        .collect()
}

#[actix_web::main]
pub async fn main(config_file: ConfigFile) -> anyhow::Result<()> {
    let runner_labels = config_file.config.runner.registration.labels.clone();
    let targets = (config_file.config.targets.iter())
        .map(|(id, c)| scheduler::Target {
            id: id.clone(),
            runner_labels: c.runner_labels.clone(),
            priority: c.priority,
        })
        .collect();

    let scheduler = Arc::new(Scheduler::new(
        Box::new(SlurmExecutor::new(config_file.clone())),
        runner_labels,
        targets,
        20, // TODO
    ));

    let resource_path = paths::find_resources_path()?;
    let html_path = paths::join!(&resource_path, "html");
    let static_path = paths::join!(&resource_path, "static");

    let mut tera = Tera::default();
    tera.add_template_file(paths::join!(&html_path, "index.html"), Some("index"))
        .unwrap();

    let server = {
        let rc_scheduler = web::Data::from(scheduler.clone());
        let rc_tera = web::Data::new(tera);
        let rc_http_config = web::Data::new(config_file.config.http.clone());
        HttpServer::new(move || {
            let static_files = actix_files::Files::new("/static", &static_path)
                .prefer_utf8(true)
                .show_files_listing()
                .files_listing_renderer(|_, req| {
                    Ok(ServiceResponse::new(
                        req.clone(),
                        HttpResponse::NotFound().finish(),
                    ))
                });
            App::new()
                .app_data(rc_scheduler.clone())
                .app_data(rc_tera.clone())
                .app_data(rc_http_config.clone())
                .service(index)
                .service(webhook_event)
                .service(static_files.clone())
        })
        .bind(&config_file.config.http.bind)?
        .run()
    };

    let update = schedule_all_pending_jobs(&config_file.config.github, &scheduler);

    let (server_result, ()) = futures_util::join!(server, update);
    Ok(server_result?)
}

#[test]
fn test_deserialize_payload() {
    use crate::github::{WorkflowJobId, WorkflowRunId};
    use crate::webhook::WorkflowStatus::InProgress;

    let json = include_str!("../testdata/workflow_job.json");
    let payload: WorkflowJobPayload = serde_json::from_str(json).unwrap();
    assert_eq!(
        payload,
        WorkflowJobPayload {
            action: InProgress,
            workflow_job: WorkflowJob {
                run_id: WorkflowRunId(940463255),
                job_id: WorkflowJobId(2832853555),
                name: String::from("Test workflow"),
                url: String::from("https://github.com/octo-org/example-workflow/runs/2832853555"),
                labels: Vec::from(["gpu", "db-app", "dc-03"].map(String::from)),
                runner_name: Some("my runner".to_owned()),
                status: InProgress,
                conclusion: None,
            },
        }
    )
}

#[test]
fn test_render_index() {
    use crate::github::WorkflowJobId;
    use crate::ipc::RunnerMetadata;
    use crate::scheduler::{
        ActiveRunner, ActiveWorkflowJob, AssignedRunner, RunnerId, RunnerInfo, RunnerState,
        RunnerTermination, SchedulerStateSnapshot, TargetId, TerminatedRunner,
        TerminatedWorkflowJob, WorkflowJobInfo, WorkflowJobState, WorkflowJobTermination,
    };
    use crate::slurm;
    use std::time::SystemTime;

    let mut tera = Tera::default();
    tera.add_raw_template("index", include_str!("../res/html/index.html"))
        .unwrap();

    let mut state = SchedulerStateSnapshot {
        active_jobs: vec![
            ActiveWorkflowJob {
                info: WorkflowJobInfo {
                    id: WorkflowJobId(42),
                    name: "Job X".to_owned(),
                    url: "https://github.com/octo-org/example-workflow/runs/0".to_owned(),
                },
                state: WorkflowJobState::Pending(vec![TargetId("label-1".to_owned())]),
                state_changed_at: SystemTime::now(),
            },
            ActiveWorkflowJob {
                info: WorkflowJobInfo {
                    id: WorkflowJobId(42),
                    name: "Job A".to_owned(),
                    url: "https://github.com/octo-org/example-workflow/runs/1".to_owned(),
                },
                state: WorkflowJobState::Pending(vec![
                    TargetId("label-1".to_owned()),
                    TargetId("label-2".to_owned()),
                ]),
                state_changed_at: SystemTime::now(),
            },
            ActiveWorkflowJob {
                info: WorkflowJobInfo {
                    id: WorkflowJobId(456),
                    name: "Job B".to_owned(),
                    url: "https://github.com/octo-org/example-workflow/runs/2".to_owned(),
                },
                state: WorkflowJobState::InProgress(AssignedRunner::Foreign("foreign".to_owned())),
                state_changed_at: SystemTime::now(),
            },
            ActiveWorkflowJob {
                info: WorkflowJobInfo {
                    id: WorkflowJobId(789),
                    name: "Job C".to_owned(),
                    url: "https://github.com/octo-org/example-workflow/runs/3".to_owned(),
                },
                state: WorkflowJobState::InProgress(AssignedRunner::Scheduled(
                    scheduler::RunnerId(0),
                )),
                state_changed_at: SystemTime::now(),
            },
            ActiveWorkflowJob {
                info: WorkflowJobInfo {
                    id: WorkflowJobId(987),
                    name: "Job on removed runner".to_owned(),
                    url: "https://github.com/octo-org/example-workflow/runs/4".to_owned(),
                },
                state: WorkflowJobState::InProgress(AssignedRunner::Scheduled(
                    scheduler::RunnerId(9),
                )),
                state_changed_at: SystemTime::now(),
            },
        ],
        active_runners: vec![
            ActiveRunner {
                info: RunnerInfo {
                    id: scheduler::RunnerId(0),
                    target: TargetId("target-a".to_string()),
                    metadata: Some(RunnerMetadata {
                        runner_name: "runner-1".to_owned(),
                        slurm_job: slurm::JobId(999),
                        host_name: "host".to_string(),
                        concurrent_id: 1,
                    }),
                },
                state: RunnerState::Running(WorkflowJobId(789)),
                state_changed_at: SystemTime::now(),
            },
            ActiveRunner {
                info: RunnerInfo {
                    id: scheduler::RunnerId(2),
                    target: TargetId("target-b".to_string()),
                    metadata: None,
                },
                state: RunnerState::Queued,
                state_changed_at: SystemTime::now(),
            },
            ActiveRunner {
                info: RunnerInfo {
                    id: scheduler::RunnerId(3),
                    target: TargetId("target-b".to_string()),
                    metadata: Some(RunnerMetadata {
                        runner_name: "runner-3".to_owned(),
                        slurm_job: slurm::JobId(998),
                        host_name: "host".to_string(),
                        concurrent_id: 1,
                    }),
                },
                state: RunnerState::Starting,
                state_changed_at: SystemTime::now(),
            },
            ActiveRunner {
                info: RunnerInfo {
                    id: scheduler::RunnerId(4),
                    target: TargetId("target-b".to_string()),
                    metadata: Some(RunnerMetadata {
                        runner_name: "runner-4".to_owned(),
                        slurm_job: slurm::JobId(997),
                        host_name: "host".to_string(),
                        concurrent_id: 1,
                    }),
                },
                state: RunnerState::Listening,
                state_changed_at: SystemTime::now(),
            },
            ActiveRunner {
                info: RunnerInfo {
                    id: scheduler::RunnerId(5),
                    target: TargetId("target-a".to_string()),
                    metadata: Some(RunnerMetadata {
                        runner_name: "runner-with-removed-job".to_owned(),
                        slurm_job: slurm::JobId(1000),
                        host_name: "host".to_string(),
                        concurrent_id: 1,
                    }),
                },
                state: RunnerState::Running(WorkflowJobId(2222)),
                state_changed_at: SystemTime::now(),
            },
        ],
        job_history: vec![
            TerminatedWorkflowJob {
                info: WorkflowJobInfo {
                    id: WorkflowJobId(885),
                    name: "historic-job".to_string(),
                    url: "https://github.com/octo-org/example-workflow/runs/2".to_string(),
                },
                assignment: None,
                termination: WorkflowJobTermination::TimedOut,
                terminated_at: SystemTime::now(),
            },
            TerminatedWorkflowJob {
                info: WorkflowJobInfo {
                    id: WorkflowJobId(886),
                    name: "historic-job".to_string(),
                    url: "https://github.com/octo-org/example-workflow/runs/3".to_string(),
                },
                assignment: Some(AssignedRunner::Scheduled(RunnerId(775))),
                termination: WorkflowJobTermination::Cancelled,
                terminated_at: SystemTime::now(),
            },
            TerminatedWorkflowJob {
                info: WorkflowJobInfo {
                    id: WorkflowJobId(887),
                    name: "historic-job".to_string(),
                    url: "https://github.com/octo-org/example-workflow/runs/4".to_string(),
                },
                assignment: Some(AssignedRunner::Scheduled(RunnerId(776))),
                termination: WorkflowJobTermination::CompletedWithSuccess,
                terminated_at: SystemTime::now(),
            },
            TerminatedWorkflowJob {
                info: WorkflowJobInfo {
                    id: WorkflowJobId(888),
                    name: "historic-job".to_string(),
                    url: "https://github.com/octo-org/example-workflow/runs/4".to_string(),
                },
                assignment: Some(AssignedRunner::Scheduled(RunnerId(777))),
                termination: WorkflowJobTermination::CompletedWithFailure,
                terminated_at: SystemTime::now(),
            },
        ],
        runner_history: vec![
            TerminatedRunner {
                info: RunnerInfo {
                    id: RunnerId(776),
                    target: TargetId("target-a".to_string()),
                    metadata: None,
                },
                termination: RunnerTermination::Completed(WorkflowJobId(999999)),
                terminated_at: SystemTime::now(),
            },
            TerminatedRunner {
                info: RunnerInfo {
                    id: RunnerId(777),
                    target: TargetId("target-b".to_string()),
                    metadata: None,
                },
                termination: RunnerTermination::Failed,
                terminated_at: SystemTime::now(),
            },
            TerminatedRunner {
                info: RunnerInfo {
                    id: RunnerId(778),
                    target: TargetId("target-a".to_string()),
                    metadata: None,
                },
                termination: RunnerTermination::Failed,
                terminated_at: SystemTime::now(),
            },
        ],
    };

    sort_snapshot(&mut state);

    let cxt = tera::Context::from_serialize(&state).unwrap();
    std::fs::write(
        "/tmp/slurmactiond.html",
        tera.render("index", &cxt).unwrap(),
    )
    .unwrap();
}
