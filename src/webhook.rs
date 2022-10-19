use std::convert::Infallible;
use std::fmt::{Debug, Display, Formatter};

use actix_web::error::ParseError;
use actix_web::http::header::{ContentType, Header, HeaderName, HeaderValue, TryIntoHeaderValue};
use actix_web::http::StatusCode;
use actix_web::{web, App, HttpMessage, HttpResponse, HttpServer, ResponseError};
use handlebars::Handlebars;
use log::{debug, error};
use serde::Deserialize;

use crate::config::{ConfigFile, GithubConfig, HttpConfig};
use crate::github;
use crate::github::{WorkflowJob, WorkflowStatus};
use crate::scheduler::{self, Scheduler};

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

async fn workflow_job_event(scheduler: &Scheduler, payload: &WorkflowJobPayload) -> StaticResult {
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
        WorkflowStatus::Completed => Ok(scheduler.job_completed(*job_id)),
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

#[actix_web::get("/")]
async fn index(
    scheduler: web::Data<Scheduler>,
    handlebars: web::Data<Handlebars<'static>>,
) -> HttpResponse {
    let html = handlebars
        .render("index", &scheduler.snapshot_state())
        .expect("Error rendering index template");
    HttpResponse::build(StatusCode::OK)
        .content_type(ContentType::html())
        .body(html)
}

async fn schedule_all_pending_jobs(github_config: &GithubConfig, scheduler: &Scheduler) {
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
    let scheduler = web::Data::new(Scheduler::new(config_file.clone()));

    let mut handlebars = Handlebars::new();
    handlebars
        .register_template_string("index", include_str!("../res/html/index.html"))
        .unwrap();

    let server = {
        let rc_scheduler = web::Data::new(scheduler.clone());
        let rc_handlebars = web::Data::new(handlebars);
        let rc_http_config = web::Data::new(config_file.config.http.clone());
        HttpServer::new(move || {
            App::new()
                .app_data(rc_scheduler.clone())
                .app_data(rc_handlebars.clone())
                .app_data(rc_http_config.clone())
                .service(index)
                .service(webhook_event)
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
            },
        }
    )
}

#[test]
fn test_render_index() {
    use crate::config::{self, TargetId};
    use crate::github::WorkflowJobId;
    use crate::ipc::RunnerMetadata;
    use crate::scheduler::{
        InternalRunnerId, RunnerInfo, RunnerState, SchedulerStateSnapshot, WorkflowJobInfo,
        WorkflowJobState,
    };
    use crate::slurm;
    use std::collections::HashMap;

    let mut handlebars = Handlebars::new();
    handlebars
        .register_template_string("index", include_str!("../res/html/index.html"))
        .unwrap();
    let state = SchedulerStateSnapshot {
        jobs: HashMap::from([
            (
                WorkflowJobId(123),
                WorkflowJobInfo {
                    name: "Job A".to_owned(),
                    url: "https://github.com/octo-org/example-workflow/runs/1".to_owned(),
                    state: WorkflowJobState::Pending(vec![TargetId("label".to_owned())]),
                },
            ),
            (
                WorkflowJobId(456),
                WorkflowJobInfo {
                    name: "Job B".to_owned(),
                    url: "https://github.com/octo-org/example-workflow/runs/2".to_owned(),
                    state: WorkflowJobState::Pending(vec![TargetId("label".to_owned())]),
                },
            ),
            (
                WorkflowJobId(789),
                WorkflowJobInfo {
                    name: "Job C".to_owned(),
                    url: "https://github.com/octo-org/example-workflow/runs/3".to_owned(),
                    state: WorkflowJobState::InProgress(InternalRunnerId(1)),
                },
            ),
        ]),
        runners: HashMap::from([
            (
                InternalRunnerId(1),
                RunnerInfo {
                    target: config::TargetId("target-a".to_string()),
                    metadata: Some(RunnerMetadata {
                        runner_name: "runner-1".to_owned(),
                        slurm_job: slurm::JobId(999),
                        concurrent_id: 1,
                    }),
                    state: RunnerState::Running(WorkflowJobId(789)),
                },
            ),
            (
                InternalRunnerId(2),
                RunnerInfo {
                    target: config::TargetId("target-b".to_string()),
                    metadata: None,
                    state: RunnerState::Queued,
                },
            ),
        ]),
    };
    println!("{}", handlebars.render("index", &state).unwrap());
}
