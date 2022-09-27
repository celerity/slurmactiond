use std::convert::Infallible;
use std::fmt::{Debug, Display, Formatter};
use std::path::{Path, PathBuf};

use actix_web::error::ParseError;
use actix_web::http::header::{Header, HeaderName, HeaderValue, TryIntoHeaderValue};
use actix_web::http::StatusCode;
use actix_web::{web, App, HttpMessage, HttpServer, ResponseError};
use anyhow::Context as _;
use log::{debug, error};
use serde::Deserialize;

use crate::github::{WorkflowRunId, WorkflowJobId};
use crate::scheduler::{self, Scheduler};
use crate::Config;

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
#[serde(rename_all = "snake_case")]
enum WorkflowStatus {
    Queued,
    InProgress,
    Completed,
}

#[derive(Deserialize, Debug, PartialEq, Eq)]
struct WorkflowJob {
    run_id: WorkflowRunId,
    #[serde(rename = "id")]
    job_id: WorkflowJobId,
    name: String,
    labels: Vec<String>,
    runner_name: Option<String>,
}

#[derive(Deserialize, Debug, PartialEq, Eq)]
struct WorkflowJobPayload {
    action: WorkflowStatus,
    workflow_job: WorkflowJob,
}

struct SharedData {
    config_path: PathBuf,
    config: Config,
    scheduler: Scheduler,
}

async fn workflow_job_event(data: &SharedData, payload: &WorkflowJobPayload) -> StaticResult {
    let SharedData {
        config_path,
        config,
        scheduler,
    } = data;
    let WorkflowJobPayload {
        action,
        workflow_job:
        WorkflowJob {
            run_id,
            job_id,
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
        WorkflowStatus::Queued => scheduler.job_enqueued(*job_id, job_labels, config_path, config),
        WorkflowStatus::InProgress => {
            scheduler.job_processing(*job_id, runner_name?.as_str(), config_path, config)
        }
        WorkflowStatus::Completed => Ok(scheduler.job_completed(*job_id)),
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
    data: web::Data<SharedData>,
) -> StaticResult {
    use hmac::Mac;
    type HmacSha256 = hmac::Hmac<sha2::Sha256>;

    let mut mac = HmacSha256::new_from_slice(data.config.http.secret.as_bytes()).unwrap();
    mac.update(&payload);
    mac.verify_slice(&sig.0 .0)
        .map_err(|_| BadRequest("HMAC mismatch".to_owned()))?;

    match event.0 {
        GithubEvent::WorkflowJob => {
            let p = serde_json::from_slice(&payload).map_err(|e| BadRequest(format!("{e:#}")))?;
            workflow_job_event(data.as_ref(), &p).await?;
            Ok(NO_CONTENT)
        }
        GithubEvent::Other => Ok(NO_CONTENT),
    }
}

#[actix_web::main]
pub async fn main(config_file: &Path) -> anyhow::Result<()> {
    let config = Config::read_from_toml_file(config_file)
        .with_context(|| format!("Reading configuration from `{}`", config_file.display()))?;

    let bind_address = config.http.bind.clone();
    let data = web::Data::new(SharedData {
        config_path: config_file.to_owned(),
        config,
        scheduler: Scheduler::new(),
    });

    HttpServer::new(move || App::new().app_data(data.clone()).service(webhook_event))
        .bind(bind_address)?
        .run()
        .await?;
    Ok(())
}

#[test]
fn test_deserialize_payload() {
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
                labels: Vec::from(["gpu", "db-app", "dc-03"].map(String::from)),
                runner_name: Some("my runner".to_owned()),
            },
        }
    )
}
