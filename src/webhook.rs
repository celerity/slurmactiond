use std::convert::Infallible;
use std::fmt::{Debug, Display, Formatter};
use std::path::{Path, PathBuf};

use actix_web::error::ParseError;
use actix_web::http::header::{Header, HeaderName, HeaderValue, TryIntoHeaderValue};
use actix_web::http::StatusCode;
use actix_web::{web, App, HttpMessage, HttpServer, ResponseError};
use anyhow::Context as _;
use log::{debug, error, info};
use serde::Deserialize;

use crate::config::TargetId;
use crate::slurm;
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

fn internal_server_error(cause: &str, e: impl Display) -> InternalServerError {
    error!("Internal Server Error: {cause} {e}");
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
    #[serde(rename = "id")]
    workflow_id: u64,
    #[serde(rename = "run_id")]
    job_id: u64,
    name: String,
    labels: Vec<String>,
}

#[derive(Deserialize, Debug, PartialEq, Eq)]
struct WorkflowJobPayload {
    action: WorkflowStatus,
    workflow_job: WorkflowJob,
}

struct SharedData {
    config_path: PathBuf,
    config: Config,
}

fn match_target<'c>(config: &'c Config, job: &WorkflowJob) -> Option<&'c TargetId> {
    let unmatched_labels: Vec<_> = (job.labels.iter())
        // GitHub only includes the "self-hosted" label if the set of labels is otherwise empty.
        // We don't require the user to list "self-hosted" in the config.
        .filter(|l| !(l == "self-hosted" || config.runner.registration.labels.contains(l)))
        .collect();
    let closest_matching_target = (config.targets.iter())
        .filter(|(_, p)| unmatched_labels.iter().all(|l| p.runner_labels.contains(l)))
        .min_by_key(|(_, p)| p.runner_labels.len()); // min: closest match
    if let Some((id, _)) = closest_matching_target {
        debug!("matched runner labels {:?} to target {}", job.labels, id.0);
        Some(id)
    } else {
        debug!("runner labels {:?} do not match any target", job.labels);
        None
    }
}

async fn workflow_job_event(data: &SharedData, payload: &WorkflowJobPayload) -> StaticResult {
    if payload.action == WorkflowStatus::Queued {
        if let Some(target) = match_target(&data.config, &payload.workflow_job) {
            info!(
                "executing SLURM job for runner job {} of workflow {} ({})",
                payload.workflow_job.job_id,
                payload.workflow_job.workflow_id,
                payload.workflow_job.name
            );
            let job = slurm::RunnerJob::spawn(&data.config_path, &data.config, target)
                .await
                .map_err(|e| internal_server_error("Submitting job to SLURM", e))?;
            actix_web::rt::spawn(async move {
                match job.join().await {
                    Ok(status) => info!("SLURM job exited with status {}", status),
                    Err(e) => error!("Running SLURM job: {e:#}"),
                }
            });
        }
    }
    Ok(NO_CONTENT)
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
                workflow_id: 2832853555,
                job_id: 940463255,
                name: String::from("Test workflow"),
                labels: Vec::from(["gpu", "db-app", "dc-03"].map(String::from)),
            },
        }
    )
}
