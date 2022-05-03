use std::io;

use actix_web::http::StatusCode;
use actix_web::web::{Data, Json};
use actix_web::{guard, web, App, FromRequest, Handler, HttpServer, Responder, Route};
use log::{debug, info};
use serde::Deserialize;

use crate::config::PartitionId;
use crate::github;
use crate::slurm::batch_submit;
use crate::Config;

type StaticContent = (&'static str, StatusCode);
type StaticResult = actix_web::Result<StaticContent>;

const NO_CONTENT: StaticContent = ("", StatusCode::NO_CONTENT);

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

fn match_partition<'c>(config: &'c Config, job: &WorkflowJob) -> Option<&'c PartitionId> {
    let unmatched_labels: Vec<_> = (job.labels.iter())
        .filter(|l| !config.runner.registration.labels.contains(l))
        .collect();
    let closest_matching_partition = (config.partitions.iter())
        .filter(|(_, p)| unmatched_labels.iter().all(|l| p.runner_labels.contains(l)))
        .min_by_key(|(_, p)| p.runner_labels.len()); // min: closest match
    if let Some((id, _)) = closest_matching_partition {
        debug!(
            "matched runner labels {:?} to partition {}",
            job.labels, id.0
        );
        Some(id)
    } else {
        debug!("runner labels {:?} do not match any partition", job.labels);
        None
    }
}

async fn workflow_job_event(
    config: Data<Config>,
    payload: Json<WorkflowJobPayload>
) -> StaticResult {
    if payload.action == WorkflowStatus::Queued {
        if let Some(part_id) = match_partition(&config, &payload.workflow_job) {
            let token_fut = github::generate_runner_registration_token(
                &config.github.entity,
                &config.github.api_token,
            );
            let token = token_fut
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{e}")))?;
            let job_id = batch_submit(&config, &part_id, &token)?;
            info!(
                "submitted SLURM job {} for runner job {} of workflow {}({})",
                job_id.0,
                payload.workflow_job.job_id,
                payload.workflow_job.workflow_id,
                payload.workflow_job.name
            );
        }
    }
    Ok(NO_CONTENT)
}

#[derive(Deserialize, Debug)]
struct PingPayload {}

async fn ping_event(_: Json<PingPayload>) -> StaticResult {
    Ok(("pong", StatusCode::OK))
}

fn github_event_route<F, Args>(event: &'static str, handler: F) -> Route
where
    F: Handler<Args>,
    Args: FromRequest + 'static,
    F::Output: Responder + 'static,
{
    web::post()
        .guard(guard::Header("X-Github-Event", event))
        .to(handler)
}

#[actix_web::main]
pub async fn main(cfg: Config) -> io::Result<()> {
    let bind_address = cfg.http.bind.clone();
    let data = Data::new(cfg);
    HttpServer::new(move || {
        App::new()
            .app_data(data.clone())
            .route("/", github_event_route("workflow_job", workflow_job_event))
            .route("/", github_event_route("ping", ping_event))
    })
    .bind(bind_address)?
    .run()
    .await
}

#[test]
fn test_deserialize_payload() {
    use crate::restapi::WorkflowStatus::InProgress;

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
