use std::io;

use actix_web::{App, HttpServer, Responder};
use actix_web::http::StatusCode;
use actix_web::web::{Data, Json};
use log::{debug, info};
use serde::Deserialize;

use crate::Config;
use crate::config::PartitionId;
use crate::slurm::batch_submit;

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
    status: WorkflowStatus,
    labels: Vec<String>,
}

#[derive(Deserialize, Debug, PartialEq, Eq)]
struct WebhookPayload {
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
        debug!("matched runner labels {:?} to partition {}", job.labels, id.0);
        Some(id)
    } else {
        debug!("runner labels {:?} do not match any partition", job.labels);
        None
    }
}

#[actix_web::post("/workflow_job")]
async fn workflow_job(data: Data<Config>, payload: Json<WebhookPayload>) -> impl Responder {
    if let Some(part_id) = match_partition(&data, &payload.workflow_job) {
        let job_id = batch_submit(&data, &part_id)?;
        info!("submitted SLURM job {} for runner job {} of workflow {}", job_id.0,
            payload.workflow_job.job_id, payload.workflow_job.workflow_id);
    }
    Result::<_, io::Error>::Ok(("", StatusCode::NO_CONTENT))
}

#[actix_web::main]
pub async fn main(cfg: Config) -> io::Result<()> {
    let bind_address = cfg.http.bind.clone();
    let data = Data::new(cfg);
    HttpServer::new(move || App::new().app_data(data.clone()).service(workflow_job))
        .bind(bind_address)?
        .run()
        .await
}

#[test]
fn test_deserialize_payload() {
    use crate::restapi::WorkflowStatus::InProgress;

    let json = include_str!("../testdata/workflow_job.json");
    let payload: WebhookPayload = serde_json::from_str(json).unwrap();
    assert_eq!(
        payload,
        WebhookPayload {
            action: InProgress,
            workflow_job: WorkflowJob {
                workflow_id: 2832853555,
                job_id: 940463255,
                name: String::from("Test workflow"),
                status: InProgress,
                labels: Vec::from(["gpu", "db-app", "dc-03"].map(String::from)),
            },
        }
    )
}
