use crate::restapi::WorkflowStatus::InProgress;
use crate::slurm;
use actix_web::{web, App, HttpServer, Responder};
use serde::Deserialize;
use std::io;

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

fn make_runner_script(job: &WorkflowJob) -> String {
    let partition = "nv";
    let runner_url = "url";
    let runner_token = "token";
    let runner_seq = 12345;
    let runner_labels = job.labels.join(",");
    format!(
        r#"!#/bin/bash
#SBATCH -p {partition}

source actions-runner-env
actions-runner/config.sh \
    --unattend \
    --url {runner_url} \
    --token {runner_token} \
    --name auto-{runner_seq} \
    --labels {runner_labels} \
    --ephemeral
actions-runner/run.sh
"#
    )
}

#[actix_web::post("/workflow_job")]
async fn workflow_job(req: web::Json<WebhookPayload>) -> impl Responder {
    format!("{:?}", req)
}

#[actix_web::main]
pub async fn main() -> io::Result<()> {
    HttpServer::new(|| App::new().service(workflow_job))
        .bind("127.0.0.1:6020")?
        .run()
        .await
}

#[test]
fn test_deserialize_payload() {
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
