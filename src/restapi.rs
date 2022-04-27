use crate::restapi::WorkflowStatus::InProgress;
use crate::slurm::BatchScript;
use crate::{slurm, Config};
use actix_web::web::{Data, Json};
use actix_web::{App, HttpServer, Responder};
use serde::Deserialize;
use shell_escape::unix::escape;
use std::borrow::Cow;
use std::io;
use std::sync::Arc;

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

#[actix_web::post("/workflow_job")]
async fn workflow_job(data: Data<Arc<Config>>, req: Json<WebhookPayload>) -> impl Responder {
    BatchScript {
        slurm: &data.slurm,
        runner: &data.action_runner,
        mapping: &data.mappings[0],
        runner_seq: 1234,
    }
        .to_string()
}

#[actix_web::main]
pub async fn main(cfg: Config) -> io::Result<()> {
    let rc_cfg = Arc::new(cfg);
    HttpServer::new(move || App::new().app_data(Data::new(rc_cfg.clone())).service(workflow_job))
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
