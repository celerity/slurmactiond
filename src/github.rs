use anyhow::Context as _;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::io;
use thiserror::Error;

use crate::util;
use awc::error::HeaderValue;
use awc::http::Method;
use awc::{http::header, Client, SendClientRequest};
use log::debug;
use regex::Regex;
use serde::{Deserialize, Serialize};

// A workflow _run_ refers to a collection of jobs, created by a trigger.
#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WorkflowRunId(pub u64);

impl Display for WorkflowRunId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// A workflow _job_ identifies the unit of work that will be picked up by a runner.
#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WorkflowJobId(pub u64);

impl Display for WorkflowJobId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Deserialize, Debug, PartialEq, Eq)]
#[serde(try_from = "&str")]
pub enum Entity {
    Organization(String),
    Repository(String, String),
}

impl Display for Entity {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Entity::Organization(org) => write!(f, "{org}"),
            Entity::Repository(org, repo) => write!(f, "{org}/{repo}"),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Error)]
#[error("Invalid GitHub entity")]
pub struct InvalidEntityError;

impl TryFrom<&str> for Entity {
    type Error = InvalidEntityError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let re = Regex::new("^([^/]+)(?:/([^/]+))?$").unwrap();
        let caps = re.captures(s).ok_or(InvalidEntityError)?;
        let get_string = |i| caps.get(i).map(|m| m.as_str().to_owned());
        match (get_string(1), get_string(2)) {
            (Some(org), None) => Ok(Entity::Organization(org)),
            (Some(org), Some(repo)) => Ok(Entity::Repository(org, repo)),
            _ => Err(InvalidEntityError),
        }
    }
}

#[derive(Deserialize, Debug, PartialEq, Eq)]
#[serde(transparent)]
pub struct ApiToken(pub String);

impl Display for ApiToken {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Deserialize, Debug, PartialEq, Eq)]
#[serde(transparent)]
pub struct RunnerRegistrationToken(pub String);

#[derive(Deserialize)]
struct TokenPayload {
    token: RunnerRegistrationToken,
}

const GITHUB: &str = "https://api.github.com";
const RUNNER_TOKEN: &str = "actions/runners/registration-token";
const USER_AGENT: &str = "slurmactiond";

trait ResultType {
    type Payload;
}
impl<T, E> ResultType for Result<T, E> {
    type Payload = T;
}
type ClientResponse = <<SendClientRequest as Future>::Output as ResultType>::Payload;

async fn api_request(
    method: &Method,
    url: &String,
    api_token: &ApiToken,
) -> anyhow::Result<ClientResponse> {
    debug!("Sending GitHub API request {method} {url}");
    let header_token = HeaderValue::try_from(format!("Token {api_token}"))
        .with_context(|| "Cannot construct GitHub API token header")?;
    let response = Client::new()
        .request(method.clone(), url.clone())
        .append_header((header::ACCEPT, "application/vnd.github.v3+json"))
        .append_header((header::USER_AGENT, USER_AGENT))
        .append_header((header::AUTHORIZATION, header_token))
        .send()
        .await
        .map_err(|e| {
            anyhow::anyhow!("{}", e)
                .context(format!("Error sending GitHub API request {method} {url}"))
        })?;
    if response.status().is_success() {
        Ok(response)
    } else {
        let mut response = response;
        let body = response
            .body()
            .await
            .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
            .with_context(|| "Cannot retrieve HTTP response body for {method} {url}")?;
        anyhow::bail!(
            "{method} {url} returned status {s}: {body}",
            s = response.status()
        );
    }
}

pub async fn generate_runner_registration_token(
    entity: &Entity,
    api_token: &ApiToken,
) -> anyhow::Result<RunnerRegistrationToken> {
    let endpoint = match entity {
        Entity::Organization(org) => format!("{GITHUB}/orgs/{org}/{RUNNER_TOKEN}"),
        Entity::Repository(org, repo) => format!("{GITHUB}/repos/{org}/{repo}/{RUNNER_TOKEN}"),
    };
    let response = api_request(&Method::POST, &endpoint, api_token).await?;
    let payload: TokenPayload = { response }.json().await?;
    Ok(payload.token)
}

#[derive(Debug, Deserialize)]
pub struct Asset {
    pub name: String,
    pub size: u64,
    #[serde(rename = "browser_download_url")]
    pub url: String,
}

#[derive(Deserialize)]
struct ReleasesPayload {
    assets: Vec<Asset>,
}

pub async fn locate_runner_tarball(platform: &str, api_token: &ApiToken) -> anyhow::Result<Asset> {
    let endpoint = format!("{GITHUB}/repos/actions/runner/releases/latest");
    let response = api_request(&Method::GET, &endpoint, api_token).await?;
    let releases: ReleasesPayload = { response }.json().await?;

    let full_release_re = Regex::new(&format!(
        "^actions-runner-{}-[0-9.]+.tar(?:\\.[a-z0-9]+)?$",
        regex::escape(&platform)
    ))
    .unwrap();

    (releases.assets.into_iter())
        .find(|a| full_release_re.is_match(&a.name))
        .ok_or_else(|| anyhow::anyhow!("No Actions Runner release found for platform {platform}"))
}

pub async fn download(url: &str, to: &mut dyn io::Write) -> anyhow::Result<()> {
    use futures_util::stream::StreamExt as _;
    let mut stream = Client::new()
        .get(url)
        .append_header((header::USER_AGENT, USER_AGENT))
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("{e:#}").context(format!("Error sending request to {url}")))?;
    while let Some(chunk) = stream.next().await {
        let bytes = chunk.with_context(|| "Error reading response from {url}")?;
        to.write(&bytes)
            .with_context(|| "Error writing response to output")?;
    }
    Ok(())
}

#[derive(Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowStatus {
    Completed,
    ActionRequired,
    Cancelled,
    Failure,
    Neutral,
    Skipped,
    Stale,
    Success,
    TimedOut,
    InProgress,
    Queued,
    Requested,
    Waiting,
}

#[derive(Deserialize, Debug, PartialEq, Eq)]
pub struct WorkflowJob {
    pub run_id: WorkflowRunId,
    #[serde(rename = "id")]
    pub job_id: WorkflowJobId,
    pub name: String,
    #[serde(rename = "html_url")]
    pub url: String,
    pub labels: Vec<String>,
    #[serde(deserialize_with = "util::empty_string_as_none")]
    pub runner_name: Option<String>,
    pub status: WorkflowStatus,
}

#[derive(Deserialize)]
struct WorkflowJobsPayload {
    jobs: Vec<WorkflowJob>,
}

async fn list_workflow_run_queued_jobs(
    owner: &str,
    repo: &str,
    run: WorkflowRunId,
    api_token: &ApiToken,
) -> anyhow::Result<Vec<WorkflowJob>> {
    let mut all_jobs = Vec::new();
    for page in 1.. {
        let payload: WorkflowJobsPayload = api_request(
            &Method::GET,
            &format!(
                "{GITHUB}/repos/{owner}/{repo}/actions/runs/{run}/jobs?per_page=100&page={page}"
            ),
            api_token,
        )
        .await?
        .json()
        .await?;

        if payload.jobs.is_empty() {
            break;
        }
        all_jobs.extend(
            payload
                .jobs
                .into_iter()
                .filter(|job| job.status == WorkflowStatus::Queued),
        );
    }
    Ok(all_jobs)
}

#[derive(Debug, Clone, Deserialize)]
struct WorkflowRun {
    #[serde(rename = "id")]
    run_id: WorkflowRunId,
}

#[derive(Deserialize)]
struct WorkflowRunsPayload {
    workflow_runs: Vec<WorkflowRun>,
}

async fn list_repo_queued_workflow_runs(
    org: &str,
    repo: &str,
    api_token: &ApiToken,
) -> anyhow::Result<Vec<WorkflowRun>> {
    let mut all_workflow_runs = Vec::new();
    for page in 1.. {
        let payload: WorkflowRunsPayload = api_request(
            &Method::GET,
            &format!(
                "{GITHUB}/repos/{org}/{repo}/actions/runs?status=queued&per_page=100&page={page}"
            ),
            api_token,
        )
        .await?
        .json()
        .await?;

        if payload.workflow_runs.is_empty() {
            break;
        }
        all_workflow_runs.extend(payload.workflow_runs.into_iter());
    }
    Ok(all_workflow_runs)
}

#[derive(Deserialize)]
struct Repository {
    name: String,
}

async fn list_org_repos(org: &str, api_token: &ApiToken) -> anyhow::Result<Vec<String>> {
    let mut all_repos = Vec::new();
    for page in 1.. {
        let payload: Vec<Repository> = api_request(
            &Method::GET,
            &format!("{GITHUB}/orgs/{org}/repos?per_page=100&page={page}"),
            api_token,
        )
        .await?
        .json()
        .await?;

        if payload.is_empty() {
            break;
        }
        all_repos.extend(payload.into_iter().map(|repo| repo.name));
    }
    Ok(all_repos)
}

pub async fn list_all_pending_workflow_jobs(
    entity: &Entity,
    api_token: &ApiToken,
) -> anyhow::Result<Vec<WorkflowJob>> {
    let (owner, all_repos) = match entity {
        Entity::Organization(org) => (org, list_org_repos(org, api_token).await?),
        Entity::Repository(owner, repo) => (owner, vec![repo.to_owned()]),
    };

    let repo_runs = util::async_map_unordered_and_flatten(all_repos, |repo| async move {
        let runs: Vec<_> = list_repo_queued_workflow_runs(owner, &repo, api_token)
            .await?
            .into_iter()
            .map(|run| (repo.clone(), run.run_id))
            .collect();
        anyhow::Result::<_>::Ok(runs)
    })
    .await?;

    let jobs = util::async_map_unordered_and_flatten(repo_runs, |(repo, run_id)| async move {
        list_workflow_run_queued_jobs(owner, &repo, run_id, api_token).await
    })
    .await?;

    Ok(jobs)
}

#[test]
fn test_entity_from_string() {
    let from = |s| -> Result<Entity, InvalidEntityError> { TryFrom::try_from(s) };
    assert_eq!(
        from("foo/bar"),
        Ok(Entity::Repository("foo".to_owned(), "bar".to_owned()))
    );
    assert_eq!(from("foo"), Ok(Entity::Organization("foo".to_owned())));
    assert_eq!(from("/"), Err(InvalidEntityError));
    assert_eq!(from(""), Err(InvalidEntityError));
    assert_eq!(from("foo/bar/baz"), Err(InvalidEntityError));
}

fn test_api<L, F>(lambda: L)
where
    L: FnOnce(ApiToken) -> F,
    F: Future,
{
    match std::env::vars().find(|(k, _)| k == "GITHUB_API_TOKEN") {
        Some((_, api_token_str)) => {
            actix_web::rt::System::new().block_on(lambda(ApiToken(api_token_str.to_owned())));
        }
        None => {
            eprintln!("warning: GITHUB_API_TOKEN not set, skipping GitHub API tests");
        }
    }
}

#[test]
fn test_locate_runner_tarball() {
    test_api(|api_token| async move {
        locate_runner_tarball("linux-x64", &api_token)
            .await
            .unwrap();
    });
}

#[test]
fn test_list_pending_workflow_jobs() {
    test_api(|api_token| async move {
        println!(
            "{:?}\n",
            list_all_pending_workflow_jobs(
                &Entity::Organization("celerity".to_owned()),
                &api_token
            )
            .await
            .unwrap()
        )
    });
}
