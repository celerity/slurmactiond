use anyhow::Context as _;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::io;
use thiserror::Error;

use awc::error::HeaderValue;
use awc::http::Method;
use awc::{http::header, Client, SendClientRequest};
use log::debug;
use regex::Regex;
use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WorkflowJobId(pub u64);

impl Display for WorkflowJobId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WorkflowId(pub u64);

impl Display for WorkflowId {
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
    use futures_util::stream::StreamExt;
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

#[test]
fn test_locate_runner_tarball() {
    match std::env::vars().find(|(k, _)| k == "GITHUB_API_TOKEN") {
        Some((_, api_token_str)) => {
            actix_web::rt::System::new()
                .block_on(locate_runner_tarball(
                    "linux-x64",
                    &ApiToken(api_token_str.to_owned()),
                ))
                .unwrap();
        }
        None => {
            eprintln!("warning: GITHUB_API_TOKEN not set, skipping GitHub API tests");
        }
    }
}
