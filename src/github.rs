use std::future::Future;
use std::io;

use awc::{Client, http::header, SendClientRequest};
use awc::error::{HeaderValue, JsonPayloadError, PayloadError, SendRequestError};
use awc::http::{Method, StatusCode};
use awc::http::header::HeaderName;
use derive_more::{Display, From};
use log::debug;
use regex::Regex;
use serde::Deserialize;

#[derive(Deserialize, Debug, Display, PartialEq, Eq)]
#[serde(try_from = "&str")]
pub enum Entity {
    #[display(fmt = "{}", _0)]
    Organization(String),
    #[display(fmt = "{}/{}", _0, _1)]
    Repository(String, String),
}

#[derive(Debug, Display, PartialEq, Eq)]
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

#[derive(Deserialize, Debug, Display, PartialEq, Eq)]
#[serde(transparent)]
#[display(fmt = "{}", _0)]
pub struct ApiToken(pub String);

#[derive(Deserialize, Debug, Display, PartialEq, Eq)]
#[serde(transparent)]
#[display(fmt = "{}", _0)]
pub struct RunnerRegistrationToken(pub String);

#[derive(Deserialize)]
struct TokenPayload {
    token: RunnerRegistrationToken,
}

#[derive(Debug, Display, From)]
pub enum ApiError {
    #[display(fmt = "{}", _0)]
    SendRequest(SendRequestError),
    #[display(fmt = "Status {}: {}", _0, _1)]
    Status(StatusCode, String),
    #[display(fmt = "{}", _0)]
    Payload(JsonPayloadError),
    #[display(fmt = "resource not found")]
    NotFound,
}

impl std::error::Error for ApiError {}

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
    method: Method,
    url: String,
    headers: &[(HeaderName, HeaderValue)],
) -> Result<ClientResponse, ApiError> {
    debug!("Sending GitHub API request {method} {url}");
    let mut request = Client::new()
        .request(method, url)
        .append_header((header::ACCEPT, "application/vnd.github.v3+json"))
        .append_header((header::USER_AGENT, USER_AGENT));
    for (name, value) in headers.into_iter() {
        request.headers_mut().append(name.clone(), value.clone());
    }
    let mut response = request.send().await?;
    if response.status().is_success() {
        Ok(response)
    } else {
        let body = response
            .body()
            .await
            .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
            .unwrap_or_else(|e| format!("Cannot retrieve HTTP response body: {e}"));
        Err(ApiError::Status(response.status(), body))
    }
}

pub async fn generate_runner_registration_token(
    entity: &Entity,
    api_token: &ApiToken,
) -> Result<RunnerRegistrationToken, ApiError> {
    let endpoint = match entity {
        Entity::Organization(org) => format!("{GITHUB}/orgs/{org}/{RUNNER_TOKEN}"),
        Entity::Repository(org, repo) => format!("{GITHUB}/repos/{org}/{repo}/{RUNNER_TOKEN}"),
    };
    let header_token = HeaderValue::try_from(format!("Token {api_token}"))
        .expect("Invalid bytes in GitHub API token");
    let headers = [(header::AUTHORIZATION, header_token)];
    let response = api_request(Method::POST, endpoint, &headers).await?;
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

pub async fn locate_runner_tarball(platform: &str) -> Result<Asset, ApiError> {
    let endpoint = format!("{GITHUB}/repos/actions/runner/releases/latest");
    let response = api_request(Method::GET, endpoint, &[]).await?;
    let releases: ReleasesPayload = { response }.json().await?;

    let full_release_re = Regex::new(&format!(
        "^actions-runner-{}-[0-9.]+.tar(?:\\.[a-z0-9]+)?$",
        regex::escape(&platform)
    ))
    .unwrap();

    (releases.assets.into_iter())
        .find(|a| full_release_re.is_match(&a.name))
        .ok_or(ApiError::NotFound)
}

#[derive(Debug, Display, From)]
pub enum DownloadError {
    #[display(fmt = "{}", _0)]
    SendRequest(SendRequestError),
    #[display(fmt = "{}", _0)]
    Payload(PayloadError),
    #[display(fmt = "{}", _0)]
    Io(io::Error),
    #[display(fmt = "Too many retries")]
    TooManyRetries,
}

pub async fn download(url: &str, to: &mut dyn io::Write) -> Result<(), DownloadError> {
    use futures_util::stream::StreamExt;
    let mut stream = Client::new()
        .get(url)
        .append_header((header::USER_AGENT, USER_AGENT))
        .send()
        .await?;
    while let Some(chunk) = stream.next().await {
        let bytes = chunk?;
        to.write(&bytes)?;
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
    actix_web::rt::System::new().block_on(locate_runner_tarball("linux-x64")).unwrap();
}
