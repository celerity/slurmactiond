use awc::{Client, ClientRequest, http::header};
use awc::error::{JsonPayloadError, SendRequestError};
use awc::http::Method;
use derive_more::{Display, From};
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
    #[display(fmt = "{}", _0)]
    Payload(JsonPayloadError),
    #[display(fmt = "resource not found")]
    NotFound,
}

impl std::error::Error for ApiError {}

const GITHUB: &str = "https://api.github.com";
const RUNNER_TOKEN: &str = "actions/runners/registration-token";

fn api_request(method: Method, url: String) -> ClientRequest {
    Client::new()
        .request(method, url)
        .append_header((header::ACCEPT, "application/vnd.github.v3+json"))
        .append_header((header::USER_AGENT, "slurmactiond"))
}

pub async fn generate_runner_registration_token(
    entity: &Entity,
    token: &ApiToken,
) -> Result<RunnerRegistrationToken, ApiError> {
    let endpoint = match entity {
        Entity::Organization(org) => format!("{GITHUB}/orgs/{org}/{RUNNER_TOKEN}"),
        Entity::Repository(org, repo) => format!("{GITHUB}/repos/{org}/{repo}/{RUNNER_TOKEN}"),
    };
    let request = api_request(Method::POST, endpoint)
        .append_header((header::AUTHORIZATION, format!("Token {token}")));
    let response = request.send().await?;
    let payload: TokenPayload = { response }.json().await?;
    Ok(payload.token)
}

#[derive(Debug, Deserialize)]
pub struct Asset {
    pub name: String,
    #[serde(rename = "browser_download_url")]
    pub url: String,
}

#[derive(Deserialize)]
struct ReleasesPayload {
    assets: Vec<Asset>,
}

pub async fn locate_runner_tarball(platform: &str) -> Result<Asset, ApiError> {
    let request = api_request(
        Method::GET,
        format!("{GITHUB}/repos/actions/runner/releases/latest"),
    );
    let releases: ReleasesPayload = request.send().await?.json().await?;

    let tarball_re = Regex::new(&format!(
        "^actions-runner-{}-[0-9.]+.tar(?:\\.[a-z0-9]+)?$",
        regex::escape(&platform)
    ))
        .unwrap();

    (releases.assets.into_iter())
        .find(|a| tarball_re.is_match(&a.name))
        .ok_or(ApiError::NotFound)
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
    let tarball = actix_web::rt::System::new().block_on(locate_runner_tarball("linux-x64"));
    assert!(tarball.is_ok());
}
