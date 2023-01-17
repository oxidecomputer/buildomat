#![allow(unused_imports)]

use std::time::Duration;

use serde::Deserialize;
use slog::Logger;
use thiserror::Error;

mod objstream;
pub mod types;
pub use objstream::{NamedObjectStream, ObjectStream};

#[derive(Error, Debug)]
pub enum Error {
    #[error("gerrit client: {0}")]
    General(String),
    #[error("gerrit client: {0}: request error: {1}")]
    Request(&'static str, reqwest::Error),
    #[error("gerrit client: {0}: invalid response: {1}")]
    InvalidResponse(&'static str, String),
    #[error("gerrit client: {0}: requested failed with status {1}")]
    BadStatus(&'static str, reqwest::StatusCode),
}

trait ErrorExt {
    fn for_req(self, name: &'static str) -> Error;
}

impl ErrorExt for reqwest::Error {
    fn for_req(self, name: &'static str) -> Error {
        if let Some(status) = self.status() {
            Error::BadStatus(name, status)
        } else {
            Error::Request(name, self)
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone)]
struct Creds {
    username: String,
    password: String,
}

pub struct Client {
    #[allow(unused)]
    log: Logger,
    client: reqwest::Client,
    url: String,
    creds: Option<Creds>,
}

pub struct ClientBuilder {
    log: Logger,
    url: String,
    creds: Option<Creds>,
}

impl ClientBuilder {
    pub fn new(log: Logger, url: &str) -> ClientBuilder {
        let url = url.trim_end_matches('/').to_string();
        ClientBuilder { log, url, creds: None }
    }

    pub fn creds(
        &mut self,
        username: &str,
        password: &str,
    ) -> &mut ClientBuilder {
        self.creds = Some(Creds {
            username: username.to_string(),
            password: password.to_string(),
        });
        self
    }

    pub fn build(self) -> Result<Client> {
        let client = reqwest::ClientBuilder::new()
            .redirect(reqwest::redirect::Policy::none())
            .user_agent("Callaghan")
            .connect_timeout(Duration::from_secs(15))
            .tcp_keepalive(Duration::from_secs(15))
            .timeout(Duration::from_secs(60))
            .build()
            .map_err(|e| Error::General(format!("building client: {}", e)))?;

        Ok(Client { log: self.log, client, creds: self.creds, url: self.url })
    }
}

impl Client {
    fn url(&self, x: &str) -> String {
        format!("{}/{}", self.url, x)
    }

    async fn get<O>(&self, name: &'static str, x: &str) -> Result<O>
    where
        for<'de> O: Deserialize<'de>,
    {
        let res = self
            .client
            .get(self.url(x))
            .send()
            .await
            .map_err(|e| e.for_req(name))?
            .error_for_status()
            .map_err(|e| e.for_req(name))?;

        Ok(parseres(name, res).await?)
    }

    pub async fn change_by_id(&self, id: &str) -> Result<types::Change> {
        self.get(
            "change_by_id",
            &format!("changes/{}/detail?o=ALL_REVISIONS&o=ALL_COMMITS", id),
        )
        .await
    }

    pub fn changes(&self) -> ObjectStream<types::Change> {
        ObjectStream::new(
            &self.client,
            objstream::ObjectStreamConfig {
                name: "changes",
                url: self.url("changes/"),
                creds: self.creds.clone(),
                query_base: vec![
                    ("o".into(), "CURRENT_REVISION".into()),
                    ("o".into(), "CURRENT_COMMIT".into()),
                    ("q".into(), "status:open".into()), /* XXX */
                    ("n".into(), "100".into()),         /* XXX */
                ],
            },
        )
    }

    pub fn projects(&self) -> NamedObjectStream<types::Project> {
        NamedObjectStream::new(
            &self.client,
            objstream::ObjectStreamConfig {
                name: "projects",
                url: self.url("projects/"),
                creds: self.creds.clone(),
                query_base: vec![("n".into(), "100".into()) /* XXX */],
            },
        )
    }
}

async fn parseres<O>(name: &'static str, res: reqwest::Response) -> Result<O>
where
    for<'de> O: Deserialize<'de>,
{
    let t = res
        .text()
        .await
        .map_err(|e| Error::InvalidResponse(name, e.to_string()))?;

    if let Some(body) = t.strip_prefix(")]}'\n") {
        Ok(serde_json::from_str(body)
            .map_err(|e| Error::InvalidResponse(name, e.to_string()))?)
    } else {
        Err(Error::InvalidResponse(
            name,
            "invalid Gerrit JSON (missing leader)".to_string(),
        ))
    }
}
