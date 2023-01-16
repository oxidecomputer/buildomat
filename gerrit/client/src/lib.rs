#![allow(unused_imports)]

use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use serde::Deserialize;
use slog::Logger;

mod objstream;
pub mod types;
pub use objstream::ObjectStream;

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
            .build()?;

        Ok(Client { log: self.log, client, creds: self.creds, url: self.url })
    }
}

impl Client {
    fn url(&self, x: &str) -> String {
        format!("{}/{}", self.url, x)
    }

    pub fn change_list(&self) -> ObjectStream<types::Change> {
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
}

async fn parseres<O>(res: reqwest::Response) -> Result<O>
where
    for<'de> O: Deserialize<'de>,
{
    let t = res.text().await?;
    if let Some(body) = t.strip_prefix(")]}'\n") {
        Ok(serde_json::from_str(body)?)
    } else {
        bail!("invalid Gerrit JSON (missing leader)");
    }
}
