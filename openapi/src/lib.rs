use std::time::Duration;

use anyhow::{bail, Result};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};

pub mod gen {
    use super::*;

    progenitor::generate_api!(spec = "openapi.json", interface = Positional);
}

pub mod prelude {
    pub use super::gen::prelude::*;
    pub use futures::StreamExt;
}
pub use gen::{types, Client, Error};

pub struct ClientBuilder {
    url: String,
    token: Option<String>,
    delegate_user: Option<String>,
}

impl ClientBuilder {
    pub fn new(url: &str) -> ClientBuilder {
        ClientBuilder { url: url.to_string(), token: None, delegate_user: None }
    }

    pub fn bearer_token<S: AsRef<str>>(&mut self, token: S) -> &mut Self {
        self.token = Some(token.as_ref().to_string());
        self
    }

    pub fn delegated_user<S: AsRef<str>>(&mut self, user: S) -> &mut Self {
        self.delegate_user = Some(user.as_ref().to_string());
        self
    }

    pub fn build(&mut self) -> Result<Client> {
        let mut dh = HeaderMap::new();

        if let Some(user) = self.delegate_user.as_deref() {
            if self.token.is_none() {
                bail!("delegated authentication requires a bearer token");
            }

            dh.insert(
                "X-Buildomat-Delegate",
                HeaderValue::from_str(user).unwrap(),
            );
        }

        if let Some(token) = self.token.as_deref() {
            dh.insert(
                AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {}", token)).unwrap(),
            );
        }

        let client = reqwest::ClientBuilder::new()
            .timeout(Duration::from_secs(3600))
            .tcp_keepalive(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(15))
            .default_headers(dh)
            .build()?;

        Ok(Client::new_with_client(&self.url, client))
    }
}
