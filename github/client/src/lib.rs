/*
 * Copyright 2024 Oxide Computer Company
 */

const USER_AGENT: &str = "buildomat-github-integration/0";
const GITHUB_API_URL: &str = "https://api.github.com";

use std::time::Duration;

use anyhow::Result;
pub use octorust::auth::JWTCredentials;
use octorust::auth::{Credentials, InstallationTokenGenerator};
pub use octorust::types;
pub use octorust::Client;

fn mk_reqwest_client() -> Result<reqwest::Client> {
    Ok(reqwest::ClientBuilder::new()
        .timeout(Duration::from_secs(45))
        .tcp_keepalive(Duration::from_secs(45))
        .connect_timeout(Duration::from_secs(30))
        .build()?)
}

pub fn app_client(jwt: JWTCredentials) -> Result<Client> {
    Ok(Client::custom(
        GITHUB_API_URL,
        USER_AGENT,
        Credentials::JWT(jwt),
        mk_reqwest_client()?,
    ))
}

pub fn install_client(jwt: JWTCredentials, install_id: i64) -> Result<Client> {
    let iat = InstallationTokenGenerator::new(install_id.try_into()?, jwt);

    Ok(Client::custom(
        GITHUB_API_URL,
        USER_AGENT,
        Credentials::InstallationToken(iat),
        mk_reqwest_client()?,
    ))
}
