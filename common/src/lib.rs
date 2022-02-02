use std::io::Read;
use std::str::FromStr;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::Result;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use reqwest::ClientBuilder;
use rusty_ulid::Ulid;
use serde::Deserialize;
use slog::{o, Drain, Logger};
#[macro_use]
extern crate diesel;

pub mod db;

pub fn read_toml<T>(n: &str) -> Result<T>
where
    for<'de> T: Deserialize<'de>,
{
    let mut f = std::fs::File::open(n)?;
    let mut buf: Vec<u8> = Vec::new();
    f.read_to_end(&mut buf)?;
    Ok(toml::from_slice(buf.as_slice())?)
}

pub fn make_log(name: &str) -> Logger {
    let dec = slog_term::TermDecorator::new().stdout().build();
    let dr = Mutex::new(
        slog_term::FullFormat::new(dec).use_original_order().build(),
    )
    .fuse();
    Logger::root(dr, o!("name" => name.to_string()))
}

pub fn to_ulid(id: &str) -> Result<Ulid> {
    Ok(Ulid::from_str(id)?)
}

pub async fn sleep_ms(ms: u64) {
    tokio::time::sleep(Duration::from_millis(ms)).await;
}

fn headers_client(dh: HeaderMap) -> Result<reqwest::Client> {
    Ok(ClientBuilder::new()
        .timeout(Duration::from_secs(15))
        .connect_timeout(Duration::from_secs(15))
        .default_headers(dh)
        .build()?)
}

pub fn bearer_client(token: &str) -> Result<reqwest::Client> {
    let mut dh = HeaderMap::new();
    dh.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {}", token)).unwrap(),
    );

    headers_client(dh)
}

pub fn delegated_client(token: &str, user: &str) -> Result<reqwest::Client> {
    let mut dh = HeaderMap::new();
    dh.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {}", token)).unwrap(),
    );
    dh.insert("X-Buildomat-Delegate", HeaderValue::from_str(user).unwrap());

    headers_client(dh)
}

pub fn genkey(len: usize) -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(|c| c as char)
        .collect()
}
