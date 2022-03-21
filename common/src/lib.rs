use std::io::Read;
use std::str::FromStr;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::Result;
use chrono::prelude::*;
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
        .timeout(Duration::from_secs(3600))
        .tcp_keepalive(Duration::from_secs(60))
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

pub trait UlidDateExt {
    fn creation(&self) -> DateTime<Utc>;
    fn age(&self) -> Duration;
}

impl UlidDateExt for Ulid {
    fn creation(&self) -> DateTime<Utc> {
        Utc.timestamp_millis(self.timestamp() as i64)
    }

    fn age(&self) -> Duration {
        let when = std::time::UNIX_EPOCH
            .checked_add(Duration::from_millis(self.timestamp()))
            .unwrap();
        std::time::SystemTime::now().duration_since(when).unwrap()
    }
}

pub fn guess_mime_type(filename: &str) -> String {
    if filename == "Cargo.lock" {
        /*
         * This file may be TOML, but is almost certainly plain text.
         */
        "text/plain".to_string()
    } else {
        new_mime_guess::from_path(std::path::PathBuf::from(filename))
            .first_or_octet_stream()
            .to_string()
    }
}
