/*
 * Copyright 2022 Oxide Computer Company
 */

use std::io::Read;
use std::str::FromStr;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::Result;
use chrono::prelude::*;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
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
    .filter_level(slog::Level::Debug)
    .fuse();
    Logger::root(dr, o!("name" => name.to_string()))
}

pub fn to_ulid(id: &str) -> Result<Ulid> {
    Ok(Ulid::from_str(id)?)
}

pub async fn sleep_ms(ms: u64) {
    tokio::time::sleep(Duration::from_millis(ms)).await;
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
        Utc.timestamp_millis_opt(self.timestamp().try_into().unwrap()).unwrap()
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

pub trait OutputExt {
    fn info(&self) -> String;
}

impl OutputExt for std::process::Output {
    fn info(&self) -> String {
        let mut out = String::new();

        if let Some(code) = self.status.code() {
            out.push_str(&format!("exit code {}", code));
        }

        /*
         * Attempt to render stderr from the command:
         */
        let stderr = String::from_utf8_lossy(&self.stderr).trim().to_string();
        let extra = if stderr.is_empty() {
            /*
             * If there is no stderr output, this command might emit its
             * failure message on stdout:
             */
            String::from_utf8_lossy(&self.stdout).trim().to_string()
        } else {
            stderr
        };

        if !extra.is_empty() {
            if !out.is_empty() {
                out.push_str(": ");
            }
            out.push_str(&extra);
        }

        out
    }
}

pub trait DurationExt {
    fn render(&self) -> String;
}

impl DurationExt for std::time::Duration {
    fn render(&self) -> String {
        let mut out = String::new();
        let mut secs = self.as_secs();
        let hours = secs / 3600;
        if hours > 0 {
            secs -= hours * 3600;
            out += &format!(" {} h", hours);
        }
        let minutes = secs / 60;
        if minutes > 0 || hours > 0 {
            secs -= minutes * 60;
            out += &format!(" {} m", minutes);
        }
        out += &format!(" {} s", secs);

        out.trim().to_string()
    }
}

pub trait DateTimeExt {
    fn age(&self) -> Duration;
}

impl DateTimeExt for DateTime<Utc> {
    fn age(&self) -> Duration {
        if let Ok(dur) = Utc::now().signed_duration_since(*self).to_std() {
            dur
        } else {
            Duration::from_secs(0)
        }
    }
}

pub trait ClientJobExt {
    fn duration(&self, from: &str, until: &str) -> Option<Duration>;
}

impl ClientJobExt for buildomat_client::types::Job {
    fn duration(&self, from: &str, until: &str) -> Option<Duration> {
        let from = if let Some(from) = self.times.get(from) {
            from
        } else {
            return None;
        };
        let until = if let Some(until) = self.times.get(until) {
            until
        } else {
            return None;
        };

        if let Ok(dur) = until.signed_duration_since(*from).to_std() {
            if dur.is_zero() {
                None
            } else {
                Some(dur)
            }
        } else {
            None
        }
    }
}

pub trait ClientIdExt {
    fn id(&self) -> Result<Ulid>;
}

impl ClientIdExt for buildomat_client::types::Worker {
    fn id(&self) -> Result<Ulid> {
        to_ulid(&self.id)
    }
}

impl ClientIdExt for buildomat_client::types::Job {
    fn id(&self) -> Result<Ulid> {
        to_ulid(&self.id)
    }
}
