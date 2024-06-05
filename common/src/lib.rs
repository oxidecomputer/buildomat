/*
 * Copyright 2024 Oxide Computer Company
 */

use std::io::{IsTerminal, Read};
use std::path::Path;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use anyhow::Result;
use chrono::prelude::*;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use regex::Regex;
use rusty_ulid::Ulid;
use serde::Deserialize;
use slog::{o, Drain, Logger};

pub fn read_toml<P: AsRef<Path>, T>(n: P) -> Result<T>
where
    for<'de> T: Deserialize<'de>,
{
    let mut f = std::fs::File::open(n.as_ref())?;
    let mut buf: Vec<u8> = Vec::new();
    f.read_to_end(&mut buf)?;
    Ok(toml::from_slice(buf.as_slice())?)
}

pub fn make_log(name: &'static str) -> Logger {
    let filter_level = match std::env::var("BUILDOMAT_DEBUG")
        .map(|v| v.to_ascii_lowercase())
        .as_deref()
    {
        Ok("yes") | Ok("1") | Ok("true") => slog::Level::Debug,
        _ => slog::Level::Info,
    };

    if std::io::stdout().is_terminal() {
        /*
         * Use a terminal-formatted logger for interactive processes.
         */
        let dec = slog_term::TermDecorator::new().stdout().build();
        let dr = Mutex::new(
            slog_term::FullFormat::new(dec).use_original_order().build(),
        )
        .filter_level(filter_level)
        .fuse();
        Logger::root(dr, o!("name" => name))
    } else {
        /*
         * Otherwise, emit bunyan-formatted records:
         */
        let dr = Mutex::new(
            slog_bunyan::with_name(name, std::io::stdout())
                .set_flush(true)
                .build(),
        )
        .filter_level(filter_level)
        .fuse();
        Logger::root(dr, o!())
    }
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

/**
 * Guess at whether this is a log file based on the filename.  Try to handle
 * both regular ".log" files and log files that have been rotated using an
 * integer suffix, e.g., ".log.0".
 */
pub fn guess_is_log_path(filename: &str) -> bool {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\.log(\.[0-9]+)?$").unwrap())
        .is_match(filename)
}

pub fn guess_mime_type(filename: &str) -> String {
    if filename == "Cargo.lock" {
        /*
         * This file may be TOML, but is almost certainly plain text.
         */
        "text/plain".to_string()
    } else if guess_is_log_path(filename) {
        /*
         * Treat any file that looks like it might be a log file as plain text.
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

/**
 * Make sure this string is not one we might mistake as a ULID.  We are somewhat
 * expansive in exclusion here: any 26 character ID composed only of letters and
 * numbers is considered ULID-like.
 */
pub fn looks_like_a_ulid(s: &str) -> bool {
    s.len() == 26 && s.chars().all(|c| c.is_ascii_alphanumeric())
}
