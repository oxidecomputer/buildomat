/*
 * Copyright 2024 Oxide Computer Company
 */

use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use bytes::Bytes;
use hyper::body::Frame;
use slog::{error, info, Logger};

pub struct Stopwatch {
    start: Instant,
    info: String,
    offset: u64,
    bytes_expected: u64,
    bytes_transferred: u64,
}

impl Stopwatch {
    pub fn start(info: String, offset: u64, bytes_expected: u64) -> Stopwatch {
        Stopwatch {
            start: Instant::now(),
            info,
            offset,
            bytes_expected,
            bytes_transferred: 0,
        }
    }

    pub fn add_bytes(&mut self, bytes: usize) {
        self.bytes_transferred =
            self.bytes_transferred.saturating_add(bytes.try_into().unwrap());
    }

    fn complete_common(&self) -> (Duration, f64) {
        let dur = Instant::now().saturating_duration_since(self.start);
        let rate_mb = (self.bytes_transferred as f64 / dur.as_secs_f64())
            / (1024.0 * 1024.0);

        (dur, rate_mb)
    }

    pub fn complete(self, log: &Logger) {
        let (dur, rate_mb) = self.complete_common();

        info!(log, "download complete: {}", self.info;
            "offset" => self.offset,
            "bytes_transferred" => self.bytes_transferred,
            "rate_mb" => rate_mb,
            "msec" => dur.as_millis(),
        );
    }

    pub fn fail(self, log: &Logger, how: &str) -> Result<Frame<Bytes>> {
        let (dur, rate_mb) = self.complete_common();
        let msg = format!("download failed: {}: {}", self.info, how);

        error!(log, "{}", msg;
            "offset" => self.offset,
            "bytes_expected" => self.bytes_expected,
            "bytes_transferred" => self.bytes_transferred,
            "rate_mb" => rate_mb,
            "msec" => dur.as_millis(),
        );

        Err(anyhow!(msg))
    }
}
