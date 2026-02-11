/*
 * Copyright 2026 Oxide Computer Company
 */

//! Generic persistent factory for buildomat.
//!
//! This factory accepts jobs for configured targets, invokes an external
//! command to execute the workload, and reports results back to the buildomat
//! server. All domain-specific knowledge (Hubris, USB probes, etc.) lives in
//! the external command, keeping this factory generic.

mod config;

use std::time::Duration;

use anyhow::{bail, Result};
use buildomat_common::*;
use getopts::Options;
use slog::{error, info};
use tokio::signal::unix::SignalKind;

use config::ConfigFile;

/// Base sleep between main loop iterations on success.
const LOOP_INTERVAL: Duration = Duration::from_secs(1);

/// Maximum sleep between main loop iterations on repeated errors.
const MAX_ERROR_BACKOFF: Duration = Duration::from_secs(60);

#[tokio::main]
async fn main() -> Result<()> {
    let mut opts = Options::new();
    opts.optopt("f", "", "configuration file", "CONFIG");

    let p = match opts.parse(std::env::args().skip(1)) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("ERROR: usage: {}", e);
            eprintln!("       {}", opts.usage("usage"));
            std::process::exit(1);
        }
    };

    let log = make_log("factory-persistent");

    let config: ConfigFile = if let Some(f) = p.opt_str("f").as_deref() {
        read_toml(f)?
    } else {
        bail!("must specify configuration file (-f)");
    };

    if config.target.is_empty() {
        bail!("must specify at least one [target.*] in configuration");
    }

    info!(log, "persistent factory starting";
        "targets" => ?config.targets(),
        "command" => &config.execution.command,
        "args" => ?config.execution.args,
        "job_dir" => %config.execution.job_dir.display(),
    );

    let client = buildomat_client::ClientBuilder::new(&config.general.baseurl)
        .bearer_token(&config.factory.token)
        .build()?;

    let mut sigterm = tokio::signal::unix::signal(SignalKind::terminate())?;
    let mut sigint = tokio::signal::unix::signal(SignalKind::interrupt())?;
    let mut sighup = tokio::signal::unix::signal(SignalKind::hangup())?;

    let mut error_backoff = LOOP_INTERVAL;

    loop {
        tokio::select! {
            _ = sigterm.recv() => {
                info!(log, "received SIGTERM, shutting down");
                break;
            }
            _ = sigint.recv() => {
                info!(log, "received SIGINT, shutting down");
                break;
            }
            _ = sighup.recv() => {
                info!(log, "received SIGHUP, shutting down");
                break;
            }
            _ = async {
                match client.factory_ping().send().await {
                    Ok(_) => {
                        error_backoff = LOOP_INTERVAL;
                        tokio::time::sleep(LOOP_INTERVAL).await;
                    }
                    Err(e) => {
                        error!(log, "factory ping error: {:?}", e;
                            "retry_in" => ?error_backoff);
                        tokio::time::sleep(error_backoff).await;
                        error_backoff = (error_backoff * 2).min(MAX_ERROR_BACKOFF);
                    }
                }
            } => {}
        }
    }

    info!(log, "factory exited");
    Ok(())
}
