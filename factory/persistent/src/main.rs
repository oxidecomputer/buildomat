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
mod factory;
mod process;
mod worker_ops;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use std::time::Duration;

use anyhow::{bail, Result};
use buildomat_common::*;
use getopts::Options;
use slog::{error, info, warn};
use tokio::signal::unix::SignalKind;

use config::ConfigFile;
use factory::{factory_loop, resolve_targets, shutdown, Central};

/// Maximum time to spend on graceful shutdown before giving up.
/// Budget: ~10s graceful_kill + ~20s for network calls.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

/// Base sleep between factory_loop iterations on success.
const LOOP_INTERVAL: Duration = Duration::from_secs(1);

/// Maximum sleep between factory_loop iterations on repeated errors.
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

    let target_names: Vec<&str> =
        config.target.keys().map(|s| s.as_str()).collect();
    info!(log, "persistent factory starting";
        "targets" => ?target_names,
        "command" => &config.execution.command,
        "job_dir" => %config.execution.job_dir.display(),
    );

    let client = buildomat_client::ClientBuilder::new(&config.general.baseurl)
        .bearer_token(&config.factory.token)
        .build()?;

    let mut central = Central {
        log,
        client,
        config,
        instances: Vec::new(),
        target_map: std::collections::HashMap::new(),
    };

    resolve_targets(&mut central).await?;

    let mut sigterm = tokio::signal::unix::signal(SignalKind::terminate())?;
    let mut sigint = tokio::signal::unix::signal(SignalKind::interrupt())?;
    let mut sighup = tokio::signal::unix::signal(SignalKind::hangup())?;

    let mut error_backoff = LOOP_INTERVAL;

    loop {
        tokio::select! {
            _ = sigterm.recv() => {
                info!(central.log, "received SIGTERM, shutting down");
                break;
            }
            _ = sigint.recv() => {
                info!(central.log, "received SIGINT, shutting down");
                break;
            }
            _ = sighup.recv() => {
                info!(central.log, "received SIGHUP, shutting down");
                break;
            }
            _ = async {
                match factory_loop(&mut central).await {
                    Ok(()) => {
                        error_backoff = LOOP_INTERVAL;
                        tokio::time::sleep(LOOP_INTERVAL).await;
                    }
                    Err(e) => {
                        error!(central.log, "factory loop error: {:?}", e;
                            "retry_in" => ?error_backoff);
                        tokio::time::sleep(error_backoff).await;
                        error_backoff = (error_backoff * 2).min(MAX_ERROR_BACKOFF);
                    }
                }
            } => {}
        }
    }

    tokio::select! {
        _ = shutdown(&mut central) => {}
        _ = sigterm.recv() => {
            warn!(central.log,
                "received second signal during shutdown, forcing exit");
        }
        _ = sigint.recv() => {
            warn!(central.log,
                "received second signal during shutdown, forcing exit");
        }
        _ = sighup.recv() => {
            warn!(central.log,
                "received second signal during shutdown, forcing exit");
        }
        _ = tokio::time::sleep(SHUTDOWN_TIMEOUT) => {
            warn!(central.log,
                "shutdown timed out after {:?}, forcing exit",
                SHUTDOWN_TIMEOUT);
        }
    }

    info!(central.log, "factory exited");
    Ok(())
}
