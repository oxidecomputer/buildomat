/*
 * Copyright 2025 Oxide Computer Company
 */

//! SP-Test Factory for Buildomat
//!
//! This factory manages hardware testbeds for SP/ROT firmware testing.
//! It polls the buildomat server for jobs, allocates testbeds, and
//! coordinates test execution via sp-runner.
//!
//! # Architecture
//!
//! The factory is a persistent service that:
//! 1. Maintains a database of testbed instances
//! 2. Polls buildomat-server for jobs matching available targets
//! 3. Allocates testbeds to workers
//! 4. Runs buildomat-agent (which invokes sp-runner)
//! 5. Cleans up after job completion
//!
//! # Testbed Management
//!
//! Testbed configuration comes from a local TOML file. This can later
//! be replaced with an Inventron backend if dynamic allocation is needed.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, bail};
use buildomat_common::*;
use getopts::Options;
use slog::{Logger, info};

mod config;
mod db;
mod executor;
mod testbed;
mod worker;

/// Central state shared across all tasks.
pub struct Central {
    pub log: Logger,
    pub db: db::Database,
    pub config: config::ConfigFile,
    pub client: buildomat_client::Client,
    pub testbeds: testbed::TestbedManager,
}

#[tokio::main(worker_threads = 4)]
async fn main() -> Result<()> {
    usdt::register_probes().unwrap();

    let mut opts = Options::new();

    opts.optopt("f", "", "configuration file", "CONFIG");
    opts.optopt("d", "", "database file", "FILE");

    let p = match opts.parse(std::env::args().skip(1)) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("ERROR: usage: {}", e);
            eprintln!("       {}", opts.usage("usage"));
            std::process::exit(1);
        }
    };

    let log = make_log("factory-sp-test");

    let config: config::ConfigFile = if let Some(f) = p.opt_str("f").as_deref()
    {
        read_toml(f)?
    } else {
        bail!("must specify configuration file (-f)");
    };

    let db = if let Some(p) = p.opt_str("d") {
        db::Database::new(log.clone(), p, None)?
    } else {
        bail!("must specify database file (-d)");
    };

    // Build testbed manager from configuration
    let testbeds = testbed::TestbedManager::from_config(&config.testbed)?;

    // Clean up any instances for testbeds no longer in config
    for i in db.active_instances()? {
        if !testbeds.exists(&i.testbed_name) {
            info!(
                log,
                "instance {} for unconfigured testbed, marking destroyed",
                i.id()
            );
            db.instance_destroy(&i)?;
        }
    }

    let client = buildomat_client::ClientBuilder::new(&config.general.baseurl)
        .bearer_token(&config.factory.token)
        .build()?;

    let c0 = Arc::new(Central { log, config, client, db, testbeds });

    // Install panic hook that exits process on thread panic
    let orig_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        orig_hook(info);
        eprintln!("FATAL: THREAD PANIC DETECTED; EXITING IN 5 SECONDS...");
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_secs(5));
            std::process::exit(101);
        });
    }));

    // Start the factory worker task
    let c = Arc::clone(&c0);
    let t_factory = tokio::task::spawn(async move {
        worker::factory_worker(c)
            .await
            .map_err(|e| anyhow::anyhow!("factory worker task failure: {}", e))
    });

    tokio::select! {
        r = t_factory => {
            match r {
                Ok(Ok(())) => bail!("factory worker task stopped unexpectedly"),
                Ok(Err(e)) => bail!("factory worker error: {}", e),
                Err(e) => bail!("factory worker panic: {}", e),
            }
        }
    }
}
