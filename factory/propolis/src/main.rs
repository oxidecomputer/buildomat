/*
 * Copyright 2023 Oxide Computer Company
 */

#![allow(unused_imports)]

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{bail, Result};
use buildomat_common::*;
use getopts::Options;
use slog::{info, o, warn, Drain, Logger};

mod config;
mod db;
mod vm;

struct Central {
    log: Logger,
    client: buildomat_client::Client,
    config: config::ConfigFile,
    db: db::Database,
}

#[tokio::main]
async fn main() -> Result<()> {
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

    let log = make_log("factory-propolis");

    let config: config::ConfigFile = if let Some(f) = p.opt_str("f").as_deref()
    {
        read_toml(f)?
    } else {
        bail!("must specify configuration file (-f)");
    };

    let db = if let Some(p) = p.opt_str("d") {
        db::Database::open(log.clone(), p)?
    } else {
        bail!("must specify database file (-d)");
    };

    let client = buildomat_client::ClientBuilder::new(&config.general.baseurl)
        .bearer_token(&config.factory.token)
        .build()?;

    /*
     * Install a custom panic hook that will try to exit the process after a
     * short delay.  This is unfortunate, but I am not sure how else to avoid a
     * panicked worker thread leaving the process stuck without some of its
     * functionality.
     */
    let orig_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        orig_hook(info);
        eprintln!("FATAL: THREAD PANIC DETECTED; EXITING IN 5 SECONDS...");
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_secs(5));
            std::process::exit(101);
        });
    }));

    let c0 = Arc::new(Central { log, config, client, db });

    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
