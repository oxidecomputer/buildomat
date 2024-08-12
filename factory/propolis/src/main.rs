/*
 * Copyright 2024 Oxide Computer Company
 */

use std::{sync::Arc, time::Duration};

use anyhow::{bail, Context, Result};
use buildomat_common::*;
use getopts::Options;
use slog::{info, o, Logger};

mod config;
mod db;
mod factory;
mod net;
mod nocloud;
mod propolis;
mod serial;
mod svc;
mod ucred;
mod vm;
mod zones;

struct Central {
    log: Logger,
    client: buildomat_client::Client,
    config: config::ConfigFile,
    db: db::Database,
    serial: serial::Serial,
}

/*
 * This factory runs on large AMD machines (e.g., 100+ SMT threads) and thus
 * ends up with far too many worker threads by default.
 */
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

    let log = make_log("factory-propolis");

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

    let sockdir = config.socketdir()?;
    if !sockdir.exists() {
        info!(log, "creating socket directory at {sockdir:?}");
        std::fs::create_dir(&sockdir)?;
    }
    let serial =
        serial::Serial::new(log.new(o!("component" => "serial")), sockdir)?;

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

    let c0 = Arc::new(Central { log, config, client, db, serial });

    let c = Arc::clone(&c0);
    let t_vm = tokio::task::spawn(async move {
        vm::vm_worker(c).await.context("VM worker task failure")
    });

    let c = Arc::clone(&c0);
    let t_factory = tokio::task::spawn(async move {
        factory::factory_task(c).await.context("factory task failure")
    });

    tokio::select! {
        _ = t_vm => bail!("VM worker task stopped early"),
        _ = t_factory => bail!("factory task stopped early"),
    }
}
