#![allow(unused_imports)]

/*
 * Copyright 2025 Oxide Computer Company
 */

use std::{
    collections::HashMap,
    os::unix::{fs::OpenOptionsExt, io::AsRawFd},
    path::Path,
    process::{Command, Stdio},
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{anyhow, bail, Context, Result};
use buildomat_types::metadata;
use debug_parser::ValueKind;

use buildomat_common::*;
use disks::Slot;
use getopts::Options;
use humility::{HiffyCaller, PathStep, ValueExt};
use iddqd::{id_upcast, IdHashItem, IdHashMap};
use slog::{info, o, Logger};

mod cleanup;
mod config;
mod db;
mod disks;
mod efi;
mod factory;
mod instance;
mod host;
mod humility;
mod pipe;

pub struct App {
    log: Logger,
    client: buildomat_client::Client,
    config: config::Config,
    db: db::Database,
    hosts: IdHashMap<host::HostManager>,
}

impl App {
    fn metadata(
        &self,
        t: &config::ConfigTarget,
    ) -> Result<metadata::FactoryMetadata> {
        /*
         * XXX
         */
        self.config.diag.apply_overrides(&t.diag).build()
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct HostId {
    model: String,
    serial: String,
}

impl FromStr for HostId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /*
         * HostId IDs should have two non-empty components separated by slashes;
         * e.g., "gimlet/BRM42220010".  These are used as keys in the
         * configuration file, and for display in logs, etc.
         */
        let t = s.split('/').collect::<Vec<_>>();
        if t.len() != 2
            || t.iter().any(|s| {
                s.trim().is_empty()
                    || s.trim() != *s
                    || s.chars().any(|c| !c.is_ascii_alphanumeric())
            })
        {
            bail!("invalid host ID {s:?}");
        }

        Ok(HostId { model: t[0].to_string(), serial: t[1].to_string() })
    }
}

impl std::fmt::Display for HostId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let HostId { model, serial } = self;

        write!(f, "{model}/{serial}")
    }
}

#[tokio::main(worker_threads = 4)]
async fn main() -> Result<()> {
    usdt::register_probes().unwrap();

    let mut opts = Options::new();

    /*
     * This option is only used when the program is copied onto the target
     * system and executed there to clean up detritus from past jobs and set up
     * the basic environment we need for the next job:
     */
    opts.optflag("C", "", "cleanup the local system (destructive!)");
    opts.optflag("S", "", "setup the local system");

    opts.optopt("f", "", "configuration file", "CONFIG");
    opts.optopt("d", "", "database file", "FILE");

    let p = opts
        .parsing_style(getopts::ParsingStyle::FloatingFrees)
        .parse(std::env::args_os().skip(1))?;

    if !p.free.is_empty() {
        bail!("unexpected arguments");
    }

    if p.opt_present("S") {
        return cleanup::setup();
    }

    if p.opt_present("C") {
        return cleanup::cleanup();
    }

    let log = make_log("factory-gimlet");

    let config = if let Some(f) = p.opt_str("f").as_deref() {
        config::load(f)?
    } else {
        bail!("must specify configuration file (-f)");
    };

    let db = if let Some(p) = p.opt_str("d") {
        db::Database::new(log.clone(), p, None)?
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

    let mut app0 = App { log, client, hosts: Default::default(), config, db };

    /*
     * Start a host manager thread for each configured host:
     */
    for h in app0.config.hosts.iter() {
        app0.hosts
            .insert_unique(host::HostManager::new(
                app0.log.new(o!(
                    "component" => "hostmgr",
                    "host" => h.id.to_string(),
                )),
                h.clone(),
                app0.config.tools.clone(),
            ))
            .unwrap();
    }

    let app0 = Arc::new(app0);

    /*
     * Interaction with the buildomat server is performed in this task.
     */
    let app = Arc::clone(&app0);
    let t_factory = tokio::task::spawn(async move {
        factory::factory_task(app).await.context("factory task failure")
    });

    /*
     * Per-instance management tasks are driven by this task:
     */
    let app = Arc::clone(&app0);
    let t_instance = tokio::task::spawn(async move {
        instance::instance_worker(app).await.context("instance task failure")
    });

    tokio::select! {
        _ = t_factory => bail!("factory task stopped early"),
        _ = t_instance => bail!("instance task stopped early"),
    }
}
