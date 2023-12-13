/*
 * Copyright 2023 Oxide Computer Company
 */

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use anyhow::{bail, Context, Result};
use buildomat_common::*;
use chrono::prelude::*;
use getopts::Options;
use slog::{info, warn, Logger};
use std::time::{Duration, Instant};

mod config;
mod db;
mod host;
mod minder;
mod pty;
mod worker;

const MARKER_BOOT: &str = "wcnCroia6U";
const MARKER_HOLD: &str = "xcvsh0tfvd";

#[derive(Default)]
struct HostState {
    last_dialtone: Option<Instant>,
    last_cycle: Option<Instant>,
    need_reset: bool,
}

struct Host {
    config: config::ConfigFileHost,
    state: Mutex<HostState>,
}

impl HostState {
    fn has_dialtone(&self) -> bool {
        if self.need_reset {
            /*
             * If a reset is pending, ignore a dialtone in case it is forged.
             */
            false
        } else if let Some(last) = self.last_dialtone {
            /*
             * If a server is holding at iPXE, we expect it to emit a dialtone
             * on the serial console every five seconds.  If we have not seen a
             * dialtone for 20 seconds, something has probably gone wrong.
             */
            Instant::now().saturating_duration_since(last).as_secs() < 20
        } else {
            false
        }
    }

    fn since_last_cycle(&self) -> Duration {
        if let Some(last) = self.last_cycle {
            Instant::now().saturating_duration_since(last)
        } else {
            Duration::MAX
        }
    }

    fn record_dialtone(&mut self) {
        if !self.need_reset {
            self.last_dialtone = Some(Instant::now());
        }
    }

    fn need_cycle(&self) -> bool {
        self.need_reset
    }

    fn record_cycle(&mut self) {
        self.need_reset = false;
        self.last_cycle = Some(Instant::now());
    }

    fn reset(&mut self) {
        self.last_dialtone = None;
        self.need_reset = true;
        self.last_cycle = None;
    }
}

struct Activity {
    nodename: String,
    time: DateTime<Utc>,
    message: Message,
}

enum Message {
    Status(String),
    SerialOutput(String),
}

struct ActivityBuilder {
    nodename: String,
}

impl ActivityBuilder {
    fn status(&self, msg: &str) -> Activity {
        Activity {
            nodename: self.nodename.to_string(),
            message: Message::Status(msg.to_string()),
            time: Utc::now(),
        }
    }

    fn serial(&self, msg: &str) -> Activity {
        Activity {
            nodename: self.nodename.to_string(),
            message: Message::SerialOutput(msg.to_string()),
            time: Utc::now(),
        }
    }
}

struct Central {
    log: Logger,
    db: db::Database,
    config: config::ConfigFile,
    client: buildomat_client::Client,
    hosts: HashMap<String, Host>,
    tx: Mutex<std::sync::mpsc::Sender<Activity>>,
}

#[tokio::main(worker_threads = 4)]
async fn main() -> Result<()> {
    let mut opts = Options::new();

    opts.optopt("b", "", "bind address:port", "BIND_ADDRESS");
    opts.optopt("f", "", "configuration file", "CONFIG");
    opts.optopt("d", "", "database file", "FILE");
    opts.optopt("S", "", "dump OpenAPI schema", "FILE");

    let p = match opts.parse(std::env::args().skip(1)) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("ERROR: usage: {}", e);
            eprintln!("       {}", opts.usage("usage"));
            std::process::exit(1);
        }
    };

    let bind_address =
        p.opt_str("b").as_deref().unwrap_or("0.0.0.0:9969").parse()?;

    if let Some(s) = p.opt_str("S") {
        return minder::dump_api(s);
    }

    let log = make_log("factory-lab");

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

    let hosts = config
        .host
        .iter()
        .map(|(id, cfg)| {
            (
                id.to_string(),
                Host {
                    config: cfg.clone(),
                    state: Mutex::new(Default::default()),
                },
            )
        })
        .collect::<HashMap<_, _>>();

    /*
     * Mark invalid any instances that use hosts that no longer appear in the
     * configuration file.
     */
    for i in db.active_instances()? {
        let destroy = if let Some(host) = hosts.get(&i.nodename) {
            if host.config.debug_os_dir.is_some() {
                warn!(
                    log,
                    "instance {} for host moved to debug mode, destroying",
                    i.id(),
                );
                true
            } else {
                info!(log, "instance {} still active", i.id());
                false
            }
        } else {
            warn!(log, "instance {} for unconfigured host, destroying", i.id());
            true
        };

        if destroy {
            db.instance_destroy(&i)?;
        }
    }

    /*
     * For each host that does not have an instance, we want to trigger a reset.
     * Hosts that have an instance may already be performing work on behalf of
     * jobs, and we do not wish to disturb them when the factory restarts.
     */
    for h in hosts.iter() {
        if db.instance_for_host(&h.1.config.nodename)?.is_some() {
            continue;
        }

        info!(log, "startup reset of host {}", h.1.config.nodename);
        h.1.state.lock().unwrap().reset();
    }

    let client = buildomat_client::ClientBuilder::new(&config.general.baseurl)
        .bearer_token(&config.factory.token)
        .build()?;

    let (tx, rx) = std::sync::mpsc::channel();
    let c0 = Arc::new(Central {
        log,
        config,
        client,
        hosts,
        db,
        tx: Mutex::new(tx),
    });

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

    /*
     * Create our local web server for interaction with iPXE and the OS on
     * hosts.
     */
    let server = minder::server(&c0, bind_address).await?;
    let t_server = server.start();

    /*
     * Create the background threads that will manage hosts via IPMI.
     */
    host::start_manager(Arc::clone(&c0), rx);

    let c = Arc::clone(&c0);
    let t_lab = tokio::task::spawn(async move {
        worker::lab_worker(c).await.context("lab worker task failure")
    });
    let c = Arc::clone(&c0);
    let t_upload = tokio::task::spawn(async move {
        worker::upload_worker(c).await.context("upload worker task failure")
    });

    loop {
        tokio::select! {
            _ = t_lab => bail!("lab worker task stopped early"),
            _ = t_upload => bail!("upload worker task stopped early"),
            _ = t_server => bail!("server task stopped early"),
        }
    }
}
