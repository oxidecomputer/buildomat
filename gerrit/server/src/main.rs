#![allow(unused_imports)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use buildomat_common::*;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use slog::{debug, error, info, o, trace, warn, Logger};
use tokio::sync::Mutex;

mod api;
mod config;

struct App {
    config: config::ConfigFile,
    log: Logger,
    gerrits: BTreeMap<String, callaghan_gerrit_client::Client>,
    data: Mutex<Data>,
}

struct Revision {
    kind: String,
    number: u32,
    commit: String,
    message: String,
}

impl Revision {
    fn bugs(&self) -> Vec<String> {
        self.message
            .lines()
            .filter_map(|l| {
                l.trim().split_ascii_whitespace().next().filter(|l| {
                    l.starts_with("stlouis#")
                        || l.starts_with("oxidecomputer/stlouis#")
                })
            })
            .map(str::to_string)
            .collect::<Vec<_>>()
    }
}

struct Change {
    number: u32,
    id: String,
    revisions: BTreeMap<u32, Revision>,
}

#[derive(Default)]
struct Project {
    changes: BTreeMap<String, Change>,
}

#[derive(Default)]
struct Gerrit {
    projects: BTreeMap<String, Project>,
}

#[derive(Default)]
struct Data {
    gerrits: BTreeMap<String, Gerrit>,
}

async fn process_changes(
    app: &App,
    gerrit: &str,
    project: &str,
    client: &callaghan_gerrit_client::Client,
) -> Result<()> {
    let log = &app.log;

    info!(log, "listing changes for {:?}/{:?}", gerrit, project);

    let mut changes = client.changes();
    while let Some(c) = changes.try_next().await? {
        if c.project != project {
            continue;
        }

        let mut data = app.data.lock().await;

        let g = data.gerrits.entry(gerrit.to_string()).or_default();
        let p = g.projects.entry(project.to_string()).or_default();

        if !p.changes.contains_key(&c.change_id) {
            info!(log, "found new change {}: {}", c._number, c.change_id);
            p.changes.insert(
                c.change_id.to_string(),
                Change {
                    id: c.change_id.to_string(),
                    number: c._number,
                    revisions: Default::default(),
                },
            );
        }

        let dc = p.changes.get_mut(&c.change_id).unwrap();

        let full = client.change_by_id(&c.change_id).await?;
        for rev in full.revisions() {
            if dc.revisions.contains_key(&rev._number) {
                continue;
            }

            info!(
                log,
                "found new revision {}/{} -> {}",
                c.change_id,
                rev._number,
                rev.commit_id(),
            );

            dc.revisions.insert(
                rev._number,
                Revision {
                    kind: rev.kind.to_string(),
                    number: rev._number,
                    commit: rev.commit_id().to_string(),
                    message: rev.commit.message.to_string(),
                },
            );
        }
    }

    Ok(())
}

async fn process_projects(
    app: &App,
    gerrit: &str,
    client: &callaghan_gerrit_client::Client,
) -> Result<()> {
    let log = &app.log;

    info!(log, "listing projects for {:?}", gerrit);

    let mut projects = client.projects();
    while let Some((name, p)) = projects.try_next().await? {
        /*
         * We always ignore the system projects:
         */
        let lcname = name.to_ascii_lowercase();
        if lcname == "all-users" || lcname == "all-projects" {
            continue;
        }

        let mut data = app.data.lock().await;

        let g = data.gerrits.entry(gerrit.to_string()).or_default();
        if g.projects.contains_key(&name) {
            continue;
        }

        info!(log, "found new project {:?} -> {:?}", name, p);
        g.projects.insert(name, Project::default());
    }

    Ok(())
}

async fn bgtask(app: Arc<App>) {
    let log = &app.log;

    loop {
        info!(log, "background task!");

        for (gerrit, client) in &app.gerrits {
            if let Err(e) = process_projects(&app, &gerrit, &client).await {
                error!(log, "process projects for {:?}: {:?}", gerrit, e);
            }
        }

        for (gerrit, client) in &app.gerrits {
            let projects = {
                if let Some(g) = app.data.lock().await.gerrits.get(gerrit) {
                    g.projects.keys().cloned().collect::<Vec<_>>()
                } else {
                    continue;
                }
            };

            for p in projects {
                if let Err(e) =
                    process_changes(&app, &gerrit, &p, &client).await
                {
                    error!(
                        log,
                        "process changes for {:?}/{:?}: {:?}", gerrit, p, e,
                    );
                }
            }
        }

        {
            let data = app.data.lock().await;

            for (gn, g) in &data.gerrits {
                debug!(log, "GERRIT {}:", gn);

                for (pn, p) in &g.projects {
                    debug!(log, "  PROJECT {}:", pn);

                    /*
                     * Sort changes by change number, not ID.
                     */
                    let mut sorted = p.changes.iter().collect::<Vec<_>>();
                    sorted.sort_by(|a, b| a.1.number.cmp(&b.1.number));

                    for (cn, c) in sorted {
                        debug!(log, "    CHANGE #{} -> {}:", c.number, cn);

                        for (rn, r) in &c.revisions {
                            debug!(
                                log,
                                "      REV #{} -> {}:", r.number, r.commit,
                            );
                            debug!(log, "        KIND: {}", r.kind);
                            debug!(log, "        BUGS: {:?}", r.bugs());
                            debug!(
                                log,
                                "        MSG: {:?}",
                                r.message.lines().next().unwrap(),
                            );
                        }
                    }
                }
            }
        }

        tokio::time::sleep(std::time::Duration::from_millis(5_000)).await;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut opts = getopts::Options::new();

    opts.optopt("b", "", "bind address:port", "BIND_ADDRESS");
    opts.optopt("f", "", "configuration file", "CONFIG");
    opts.optopt("S", "", "dump OpenAPI schema", "FILE");

    let p = match opts.parse(std::env::args().skip(1)) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("ERROR: usage: {}", e);
            eprintln!("       {}", opts.usage("usage"));
            std::process::exit(1);
        }
    };

    let mut ad = dropshot::ApiDescription::new();
    ad.register(api::gerrit::projects_list).api_check()?;

    if let Some(s) = p.opt_str("S") {
        let mut f = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&s)?;
        ad.openapi("Callaghan", "1.0").write(&mut f)?;
        return Ok(());
    }

    let bind_address =
        p.opt_str("b").as_deref().unwrap_or("127.0.0.1:9981").parse()?;

    let config: config::ConfigFile = if let Some(f) = p.opt_str("f").as_deref()
    {
        read_toml(f)?
    } else {
        bail!("must specify configuration file (-f)");
    };

    let log = make_log("callaghan");

    let gerrits = config
        .gerrit
        .iter()
        .map(|(name, cg)| {
            let client = callaghan_gerrit_client::ClientBuilder::new(
                log.clone(),
                &cg.url,
            )
            .build()?;
            Ok((name.to_string(), client))
        })
        .collect::<Result<_>>()?;

    let app0 = Arc::new(App {
        data: Default::default(),
        log: log.clone(),
        config,
        gerrits,
    });

    /*
     * Start the background tasks that implement most of the CI functionality.
     */
    let app = Arc::clone(&app0);
    let t_bg = tokio::task::spawn(async move {
        bgtask(app).await;
    });

    let server = dropshot::HttpServerStarter::new(
        #[allow(clippy::needless_update)]
        &dropshot::ConfigDropshot {
            request_body_max_bytes: 10 * 1024 * 1024,
            bind_address,
            ..Default::default()
        },
        ad,
        app0,
        &log,
    )
    .map_err(|e| anyhow!("server startup failure: {:?}", e))?;

    let server_task = server.start();

    loop {
        tokio::select! {
            _ = t_bg => bail!("background task stopped early"),
            _ = server_task => bail!("server stopped early"),
        }
    }
}
