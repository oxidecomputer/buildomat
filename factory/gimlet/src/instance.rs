/*
 * Copyright 2026 Oxide Computer Company
 */

use std::{collections::HashSet, sync::Arc, time::Duration};

use anyhow::{bail, Result};
use chrono::prelude::*;
use slog::{error, info, o, Logger};

use crate::{
    db::{InstanceId, InstanceState},
    host::HostManager,
    App,
};

pub(crate) async fn instance_worker(c: Arc<App>) -> Result<()> {
    let log = c.log.new(o!("component" => "instance"));

    /*
     * Keep track of the instances for which we have kicked off the per-instance
     * worker task:
     */
    let mut instances_with_workers: HashSet<InstanceId> = Default::default();

    loop {
        if let Err(e) = clean_inactive(&log, &c, &instances_with_workers).await
        {
            error!(log, "clean inactive task error: {:?}", e);
        }

        if let Err(e) =
            instance_worker_start(&log, &c, &mut instances_with_workers).await
        {
            error!(log, "instance worker start error: {:?}", e);
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn clean_inactive(
    _log: &Logger,
    c: &Arc<App>,
    instances_with_workers: &HashSet<InstanceId>,
) -> Result<()> {
    let instances = c.db.instances_active()?;

    c.hosts
        .iter()
        /*
         * Elide any hosts that are in use by an active instance:
         */
        .filter(|hm| instances.iter().all(|i| &i.id().host() != hm.id()))
        /*
         * Elide any hosts that still have an instance worker task running:
         */
        .filter(|hm| {
            instances_with_workers.iter().all(|i| &i.host() != hm.id())
        })
        .for_each(|hm| {
            hm.make_ready().ok();
        });

    Ok(())
}

async fn instance_worker_start(
    log: &Logger,
    c: &Arc<App>,
    instances_with_workers: &mut HashSet<InstanceId>,
) -> Result<()> {
    /*
     * Ensure that we have an instance worker task spawned for each active
     * instance.
     */
    let instances = c.db.instances_active()?;

    for i in instances {
        let id = i.id();

        if instances_with_workers.contains(&id) {
            continue;
        }
        instances_with_workers.insert(id.clone());

        let c = Arc::clone(&c);
        let log = log.new(o!(
            "component" => "instance_worker",
            "instance" => id.to_string(),
        ));
        tokio::spawn(async move {
            instance_worker_one_noerr(&log, &c, id).await;
        });
    }

    Ok(())
}

async fn instance_worker_one_noerr(log: &Logger, c: &App, id: InstanceId) {
    let ist = if let Some(i) = c.db.instance_get(&id).ok().flatten() {
        i.state.to_string()
    } else {
        "?".to_string()
    };
    info!(log, "instance worker starting"; "initial_state" => ist);

    let hm = c.hosts.get(&id.host()).unwrap();

    loop {
        match instance_worker_one(log, c, &id, hm).await {
            Ok(DoNext::Immediate) => continue,
            Ok(DoNext::Sleep) => (),
            Ok(DoNext::Shutdown) => {
                info!(log, "instance worker shutting down");
                return;
            }
            Err(e) => {
                error!(log, "instance worker error: {:?}", e);
            }
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

enum DoNext {
    Sleep,
    Immediate,
    Shutdown,
}

async fn instance_worker_one(
    log: &Logger,
    c: &App,
    id: &InstanceId,
    hm: &HostManager,
) -> Result<DoNext> {
    let Some(i) = c.db.instance_get(id)? else {
        bail!("no instance {id} in the database?");
    };

    let Some(targ) = c.config.targets.get(&i.target) else {
        bail!(
            "instance {id} has target {} that is not in config file",
            i.target,
        );
    };

    match i.state {
        InstanceState::Preinstall => {
            /*
             * Make the host ready.  This may involve power cycling it, booting
             * the housekeeping image, and cleaning out the disks, so it could
             * take some time.
             */
            if !hm.make_ready()? {
                return Ok(DoNext::Sleep);
            }

            info!(log, "instance {id} now installing");
            c.db.instance_new_state(id, InstanceState::Installing)?;
            Ok(DoNext::Immediate)
        }
        InstanceState::Installing => {
            if !hm.make_ready()? {
                /*
                 * The host should already be ready at this point, but if the
                 * factory is interrupted we might have to do it again before we
                 * can start the instance.
                 */
                return Ok(DoNext::Sleep);
            }

            hm.start(&c.config.general.baseurl, &i.bootstrap, &targ.os_dir)?;

            info!(log, "instance {id} now installed");
            c.db.instance_new_state(id, InstanceState::Installed)?;
            Ok(DoNext::Immediate)
        }
        InstanceState::Installed => {
            if i.panicked {
                if c.db.instance_next_event_to_upload(&i)?.is_some() {
                    /*
                     * Wait for all outstanding events to upload first.
                     */
                    return Ok(DoNext::Sleep);
                }

                info!(log, "instance {id} has panicked; destroying");
                c.db.instance_new_state(id, InstanceState::Destroying)?;
                return Ok(DoNext::Immediate);
            }

            if let Some(panic) = hm.report_panic() {
                info!(log, "reporting panic for instance {id}: {panic:?}");
                let now = Utc::now();

                c.db.instance_append(id, "panic", "host panic detected!", now)?;

                for l in panic.lines {
                    c.db.instance_append(id, "panic", l.trim_end(), now)?;
                }

                /*
                 * Mark the instance as panicked.  When the panic output has
                 * been fully uploaded, we will start tearing down the instance.
                 */
                c.db.instance_mark_panicked(id)?;
            }

            Ok(DoNext::Sleep)
        }
        InstanceState::Destroying => {
            /*
             * Begin cleaning the machine, and wait for it to be ready.
             */
            if !hm.make_ready()? {
                return Ok(DoNext::Sleep);
            }

            info!(log, "instance {id} now destroyed");
            c.db.instance_new_state(id, InstanceState::Destroyed)?;
            Ok(DoNext::Immediate)
        }
        InstanceState::Destroyed => {
            info!(log, "instance {id} completely cleaned up");
            Ok(DoNext::Shutdown)
        }
    }
}
