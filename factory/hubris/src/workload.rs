/*
 * Copyright 2024 Oxide Computer Company
 */

use std::{collections::HashSet, sync::Arc, time::Duration};

use crate::{db::types::*, Central};
use anyhow::{anyhow, bail, Result};
use buildomat_common::OutputExt;
use slog::{debug, error, info, o, warn, Logger};

const FMRI: &str = "svc:/site/buildomat/hubris-agent:default";
const SERVICE: &str = "site/buildomat/hubris-agent";

pub(crate) async fn workload_worker(c: Arc<Central>) -> Result<()> {
    let log = c.log.new(o!("component" => "worker"));

    /*
     * Keep track of the instances for which we have kicked off the per-instance
     * worker task:
     */
    let mut instances_with_workers: HashSet<InstanceId> = Default::default();

    loop {
        if let Err(e) =
            workload_worker_one(&log, &c, &mut instances_with_workers).await
        {
            error!(log, "worker error: {:?}", e);
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn workload_worker_one(
    log: &Logger,
    c: &Arc<Central>,
    instances_with_workers: &mut HashSet<InstanceId>,
) -> Result<()> {
    debug!(log, "locating SMF service instance...");

    let mut scf = smf::Scf::new()?;
    let loc = scf.scope_local()?;

    /*
     * We expect the service to exist all the time, as it is imported from the
     * service bundle.
     */
    let svc = loc
        .get_service("site/buildomat/hubris-agent")?
        .ok_or_else(|| anyhow!("could not locate service {SERVICE:?}"))?;

    /*
     * We create an instance for each worker, named "bmat-{seq:08x}".  Make sure
     * we have an instance record for each SMF service instance found on the
     * system.
     */
    let mut insts = svc.instances()?;
    while let Some(i) = insts.next().transpose()? {
        let name = i.name()?;

        let id = match InstanceId::from_service_instance(
            &c.config.nodename,
            &name,
        ) {
            Ok(id) => id,
            Err(e) => {
                /*
                 * XXX We should remove this SMF service instance.
                 */
                error!(log, "{e}");
                continue;
            }
        };

        if c.db.instance_get(&id)?.is_some() {
            continue;
        }

        /*
         * XXX We should remove this zone.
         */
        error!(log, "SMF service instance for {id} has no instance");
    }

    /*
     * Now that the book-keeping is out of the way, ensure that we have an
     * instance worker spawned for each active instance.
     */
    let instances = c.db.instances_active()?;

    for i in instances {
        let id = i.id();

        if instances_with_workers.contains(&id) {
            continue;
        }
        instances_with_workers.insert(id.clone());

        let c = Arc::clone(c);
        tokio::spawn(async move {
            instance_worker(c, id).await;
        });
    }

    Ok(())
}

async fn instance_worker(c: Arc<Central>, id: InstanceId) {
    let log = c.log.new(o!(
        "component" => "instance_worker",
        "instance" => id.to_string(),
    ));

    info!(log, "instance worker starting");

    //let mut mon: Option<crate::svc::Monitor> = None;
    //let mut ser: Option<crate::serial::SerialForZone> = None;

    loop {
        match instance_worker_one(&log, &c, &id).await {
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
    c: &Central,
    id: &InstanceId,
) -> Result<DoNext> {
    let i = c.db.instance_get(id)?;

    let Some(i) = i else {
        bail!("no instance {id} in the database?");
    };
    let id = i.id();

    let Some(targ) = c.config.target.get(&i.target) else {
        bail!(
            "instance {id} has target {} that is not in config file",
            i.target,
        );
    };

    match i.state {
        InstanceState::Unconfigured => {
            let mut scf = smf::Scf::new()?;
            let loc = scf.scope_local()?;
            let svc =
                loc.get_service("site/buildomat/hubris-agent")?.ok_or_else(
                    || anyhow!("could not locate service {SERVICE:?}"),
                )?;

            /*
             * XXX we need to create the home dataset for the build user
             */

            /*
             * Create the SMF service instance...
             */
            let name = i.id().service_instance_name();

            let smfi = if let Some(smfi) = svc.get_instance(&name)? {
                info!(log, "SMF instance {name:?} existed already");
                smfi
            } else {
                svc.add_instance(&name)?
            };

            let fmri = smfi.fmri()?;
            info!(log, "created SMF instance {fmri}");

            if smfi.get_running_snapshot().is_err() {
                /*
                 * XXX workaround for SMF bullshit
                 */
                smfi.disable(false)?;
            }

            smfi.enable(true)?;
            info!(log, "enabled SMF instance {fmri}");

            c.db.instance_new_state(&id, InstanceState::Configured)?;
            Ok(DoNext::Immediate)
        }
        InstanceState::Configured => {
            /*
             * XXX we need to monitor the SMF service to make sure it is OK...
             */
            bail!("nyi"),
        }
        InstanceState::Destroying => {
            /*
             * XXX we need to tear down the SMF service...
             */

            /*
             * XXX we need to ensure all processes owned by the builder
             * uid are killed...  see: sigsend(2) / sigsendset(2)
             */

            /*
             * XXX we need to remove the home directory dataset...
             */
            bail!("nyi"),
        }
        InstanceState::Destroyed => bail!("nyi"),
    }
}
