use std::{
    collections::{BTreeSet, HashMap},
    net::{IpAddr, Ipv6Addr},
    os::fd::AsRawFd,
    path::PathBuf,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{
    db::types::*,
    net::{dladm_create_vnic, dladm_delete_vnic, dladm_vnic_get, Vnic},
    Central,
};
use anyhow::{anyhow, bail, Result};
use buildomat_client::types::{
    FactoryWhatsNext, FactoryWorkerAssociate, FactoryWorkerCreate,
};
use buildomat_common::OutputExt;
use rusty_ulid::Ulid;
use slog::{debug, error, info, o, warn, Logger};
use tokio::process::Command;
use zone::Zone;

async fn factory_task_one(log: &Logger, c: &Arc<Central>) -> Result<()> {
    let instances = {
        let h = c.db.connect().await?;
        h.instances_active()?
    };

    /*
     * For each instance, check to see if its worker record still exists.  If it
     * does not, mark the instance as destroying.
     */
    for i in instances {
        if matches!(i.state, InstanceState::Destroying) {
            /*
             * We don't need to keep looking at instances that are currently
             * being destroyed.
             */
            continue;
        }

        let w = c
            .client
            .factory_worker_get()
            .worker(&i.worker)
            .send()
            .await?
            .into_inner();

        let destroy = match w.worker {
            Some(w) => {
                debug!(log, "instance {} is for worker {}", i.id, w.id);

                if let Some(expected) = w.private.as_deref() {
                    if expected != i.id.to_string() {
                        error!(
                            log,
                            "instance {} for worker {} does not match \
                                expected instance ID {} from core server",
                            i.id,
                            w.id,
                            expected,
                        );
                        continue;
                    }
                } else {
                    /*
                     * This can occur if we crash after creating the instance
                     * but before associating it.
                     */
                    info!(
                        log,
                        "associating instance {} with worker {}", i.id, w.id,
                    );
                    c.client
                        .factory_worker_associate()
                        .worker(&w.id)
                        .body_map(|b| b.private(i.id.to_string()))
                        .send()
                        .await?;
                }

                if w.recycle {
                    /*
                     * If the worker has been deleted through the
                     * administrative API then we need to tear it down
                     * straight away.
                     */
                    warn!(log, "worker {} recycled, destroying it", w.id);
                    true
                } else {
                    /*
                     * Otherwise, this is a regular active worker that does
                     * not need to be destroyed.
                     */
                    if !w.online {
                        /*
                         * If the worker has not yet bootstrapped try to renew
                         * the lease with the core server.  This should prevent
                         * duplicate instance creation when creation or
                         * bootstrap is taking longer than expected.
                         */
                        info!(
                            log,
                            "renew lease {} for worker {}", i.lease, w.id
                        );
                        c.client
                            .factory_lease_renew()
                            .job(&i.lease)
                            .send()
                            .await?;
                    }
                    false
                }
            }
            None => {
                warn!(
                    log,
                    "instance {} is worker {} which no longer exists",
                    i.id,
                    i.worker,
                );
                true
            }
        };

        if destroy {
            let mut h = c.db.connect().await?;
            h.instance_new_state(&i.id, InstanceState::Destroying)?;
        }
    }

    /*
     * At this point we have examined all of the instances which exist.  If
     * there are any worker records left that do not have an associated
     * instance, they must be scrubbed from the database as detritus from prior
     * failed runs.
     */
    for w in c.client.factory_workers().send().await?.into_inner() {
        let instance_id =
            w.private.as_deref().and_then(|i| InstanceId::from_str(i).ok());

        let rm = if let Some(instance_id) = instance_id {
            /*
             * There is a record of a particular instance ID for this worker.
             * Check to see if that instance exists.
             */
            let i = {
                let mut h = c.db.connect().await?;
                h.instance_get(&instance_id)?
            };

            if let Some(i) = i {
                if matches!(i.state, InstanceState::Destroyed) {
                    /*
                     * The instance exists, but is terminated.  Delete the
                     * worker.
                     */
                    info!(
                        log,
                        "deleting worker {} for terminated instance {}",
                        w.id,
                        instance_id
                    );
                    true
                } else {
                    /*
                     * The instance exists but is not yet terminated.
                     */
                    false
                }
            } else {
                /*
                 * The instance does not exist.  Make this a warning unless we
                 * were already instructed by the core server to recycle the
                 * worker.
                 */
                if w.recycle {
                    info!(
                        log,
                        "deleting recycled worker {} with \
                            missing instance {}",
                        w.id,
                        instance_id
                    );
                } else {
                    warn!(
                        log,
                        "clearing worker {} with missing instance {}",
                        w.id,
                        instance_id
                    );
                }
                true
            }
        } else {
            /*
             * The worker record was never associated with an instance.  This
             * generally should not happen -- we would have associated the
             * worker with an instance that was created for it if we found one
             * earlier.
             */
            warn!(log, "clearing old worker {} with no instance", w.id);
            true
        };
        if rm {
            c.client.factory_worker_destroy().worker(&w.id).send().await?;
        }
    }

    let active = {
        let h = c.db.connect().await?;
        h.slots_active()?
    };

    let mut free = BTreeSet::new();
    for n in 0..c.config.slots {
        if !active.contains(&n) {
            free.insert(n);
        }
    }

    debug!(log, "slots"; "active" => ?active, "free" => ?free);

    let Some(slot) = free.first() else {
        return Ok(());
    };

    /*
     * Check to see if the server requires any new workers.
     */
    let res = c
        .client
        .factory_lease()
        .body_map(|b| {
            b.supported_targets(
                c.config.target.keys().cloned().collect::<Vec<_>>(),
            )
        })
        .send()
        .await?
        .into_inner();

    let Some(lease) = res.lease else {
        return Ok(());
    };

    info!(log, "lease from server: {lease:?}");

    /*
     * Locate target-specific configuration.
     */
    if !c.config.target.contains_key(&lease.target) {
        error!(log, "server wants target we do not support: {lease:?}");
        return Ok(());
    };

    let w = c
        .client
        .factory_worker_create()
        .body_map(|b| b.target(&lease.target).wait_for_flush(false))
        .send()
        .await?;

    let instance_id = {
        let mut h = c.db.connect().await?;
        h.instance_create(
            &c.config.nodename,
            &w.id,
            &lease.job,
            &lease.target,
            &w.bootstrap,
            *slot,
        )?
    };
    info!(log, "created instance: {instance_id} [slot {slot}]");

    /*
     * Record the instance ID against the worker for which it was created:
     */
    c.client
        .factory_worker_associate()
        .worker(&w.id)
        .body_map(|b| b.private(instance_id.to_string()))
        .send()
        .await?;

    Ok(())
}

pub(crate) async fn factory_task(c: Arc<Central>) -> Result<()> {
    let log = c.log.new(o!("component" => "factory_task"));

    loop {
        if let Err(e) = factory_task_one(&log, &c).await {
            error!(log, "factory task error: {:?}", e);
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
