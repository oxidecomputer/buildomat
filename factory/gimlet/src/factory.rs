/*
 * Copyright 2024 Oxide Computer Company
 */

use std::{
    collections::{BTreeSet, HashSet},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use crate::{
    db::{types::*, CreateInstance},
    App,
};
use anyhow::Result;
use slog::{debug, error, info, o, warn, Logger};

async fn factory_task_one(log: &Logger, c: &Arc<App>) -> Result<()> {
    /*
     * Keep track of the hosts that are active.
     */
    let mut inactive_hosts =
        c.hosts.iter().map(|hm| hm.id().clone()).collect::<HashSet<_>>();

    let instances = c.db.instances_active()?;

    /*
     * For each instance, check to see if its worker record still exists.  If it
     * does not, mark the instance as destroying.
     */
    for i in instances {
        inactive_hosts.remove(&i.id().host());

        if matches!(i.state, InstanceState::Destroying) {
            /*
             * We don't need to keep looking at instances that are currently
             * being destroyed.
             */
            continue;
        }

        let id = i.id();
        let hid = id.host();

        /*
         * Look up the host for this instance.
         */
        let Some(hm) = c.hosts.get(&hid) else {
            error!(log, "instance {id} has missing host {hid}; destroying");
            c.db.instance_new_state(&id, InstanceState::Destroying)?;
            continue;
        };

        let w = c
            .client
            .factory_worker_get()
            .worker(&i.worker)
            .send()
            .await?
            .into_inner();

        let destroy = match w.worker {
            Some(w) => {
                debug!(log, "instance {id} is for worker {}", w.id);

                if let Some(expected) = w.private.as_deref() {
                    if expected != id.to_string() {
                        error!(
                            log,
                            "instance {id} for worker {} does not match \
                                expected instance ID {} from core server",
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
                        "associating instance {id} with worker {}", w.id,
                    );

                    /*
                     * We need to be able to load the target configuration in
                     * order to get any potential per-target diagnostic
                     * configuration.
                     */
                    let Some(targ) = c.config.target.get(&i.target) else {
                        error!(
                            log,
                            "instance {id} for worker {} has target {} which \
                            no longer exists?",
                            w.id,
                            i.target,
                        );
                        continue;
                    };
                    // XXX let md = c.metadata(targ)?;

                    c.client
                        .factory_worker_associate()
                        .worker(&w.id)
                        .body_map(|b| {
                            b.private(id.to_string())
                                .ip(Some(hm.ip().to_string()))
                            // XXX .metadata(Some(md))
                        })
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
                    "instance {id} is worker {} which no longer exists",
                    i.worker,
                );
                true
            }
        };

        if destroy {
            c.db.instance_new_state(&id, InstanceState::Destroying)?;
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
            let i = c.db.instance_get(&instance_id)?;

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

    /*
     * Check through the inactive hosts.  Kick off cleaning on any hosts that
     * are not currently ready.
     */
    inactive_hosts.retain(|hid| {
        let hm = c.hosts.get(hid).unwrap();

        if hm.is_ready() {
            true
        } else {
            hm.clean();
            false
        }
    });

    /*
     * Are there any hosts ready to run a job?
     */
    let Some(ready) = inactive_hosts.iter().next().cloned() else {
        /*
         * If we are not going to check for workers, we should explicitly ping
         * the server so it knows we are online:
         */
        c.client.factory_ping().send().await?;
        return Ok(());
    };
    let hm = c.hosts.get(&ready).unwrap();

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
    let Some(targ) = c.config.target.get(&lease.target) else {
        error!(log, "server wants target we do not support: {lease:?}");
        return Ok(());
    };

    let w = c
        .client
        .factory_worker_create()
        .body_map(|b| b.target(&lease.target).wait_for_flush(false))
        .send()
        .await?;

    let instance_id = c.db.instance_create(
        hm.id(),
        CreateInstance {
            worker: w.id.to_string(),
            lease: lease.job.to_string(),
            target: lease.target.to_string(),
            bootstrap: w.bootstrap.to_string(),
        },
    )?;
    info!(log, "created instance: {instance_id} [host {}]", hm.id());

    /*
     * Record the instance ID against the worker for which it was created:
     */
    //let md = c.metadata(targ)?;
    c.client
        .factory_worker_associate()
        .worker(&w.id)
        .body_map(|b| {
            b.private(instance_id.to_string()).ip(Some(hm.ip().to_string()))
            // XXX .metadata(Some(md))
        })
        .send()
        .await?;

    Ok(())
}

pub(crate) async fn factory_task(c: Arc<App>) -> Result<()> {
    let log = c.log.new(o!("component" => "factory_task"));

    loop {
        if let Err(e) = factory_task_one(&log, &c).await {
            error!(log, "factory task error: {:?}", e);
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
