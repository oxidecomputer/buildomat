/*
 * Copyright 2024 Oxide Computer Company
 */

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use buildomat_types::metadata;
use slog::{debug, error, info, o, trace, warn, Logger};

use super::Central;

/*
 * We serialise instance IDs as a string: "nodename/sequencenumber"; e.g.,
 * "gzunda/5334".
 */
fn parse_instance_id(
    private: &str,
) -> Result<(String, super::db::InstanceSeq)> {
    let t = private.splitn(2, '/').collect::<Vec<_>>();
    if t.len() != 2 {
        bail!("invalid instance id");
    }

    if t[0].trim().is_empty() {
        bail!("invalid nodename");
    }
    let seq = super::db::InstanceSeq::from_str(t[1])
        .context("invalid sequence number")?;

    Ok((t[0].to_string(), seq))
}

async fn lab_worker_one(log: &Logger, c: &Central) -> Result<()> {
    /*
     * Examine all active instances to check the validity of any prior worker
     * assignments.
     */
    for i in c.db.active_instances()? {
        if i.should_teardown() {
            /*
             * This instance is already being torn down.
             */
            continue;
        }

        /*
         * Fetch the state for this worker from the core server:
         */
        let w = if let Some(w) = c
            .client
            .factory_worker_get()
            .worker(&i.worker)
            .send()
            .await?
            .into_inner()
            .worker
        {
            debug!(log, "instance {} is for worker {}", i.id(), w.id);
            w
        } else {
            warn!(
                log,
                "instance {} is for worker {} which no longer exists",
                i.id(),
                i.worker,
            );
            c.hosts.get(&i.nodename).unwrap().state.lock().unwrap().reset();
            c.db.instance_destroy(&i)?;
            continue;
        };

        /*
         * Confirm that our factory-private data for this worker is consistent.
         */
        if let Some(expected) = w.private.as_deref() {
            if expected != i.id() {
                error!(
                    log,
                    "instance {} for worker {} does not match expected \
                    instance {} from DB",
                    i.id(),
                    w.id,
                    expected
                );
                continue;
            }
        } else {
            /*
             * For some reason there is no private data for this worker.
             * This is unexpected.
             * XXX Should we retry worker association?
             */
            error!(
                log,
                "instance {} for worker {} has no private data?",
                i.id(),
                w.id
            );
            continue;
        }

        if w.recycle {
            info!(log, "worker {} recycled, destroy instance {}", w.id, i.id());
            c.hosts.get(&i.nodename).unwrap().state.lock().unwrap().reset();
            c.db.instance_destroy(&i)?;
            continue;
        }
    }

    /*
     * At this point we have examined all of our active instances.  If there are
     * any worker records left that do not have an associate instance, they must
     * be scrubbed as detritus from prior failed runs.
     */
    for w in c.client.factory_workers().send().await?.into_inner() {
        let rm = if let Some(p) = w.private.as_deref() {
            if let Ok(ii) = parse_instance_id(p) {
                if let Some(i) = c.db.instance_get(&ii.0, ii.1)? {
                    if i.destroyed() {
                        /*
                         * This instance has been destroyed locally.
                         */
                        true
                    } else {
                        /*
                         * This instance is still active.
                         */
                        false
                    }
                } else {
                    /*
                     * This instance does not exist.
                     */
                    true
                }
            } else {
                /*
                 * Invalid instance ID stored on worker.
                 */
                true
            }
        } else {
            /*
             * This worker record was never associated with a lab host.  Destroy
             * it.
             */
            true
        };

        if rm {
            c.client.factory_worker_destroy().worker(&w.id).send().await?;
        }
    }

    /*
     * Build a list of hosts that do not have an active instance.
     */
    let ready_hosts = c
        .hosts
        .iter()
        .filter(|(_, host)| {
            /*
             * For a host to be considered ready, it must be sitting at the iPXE
             * hold point and emitting a regular dialtone.
             */
            host.config.debug_os_dir.is_none()
                && host.state.lock().unwrap().has_dialtone()
        })
        .map(|(nodename, _)| {
            Ok((
                nodename.to_string(),
                c.db.instance_for_host(nodename.as_str())?,
            ))
        })
        .collect::<Result<Vec<_>>>()?
        .drain(..)
        .filter(|(_, instance)| instance.is_none())
        .map(|(nodename, _)| nodename)
        .collect::<Vec<_>>();

    /*
     * From the available hosts, determine which targets are supported and
     * available.
     */
    let supported_targets = ready_hosts
        .iter()
        .flat_map(|nodename| {
            c.config
                .target
                .iter()
                .filter(|(_, target)| target.runs_on_node(nodename.as_str()))
                .map(|(id, _)| id.to_string())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    if supported_targets.is_empty() {
        /*
         * If we are not going to check for workers, we should explicitly ping
         * the server so it knows we are online:
         */
        c.client.factory_ping().send().await?;
    } else {
        /*
         * Check to see if the server requires any new workers.
         */
        if let Some(lease) = c
            .client
            .factory_lease()
            .body_map(|body| body.supported_targets(supported_targets))
            .send()
            .await?
            .into_inner()
            .lease
        {
            /*
             * The core server has requested a new worker for a particular
             * target.  Locate the first idle host that meets that requirement
             * and create an instance.
             */
            if let Some(t) = c.config.target.get(&lease.target) {
                /*
                 * From the global list of ready hosts, we want to pick any node
                 * that supports this particular target:
                 */
                let ready_for_target = ready_hosts
                    .iter()
                    .find(|nodename| t.runs_on_node(nodename.as_str()))
                    .cloned();

                if let Some(nodename) = ready_for_target {
                    /*
                     * This host is ready and available.  Create a worker, then
                     * create an instance on this host, then associate it with
                     * the worker.
                     */
                    let w = c
                        .client
                        .factory_worker_create()
                        .body_map(|body| {
                            body.target(&lease.target).wait_for_flush(true)
                        })
                        .send()
                        .await?;
                    info!(
                        log,
                        "created worker {} of target {}", w.id, lease.target
                    );

                    let i = c.db.instance_create(
                        &nodename,
                        &lease.target,
                        &w.id,
                        &w.bootstrap,
                    )?;
                    info!(
                        log,
                        "created instance {} for worker {}",
                        i.id(),
                        w.id
                    );

                    let mut addresses = Vec::new();
                    let hc = c.config.host.get(&nodename).unwrap();
                    if let Some(ips) = hc.extra_ips.as_ref() {
                        addresses.push(metadata::FactoryAddresses {
                            name: "extra".to_string(),
                            cidr: ips.cidr.to_string(),
                            first: ips.first.to_string(),
                            count: ips.count,
                            gateway: hc.gateway.as_deref().map(str::to_string),
                            routed: false,
                        });
                    }

                    c.client
                        .factory_worker_associate()
                        .worker(&w.id)
                        .body_map(|body| {
                            body.private(i.id()).metadata(Some(
                                metadata::FactoryMetadata::V1(
                                    metadata::FactoryMetadataV1 {
                                        addresses,
                                        root_password_hash: None,
                                        dump_to_rpool: None,
                                    },
                                ),
                            ))
                        })
                        .send()
                        .await?;
                    info!(
                        log,
                        "associated instance {} with worker {}",
                        i.id(),
                        w.id
                    );
                } else {
                    /*
                     * This should not occur, as we built the supported target
                     * list above based on the ready nodes.
                     */
                    warn!(
                    log,
                    "server asked for target (host {}) that is not ready: {}",
                    t.nodenames().join(", or "),
                    lease.target
                );
                }
            } else {
                warn!(
                    log,
                    "server asked for target we did not announce: {}",
                    lease.target
                );
            }
        }
    }

    trace!(log, "worker pass complete");
    Ok(())
}

pub(crate) async fn lab_worker(c: Arc<Central>) -> Result<()> {
    let log = c.log.new(o!("component" => "worker"));

    let delay = Duration::from_secs(7);

    info!(log, "start lab worker task");

    loop {
        if let Err(e) = lab_worker_one(&log, &c).await {
            error!(log, "worker error: {:?}", e);
        }

        tokio::time::sleep(delay).await;
    }
}

async fn upload_worker_one(log: &Logger, c: &Central) -> Result<()> {
    'outer: for i in c.db.active_instances()? {
        debug!(log, "upload worker processing {}...", i.id());

        while let Some(ie) = c.db.instance_next_event_to_upload(&i)? {
            debug!(
                log,
                "upload worker processing {} event #{}...",
                i.id(),
                ie.seq,
            );

            let res = c
                .client
                .factory_worker_append()
                .worker(&i.worker)
                .body_map(|body| {
                    body.payload(&ie.payload).stream(&ie.stream).time(ie.time.0)
                })
                .send()
                .await?;

            if res.retry {
                /*
                 * The factory is not yet ready to receive this event record,
                 * and has asked us to hold onto it and try again soon.
                 */
                debug!(
                    log,
                    "not ready to process {} event #{}",
                    i.id(),
                    ie.seq,
                );
                continue 'outer;
            }

            /*
             * The record was accepted or ignored by the core API server and
             * does not need to be uploaded again.
             */
            c.db.instance_mark_event_uploaded(&i, &ie)?;
        }

        if !i.flushed {
            let w =
                c.client.factory_worker_get().worker(&i.worker).send().await?;
            if let Some(w) = &w.worker {
                if w.online {
                    /*
                     * The agent within the guest is online and ready to receive
                     * a job, and we have managed to upload any early boot logs
                     * we have been saving.  Report that we have flushed those
                     * logs so that the job can start.
                     */
                    c.client
                        .factory_worker_flush()
                        .worker(&i.worker)
                        .send()
                        .await?;
                    c.db.instance_mark_flushed(&i)?;
                    info!(
                        log,
                        "boot logs flushed for worker {} instance {}",
                        w.id,
                        i.id()
                    );
                }
            }
        }
    }

    Ok(())
}

pub(crate) async fn upload_worker(c: Arc<Central>) -> Result<()> {
    let log = c.log.new(o!("component" => "uploader"));

    let delay = Duration::from_millis(250);

    info!(log, "start worker upload task");

    loop {
        if let Err(e) = upload_worker_one(&log, &c).await {
            error!(log, "upload worker error: {:?}", e);
        }

        tokio::time::sleep(delay).await;
    }
}
