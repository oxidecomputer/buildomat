use std::{
    collections::HashMap,
    net::{IpAddr, Ipv6Addr},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use crate::{db::types::*, Central};
use anyhow::{bail, Result};
use buildomat_client::types::{
    FactoryWhatsNext, FactoryWorkerAssociate, FactoryWorkerCreate,
};
use buildomat_common::OutputExt;
use propolis_client::types::{
    InstanceEnsureRequest, InstanceProperties, InstanceStateRequested,
};
use slog::{debug, error, info, o, warn, Logger};
use tokio::process::Command;
use uuid::Uuid;
use zone::Zone;

async fn zone_ip_address(
    log: &Logger,
    zone: &str,
    iface: &str,
) -> Result<IpAddr> {
    let mut cmd = Command::new("/usr/sbin/zlogin");
    cmd.env_clear();
    cmd.arg(zone);
    cmd.arg("/usr/sbin/ipadm");
    cmd.arg("-p");
    cmd.arg("-o").arg("addr");
    cmd.arg(format!("{iface}/v6"));

    let res = cmd.output().await?;

    if res.status.success() {
        let out = String::from_utf8(res.stdout)?;
        if let Some((a, _)) = out.trim().split_once('%') {
            Ok(IpAddr::V6(Ipv6Addr::from_str(a)?))
        } else {
            bail!("malformed IPv6 IP? {out:?}");
        }
    } else {
        bail!("svcs error (zone {zone:?}): {}", res.info());
    }
}

async fn zone_smf_state(
    log: &Logger,
    zone: &str,
    fmri: &str,
) -> Result<String> {
    let mut cmd = Command::new("/usr/sbin/zlogin");
    cmd.env_clear();
    cmd.arg(zone);
    cmd.arg("/usr/bin/svcs");
    cmd.arg("-H");
    cmd.arg("-o").arg("sta");
    cmd.arg(fmri);

    let res = cmd.output().await?;

    if res.status.success() {
        let out = String::from_utf8(res.stdout)?.trim().to_string();
        Ok(out)
    } else {
        bail!("svcs error (zone {zone:?}): {}", res.info());
    }
}

async fn vm_worker_one(log: &Logger, c: &Central) -> Result<()> {
    /*
     * List the zones that exist on the system.  We create zones with a name
     * that has the prefix "bmat-"; ignore any other zones.
     */
    debug!(log, "listing zones...");
    let zones = zone::Adm::list()
        .await?
        .into_iter()
        .filter(|z| z.name().starts_with("bmat-"))
        .map(|z| (z.name().to_string(), z))
        .collect::<HashMap<String, Zone>>();

    debug!(log, "found zones: {:?}", zones);

    /*
     * Make sure we have an instance record for each zone found on the system.
     */
    for zone in zones.values() {
        /*
         * Each zone is named for a V4 UUID that we can also use as the UUID for
         * propolis-server.  This eases debugging, by making it obvious which
         * bhyve VM belongs to which zone.
         */
        let id =
            match Uuid::from_str(zone.name().strip_prefix("bmat-").unwrap()) {
                Ok(id) => InstanceId::from(id),
                Err(e) => {
                    /*
                     * XXX We should remove this zone.
                     */
                    error!(log, "invalid zone UUID: {:?}: {e}", zone.name());
                    continue;
                }
            };

        let mut h = c.db.connect().await?;
        if h.instance_get(id)?.is_some() {
            continue;
        }

        /*
         * XXX We should remove this zone.
         */
        error!(log, "zone with UUID {id} has no instance");
    }

    let instances = {
        let h = c.db.connect().await?;
        h.instances_active()?
    };

    /*
     * For each instance, check to see if its worker record still exists.  If it
     * does not, mark the instance as destroying.
     */
    for i in instances {
        let w = c.client.factory_worker_get(&i.worker).await?.into_inner();

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
                        .factory_worker_associate(
                            &w.id,
                            &buildomat_client::types::FactoryWorkerAssociate {
                                private: i.id.to_string(),
                            },
                        )
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
            h.instance_new_state(i.id, InstanceState::Destroying);
        }
    }

    /*
     * At this point we have examined all of the instances which exist.  If
     * there are any worker records left that do not have an associated
     * instance, they must be scrubbed from the database as detritus from prior
     * failed runs.
     */
    for w in c.client.factory_workers().await?.into_inner() {
        let instance_id = w
            .private
            .as_deref()
            .and_then(|i| Uuid::from_str(i).ok().map(InstanceId::from));

        let rm = if let Some(instance_id) = instance_id {
            /*
             * There is a record of a particular instance ID for this worker.
             * Check to see if that instance exists.
             */
            let i = {
                let mut h = c.db.connect().await?;
                h.instance_get(instance_id)?
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
            c.client.factory_worker_destroy(&w.id).await?;
        }
    }

    let limit = 1usize;
    let count = {
        let h = c.db.connect().await?;
        let x = h.instances_active()?;
        x.len()
    };
    let freeslots = limit.saturating_sub(count);

    info!(log, "worker stats";
        "instances" => count,
        "freeslots" => freeslots,
    );

    let mut created = 0;
    while created < freeslots {
        /*
         * Check to see if the server requires any new workers.
         */
        let res = c
            .client
            .factory_lease(&FactoryWhatsNext { supported_targets: vec!["XXX".into()] })
            .await?
            .into_inner();

        let lease = if let Some(lease) = res.lease {
            lease
        } else {
            break;
        };

        // XXX /*
        // XXX  * Locate target-specific configuration.
        // XXX  */
        // XXX let t = if let Some(t) = c.config.target.get(&lease.target) {
        // XXX     t
        // XXX } else {
        // XXX     error!(log, "server wants target we do not support: {:?}", lease);
        // XXX     break;
        // XXX };

        let w = c
            .client
            .factory_worker_create(&FactoryWorkerCreate {
                target: lease.target.to_string(),
                job: None,
                wait_for_flush: false,
            })
            .await?;

        let instance_id = {
            let h = c.db.connect().await?;
            h.instance_create(&w.id, &lease.target, &w.bootstrap)?
        };
        created += 1;
        info!(log, "created instance: {}", instance_id);

        /*
         * Record the instance ID against the worker for which it was created:
         */
        c.client
            .factory_worker_associate(
                &w.id,
                &FactoryWorkerAssociate { private: instance_id.to_string() },
            )
            .await?;
    }

    /*
     * Now that the book-keeping is out of the way, we actually need to progress
     * VM setup through the state machine for all active instances.
     */
    let instances = {
        let h = c.db.connect().await?;
        h.instances_active()?
    };

    for i in instances {
        let zn = i.zonename();

        match i.state {
            InstanceState::Unconfigured => {
                /*
                 * The zone needs to be configured.
                 */
                info!(log, "zone {zn}: configuring...");
                let mut zc = zone::Config::new(&zn);
                zc.get_global()
                    .set_path(format!("/data/vm/buildomat/{zn}"))
                    .set_brand("omicron1")
                    .set_ip_type(zone::IpType::Exclusive)
                    .set_autoboot(false);
                /*
                 * Add the control VNIC that the host will use to talk to
                 * propolis in the zone:
                 */
                zc.add_net(&zone::Net {
                    physical: "internal0zone0".into(), /* XXX */
                    ..Default::default()
                });
                /*
                 * Add the guest VNIC that propolis will use for the virtio NIC:
                 */
                zc.add_net(&zone::Net {
                    physical: "illumos0".into(), /* XXX */
                    ..Default::default()
                });
                match zc.run().await {
                    Ok(_) => {
                        info!(log, "zone {zn} configured!");
                        let mut h = c.db.connect().await?;
                        h.instance_new_state(i.id, InstanceState::Configured)?;
                    }
                    Err(e) => {
                        error!(log, "zone {zn} config error: {e}");
                        continue;
                    }
                }
            }
            InstanceState::Configured => {
                /*
                 * The zone needs to be installed.  The root disk image needs to
                 * be copied into the zone.  The cpio metadata disk needs to be
                 * generated and stored in the zone as well.
                 */
                match zone::Adm::new(&zn).install(Default::default()).await {
                    Ok(_) => {
                        info!(log, "zone {zn} installed!");
                        let mut h = c.db.connect().await?;
                        h.instance_new_state(i.id, InstanceState::Installed)?;
                    }
                    Err(e) => {
                        error!(log, "zone {zn} install error: {e}");
                        continue;
                    }
                }
            }
            InstanceState::Installed => {
                /*
                 * The zone needs to be booted.
                 */
                match zone::Adm::new(&zn).boot().await {
                    Ok(_) => {
                        info!(log, "zone {zn} booted!");
                        let mut h = c.db.connect().await?;
                        h.instance_new_state(i.id, InstanceState::ZoneOnline)?;
                    }
                    Err(e) => {
                        error!(log, "zone {zn} boot error: {e}");
                        continue;
                    }
                }

                /*
                 * Wait for the propolis-server service to hit a terminal
                 * state:
                 */
                loop {
                    let st =
                        zone_smf_state(log, &zn, "milestone/multi-user-server")
                            .await;

                    match st {
                        Ok(s) => {
                            if s == "ON" {
                                let mut h = c.db.connect().await?;
                                h.instance_new_state(
                                    i.id,
                                    InstanceState::ZoneOnline,
                                )?;
                                break;
                            }

                            info!(log, "mus is @ {s}");
                        }
                        Err(e) => {
                            bail!("zone smf state error: {e}");
                        }
                    }

                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
            }
            InstanceState::ZoneOnline => {
                /*
                 * The zone is online and we need to create the propolis
                 * instance.
                 */
                let ip = zone_ip_address(log, &zn, "internal0zone0").await?;
                let client = propolis_client::Client::new("http://{ip}:6611");

                let res = client
                    .instance_ensure()
                    .body(
                        InstanceEnsureRequest::builder()
                            .properties(InstanceProperties {
                                id: i.id.into(),
                                name: "buildomat".into(), /* XXX */
                                memory: 16 * 1024,
                                vcpus: 4,
                                bootrom_id: Default::default(),
                                description: "".into(),
                                image_id: Default::default(),
                            })
                            .disks(vec![])
                            .nics(vec![]),
                    )
                    .send()
                    .await?;
            }
            InstanceState::InstanceCreated => {
                /*
                 * The propolis instance is created.  Kick off the serial port
                 * listening task and then boot the guest.
                 */
                let ip = zone_ip_address(log, &zn, "internal0zone0").await?;
                let client = propolis_client::Client::new("http://{ip}:6611");

                /*
                 * XXX the actual serial port bit?
                 */

                let res = client
                    .instance_state_put()
                    .body(InstanceStateRequested::Run)
                    .send()
                    .await?;
            }
            InstanceState::InstanceRunning => {
                /*
                 * We don't need to do anything here, probably, other than
                 * monitor the health of the VM?
                 */
            }
            InstanceState::Destroying => {
                /*
                 * Begin tearing down the zone.
                 */
            }
            InstanceState::Destroyed => unreachable!(),
        }
    }

    Ok(())
}

pub(crate) async fn vm_worker(c: Arc<Central>) -> Result<()> {
    let log = c.log.new(o!("component" => "worker"));

    loop {
        if let Err(e) = vm_worker_one(&log, &c).await {
            error!(log, "worker error: {:?}", e);
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
