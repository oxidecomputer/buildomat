use std::{collections::HashMap, str::FromStr, sync::Arc, time::Duration};

use crate::{db::types::*, Central};
use anyhow::Result;
use slog::{debug, error, o, Logger};
use uuid::Uuid;
use zone::Zone;

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

        let h = c.db.connect().await?;
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
                zc.add_net(&zone::Net {
                    physical: "illumos0".into(), /* XXX */
                    ..Default::default()
                });
                match zc.run().await {
                    Ok(_) => {
                        info!(log, "zone {zn} configured!");
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
            }
            InstanceState::ZoneOnline => {
                /*
                 * The zone is online and we need to create the propolis
                 * instance.
                 */
            }
            InstanceState::InstanceCreated => {
                /*
                 * The propolis instance is created.  Kick off the serial port
                 * listening task and then boot the guest.
                 */
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
