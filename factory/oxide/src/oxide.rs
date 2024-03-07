/*
 * Copyright 2024 Oxide Computer Company
 */

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result};
use buildomat_client::types::*;
use buildomat_types::metadata;
use chrono::prelude::*;
use rusty_ulid::Ulid;
use slog::{debug, error, info, o, warn, Logger};

use oxide::context::Context;

use oxide::ClientImagesExt;
use oxide::ClientInstancesExt;
use oxide::ClientDisksExt;
use oxide::types::InstanceDiskAttachment;
use oxide::types::DiskSource;
use oxide::types::ByteCount;
use oxide::types::ExternalIpCreate;
use oxide::types::ExternalIp;

use super::{config::ConfigFileOxideTarget, Central, ConfigFile};

#[derive(Debug)]
#[allow(dead_code)]
struct Instance {
    id: String,
    state: String,
    ip: Option<String>,
    worker_id: Option<Ulid>,
    lease_id: Option<Ulid>,
    launch_time: Option<DateTime<Utc>>,
}

impl Instance {
    fn age_secs(&self) -> u64 {
        self.launch_time
            .map(|dt| {
                Utc::now()
                    .signed_duration_since(dt)
                    .to_std()
                    .unwrap_or_else(|_| Duration::from_secs(0))
                    .as_secs()
            })
            .unwrap_or(0)
    }
}

async fn destroy_instance(
    log: &Logger,
    context: &Context,
    id: &str,
) -> Result<()> {
    info!(log, "destroying instance {}...", id);

    let disks = context.client()?.instance_disk_list().instance(id).send().await?;
    let disk_id = disks.items[0].id;

    context.client()?.instance_stop().instance(id).send().await?;
    loop {
        match context.client()?.instance_delete().instance(id).send().await {
            Ok(_) => break,
            Err(_) => continue,
        }
    }

    context.client()?.disk_delete().disk(disk_id).send().await?;
    
    Ok(())
}

async fn create_instance(
    log: &Logger,
    context: &Context,
    config: &ConfigFile,
    target: &ConfigFileOxideTarget,
    worker: &FactoryWorker,
    lease_id: &str,
) -> Result<String> {
    let id = worker.id.to_string();

    let script = include_str!("../scripts/user_data.sh")
        .replace("%URL%", &config.general.baseurl)
        .replace("%STRAP%", &worker.bootstrap);

    let script = base64::encode(&script);

    let img = match context
        .client()?
        .image_view()
        .image(target.image.clone())
        .send()
        .await
    {
        Ok(i) => i,
        Err(_) => {
            context
                .client()?
                .image_view()
                .image(target.image.clone())
                .project(config.oxide.project.clone())
                .send()
                .await?
        }
    };

    info!(log, "creating an instance (worker {})...", id);

    // Makeshift Tags
    let desc = format!("buildomat:worker_id={},lease_id={}", id, lease_id);

    let res = context
        .client()?
        .instance_create()
        .project(&config.oxide.project)
        .body_map(|body| {
            body.name(format!("oxide-worker-{}", id.to_lowercase()))
                .description(desc)
                .disks(vec![InstanceDiskAttachment::Create {
                    description: format!("Buildomat Worker {}", id),
                    disk_source: DiskSource::Image { image_id: img.id },
                    name: format!("oxide-worker-{}-disk", id.to_lowercase()).try_into().unwrap(),
                    size: ByteCount(target.disk_size.parse().unwrap()),
                }])
                .external_ips(vec![ExternalIpCreate::Ephemeral { pool: None }])
                .hostname(format!("oxide-worker-{}", id))
                .memory(ByteCount(target.memory.parse().unwrap()))
                .ncpus(target.cpu_cnt)
                .start(true)
                .user_data(script)
        })
        .send()
        .await?;

    Ok(res.id.into())
}

async fn instances(
    _log: &Logger,
    context: &Context,
    config: &ConfigFile,
) -> Result<HashMap<String, Instance>> {
    let res = context
        .client()?
        .instance_list()
        .project(config.oxide.project.clone())
        .send()
        .await?;

    let mut out = HashMap::new();
    for i in res.items.iter() {
        let desc = match i.description.strip_prefix("buildomat:") {
            Some(d) => d,
            None => continue,
        };

        let id = i.id.to_string();
        let state = i.run_state.to_string();

        let launch_time = Some(i
            .time_created);

        let desc : Vec<_> = desc.split(",").collect();

        let worker_id = desc[0]
            .strip_prefix("worker_id=")
            .map(|s| Ulid::from_str(&s).ok())
            .unwrap_or_default();
        let lease_id = desc[1]
            .strip_prefix("lease_id=")
            .map(|s| Ulid::from_str(&s).ok())
            .unwrap_or_default();

            let ip = context.client()?
        .instance_external_ip_list()
        .instance(id.clone())
        .send()
        .await?;

        let ip = Some(match ip.items[0] {
            ExternalIp::Ephemeral { ip } => ip.to_string(),
            ExternalIp::Floating { ip, .. }  => ip.to_string(),
        });

        out.insert(
            id.to_string(),
            Instance { id, state, worker_id, lease_id, ip, launch_time },
        );
    }

    Ok(out)
}

async fn oxide_worker_one(
    log: &Logger,
    c: &Central,
    context: &Context,
    config: &ConfigFile,
) -> Result<()> {
    /*
     * Get a complete list of instances that have the buildomat tag.
     */
    debug!(log, "scanning for instances...");
    let insts = instances(log, context, config).await?;

    info!(log, "found instances: {:?}", insts);

    /*
     * For each instance, check to see if its worker record still exists.  If it
     * does not, delete the instance now.
     */
    for i in insts.values() {
        let destroy = if let Some(id) = &i.worker_id {
            /*
             * Request information about this worker from the core server.
             */
            let w = c
                .client
                .factory_worker_get()
                .worker(id.to_string())
                .send()
                .await?
                .into_inner();
            match w.worker {
                Some(w) => {
                    debug!(log, "instance {} is for worker {}", i.id, w.id);

                    if let Some(expected) = w.private.as_deref() {
                        if expected != i.id {
                            error!(
                                log,
                                "instance {} for worker {} does not \
                                match expected instance ID {} from DB",
                                i.id,
                                w.id,
                                expected
                            );
                            continue;
                        }
                    } else {
                        /*
                         * This can occur if we crash after creating the
                         * instance but before associating it.
                         */
                        info!(
                            log,
                            "associating instance {} with worker {}",
                            i.id,
                            w.id
                        );
                        c.client
                            .factory_worker_associate()
                            .worker(&w.id)
                            .body_map(|body| {
                                body.private(&i.id).metadata(Some(
                                    metadata::FactoryMetadata::V1(
                                        metadata::FactoryMetadataV1 {
                                            addresses: Default::default(),
                                            root_password_hash: None
                                        },
                                    ),
                                ))
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
                    } else if i.state == "stopped" {
                        /*
                         * Terminate any instances which stop themselves.
                         */
                        warn!(log, "instance {} stopped, destroying!", i.id);
                        true
                    } else if !w.online && i.age_secs() > 5 * 60 {
                        /*
                         * We have seen a recent spate of AWS instances that
                         * hang in early boot.  They get destroyed eventually,
                         * but we should terminate them more promptly than that
                         * to avoid being billed for AWS bullshit.
                         */
                        error!(
                            log,
                            "instance {} hung; destroying after {} seconds",
                            i.id,
                            i.age_secs(),
                        );
                        true
                    } else {
                        /*
                         * Otherwise, this is a regular active worker that does
                         * not need to be destroyed.
                         */
                        if !w.online {
                            if let Some(lid) = &i.lease_id {
                                /*
                                 * If the worker has not yet bootstrapped, and
                                 * we have a lease on file for this instance,
                                 * try to renew it with the core server.  This
                                 * should prevent duplicate instance creation
                                 * when creation or bootstrap is taking longer
                                 * than expected.
                                 */
                                info!(
                                    log,
                                    "renew lease {} for worker {}", lid, w.id
                                );
                                c.client
                                    .factory_lease_renew()
                                    .job(lid.to_string())
                                    .send()
                                    .await?;
                            }
                        }
                        false
                    }
                }
                None => {
                    warn!(
                        log,
                        "instance {} is worker {} which no longer \
                        exists",
                        i.id,
                        id,
                    );
                    true
                }
            }
        } else {
            /*
             * This should not happen, and likely represents a serious problem:
             * either this software is not correctly tracking instances, or the
             * operator is creating instances in the AWS account that was set
             * aside for unprivileged build VMs.
             */
            warn!(log, "instance {} has no worker id; ignoring...", i.id);
            continue;
        };

        if destroy
            && (&i.state == "running"
                || &i.state == "stopped"
                || &i.state == "pending")
        {
            destroy_instance(log, context, &i.id).await?;
        }
    }

    /*
     * At this point we have examined all of the instances which exist.  If
     * there are any worker records left that do not have an associated
     * instance, they must be scrubbed from the database as detritus from prior
     * failed runs.
     */
    for w in c.client.factory_workers().send().await?.into_inner() {
        let rm = if let Some(instance_id) = w.private.as_deref() {
            /*
             * There is a record of a particular instance ID for this worker.
             * Check to see if that instance exists.
             */
            if let Some(i) = insts.get(&instance_id.to_string()) {
                if i.state == "terminated" {
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
     * Count the number of instances we can see in AWS that are not yet
     * completely destroyed.
     */
    let c_insts = insts.values().filter(|i| &i.state != "terminated").count();

    /*
     * Calculate the total number of workers we are willing to create if
     * required.  The hard limit on total instances includes all instances
     * regardless of where they are in the worker lifecycle.
     */
    let freeslots = config.oxide.limit_total.saturating_sub(c_insts);

    info!(log, "worker stats";
        "instances" => c_insts,
        "freeslots" => freeslots,
    );

    let mut created = 0;
    while created < freeslots {
        /*
         * Check to see if the server requires any new workers.
         */
        let res = c
            .client
            .factory_lease()
            .body_map(|body| body.supported_targets(c.targets.clone()))
            .send()
            .await?
            .into_inner();

        let lease = if let Some(lease) = res.lease {
            lease
        } else {
            break;
        };

        /*
         * Locate target-specific configuration.
         */
        let t = if let Some(t) = c.config.target.get(&lease.target) {
            t
        } else {
            error!(log, "server wants target we do not support: {:?}", lease);
            break;
        };

        let w = c
            .client
            .factory_worker_create()
            .body_map(|body| body.target(&lease.target))
            .send()
            .await?;

        let instance_id =
            create_instance(log, context, config, t, &w, &lease.job).await?;
        created += 1;
        info!(log, "created instance: {}", instance_id);

        /*
         * Record the instance ID against the worker for which it was created:
         */
        c.client
            .factory_worker_associate()
            .worker(&w.id)
            .body_map(|body| {
                body.private(&instance_id).metadata(Some(
                    metadata::FactoryMetadata::V1(
                        metadata::FactoryMetadataV1 {
                            addresses: Default::default(),
                            root_password_hash: None
                        },
                    ),
                ))
            })
            .send()
            .await?;
    }

    if freeslots == 0 {
        /*
         * If we are not going to check for workers, we should explicitly ping
         * the server so it knows we are online:
         */
        c.client.factory_ping().send().await?;
    }

    info!(log, "worker pass complete");
    Ok(())
}

pub(crate) async fn oxide_worker(c: Arc<Central>) -> Result<()> {
    let log = c.log.new(o!("component" => "worker"));

    std::env::set_var("OXIDE_HOST", c.config.oxide.rack_host.clone());
    std::env::set_var("OXIDE_TOKEN", c.config.oxide.rack_token.clone());
    let oxide_config = oxide::config::Config::default();
    let context = oxide::context::Context::new(oxide_config)?;

    let delay = Duration::from_secs(7);

    info!(log, "start Oxide worker task");

    loop {
        if let Err(e) = oxide_worker_one(&log, &c, &context, &c.config).await {
            error!(log, "worker error: {:?}", e);
        }

        tokio::time::sleep(delay).await;
    }
}
