/*
 * Copyright 2024 Oxide Computer Company
 */

use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};
use std::{collections::HashMap, time::SystemTime};

use anyhow::{anyhow, bail, Result};
use aws_config::Region;
use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
use aws_sdk_ec2::config::Credentials;
use aws_sdk_ec2::types::{
    BlockDeviceMapping, EbsBlockDevice, Filter,
    InstanceNetworkInterfaceSpecification, InstanceType, ResourceType, Tag,
    TagSpecification,
};
use base64::Engine;
use buildomat_client::types::*;
use rusty_ulid::Ulid;
use slog::{debug, error, info, o, warn, Logger};

use super::{config::ConfigFileAwsTarget, Central, ConfigFile};

#[derive(Debug)]
struct Instance {
    id: String,
    state: String,
    ip: Option<String>,
    worker_id: Option<Ulid>,
    lease_id: Option<Ulid>,
    launch_time: Option<aws_sdk_ec2::primitives::DateTime>,
}

impl From<(&aws_sdk_ec2::types::Instance, &str)> for Instance {
    fn from((i, tag): (&aws_sdk_ec2::types::Instance, &str)) -> Self {
        let id = i.instance_id.as_ref().unwrap().to_string();
        let state =
            i.state.as_ref().unwrap().name.as_ref().unwrap().to_string();

        let launch_time = i.launch_time;

        let worker_id = i
            .tags()
            .tag(&format!("{tag}-worker_id"))
            .as_ref()
            .and_then(|v| Ulid::from_str(v).ok());

        let lease_id = i
            .tags()
            .tag(&format!("{tag}-lease_id"))
            .as_ref()
            .and_then(|v| Ulid::from_str(v).ok());

        let ip = i.private_ip_address.clone();

        Instance { id, state, worker_id, lease_id, ip, launch_time }
    }
}

impl Instance {
    fn age_secs(&self) -> u64 {
        self.launch_time
            .map(|dt| {
                let when: u64 = dt.to_millis().unwrap().try_into().unwrap();
                let now: u64 = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis()
                    .try_into()
                    .unwrap();

                (if when <= now { now - when } else { 0 }) / 1000
            })
            .unwrap_or(0)
    }
}

trait TagExtractor {
    fn tag(&self, n: &str) -> Option<String>;
}

impl TagExtractor for &[Tag] {
    fn tag(&self, n: &str) -> Option<String> {
        self.iter()
            .find(|t| t.key() == Some(n))
            .and_then(|t| t.value().map(str::to_string))
    }
}

async fn destroy_instance(
    log: &Logger,
    ec2: &aws_sdk_ec2::Client,
    id: &str,
    force_stop: bool,
) -> Result<()> {
    /*
     * Before terminating an instance, attempt to initiate a forced shutdown.
     * There is regrettably no force flag for termination (!) and if we don't do
     * this first, AWS will sometimes wait rather a long (and billable) time
     * before actually terminating a guest.  It is difficult, as the saying
     * goes, to get a man to understand something, when his salary depends upon
     * his not understanding it.
     */
    if force_stop {
        info!(log, "forcing stop of instance {id}...");
        ec2.stop_instances().instance_ids(id).force(true).send().await?;
    }

    info!(log, "terminating instance {id}...");
    ec2.terminate_instances().instance_ids(id).send().await?;

    Ok(())
}

async fn create_instance(
    log: &Logger,
    ec2: &aws_sdk_ec2::Client,
    config: &ConfigFile,
    target: &ConfigFileAwsTarget,
    worker: &FactoryWorker,
    lease_id: &str,
) -> Result<Instance> {
    let id = worker.id.to_string();

    let script = include_str!("../scripts/user_data.sh")
        .replace("%NODENAME%", &format!("w-{id}"))
        .replace("%URL%", &config.general.baseurl)
        .replace("%STRAP%", &worker.bootstrap);

    let script = base64::engine::general_purpose::STANDARD.encode(&script);

    info!(log, "creating an instance (worker {})...", id);
    let res = ec2
        .run_instances()
        .image_id(&target.ami)
        .instance_type(InstanceType::from_str(&target.instance_type)?)
        .key_name(&config.aws.key)
        .min_count(1)
        .max_count(1)
        .tag_specifications(
            TagSpecification::builder()
                .resource_type(ResourceType::Instance)
                .tags(
                    Tag::builder().key("Name").value(format!("w-{id}")).build(),
                )
                .tags(
                    Tag::builder()
                        .key(&config.aws.tag)
                        .value("1".to_string())
                        .build(),
                )
                .tags(
                    Tag::builder()
                        .key(config.aws.tagkey_worker())
                        .value(&id)
                        .build(),
                )
                .tags(
                    Tag::builder()
                        .key(config.aws.tagkey_lease())
                        .value(lease_id)
                        .build(),
                )
                .build(),
        )
        .block_device_mappings(
            BlockDeviceMapping::builder()
                .device_name("/dev/sda1")
                .ebs(
                    EbsBlockDevice::builder()
                        .volume_size(target.root_size_gb)
                        .build(),
                )
                .build(),
        )
        .network_interfaces(
            InstanceNetworkInterfaceSpecification::builder()
                .subnet_id(&config.aws.subnet)
                .device_index(0)
                .associate_public_ip_address(false)
                .groups(&config.aws.security_group)
                .build(),
        )
        .user_data(script)
        .send()
        .await?;

    let mut instances = res
        .instances()
        .into_iter()
        .map(|i| Instance::from((i, config.aws.tag.as_str())))
        .collect::<Vec<_>>();

    if instances.len() != 1 {
        bail!("wanted one instance, got {instances:?}");
    } else {
        Ok(instances.pop().unwrap())
    }
}

async fn instances(
    _log: &Logger,
    ec2: &aws_sdk_ec2::Client,
    tag: &str,
    vpc: &str,
) -> Result<HashMap<String, Instance>> {
    let res = ec2
        .describe_instances()
        .filters(Filter::builder().name("tag-key").values(tag).build())
        .filters(Filter::builder().name("vpc-id").values(vpc).build())
        .send()
        .await?;
    if res.next_token.is_some() {
        bail!("did not expect more than one page of results for now");
    }

    Ok(res
        .reservations()
        .iter()
        .map(|r| {
            r.instances().iter().map(|i| {
                let i = Instance::from((i, tag));
                (i.id.to_string(), i)
            })
        })
        .flatten()
        .collect())
}

async fn aws_worker_one(
    log: &Logger,
    c: &Central,
    ec2: &aws_sdk_ec2::Client,
    config: &ConfigFile,
) -> Result<()> {
    /*
     * Get a complete list of instances that have the buildomat tag.
     */
    debug!(log, "scanning for instances...");
    let insts = instances(log, ec2, &config.aws.tag, &config.aws.vpc).await?;

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
                        let t = if let Some(t) = c.config.target.get(&w.target)
                        {
                            t
                        } else {
                            error!(
                                log,
                                "instance {} worker {} unknown target: {:?}",
                                i.id,
                                w.id,
                                w.target,
                            );
                            continue;
                        };

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
                                body.private(&i.id)
                                    .ip(i.ip.clone())
                                    .metadata(Some(c.metadata(t)))
                            })
                            .send()
                            .await?;
                    }

                    if w.hold {
                        /*
                         * If a worker is held, we don't want to mess with it at
                         * all.
                         */
                        debug!(
                            log,
                            "instance {} is for held worker {}", i.id, w.id
                        );
                        continue;
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
            destroy_instance(log, ec2, &i.id, &i.state == "running").await?;
        }
    }

    /*
     * At this point we have examined all of the instances which exist.  If
     * there are any worker records left that do not have an associated
     * instance, they must be scrubbed from the database as detritus from prior
     * failed runs.
     */
    let mut nheld = 0;
    for w in c.client.factory_workers().send().await?.into_inner() {
        if w.hold {
            /*
             * If a worker is held, we don't want to mess with it at all.
             */
            nheld += 1;
            continue;
        }

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
                } else if !w.online && i.age_secs() > 8 * 60 {
                    /*
                     * We have seen a recent spate of AWS instances that
                     * hang in early boot.  They get destroyed eventually,
                     * but we should terminate them more promptly than that
                     * to avoid being billed for AWS bullshit.
                     */
                    error!(
                        log,
                        "worker {} instance {} hung; destroying after {} secs",
                        w.id,
                        i.id,
                        i.age_secs(),
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
    let freeslots = config.aws.limit_total.saturating_sub(c_insts);

    info!(log, "worker stats";
        "instances" => c_insts,
        "freeslots" => freeslots,
        "nheld" => nheld,
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

        let i = create_instance(log, ec2, config, t, &w, &lease.job).await?;
        created += 1;
        info!(
            log,
            "created instance: {} (IP {})",
            i.id,
            i.ip.as_deref().unwrap_or("?"),
        );

        /*
         * Record the instance ID against the worker for which it was created:
         */
        c.client
            .factory_worker_associate()
            .worker(&w.id)
            .body_map(|body| {
                body.private(&i.id).ip(i.ip).metadata(Some(c.metadata(t)))
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

pub(crate) async fn aws_worker(c: Arc<Central>) -> Result<()> {
    let log = c.log.new(o!("component" => "worker"));

    let region = RegionProviderChain::first_try(Region::new(
        c.config.aws.region.clone(),
    ))
    .region()
    .await
    .ok_or_else(|| anyhow!("could not select region"))?;
    let creds = Credentials::new(
        &c.config.aws.access_key_id,
        &c.config.aws.secret_access_key,
        None,
        None,
        "config-file",
    );

    let cfg = aws_config::defaults(BehaviorVersion::v2024_03_28())
        .region(region)
        .credentials_provider(creds)
        .load()
        .await;

    let ec2 = aws_sdk_ec2::Client::new(&cfg);

    let delay = Duration::from_secs(7);

    info!(log, "start AWS worker task");

    loop {
        if let Err(e) = aws_worker_one(&log, &c, &ec2, &c.config).await {
            error!(log, "worker error: {:?}", e);
        }

        tokio::time::sleep(delay).await;
    }
}
