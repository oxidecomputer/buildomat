/*
 * Copyright 2024 Oxide Computer Company
 */

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Result};
use buildomat_client::types::*;
use chrono::prelude::*;
use rusoto_core::{HttpClient, Region};
use rusoto_credential::StaticProvider;
use rusoto_ec2::{
    BlockDeviceMapping, DescribeInstancesRequest, EbsBlockDevice, Ec2,
    Ec2Client, Filter, InstanceNetworkInterfaceSpecification,
    RunInstancesRequest, Tag, TagSpecification, TerminateInstancesRequest,
};
use rusty_ulid::Ulid;
use slog::{debug, error, info, o, warn, Logger};

use super::{config::ConfigFileAwsTarget, Central, ConfigFile};

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

trait TagExtractor {
    fn tag(&self, n: &str) -> Option<String>;
}

impl TagExtractor for Option<Vec<Tag>> {
    fn tag(&self, n: &str) -> Option<String> {
        if let Some(tags) = self.as_ref() {
            for tag in tags.iter() {
                if let Some(k) = tag.key.as_deref() {
                    if k == n {
                        return tag.value.clone();
                    }
                }
            }
        }

        None
    }
}

async fn destroy_instance(
    log: &Logger,
    ec2: &Ec2Client,
    id: &str,
) -> Result<()> {
    info!(log, "destroying instance {}...", id);
    ec2.terminate_instances(TerminateInstancesRequest {
        instance_ids: vec![id.to_string()],
        ..Default::default()
    })
    .await?;

    Ok(())
}

async fn create_instance(
    log: &Logger,
    ec2: &Ec2Client,
    config: &ConfigFile,
    target: &ConfigFileAwsTarget,
    worker: &FactoryWorker,
    lease_id: &str,
) -> Result<String> {
    let id = worker.id.to_string();

    let script = include_str!("../scripts/user_data.sh")
        .replace("%URL%", &config.general.baseurl)
        .replace("%STRAP%", &worker.bootstrap);

    let script = base64::encode(&script);

    info!(log, "creating an instance (worker {})...", id);
    let res = ec2
        .run_instances(RunInstancesRequest {
            image_id: Some(target.ami.to_string()),
            instance_type: Some(target.instance_type.to_string()),
            key_name: Some(config.aws.key.to_string()),
            min_count: 1,
            max_count: 1,
            tag_specifications: Some(vec![TagSpecification {
                resource_type: Some("instance".to_string()),
                tags: Some(vec![
                    Tag {
                        key: Some("Name".to_string()),
                        value: Some(format!("w-{}", id)),
                    },
                    Tag {
                        key: Some(config.aws.tag.to_string()),
                        value: Some("1".to_string()),
                    },
                    Tag {
                        key: Some(format!("{}-worker_id", config.aws.tag)),
                        value: Some(id),
                    },
                    Tag {
                        key: Some(format!("{}-lease_id", config.aws.tag)),
                        value: Some(lease_id.to_string()),
                    },
                ]),
            }]),
            block_device_mappings: Some(vec![BlockDeviceMapping {
                device_name: Some("/dev/sda1".to_string()),
                ebs: Some(EbsBlockDevice {
                    volume_size: Some(target.root_size_gb),
                    ..Default::default()
                }),
                ..Default::default()
            }]),
            network_interfaces: Some(vec![
                InstanceNetworkInterfaceSpecification {
                    subnet_id: Some(config.aws.subnet.to_string()),
                    device_index: Some(0),
                    associate_public_ip_address: Some(false),
                    groups: Some(vec![config.aws.security_group.to_string()]),
                    ..Default::default()
                },
            ]),
            user_data: Some(script),
            ..Default::default()
        })
        .await?;

    let mut ids = Vec::new();
    if let Some(insts) = &res.instances {
        for i in insts.iter() {
            ids.push(i.instance_id.as_deref().unwrap().to_string());
        }
    }

    if ids.len() != 1 {
        bail!("wanted one instance, got {:?}", ids);
    } else {
        Ok(ids[0].to_string())
    }
}

async fn instances(
    _log: &Logger,
    ec2: &Ec2Client,
    tag: &str,
    vpc: &str,
) -> Result<HashMap<String, Instance>> {
    let filters = Some(vec![
        Filter {
            name: Some("tag-key".to_string()),
            values: Some(vec![tag.to_string()]),
        },
        Filter {
            name: Some("vpc-id".to_string()),
            values: Some(vec![vpc.to_string()]),
        },
    ]);
    let res = ec2
        .describe_instances(DescribeInstancesRequest {
            filters,
            ..Default::default()
        })
        .await?;
    if res.next_token.is_some() {
        bail!("did not expect more than one page of results for now");
    }

    let mut out = HashMap::new();
    if let Some(resv) = &res.reservations {
        for r in resv.iter() {
            if let Some(insts) = &r.instances {
                for i in insts.iter() {
                    let id = i.instance_id.as_ref().unwrap().to_string();
                    let state = i
                        .state
                        .as_ref()
                        .unwrap()
                        .name
                        .as_ref()
                        .unwrap()
                        .to_string();

                    let launch_time = i
                        .launch_time
                        .as_ref()
                        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                        .map(|dt| dt.into());

                    let worker_id = i
                        .tags
                        .tag(&format!("{}-worker_id", tag))
                        .map(|s| Ulid::from_str(&s).ok())
                        .unwrap_or_default();

                    let lease_id = i
                        .tags
                        .tag(&format!("{}-lease_id", tag))
                        .map(|s| Ulid::from_str(&s).ok())
                        .unwrap_or_default();

                    let ip = i.private_ip_address.clone();

                    out.insert(
                        id.to_string(),
                        Instance {
                            id,
                            state,
                            worker_id,
                            lease_id,
                            ip,
                            launch_time,
                        },
                    );
                }
            }
        }
    }

    Ok(out)
}

async fn aws_worker_one(
    log: &Logger,
    c: &Central,
    ec2: &Ec2Client,
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
                            .body_map(|body| body.private(&i.id))
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
            destroy_instance(log, ec2, &i.id).await?;
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
    let freeslots = config.aws.limit_total.saturating_sub(c_insts);

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
            create_instance(log, ec2, config, t, &w, &lease.job).await?;
        created += 1;
        info!(log, "created instance: {}", instance_id);

        /*
         * Record the instance ID against the worker for which it was created:
         */
        c.client
            .factory_worker_associate()
            .worker(&w.id)
            .body_map(|body| body.private(&instance_id))
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

    let credprov = StaticProvider::new_minimal(
        c.config.aws.access_key_id.clone(),
        c.config.aws.secret_access_key.clone(),
    );
    let ec2 = Ec2Client::new_with(
        HttpClient::new()?,
        credprov,
        Region::from_str(&c.config.aws.region)?,
    );

    let delay = Duration::from_secs(7);

    info!(log, "start AWS worker task");

    loop {
        if let Err(e) = aws_worker_one(&log, &c, &ec2, &c.config).await {
            error!(log, "worker error: {:?}", e);
        }

        tokio::time::sleep(delay).await;
    }
}
