use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Result};
use rusoto_core::{HttpClient, Region};
use rusoto_credential::StaticProvider;
use rusoto_ec2::{
    BlockDeviceMapping, DescribeInstancesRequest, EbsBlockDevice, Ec2,
    Ec2Client, Filter, InstanceNetworkInterfaceSpecification,
    RunInstancesRequest, Tag, TagSpecification, TerminateInstancesRequest,
};
use rusty_ulid::Ulid;
use slog::{debug, error, info, warn, Logger};

use super::{db, Central, ConfigFile};

#[derive(Debug)]
struct Instance {
    id: String,
    state: String,
    ip: Option<String>,
    worker_id: Option<Ulid>,
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
    worker: &db::Worker,
) -> Result<String> {
    let id = worker.id.to_string();
    let mut script = String::new();
    script += "#!/usr/bin/bash\n";

    script += "set -o errexit\n";
    script += "set -o pipefail\n";

    script += "while :; do\n";
    script += "\trm -f /var/tmp/agent\n";
    script += "\tif ! curl -sSf -o /var/tmp/agent '%URL%/file/agent'; then\n";
    script += "\t\tsleep 1\n";
    script += "\t\tcontinue\n";
    script += "\tfi\n";
    script += "\tchmod +rx /var/tmp/agent\n";
    script += "\tif ! /var/tmp/agent install '%URL%' '%STRAP%'; then\n";
    script += "\t\tsleep 1\n";
    script += "\t\tcontinue\n";
    script += "\tfi\n";
    script += "\tbreak\n";
    script += "done\n";

    script += "exit 0\n";

    let script = script
        .replace("%URL%", &config.general.baseurl)
        .replace("%STRAP%", &worker.bootstrap);

    let script = base64::encode(&script);

    info!(log, "creating an instance (worker {})...", id);
    let res = ec2
        .run_instances(RunInstancesRequest {
            image_id: Some(config.aws.ami.to_string()),
            instance_type: Some(config.aws.instance_type.to_string()),
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
                ]),
            }]),
            block_device_mappings: Some(vec![BlockDeviceMapping {
                device_name: Some("/dev/sda1".to_string()),
                ebs: Some(EbsBlockDevice {
                    volume_size: Some(config.aws.root_size_gb),
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

                    let worker_id = i
                        .tags
                        .tag(&format!("{}-worker_id", tag))
                        .map(|s| Ulid::from_str(&s).ok())
                        .unwrap_or_default();

                    let ip = i.private_ip_address.clone();

                    out.insert(
                        id.to_string(),
                        Instance { id, state, worker_id, ip },
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
             * Load the worker record from the database:
             */
            match c.db.worker_get(id) {
                Ok(w) => {
                    debug!(log, "instance {} is for worker {}", i.id, w.id);

                    if let Some(expected) = w.instance_id.as_deref() {
                        if expected != i.id {
                            error!(
                                log,
                                "instance {} for job {} does not \
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
                        c.db.worker_associate(&w.id, &i.id)?;
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
                        false
                    }
                }
                Err(e) => {
                    warn!(
                        log,
                        "instance {} is worker {} which no longer \
                        exists: {:?}",
                        i.id,
                        id,
                        e
                    );
                    true
                }
            }
        } else {
            warn!(log, "instance {} has no worker id; ignoring...", i.id);
            continue;
        };

        if destroy && (&i.state == "running" || &i.state == "stopped") {
            destroy_instance(log, ec2, &i.id).await?;
        }
    }

    /*
     * At this point we have examined all of the instances which exist.  If
     * there are any worker records left that do not have an associated
     * instance, they must be scrubbed from the database as detritus from prior
     * failed runs.
     */
    for w in c.db.workers()?.iter() {
        if w.deleted {
            continue;
        }

        if !w.recycle {
            /*
             * Check to see if this worker has been assigned a job which has
             * completed.
             */
            let jobs = c.db.worker_jobs(&w.id)?;
            if !jobs.is_empty() && jobs.iter().all(|j| j.complete) {
                info!(
                    log,
                    "worker {} assigned jobs are complete, recycle", w.id
                );
                c.db.worker_recycle(&w.id)?;
            }
        }

        let rm = if let Some(instance_id) = w.instance_id.as_deref() {
            if let Some(i) = insts.get(&instance_id.to_string()) {
                if i.state == "terminated" {
                    info!(
                        log,
                        "deleting worker {} for terminated instance {}",
                        w.id,
                        instance_id
                    );
                    true
                } else {
                    false
                }
            } else {
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
            warn!(log, "clearing old worker {} with no instance", w.id);
            true
        };

        if rm {
            c.db.worker_destroy(&w.id)?;
        }
    }

    /*
     * Workers move through several relevant states in their life cycle.  First,
     * a worker database entry is created.  This records the intent to create a
     * particular virtual machine.  Once created, the instance ID is recorded
     * against that worker.  Once booted, the agent in that instance bootstraps
     * itself and we have bidirectional communication.
     *
     * Only once bootstrapped is the worker prepared to receive a job, but
     * billing begins as soon as we provision.  An idle bootstrapped agent that
     * sits for 12 hours doing nothing is not a great use of money -- unless low
     * latency from job scheduling to execution starting is more important than
     * cost.
     *
     * We track the notion of "pending" workers: those that have not yet been
     * assigned to a job, regardless of where they are in the provisioning life
     * cycle.
     *
     * Once assigned a job, the worker is "active".  The worker will execute
     * the job script and post results.  Eventually the job will complete or
     * time out and the instance will be shut down and destroyed.  A request to
     * destroy the worker may have been made (recycle is set), but the worker
     * still costs money until it is fully destroyed so we consider
     * pre-destruction recycled workers active for capping purposes.
     *
     * Once destroyed, a worker no longer costs money and it is "complete".
     * Eventually we will archive all records relating to a worker so that they
     * are no longer in the database. (XXX)
     */
    let mut c_pending = 0usize;
    let mut c_active = 0usize;
    let mut c_complete = 0usize;
    for w in c.db.workers()?.iter() {
        if w.deleted {
            c_complete += 1;
            continue;
        }

        if w.recycle {
            /*
             * Any worker that is marked to be recycled but not yet destroyed is
             * considered "active", whether it has had a job assigned or not.
             * This frees up a slot in the spare pool if we recycle a spare
             * worker, without impacting our total cap accounting.
             */
            c_active += 1;
            continue;
        }

        let jobs = c.db.worker_jobs(&w.id)?;
        if jobs.is_empty() {
            /*
             * This worker has not yet been assigned a job.
             */
            c_pending += 1;
            continue;
        }

        c_active += 1;
    }

    /*
     * Count the number of instances we can see in AWS that are not yet
     * completely destroyed.
     */
    let c_insts = insts.values().filter(|i| &i.state != "terminated").count();

    /*
     * Get an estimate of the number of jobs that have been created but not yet
     * assigned to a worker.
     */
    let needfree = c.inner.lock().unwrap().needfree;

    /*
     * Calculate the total number of workers we are willing to create if
     * required.  The hard limit on total instances includes both pending
     * and active workers.
     */
    let freeslots = config
        .aws
        .limit_total
        .saturating_sub(c_active)
        .saturating_sub(c_pending);

    /*
     * Determine how many pending workers we require based on pressure
     * from two sources:
     *  - the number of outstanding jobs not yet assigned to a worker
     *  - the configured size of the pool of spare workers
     */
    let needpending = needfree.saturating_add(config.aws.limit_spares);

    /*
     * Determine how many workers to create to bring the pending pool size
     * up to the desired size, without exceeding the absolute limit on
     * instances:
     */
    let ncreate = freeslots.min(needpending.saturating_sub(c_pending));

    info!(log, "worker stats";
        "pending" => c_pending,
        "active" => c_active,
        "complete" => c_complete,
        "instances" => c_insts,
        "needfree" => needfree,
        "needpending" => needpending,
        "freeslots" => freeslots,
        "ncreate" => ncreate,
    );

    if c_insts > c_pending + c_active {
        /*
         * There should never be more instances than workers.  If there are, we
         * are not correctly tracking things and should create no new instances.
         */
        bail!(
            "only {} active/pending workers, but {} active instances",
            c_pending + c_active,
            c_insts
        );
    }

    let mut created = 0;
    while created < ncreate {
        if c.inner.lock().unwrap().hold {
            /*
             * The operator has requested that we not create any new workers.
             */
            break;
        }

        /*
         * Create workers to fill the standby battery.  Start by assigning a
         * worker ID and bootstrap token:
         */
        let w = c.db.worker_create()?;

        let instance_id = create_instance(log, ec2, config, &w).await?;
        created += 1;
        info!(log, "created instance: {}", instance_id);

        /*
         * Record the instance ID against the worker for which it was created:
         */
        c.db.worker_associate(&w.id, &instance_id)?;
    }

    info!(log, "worker pass complete");
    Ok(())
}

pub(crate) async fn aws_worker(log: Logger, c: Arc<Central>) -> Result<()> {
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

    /*
     * First, look for any instances with the specified tag.
     */

    loop {
        if let Err(e) = aws_worker_one(&log, &c, &ec2, &c.config).await {
            error!(log, "worker error: {:?}", e);
        }

        tokio::time::sleep(delay).await;
    }
}
