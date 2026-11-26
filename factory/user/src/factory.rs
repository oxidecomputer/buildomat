/*
 * Copyright 2026 Oxide Computer Company
 */

use crate::config::ConfigFile;
use crate::db::{types::*, Worker};
use crate::Central;
use anyhow::Result;
use slog::{debug, error, info, o, warn, Logger};
use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

async fn reconcile_with_server(log: &Logger, c: &Arc<Central>) -> Result<()> {
    /*
     * For each worker we are tracking in the local database, check to see if
     * its server record still exists.  If it does not, destroy the worker.
     */
    for worker in c.db.workers()? {
        match worker.state {
            WorkerState::Unconfigured
            | WorkerState::Configured
            | WorkerState::Broken => {}
            WorkerState::Destroying | WorkerState::Destroyed => {
                /*
                 * We don't need to keep looking at workers that are currently
                 * being destroyed.
                 */
                continue;
            }
        }

        let id = worker.id;
        let leased_job = worker.leased_job;
        let server_worker = c
            .client
            .factory_worker_get()
            .worker(id.to_string())
            .send()
            .await?
            .into_inner()
            .worker;

        if let Some(server_worker) = server_worker {
            if server_worker.recycle {
                /*
                 * If the worker has been deleted through the administrative API
                 * then we need to tear it down straight away.
                 */
                info!(log, "worker {id} recycled, destroying it");
                c.db.worker_new_state(id, WorkerState::Destroying)?;
            } else if !matches!(worker.state, WorkerState::Broken)
                && !server_worker.online
            {
                /*
                 * If the worker has not yet bootstrapped try to renew the
                 * lease with the server.  This should prevent duplicate
                 * instance creation when creation or bootstrap is taking
                 * longer than expected.
                 */
                debug!(
                    log,
                    "worker {id} has not bootstrapped yet, \
                     renewing lease for job {leased_job}"
                );
                c.client
                    .factory_lease_renew()
                    .job(leased_job.to_string())
                    .send()
                    .await?;
            }
        } else {
            warn!(log, "worker {id} missing from the server, destroying it");
            c.db.worker_new_state(id, WorkerState::Destroying)?;
        };
    }

    /*
     * At this point we have examined all of the workers which exist in the
     * local database.  If there are any worker records on the server left that
     * do not have an associated record in the factory, they must be scrubbed
     * from the server as detritus from prior failed runs.
     */
    for worker in c.client.factory_workers().send().await?.into_inner() {
        let id = WorkerId::from_str(&worker.id)?;

        /*
         * There is a record of a particular instance ID for this worker.
         * Check to see if that instance exists.
         */
        if let Some(local) = c.db.worker_get(id)? {
            if matches!(local.state, WorkerState::Destroyed) {
                /*
                 * The worker exists, but is terminated.  Delete the
                 * worker on the server too.
                 */
                info!(log, "deleting terminated worker {id} from the server");
                c.client
                    .factory_worker_destroy()
                    .worker(&worker.id)
                    .send()
                    .await?;
                c.db.worker_delete(id)?;
            } else {
                if worker.private.is_none() {
                    /*
                     * Starting the worker might've failed before we could
                     * associate the slot with it.  Rectify that now.
                     */
                    warn!(
                        log,
                        "worker {id} doesn't have a slot associated on the \
                         server, associating it"
                    );
                    c.client
                        .factory_worker_associate()
                        .worker(&worker.id)
                        .body_map(|b| b.private(&local.slot))
                        .send()
                        .await?;
                }
            }
        } else {
            /*
             * The worker does not exist in the local database.
             */
            warn!(log, "clearing unknown worker {id} from the server");
            c.client.factory_worker_destroy().worker(&worker.id).send().await?;
        }
    }

    Ok(())
}

async fn start_worker(log: &Logger, c: &Arc<Central>) -> Result<bool> {
    let avail = available_slots(log, &c.config, &c.db.workers()?);

    /*
     * Check to see if the server requires any new workers.
     */
    let lease = c
        .client
        .factory_lease()
        .body_map(|b| {
            b.supported_targets(
                avail.keys().map(|t| t.to_string()).collect::<Vec<_>>(),
            )
        })
        .send()
        .await?
        .into_inner();
    let Some(lease) = lease.lease else {
        return Ok(false);
    };

    /*
     * Locate a slot for the target the server asked for.
     */
    let Some(slot) = avail.get(&lease.target.parse()?).and_then(|s| s.first())
    else {
        warn!(
            log,
            "server wants target {:?}, but no slots for it are available",
            lease.target
        );
        return Ok(false);
    };

    /*
     * Create the worker on the server and save it in the database.  There could
     * be a failure between the two parts, but reconciliation will handle it (by
     * deleting the worker from the server).
     */
    let worker = c
        .client
        .factory_worker_create()
        .body_map(|b| b.target(&lease.target).wait_for_flush(false))
        .send()
        .await?;
    c.db.worker_create(Worker {
        id: WorkerId::from_str(&worker.id)?,
        slot: slot.clone(),
        state: WorkerState::Unconfigured,
        leased_job: JobId::from_str(&lease.job)?,
        bootstrap: worker.bootstrap.to_string(),
    })?;
    info!(log, "created worker {}", worker.id);

    /*
     * Record the slot we used in the server.  If it fails, reconciliation will
     * take care of updating the value on the server.
     */
    c.client
        .factory_worker_associate()
        .worker(&worker.id)
        .body_map(|b| b.private(slot))
        .send()
        .await?;

    Ok(true)
}

fn available_slots(
    log: &Logger,
    config: &ConfigFile,
    workers: &[Worker],
) -> BTreeMap<TargetId, Vec<String>> {
    /*
     * When slot A is configured to conflict with B, that also implies slot B
     * conflicts with slot A, even if that is not explicitly configured.
     */
    let mut reverse_conflicts: BTreeMap<_, Vec<_>> = BTreeMap::new();
    for (name, slot) in &config.slots {
        for conflict in &slot.conflicts_with_slots {
            reverse_conflicts.entry(conflict).or_default().push(name);
        }
    }

    let mut used_slots = workers
        .iter()
        .filter(|w| !matches!(w.state, WorkerState::Destroyed))
        .map(|w| &w.slot)
        .collect::<BTreeSet<_>>();
    for used in used_slots.clone() {
        /*
         * The slot might not be in the configuration if the worker is created
         * and later its slot is removed from the config.
         */
        let Some(slot) = config.slots.get(used) else { continue };

        /*
         * Mark as used any slot conflicting with an used slot.
         */
        used_slots.extend(slot.conflicts_with_slots.iter());
        if let Some(reverse) = reverse_conflicts.get(used) {
            used_slots.extend(reverse.iter());
        }
    }
    debug!(log, "slots currently in use: {used_slots:?}");

    let mut result: BTreeMap<_, Vec<_>> = BTreeMap::new();
    for (name, slot) in &config.slots {
        if used_slots.contains(name) {
            continue;
        }
        result.entry(slot.target).or_default().push(name.clone());
    }
    result
}

async fn factory_iteration(log: &Logger, c: &Arc<Central>) -> Result<()> {
    reconcile_with_server(log, c).await?;
    while start_worker(log, c).await? {}
    Ok(())
}

pub(crate) async fn factory_loop(c: Arc<Central>) {
    let log = c.log.new(o!("component" => "factory"));

    loop {
        if let Err(e) = factory_iteration(&log, &c).await {
            error!(log, "factory task error: {:?}", e);
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ConfigBuilder;
    use buildomat_common::{make_test_log, make_test_rng, make_test_ulid};
    use expect_test::expect;
    use rand::Rng;

    #[test]
    fn test_available_slots_with_no_workers() {
        let (log, _, cfg) = prep_available_slots_test();

        /*
         * No workers currently running, all slots should be available.
         */
        expect![[r#"
            {
                TargetId(0000000000304KTDGRJWP5BP8H): [
                    "a",
                ],
                TargetId(0000000000D8PRE83ZY7H9CR7C): [
                    "b",
                    "c",
                ],
                TargetId(0000000000EMY607JJG65KGMWT): [
                    "d",
                ],
            }
        "#]]
        .assert_debug_eq(&available_slots(&log, &cfg, &[]));
    }

    #[test]
    fn test_available_slots_with_only_slot_for_target() {
        let (log, mut rng, cfg) = prep_available_slots_test();

        /*
         * A worker is running in slot A, the first target should not be here.
         */
        expect![[r#"
            {
                TargetId(0000000000D8PRE83ZY7H9CR7C): [
                    "b",
                    "c",
                ],
                TargetId(0000000000EMY607JJG65KGMWT): [
                    "d",
                ],
            }
        "#]]
        .assert_debug_eq(&available_slots(&log, &cfg, &[w(&mut rng, "a")]));
    }

    #[test]
    fn test_available_slots_with_multiple_slots_for_target() {
        let (log, mut rng, cfg) = prep_available_slots_test();

        /*
         * A worker is running in slot B, all targets should still be there.
         */
        expect![[r#"
            {
                TargetId(0000000000304KTDGRJWP5BP8H): [
                    "a",
                ],
                TargetId(0000000000D8PRE83ZY7H9CR7C): [
                    "c",
                ],
                TargetId(0000000000EMY607JJG65KGMWT): [
                    "d",
                ],
            }
        "#]]
        .assert_debug_eq(&available_slots(&log, &cfg, &[w(&mut rng, "b")]));
    }

    #[test]
    fn test_available_slots_with_declared_conflict() {
        let (log, mut rng, cfg) = prep_available_slots_test();

        /*
         * A worker is running in slot D, its target should not be there.  Also,
         * since slot D conflicts with slot C, slot C should also be missing.
         */
        let used_d = available_slots(&log, &cfg, &[w(&mut rng, "d")]);
        expect![[r#"
            {
                TargetId(0000000000304KTDGRJWP5BP8H): [
                    "a",
                ],
                TargetId(0000000000D8PRE83ZY7H9CR7C): [
                    "b",
                ],
            }
        "#]]
        .assert_debug_eq(&used_d);

        /*
         * Conflicts are bidirectional.
         */
        let used_c = available_slots(&log, &cfg, &[w(&mut rng, "c")]);
        assert_eq!(used_c, used_d);
    }

    #[test]
    fn test_available_slots_with_all_slots_busy() {
        let (log, mut rng, cfg) = prep_available_slots_test();

        /*
         * No targets should be available.
         */
        expect![[r#"
            {}
        "#]]
        .assert_debug_eq(&available_slots(
            &log,
            &cfg,
            &[
                w(&mut rng, "a"),
                w(&mut rng, "b"),
                w(&mut rng, "c"),
                w(&mut rng, "d"),
            ],
        ));
    }

    #[test]
    fn test_available_slots_with_unknown_busy_slot() {
        let (log, mut rng, cfg) = prep_available_slots_test();

        /*
         * The unknown slot should be ignored.
         */
        expect![[r#"
            {
                TargetId(0000000000304KTDGRJWP5BP8H): [
                    "a",
                ],
                TargetId(0000000000D8PRE83ZY7H9CR7C): [
                    "b",
                    "c",
                ],
                TargetId(0000000000EMY607JJG65KGMWT): [
                    "d",
                ],
            }
        "#]]
        .assert_debug_eq(&available_slots(&log, &cfg, &[w(&mut rng, "lol")]));
    }

    fn prep_available_slots_test() -> (Logger, impl Rng, ConfigFile) {
        let log = make_test_log();
        let mut rng = make_test_rng();

        let target1 = TargetId(make_test_ulid(&mut rng));
        let target2 = TargetId(make_test_ulid(&mut rng));
        let target3 = TargetId(make_test_ulid(&mut rng));

        let cfg = ConfigBuilder::new()
            .slot("a", target1, |_| {})
            .slot("b", target2, |_| {})
            .slot("c", target2, |_| {})
            .slot("d", target3, |s| s.conflicts_with_slots = vec!["c".into()])
            .build();

        (log, rng, cfg)
    }

    fn w<R: Rng>(rng: &mut R, slot: &str) -> Worker {
        Worker {
            id: WorkerId(make_test_ulid(rng)),
            slot: slot.to_string(),
            state: WorkerState::Configured,
            leased_job: JobId(make_test_ulid(rng)),
            bootstrap: String::new(),
        }
    }
}
