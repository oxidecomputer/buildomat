/*
 * Copyright 2021 Oxide Computer Company
 */

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use chrono::prelude::*;
#[allow(unused_imports)]
use slog::{error, info, warn, Logger};

use super::db::{FactoryId, JobId};
use super::Central;

/*
 * Give a factory a minute to create a worker, or to extend the lease.
 */
const LEASE_LENGTH: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub struct Lease {
    pub job: JobId,
    pub factory: FactoryId,
    pub expiry: Instant,
}

#[derive(Default)]
pub struct Leases {
    pub leases: BTreeMap<JobId, Lease>,
}

impl Leases {
    pub fn renew_lease(&mut self, job: JobId, factory: FactoryId) -> bool {
        if let Some(l) = self.leases.get_mut(&job) {
            if l.factory != factory {
                false
            } else {
                l.expiry = Instant::now().checked_add(LEASE_LENGTH).unwrap();
                true
            }
        } else {
            false
        }
    }

    pub fn take_lease(&mut self, job: JobId, factory: FactoryId) -> bool {
        if self.leases.contains_key(&job) {
            return false;
        }

        let old = self.leases.insert(
            job,
            Lease {
                job,
                factory,
                expiry: Instant::now().checked_add(LEASE_LENGTH).unwrap(),
            },
        );
        assert!(old.is_none());
        true
    }
}

async fn job_assignment_one(log: &Logger, c: &Central) -> Result<()> {
    /*
     * Grab a list of free workers that we can assign to jobs.
     */
    let mut freeworkers =
        c.db.free_workers()?.iter().map(|w| w.id).collect::<Vec<_>>();

    for j in c.db.jobs_active()?.iter() {
        assert!(!j.complete);
        assert!(!j.waiting);

        if let Some(wid) = j.worker {
            /*
             * This job has already been assigned.  Check to see if the worker
             * still exists.
             */
            let w = c.db.worker_get(wid)?;
            if w.deleted || w.recycle {
                info!(
                    log,
                    "failing job {}, assigned to deleted worker {}", j.id, w.id
                );

                c.db.job_append_event(
                    &j.id,
                    None,
                    "control",
                    Utc::now(),
                    None,
                    "worker failed without completing job",
                )?;
                c.db.job_complete(j.id, true)?;
            }
            continue;
        }

        if let Some(fw) = freeworkers.pop() {
            info!(log, "assigning job {} to worker {}", j.id, fw);
            c.db.worker_assign_job(fw, j.id)?;
            continue;
        }
    }

    Ok(())
}

async fn job_waiters_one(log: &Logger, c: &Central) -> Result<()> {
    /*
     * Look at jobs that are waiting for inputs or dependency satisfaction.
     */
    for j in c.db.jobs_waiting()?.iter() {
        assert!(j.waiting);

        let inputs = c.db.job_inputs(&j.id)?;
        if inputs.iter().any(|(_, f)| f.is_none()) {
            /*
             * At least one input file is not yet uploaded.
             */
            continue;
        }

        info!(log, "waking up job {}", j.id);
        c.db.job_wakeup(&j.id)?;
    }

    Ok(())
}

async fn recycle_on_complete_one(log: &Logger, c: &Central) -> Result<()> {
    /*
     * First, check to see if there are any workers that have completed the
     * job they have been assigned.
     */
    for w in c.db.workers()?.iter() {
        if w.deleted || w.recycle {
            continue;
        }

        if w.token.is_none() && w.age().as_secs() > 1800 {
            /*
             * This worker has existed for half an hour but has not completed
             * bootstrap.  We should recycle it so that we can create a new one.
             */
            warn!(
                log,
                "worker {} (factory {} private {:?} \
                never bootstrapped, recycling!",
                w.id,
                w.factory(),
                w.factory_private,
            );
            c.db.worker_recycle(&w.id)?;
            continue;
        }

        let jobs = c.db.worker_jobs(&w.id)?;
        if !jobs.is_empty() && jobs.iter().all(|j| j.complete) {
            info!(log, "worker {} assigned jobs are complete, recycle", w.id,);
            c.db.worker_recycle(&w.id)?;
        }
    }

    Ok(())
}

async fn lease_cleanup_one(log: &Logger, c: &Central) -> Result<()> {
    /*
     * Clean up the lease table.
     */
    let leases = c
        .inner
        .lock()
        .unwrap()
        .leases
        .leases
        .keys()
        .copied()
        .collect::<Vec<_>>();
    let remove = leases
        .iter()
        .map(|id| Ok((id, c.db.job_by_id_opt(id)?)))
        .collect::<Result<Vec<_>>>()?
        .iter()
        .filter(|(_, job)| {
            if let Some(job) = job {
                /*
                 * This job has been assigned to a worker.
                 */
                job.worker.is_some()
            } else {
                /*
                 * The job no longer appears in the database for whatever
                 * reason, so it should definitely be removed.
                 */
                true
            }
        })
        .map(|(id, _)| **id)
        .collect::<Vec<_>>();
    let now = Instant::now();
    c.inner.lock().unwrap().leases.leases.retain(|id, lease| {
        if remove.iter().any(|rid| rid == id) {
            info!(log, "job {} removed from lease table", id);
            false
        } else if lease.expiry.saturating_duration_since(now).is_zero() {
            warn!(log, "job {} factory {} lease expired", id, lease.factory);
            false
        } else {
            true
        }
    });

    Ok(())
}

pub(crate) async fn job_assignment(log: Logger, c: Arc<Central>) -> Result<()> {
    let delay = Duration::from_secs(1);
    info!(log, "start job assignment task");

    loop {
        if let Err(e) = lease_cleanup_one(&log, &c).await {
            error!(log, "factory lease cleanup task error: {:?}", e);
        }

        if let Err(e) = recycle_on_complete_one(&log, &c).await {
            error!(log, "worker recycle task error: {:?}", e);
        }

        if let Err(e) = job_waiters_one(&log, &c).await {
            error!(log, "job waiters task error: {:?}", e);
        }

        if let Err(e) = job_assignment_one(&log, &c).await {
            error!(log, "job assignment task error: {:?}", e);
        }

        tokio::time::sleep(delay).await;
    }
}
