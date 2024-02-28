/*
 * Copyright 2024 Oxide Computer Company
 */

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use chrono::prelude::*;
#[allow(unused_imports)]
use slog::{error, info, warn, Logger};

use super::db::{FactoryId, JobId, TargetId, WorkerId};
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
     * Grab a list of free workers that we can assign to jobs and sort them into
     * a separate queue per target type.
     */
    let mut freeworkers: HashMap<TargetId, Vec<WorkerId>> = Default::default();
    c.db.free_workers()?.iter().for_each(|w| {
        freeworkers.entry(w.target()).or_default().push(w.id);
    });

    for j in c.db.jobs_active(10_000)?.iter() {
        assert!(!j.complete);
        assert!(!j.waiting);
        assert!(!j.is_archived());

        if j.cancelled {
            /*
             * If the user or an administrator has cancelled a job, we need to
             * recycle its worker.
             */
            if let Some(wid) = j.worker {
                let w = c.db.worker(wid)?;
                if !w.deleted && !w.recycle {
                    info!(
                        log,
                        "recycling worker {} for cancelled job {}", w.id, j.id,
                    );
                    c.db.worker_recycle(w.id)?;
                }
                info!(log, "failing job {}, cancelled while running", j.id);
            } else {
                /*
                 * This job was cancelled before we were able to assign it to a
                 * worker.
                 */
                info!(log, "failing job {}, cancelled before assignment", j.id);
            }
            c.complete_job(log, j.id, true)?;
            continue;
        }

        if let Some(wid) = j.worker {
            /*
             * This job has already been assigned.  Check to see if the worker
             * still exists.
             */
            let w = c.db.worker(wid)?;
            if w.deleted || w.recycle {
                info!(
                    log,
                    "failing job {}, assigned to deleted worker {}", j.id, w.id
                );

                c.db.job_append_event(
                    j.id,
                    None,
                    "control",
                    Utc::now(),
                    None,
                    "worker failed without completing job",
                )?;
                c.complete_job(log, j.id, true)?;
            }
            continue;
        }

        /*
         * We must take care to assign jobs only to workers of the correct
         * target type.
         */
        if let Some(fwq) = freeworkers.get_mut(&j.target()) {
            if let Some(fw) = fwq.pop() {
                info!(log, "assigning job {} to worker {}", j.id, fw);
                c.db.worker_assign_job(fw, j.id)?;
                continue;
            }
        }
    }

    Ok(())
}

async fn job_waiters_one(log: &Logger, c: &Central) -> Result<()> {
    /*
     * Look at jobs that are waiting for inputs or dependency satisfaction.
     *
     * Process dependencies first.
     */
    'job: for j in c.db.jobs_waiting(10_000)?.iter() {
        assert!(j.waiting);
        assert!(!j.is_archived());

        if j.cancelled {
            /*
             * This job was cancelled before it was ready to run.
             */
            info!(log, "failing job {}, cancelled while waiting", j.id);
            c.complete_job(log, j.id, true)?;
            continue 'job;
        }

        /*
         * First, check for any jobs that this job depends on.
         */
        let depends = c.db.job_depends(j.id)?;
        for d in depends.iter() {
            if d.satisfied {
                /*
                 * This dependency has been satisfied already.
                 */
                continue;
            }

            let failmsg = if let Some(pj) = c.db.job_opt(d.prior_job)? {
                /*
                 * The job exists!  Check on the status.
                 */
                if !pj.complete {
                    /*
                     * If the prior job is not yet complete, we do not need to
                     * check anything else about this job.
                     */
                    continue 'job;
                }

                if pj.failed {
                    if d.on_failed {
                        c.db.job_depend_satisfy(j.id, d)?;
                        continue;
                    } else {
                        format!(
                            "this job depends on job {} ({}) completing, \
                            but it has failed; failing job",
                            d.prior_job, d.name,
                        )
                    }
                } else if d.on_completed {
                    c.db.job_depend_satisfy(j.id, d)?;
                    continue;
                } else {
                    format!(
                        "this job depends on job {} ({}) failing \
                        but it has completed; failing job",
                        d.prior_job, d.name,
                    )
                }
            } else {
                /*
                 * If the job on which we depend no longer exists, this
                 * dependency can never be satisfied and we must fail this job.
                 */
                format!(
                    "this job depends on job {} ({}) which has \
                    been deleted; failing job",
                    d.prior_job, d.name,
                )
            };

            c.db.job_append_event(
                j.id,
                None,
                "control",
                Utc::now(),
                None,
                &failmsg,
            )?;
            c.complete_job(log, j.id, true)?;
            continue 'job;
        }

        let inputs = c.db.job_inputs(j.id)?;
        if inputs.iter().any(|jif| jif.file.is_none()) {
            /*
             * At least one input file is not yet uploaded.
             */
            continue 'job;
        }

        /*
         * If all dependencies and inputs are accounted for, release the job for
         * execution.
         */
        info!(log, "waking up job {}", j.id);
        c.db.job_wakeup(j.id)?;
    }

    Ok(())
}

async fn recycle_on_complete_one(log: &Logger, c: &Central) -> Result<()> {
    /*
     * First, check to see if there are any workers that have completed the
     * job they have been assigned.
     */
    for w in c.db.workers_active()?.iter() {
        assert!(!w.deleted);

        if w.recycle {
            continue;
        }

        if w.is_held() {
            /*
             * If this worker is marked on hold, leave it alone no matter how
             * long it may have sat idle.
             */
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
            c.db.worker_recycle(w.id)?;
            continue;
        }

        let jobs = c.db.worker_jobs(w.id)?;
        if !jobs.is_empty() && jobs.iter().all(|j| j.complete) {
            info!(log, "worker {} assigned jobs are complete, recycle", w.id,);
            c.db.worker_recycle(w.id)?;
        }
    }

    Ok(())
}

async fn lease_cleanup_one(log: &Logger, c: &Central) -> Result<()> {
    /*
     * Clean up the lease table.
     */
    let mut leases = c
        .inner
        .lock()
        .unwrap()
        .leases
        .leases
        .keys()
        .copied()
        .collect::<Vec<_>>();
    let remove = leases
        .drain(..)
        .map(|id| Ok((id, c.db.job_opt(id)?)))
        .collect::<Result<Vec<_>>>()?
        .drain(..)
        .filter(|(_, job)| {
            if let Some(job) = job {
                /*
                 * This job has been assigned to a worker, or it has been
                 * cancelled before being assigned.
                 */
                job.worker.is_some() || job.cancelled
            } else {
                /*
                 * The job no longer appears in the database for whatever
                 * reason, so it should definitely be removed.
                 */
                true
            }
        })
        .map(|(id, _)| id)
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
