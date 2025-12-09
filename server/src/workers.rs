/*
 * Copyright 2025 Oxide Computer Company
 */

use chrono::prelude::*;
use std::time::Duration;
use std::{sync::Arc, time::Instant};

use anyhow::Result;
#[allow(unused_imports)]
use slog::{error, info, warn, Logger};

use super::Central;

const TEN_MINUTES: Duration = Duration::from_secs(10 * 60);

async fn worker_liveness_one(log: &Logger, c: &Central) -> Result<()> {
    for w in c.db.workers_without_pings(TEN_MINUTES)? {
        assert!(!w.deleted);
        assert!(!w.recycle);
        assert!(!w.is_held());

        let ping = w.lastping.unwrap().age().as_secs();
        warn!(log, "worker stopped responding to pings!";
            "id" => w.id.to_string(),
            "lastping" => ping);

        /*
         * Record in the database that the worker has failed.  This routine will
         * take care of reporting failure in any assigned jobs, marking the
         * worker as held, etc.
         */
        let failed_jobs =
            c.db.worker_mark_failed(w.id, "agent became unresponsive")?;
        if !failed_jobs.is_empty() {
            let jobs = failed_jobs
                .into_iter()
                .map(|j| j.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            warn!(log, "worker {} failing caused job {jobs} to fail", w.id);
        }
    }

    Ok(())
}

async fn worker_cleanup_one(log: &Logger, c: &Central) -> Result<()> {
    /*
     * We want to set a rough timeout for job execution to prevent things
     * getting hung and running forever.
     */
    for w in c.db.workers_active()? {
        assert!(!w.deleted);

        if w.recycle {
            continue;
        }

        if w.is_held() {
            /*
             * If the worker is marked on hold, leave it and any related jobs
             * alone.
             */
            continue;
        }

        let jobs = c.db.worker_jobs(w.id)?;
        if jobs.is_empty() {
            /*
             * Idle workers should be assigned relatively promptly.  If a worker
             * has bootstrapped but not been assigned for some time, tear it
             * down.
             */
            if w.agent_ok() && w.age() > Duration::from_secs(30 * 60) {
                info!(log, "recycling surplus worker {} after 30m idle", w.id);
                c.db.worker_recycle(w.id)?;
            }
            continue;
        }

        for j in jobs {
            if j.failed || j.complete {
                /*
                 * This will get cleaned up in the usual way.
                 */
                continue;
            }

            if j.worker.is_none() {
                /*
                 * This job has not yet been assigned to a worker.  We do not
                 * want to cancel jobs that have merely been queued for a long
                 * time.
                 */
                continue;
            }

            /*
             * Determine when we assigned this job to a worker:
             */
            let times = c.db.job_times(j.id)?;
            let Some(atime) = times.get("assigned") else {
                continue;
            };
            let age = Utc::now()
                .signed_duration_since(atime)
                .to_std()
                .unwrap_or_else(|_| std::time::Duration::from_secs(0));

            if age.as_secs() > c.config.job.max_runtime {
                warn!(
                    log,
                    "job {} duration {} exceeds {} seconds; \
                        recycling worker {}",
                    j.id,
                    age.as_secs(),
                    c.config.job.max_runtime,
                    w.id,
                );
                c.db.job_append_event(
                    j.id,
                    None,
                    "control",
                    Utc::now(),
                    None,
                    &format!(
                        "job duration {} exceeds {} seconds; aborting",
                        age.as_secs(),
                        c.config.job.max_runtime,
                    ),
                )?;
                c.db.worker_recycle(w.id)?;
            }
        }
    }

    Ok(())
}

pub(crate) async fn worker_cleanup(log: Logger, c: Arc<Central>) -> Result<()> {
    let start = Instant::now();
    let mut liveness_checks = false;

    let delay = Duration::from_secs(47);
    info!(log, "start worker cleanup task");

    loop {
        if let Err(e) = worker_cleanup_one(&log, &c).await {
            error!(log, "worker cleanup task error: {:?}", e);
        }

        if !liveness_checks {
            /*
             * The liveness worker will compare worker ping times against a
             * threshold.  The ping times are stored in the database, but if the
             * server is down for a measurable period agents might not have had
             * a chance to phone in again and update them immediately after the
             * server comes back.  Only check liveness once the server has been
             * up long enough to know the difference:
             */
            if Instant::now().saturating_duration_since(start) > TEN_MINUTES {
                info!(log, "starting worker liveness checks");
                liveness_checks = true;
            }
        } else if let Err(e) = worker_liveness_one(&log, &c).await {
            error!(log, "worker liveness task error: {:?}", e);
        }

        tokio::time::sleep(delay).await;
    }
}
