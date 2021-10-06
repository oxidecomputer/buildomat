/*
 * Copyright 2021 Oxide Computer Company
 */

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use chrono::prelude::*;
#[allow(unused_imports)]
use slog::{error, info, warn, Logger};

use super::Central;

async fn job_assignment_one(log: &Logger, c: &Central) -> Result<()> {
    /*
     * Grab a list of free workers that we can assign to jobs.
     */
    let mut freeworkers =
        c.db.free_workers()?.iter().map(|w| w.id).collect::<Vec<_>>();
    let mut needfree = 0;

    for j in c.db.jobs()?.iter() {
        if j.complete {
            /*
             * Don't do anything with jobs that have already been completed.
             */
            continue;
        }

        if let Some(wid) = &j.worker {
            /*
             * This job has already been assigned.  Check to see if the worker
             * still exists.
             */
            let w = c.db.worker_get(wid)?;
            if w.deleted || w.recycle {
                info!(
                    log,
                    "failing job {}, assigned to deleted worker {}", j.id, wid
                );

                c.db.job_append_event(
                    &j.id,
                    None,
                    "control",
                    &Utc::now(),
                    "worker failed without completing job",
                )?;
                c.db.job_complete(&j.id, true)?;
            }
            continue;
        }

        if let Some(fw) = freeworkers.pop() {
            info!(log, "assigning job {} to worker {}", j.id, fw);
            c.db.worker_assign_job(&fw, &j.id)?;
            continue;
        }

        /*
         * Count jobs that have not yet been assigned to a worker.
         */
        needfree += 1;
    }

    /*
     * Notify worker management that we have jobs that do not have assigned
     * workers yet.
     */
    c.inner.lock().unwrap().needfree = needfree;

    Ok(())
}

pub(crate) async fn job_assignment(log: Logger, c: Arc<Central>) -> Result<()> {
    let delay = Duration::from_secs(1);
    info!(log, "start job assignment task");

    loop {
        if let Err(e) = job_assignment_one(&log, &c).await {
            error!(log, "job assignment task error: {:?}", e);
        }

        tokio::time::sleep(delay).await;
    }
}
