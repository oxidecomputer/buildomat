/*
 * Copyright 2021 Oxide Computer Company
 */

use chrono::prelude::*;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
#[allow(unused_imports)]
use slog::{error, info, warn, Logger};

use super::Central;

async fn worker_cleanup_one(log: &Logger, c: &Central) -> Result<()> {
    /*
     * We want to set a rough timeout for job execution to prevent things
     * getting hung and running forever.
     */
    for w in c.db.workers()? {
        if w.recycle || w.deleted {
            continue;
        }

        let jobs = c.db.worker_jobs(w.id)?;
        if jobs.is_empty() {
            continue;
        }

        for j in jobs {
            if j.failed || j.complete {
                /*
                 * This will get cleaned up in the usual way.
                 */
                continue;
            }

            /*
             * Determine when we assigned this job to a worker by looking at the
             * timestamp on the first control event.
             */
            let control =
                c.db.job_events(j.id, 0)?
                    .iter()
                    .find(|jev| jev.stream == "control")
                    .cloned();
            if let Some(control) = control {
                if control.age().as_secs() > c.config.job.max_runtime {
                    warn!(
                        log,
                        "job {} duration {} exceeds {} seconds; \
                        recycling worker {}",
                        j.id,
                        control.age().as_secs(),
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
                            control.age().as_secs(),
                            c.config.job.max_runtime,
                        ),
                    )?;
                    c.db.worker_recycle(w.id)?;
                }
            }
        }
    }

    Ok(())
}

pub(crate) async fn worker_cleanup(log: Logger, c: Arc<Central>) -> Result<()> {
    let delay = Duration::from_secs(47);
    info!(log, "start worker cleanup task");

    loop {
        if let Err(e) = worker_cleanup_one(&log, &c).await {
            error!(log, "worker cleanup task error: {:?}", e);
        }

        tokio::time::sleep(delay).await;
    }
}
