/*
 * Copyright 2021 Oxide Computer Company
 */

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
#[allow(unused_imports)]
use slog::{error, info, warn, Logger};

use super::Central;

async fn chunk_cleanup_one(log: &Logger, c: &Central) -> Result<()> {
    /*
     * Get a list of chunk directories.  Each directory will be the ID of a job.
     * If the job is complete, we can remove the entire directory.
     */
    let mut dir = std::fs::read_dir(c.chunk_dir()?)?;

    while let Some(ent) = dir.next().transpose()? {
        if !ent.file_type()?.is_dir() {
            error!(
                log,
                "unexpected item in chunk dir: {:?}: {:?}",
                ent.path(),
                ent
            );
            continue;
        }

        let id = if let Ok(name) = ent.file_name().into_string() {
            match rusty_ulid::Ulid::from_str(name.as_str()) {
                Ok(id) => super::db::JobId(id),
                Err(e) => {
                    error!(
                        log,
                        "chunk directory not ulid: {:?}: {:?}",
                        ent.path(),
                        e
                    );
                    continue;
                }
            }
        } else {
            error!(log, "unexpected item in chunk dir: {:?}", ent.path());
            continue;
        };

        match c.db.job_by_id_opt(&id) {
            Ok(Some(job)) => {
                if !job.complete {
                    continue;
                }

                info!(log, "job {} is complete; removing {:?}", id, ent.path());
            }
            Ok(None) => {
                warn!(log, "job {} not found; removing {:?}", id, ent.path());
            }
            Err(e) => {
                error!(log, "could not look up job {}: {:?}", id, e);
                continue;
            }
        }

        std::fs::remove_dir_all(ent.path())?;
    }

    Ok(())
}

pub(crate) async fn chunk_cleanup(log: Logger, c: Arc<Central>) -> Result<()> {
    let delay = Duration::from_secs(73);
    info!(log, "start chunk cleanup task");

    loop {
        if let Err(e) = chunk_cleanup_one(&log, &c).await {
            error!(log, "chunk cleanup task error: {:?}", e);
        }

        tokio::time::sleep(delay).await;
    }
}
