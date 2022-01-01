/*
 * Copyright 2021 Oxide Computer Company
 */

use std::io::ErrorKind;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Result};
use chrono::prelude::*;
use rusoto_s3::S3;
#[allow(unused_imports)]
use slog::{debug, error, info, warn, Logger};

use super::{db, Central};

async fn archive_outputs_one(
    log: &Logger,
    c: &Central,
    s3: &rusoto_s3::S3Client,
) -> Result<()> {
    while let Some(jo) = c.db.job_output_next_unarchived()? {
        let key = c.output_object_key(&jo.job, &jo.id);
        info!(
            log,
            "uploading output {} path {:?} from job {} at {}:{}",
            jo.id,
            jo.path,
            jo.job,
            c.config.storage.bucket,
            key
        );

        /*
         * Open the job output file in the local store.  We first need to
         * determine the total size to include in the put request.
         */
        let p = c.output_path(&jo.job, &jo.id)?;

        let f = tokio::fs::File::open(&p).await?;
        let content_length = Some(f.metadata().await?.len().try_into()?);

        let stream = tokio_util::io::ReaderStream::new(f);
        let body = Some(rusoto_core::ByteStream::new(stream));

        let res = s3
            .put_object(rusoto_s3::PutObjectRequest {
                bucket: c.config.storage.bucket.to_string(),
                key: key.clone(),
                content_length,
                body,
                ..Default::default()
            })
            .await?;

        info!(
            log,
            "uploaded output {} path {:?} from job {} at {}:{}",
            jo.id, jo.path, jo.job, c.config.storage.bucket, key;
            "etag" => res.e_tag, "version" => res.version_id
        );

        c.db.job_output_mark_archived(&jo, Utc::now())?;
    }

    debug!(log, "no more outputs to upload");
    Ok(())
}

async fn clean_outputs_one(log: &Logger, c: &Central) -> Result<()> {
    let mut ents = c.output_dir()?.read_dir()?;
    while let Some(ent) = ents.next().transpose()? {
        let md = ent.path().symlink_metadata()?;
        if !md.is_dir() {
            warn!(log, "unexpected entry at {:?}", ent.path());
            continue;
        }

        /*
         * Directories in the output directory are named for the ID of their
         * job.
         */
        let jid: db::JobId = if let Some(name) = ent.file_name().to_str() {
            match name.parse() {
                Ok(id) => id,
                Err(e) => {
                    warn!(
                        log,
                        "directory name not Job ID at {:?}: {:?}",
                        ent.path(),
                        e
                    );
                    continue;
                }
            }
        } else {
            warn!(log, "invalid directory name at {:?}", ent.path());
            continue;
        };

        /*
         * Look up the job to see if it is complete.
         */
        let job = if let Some(job) = c.db.job_by_id_opt(&jid)? {
            if !job.complete {
                /*
                 * Ignore present-but-incomplete jobs.
                 */
                continue;
            }
            job
        } else {
            warn!(
                log,
                "output directory for job not in database: {:?}",
                ent.path()
            );
            continue;
        };

        /*
         * Inspect each file in the output directory for this job.
         */
        let mut ents = ent.path().read_dir()?;
        while let Some(ent) = ents.next().transpose()? {
            let md = ent.path().symlink_metadata()?;
            if !md.is_file() {
                warn!(log, "unexpected entry at {:?}", ent.path());
                continue;
            }

            /*
             * Files in the job output directory are named for the ID of
             * the particular output.
             */
            let oid: db::JobOutputId =
                if let Some(name) = ent.file_name().to_str() {
                    match name.parse() {
                        Ok(id) => id,
                        Err(e) => {
                            warn!(
                                log,
                                "directory name not Output ID at {:?}: {:?}",
                                ent.path(),
                                e
                            );
                            continue;
                        }
                    }
                } else {
                    warn!(log, "invalid directory name at {:?}", ent.path());
                    continue;
                };

            let output =
                if let Some(output) = c.db.job_output_by_id_opt(&jid, &oid)? {
                    if output.time_archived.is_none() {
                        /*
                         * Ignore outputs not yet archived to the object store.
                         */
                        continue;
                    }
                    output
                } else {
                    warn!(
                        log,
                        "output file for job output not in database: {:?}",
                        ent.path()
                    );
                    continue;
                };

            info!(
                log,
                "removing archived job output {} for job {} at {:?}",
                output.id,
                job.id,
                ent.path()
            );
            std::fs::remove_file(ent.path())?;
        }

        /*
         * Once we have tried to remove all of the files, try to remove the
         * directory.  This will fail if it is not empty, and that's alright.
         */
        match std::fs::remove_dir(ent.path()) {
            Ok(()) => info!(log, "removed empty directory at {:?}", ent.path()),
            Err(e) if e.kind() == ErrorKind::AlreadyExists => continue,
            Err(e) => {
                bail!("could not remove directory {:?}: {:?}", ent.path(), e);
            }
        }
    }

    Ok(())
}

pub(crate) async fn archive_outputs(
    log: Logger,
    c: Arc<Central>,
) -> Result<()> {
    let delay = Duration::from_secs(15);

    info!(log, "start output archive task");

    loop {
        if let Err(e) = archive_outputs_one(&log, &c, &c.s3).await {
            error!(log, "output archive task error: {:?}", e);
        }

        if let Err(e) = clean_outputs_one(&log, &c).await {
            error!(log, "output clean task error: {:?}", e);
        }

        tokio::time::sleep(delay).await;
    }
}
