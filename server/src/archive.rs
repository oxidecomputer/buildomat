/*
 * Copyright 2023 Oxide Computer Company
 */

use std::io::ErrorKind;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Result};
use chrono::prelude::*;
#[allow(unused_imports)]
use slog::{debug, error, info, warn, Logger};

use super::{db, Central};

async fn archive_files_one(
    log: &Logger,
    c: &Central,
    s3: &aws_sdk_s3::Client,
) -> Result<()> {
    while let Some(jf) = c.db.job_file_next_unarchived()? {
        let key = c.file_object_key(jf.job, jf.id);
        info!(
            log,
            "uploading file {} from job {} at {}:{}",
            jf.id,
            jf.job,
            c.config.storage.bucket,
            key
        );

        /*
         * Open the job file in the local store.  We first need to determine the
         * total size to include in the put request, and confirm that the size
         * in the database matches the local file size.
         */
        let p = c.file_path(jf.job, jf.id)?;

        let f = tokio::fs::File::open(&p).await?;
        let file_size = f.metadata().await?.len();
        if file_size != jf.size.0 {
            bail!(
                "local file {:?} size {} != database size {}",
                p,
                file_size,
                jf.size.0,
            );
        }

        let stream = aws_smithy_http::byte_stream::ByteStream::read_from()
            .file(f)
            .build()
            .await?;

        let res = s3
            .put_object()
            .bucket(&c.config.storage.bucket)
            .key(&key)
            .content_length(file_size.try_into()?)
            .body(stream)
            .send()
            .await?;

        info!(
            log,
            "uploaded file {} from job {} at {}:{}",
            jf.id, jf.job, c.config.storage.bucket, key;
            "etag" => res.e_tag, "version" => res.version_id
        );

        c.db.job_file_mark_archived(&jf, Utc::now())?;
    }

    debug!(log, "no more files to upload");
    Ok(())
}

async fn clean_files_one(log: &Logger, c: &Central) -> Result<()> {
    let mut ents = c.file_dir()?.read_dir()?;
    while let Some(ent) = ents.next().transpose()? {
        let md = ent.path().symlink_metadata()?;
        if !md.is_dir() {
            warn!(log, "unexpected entry at {:?}", ent.path());
            continue;
        }

        /*
         * Directories in the file directory are named for the ID of their job.
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
        let job = if let Some(job) = c.db.job_by_id_opt(jid)? {
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
                "file directory for job not in database: {:?}",
                ent.path()
            );
            continue;
        };

        /*
         * Inspect each file in the file directory for this job.
         */
        let mut ents = ent.path().read_dir()?;
        while let Some(ent) = ents.next().transpose()? {
            let md = ent.path().symlink_metadata()?;
            if !md.is_file() {
                warn!(log, "unexpected entry at {:?}", ent.path());
                continue;
            }

            /*
             * Files in the job file directory are named for the ID of
             * the particular file.
             */
            let fid: db::JobFileId =
                if let Some(name) = ent.file_name().to_str() {
                    match name.parse() {
                        Ok(id) => id,
                        Err(e) => {
                            warn!(
                                log,
                                "directory name not File ID at {:?}: {:?}",
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

            let file = if let Some(file) = c.db.job_file_by_id_opt(jid, fid)? {
                if file.time_archived.is_none() {
                    /*
                     * Ignore files not yet archived to the object store.
                     */
                    continue;
                }
                file
            } else {
                /*
                 * If the server is interrupted during commit of a file, then
                 * that partial file will continue to exist in the output
                 * directory after restart.  There will be no record of the file
                 * in the database, though, so we can safely remove it:
                 */
                warn!(
                    log,
                    "removing file not found in database for job {}: {:?}",
                    job.id,
                    ent.path(),
                );
                std::fs::remove_file(ent.path())?;
                continue;
            };

            info!(
                log,
                "removing archived job file {} for job {} at {:?}",
                file.id,
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

pub(crate) async fn archive_files(log: Logger, c: Arc<Central>) -> Result<()> {
    let delay = Duration::from_secs(15);

    info!(log, "start file archive task");

    loop {
        if let Err(e) = archive_files_one(&log, &c, &c.s3).await {
            error!(log, "file archive task error: {:?}", e);
        }

        if let Err(e) = clean_files_one(&log, &c).await {
            error!(log, "file clean task error: {:?}", e);
        }

        tokio::time::sleep(delay).await;
    }
}
