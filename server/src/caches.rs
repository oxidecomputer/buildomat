/*
 * Copyright 2026 Oxide Computer Company
 */

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Error, Result};
use aws_sdk_s3::error::ProvideErrorMetadata as _;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use slog::{debug, error, info, warn, Logger};

use crate::db::CachePendingUpload;
use crate::Central;

async fn persist_upload(
    log: &Logger,
    c: &Arc<Central>,
    upload: &CachePendingUpload,
) -> Result<()> {
    /*
     * We cannot have more than one cache for the same owner and name.  If one
     * already exists, we have to discard this one, or inserting the upload in
     * the database would fail.
     *
     * It's fine to do the check at the start of the function, as only one
     * persist job will run at a time in the background task.
     */
    if c.db.cache_file_by_name(upload.owner, &upload.name)?.is_some() {
        warn!(
            log,
            "there is already a cache with owner {} and name {:?}, \
             discarding cache ID {:?}",
            upload.owner,
            upload.name,
            upload.id,
        );

        /*
         * Aborting the multipart upload will both cleanup the temporary chunks
         * stored in S3 and avoid creating the actual object in S3.
         */
        let response =
            c.s3.abort_multipart_upload()
                .bucket(&c.config.storage.bucket)
                .key(c.cache_object_key(upload.id))
                .upload_id(&upload.s3_upload_id)
                .send()
                .await;
        match response {
            /*
             * The NoSuchUpload error would happen if the multipart upload is
             * not present in S3.  In those cases the data is lost anyway, so we
             * should discard the upload from our database instead of erroring
             * out (which would lead to a retry a second later).
             */
            Err(err) if err.code() != Some("NoSuchUpload") => {
                return Err(Error::from(err).context(format!(
                    "failed to abort multipart upload {:?} for cache {}",
                    upload.s3_upload_id, upload.id
                )));
            }
            _ => {
                c.db.discard_cache_upload(upload.id)?;
                return Ok(());
            }
        }
    }

    info!(log, "persisting upload of cache {}", upload.id);
    /*
     * Note that AWS documents that completing a multipart upload could take
     * several minutes in case of large objects.
     */
    let response =
        c.s3.complete_multipart_upload()
            .bucket(&c.config.storage.bucket)
            .key(c.cache_object_key(upload.id))
            .upload_id(&upload.s3_upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(
                        upload
                            .etags
                            .as_ref()
                            .ok_or_else(|| {
                                anyhow!("missing etags in finished upload")
                            })?
                            .iter()
                            .enumerate()
                            .map(|(idx, etag)| {
                                CompletedPart::builder()
                                    .part_number(idx as i32 + 1)
                                    .e_tag(etag)
                                    .build()
                            })
                            .collect(),
                    ))
                    .build(),
            )
            .send()
            .await;
    match response {
        Ok(_) => {
            c.db.persist_cache_upload(upload.id)?;
            info!(log, "successfully persisted cache {}", upload.id);
            Ok(())
        }
        Err(err) if err.code() == Some("NoSuchUpload") => {
            /*
             * There might be a case where the buildomat server crashes after
             * completing the multipart upload but before updating the database.
             * In the most cases that should not result in a problem, as the job
             * would be retried and the S3 API appears to be idempotent when
             * dealing with multipart uploads (that's undocumented though).
             *
             * In case buildomat crashes for so long that we are past the
             * idempotency window, S3 is documented to return a NoSuchUpload
             * error.  In that case we discard the cache upload to remove it
             * from the queue: a worker will have to upload it again.
             */
            c.db.discard_cache_upload(upload.id)?;
            warn!(log, "multipart upload for cache {} disappeared", upload.id);
            Ok(())
        }
        Err(err) => Err(Error::from(err).context(format!(
            "failed to complete multipart upload {:?} for cache {}",
            upload.s3_upload_id, upload.id
        ))),
    }
}

pub(crate) async fn persist_uploads_task(log: Logger, c: Arc<Central>) {
    let delay = Duration::from_secs(1);
    info!(log, "start persist cache uploads task");

    loop {
        tokio::time::sleep(delay).await;

        let to_persist = match c.db.finished_cache_uploads() {
            Ok(to_persist) => to_persist,
            Err(err) => {
                error!(log, "failed to get finished cache uploads: {err}");
                continue;
            }
        };
        if to_persist.is_empty() {
            debug!(log, "no cache uploads to persist");
            continue;
        }

        info!(log, "found {} cache uploads to persist", to_persist.len());
        for upload in to_persist {
            if let Err(err) = persist_upload(&log, &c, &upload).await {
                error!(log, "failed to persist cache {}: {err}", upload.id);
            }
        }
    }
}
