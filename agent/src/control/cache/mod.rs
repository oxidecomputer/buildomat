/*
 * Copyright 2026 Oxide Computer Company
 */

use std::borrow::BorrowMut as _;
use std::fs::File;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Error, Result};
use buildomat_common::{render_bytes, DurationExt as _};
use reqwest::header::ETAG;
use reqwest::Client as ReqwestClient;
use tar::{Archive as TarArchive, Builder as TarBuilder};
use tempfile::{NamedTempFile, TempPath};
use tokio::fs::File as TokioFile;
use tokio::io::{AsyncReadExt, AsyncSeekExt as _, AsyncWriteExt as _};
use tokio::sync::Semaphore;
use tokio::task::spawn_blocking;
use zstd::stream::read::Decoder as ZstdReadDecoder;
use zstd::stream::write::Encoder as ZstdWriteEncoder;

use crate::control::protocol::Payload;
use crate::control::Stuff;

const ZSTD_COMPRESSION_LEVEL: i32 = 3;
const PARALLEL_UPLOADS: usize = 30;

pub async fn restore(stuff: &mut Stuff, name: &str) -> Result<()> {
    /*
     * Retrieve the presigned download URL.
     */
    let download_url =
        match stuff.req_retry(Payload::CacheUrl(name.into())).await? {
            Payload::CacheUrlResponse(Some(url)) => {
                eprintln!("cache hit: {name}");
                url
            }
            Payload::CacheUrlResponse(None) => {
                eprintln!("cache miss: {name}");
                return Ok(());
            }
            other => bail!("unexpected response: {other:?}"),
        };

    /*
     * Download the archive into a temporary file.
     */
    let start = Instant::now();
    let mut response = reqwest::get(&download_url).await?.error_for_status()?;
    let mut temp_file = tokio_temp_file()?;
    while let Some(mut item) = response.chunk().await? {
        temp_file.as_file_mut().write_all_buf(item.borrow_mut()).await?;
    }
    let temp_path = temp_file.into_temp_path();
    eprintln!(
        "downloaded cache in {}, cache size is {}",
        start.elapsed().render(),
        render_bytes(temp_path.metadata()?.len())
    );

    /*
     * Extract the contents of the cache into the current working directory.
     * This is done within a spawn_blocking as decompressing in the async task
     * risks stalling the executor.
     */
    let start = Instant::now();
    spawn_blocking(move || {
        let file = File::open(temp_path)?;
        let mut archive = TarArchive::new(ZstdReadDecoder::new(file)?);
        archive.unpack(std::env::current_dir()?)?;
        Ok::<_, Error>(())
    })
    .await??;
    eprintln!("extracted cache in {}", start.elapsed().render());

    Ok(())
}

pub async fn save(
    stuff: &mut Stuff,
    name: &str,
    paths: Vec<PathBuf>,
) -> Result<()> {
    /*
     * We want to avoid generating the archive if the cache is already present,
     * to avoid wasting CI cycles creating compressed archives.
     *
     * This check is only advisory, as the actual check is done when we begin
     * the cache upload: we thus avoid retrying or handling errors.
     */
    match stuff.req(Payload::CacheUrl(name.into())).await? {
        Payload::CacheUrlResponse(Some(_)) => {
            eprintln!("a cache named {name} already exists, skipping upload");
            return Ok(());
        }
        _ => {}
    }

    /*
     * Build the archive that we will upload.
     */
    let start = Instant::now();
    let paths_len = paths.len();
    let archive = spawn_blocking(move || create_archive(&paths)).await??;
    let archive_size = archive.metadata()?.len();
    eprintln!(
        "created archive with {} files in {}, archive size is {}",
        paths_len,
        start.elapsed().render(),
        render_bytes(archive_size),
    );

    /*
     * Begin an upload with the buildomat server, returning the pre-signed
     * upload URLs we have to use, along with the relevant metadata needed to
     * complete the upload.
     */
    let upload = match stuff
        .req_retry(Payload::BeginCacheUpload {
            name: name.into(),
            size_bytes: archive_size,
        })
        .await?
    {
        Payload::BeginCacheUploadOk(upload) => upload,
        Payload::BeginCacheUploadSkip => {
            eprintln!("a cache named {name} already exists, skipping upload");
            return Ok(());
        }
        other => bail!("unexpected response: {other:?}"),
    };
    eprintln!("registered buildomat cache with ID {}", upload.cache_id);

    /*
     * To upload caches we use S3's multipart uploads feature, allowing us to
     * upload chunks of the file in separate requests.  The reason why we chose
     * to adopt them is reliability: very large upload requests are more likely
     * to fail, and in case of errors we can retry uploading only the failed
     * chunk rather than the whole file.
     *
     * Since we are already using multipart uploads, we can speed up the upload
     * by uploading individual chunks in parallel.  We thus start Tokio tasks
     * for every chunk at the start of the upload, and use a semaphore to limit
     * how many concurrent requests we send to S3.
     */
    let start = Instant::now();
    let http = ReqwestClient::new();
    let rate_limit = Arc::new(Semaphore::new(PARALLEL_UPLOADS));
    let chunks_count = upload.chunk_upload_urls.len();
    let mut tasks = Vec::new();
    for (idx, upload_url) in upload.chunk_upload_urls.into_iter().enumerate() {
        let http = http.clone();
        let rate_limit = rate_limit.clone();
        let archive = archive.to_path_buf();
        let chunk_size = upload.chunk_size_bytes;
        tasks.push(tokio::spawn(async move {
            /*
             * Do not add any code before acquiring the rate limit permit, as
             * that code would be executed for all chunks in parallel.
             */
            let _permit = rate_limit.acquire().await?;

            let mut file = TokioFile::open(archive).await?;
            file.seek(SeekFrom::Start(idx as u64 * chunk_size as u64)).await?;

            /*
             * Each chunk other than the last will contain exactly as many
             * bytes as requested by the server.  The last chunk will contain
             * the remainder of the file.
             */
            let content = if idx == chunks_count - 1 {
                let mut content = Vec::new();
                file.read_to_end(&mut content).await?;
                content
            } else {
                let mut content = vec![0; chunk_size as _];
                file.read_exact(&mut content).await?;
                content
            };

            /*
             * Upload the chunk and return the ETag header.  There is no need to
             * authenticate, as the server returns presigned URLs.
             */
            let mut attempt = 1;
            loop {
                let response = http
                    .put(&upload_url)
                    .body(content.clone())
                    .send()
                    .await
                    .and_then(|result| result.error_for_status());
                match response {
                    Ok(response) => {
                        return Ok::<_, Error>(
                            response
                                .headers()
                                .get(ETAG)
                                .ok_or_else(|| anyhow!("no ETag in response"))?
                                .to_str()?
                                .to_string(),
                        );
                    }
                    Err(e) => {
                        if attempt >= 5 {
                            bail!("failed to upload chunk {idx}: {e}");
                        }
                        attempt += 1;

                        eprintln!("failed to upload chunk {idx}, retrying...");
                        tokio::time::sleep(Duration::from_secs(attempt)).await;
                    }
                }
            }
        }));
    }

    /*
     * Join all the chunk upload tasks.  It's load bearing that we join them in
     * the same order as the upload URLs returned by the server, as we need to
     * send the corresponding ETag header values in that order.
     */
    let mut uploaded_etags = Vec::new();
    let mut poison = false;
    for (idx, task) in tasks.into_iter().enumerate() {
        match task.await? {
            Ok(etag) => uploaded_etags.push(etag),
            Err(e) => {
                eprintln!("ERROR: failed to upload chunk {idx}: {e:?}");
                poison = true;
            }
        }
    }
    if poison {
        bail!("one or more chunks failed to upload");
    }

    eprintln!("uploaded {chunks_count} chunks in {}", start.elapsed().render());

    /*
     * Complete the upload of the cache, making it available to other jobs.
     */
    match stuff
        .req_retry(Payload::CompleteCacheUpload {
            cache_id: upload.cache_id,
            uploaded_etags,
        })
        .await?
    {
        Payload::Ack => {}
        other => bail!("unexpected response: {other:?}"),
    }
    eprintln!("cache upload for {name} finished");

    Ok(())
}

/*
 * This function is sync because we need to compress things while archiving
 * them, so we'd need to run parts of it within spawn_blocking anyway.  Making
 * the whole function sync and calling it within spawn_blocking lets us use the
 * more convenient sync APIs for compression and archiving.
 */
fn create_archive(paths: &[PathBuf]) -> Result<TempPath> {
    let mut archive = TarBuilder::new(ZstdWriteEncoder::new(
        NamedTempFile::new()?,
        ZSTD_COMPRESSION_LEVEL,
    )?);
    for path in paths {
        archive.append_path(path)?;
    }
    archive.finish()?;
    /*
     * Calling .into_temp_path() closes the fd, while still ensuring the
     * underlying file gets removed when dropped.
     */
    Ok(archive.into_inner()?.finish()?.into_temp_path())
}

fn tokio_temp_file() -> Result<NamedTempFile<TokioFile>> {
    let (file, path) = NamedTempFile::new()?.into_parts();
    Ok(NamedTempFile::from_parts(TokioFile::from_std(file), path))
}
