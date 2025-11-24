/*
 * Copyright 2024 Oxide Computer Company
 */

use super::sublude::*;

use anyhow::{Result, anyhow, bail};
use dropshot::Body;
use hyper::{Response, body::Frame};
use slog::{Logger, o};
use tokio::sync::mpsc;

const BUF_COUNT: usize = 10;

pub fn unruin_content_length(cl: Option<i64>) -> Result<u64> {
    cl.and_then(|cl| cl.try_into().ok()).ok_or_else(|| {
        anyhow!("invalid content-length from object store: {cl:?}")
    })
}

/**
 * Produce a download response for a given object in the object store.
 *
 * If "head_only" is true, it will be a HEAD response without a body; otherwise,
 * a GET response with the file contents.  If a range is provided, it will be
 * assessed against the actual file size and a partial (206) response will be
 * returned, for both GET and HEAD.
 */
pub async fn stream_from_s3(
    log: &Logger,
    info: String,
    s3: &aws_sdk_s3::Client,
    bucket: &str,
    key: String,
    range: Option<PotentialRange>,
    head_only: bool,
) -> Result<Response<Body>> {
    let log = log.new(o!("download" => "s3"));
    let file_size: u64;
    let (mut obj, want_bytes, crange) = if let Some(range) = range {
        /*
         * If this is a range request, we first need to determine the total file
         * size.
         */
        let head = s3.head_object().bucket(bucket).key(&key).send().await?;
        file_size = unruin_content_length(head.content_length())?;

        match range.single_range(file_size) {
            Some(hr) => {
                if head_only {
                    return make_head_response(Some(hr), file_size, None);
                }

                /*
                 * Begin fetching the resource from the object store with an
                 * appropriate Range header so that we get the expected portion
                 * of the file:
                 */
                let obj = s3
                    .get_object()
                    .bucket(bucket)
                    .key(&key)
                    .range(hr.to_range())
                    .send()
                    .await?;

                /*
                 * Confirm that the Content-Range header we got back from the
                 * object store matches what we're going to pass on to the
                 * client:
                 */
                let got = obj.content_range();
                let want = Some(hr.to_content_range());
                if got != want.as_deref() {
                    bail!(
                        "backend content-range mismatch: \
                        want {want:?}, got {got:?}"
                    );
                }

                (obj, hr.content_length(), Some(hr))
            }
            None => {
                /*
                 * Return a HTTP 416 Range Not Satisfiable to the client.
                 */
                return Ok(bad_range_response(file_size));
            }
        }
    } else {
        /*
         * If this is not a range request, just fetch the whole file.
         */
        let obj = s3.get_object().bucket(bucket).key(&key).send().await?;
        file_size = unruin_content_length(obj.content_length())?;

        if head_only {
            return make_head_response(None, file_size, None);
        }

        (obj, file_size, None)
    };

    let (tx, rx) = mpsc::channel(BUF_COUNT);

    let mut sw = Stopwatch::start(
        info,
        crange.as_ref().map(|sr| sr.start()).unwrap_or(0),
        want_bytes,
    );
    tokio::task::spawn(async move {
        let mut read_bytes = 0;

        loop {
            assert!(read_bytes <= want_bytes);
            if read_bytes == want_bytes {
                /*
                 * We've seen enough!
                 */
                sw.complete(&log);
                return;
            }

            /*
             * Determine how many bytes we need to read this time:
             */
            let remaining = want_bytes.checked_sub(read_bytes).unwrap();

            match obj.body.try_next().await {
                Ok(None) => {
                    /*
                     * We are careful to issue reads only when we need at least
                     * one byte of data, so hitting EOF is unexpected.
                     */
                    let e = sw.fail(
                        &log,
                        "unexpected end of stream from object store",
                    );
                    tx.send(e).await.ok();
                    return;
                }
                Ok(Some(data)) => {
                    let len: u64 = data.len().try_into().unwrap();
                    if len > remaining {
                        let x = len - remaining;
                        let e = sw.fail(
                            &log,
                            &format!("overrun from object store ({x} bytes)"),
                        );
                        tx.send(e).await.ok();
                        return;
                    }

                    read_bytes = read_bytes.checked_add(len).unwrap();

                    /*
                     * Pass the read bytes onto the client.
                     */
                    sw.add_bytes(data.len());
                    if tx.send(Ok(Frame::data(data))).await.is_err() {
                        sw.fail(&log, "interrupted on client side").ok();
                        return;
                    }
                }
                Err(e) => {
                    let e = sw.fail(&log, &format!("object store error: {e}"));

                    /*
                     * Push the error to the client side stream, but ignore a
                     * second failure on the client side.
                     */
                    tx.send(e).await.ok();
                    return;
                }
            }
        }
    });

    if let Some(crange) = &crange {
        assert_eq!(crange.content_length(), want_bytes);
    }
    make_get_response(crange, file_size, None, rx)
}
