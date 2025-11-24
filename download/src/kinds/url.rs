/*
 * Copyright 2024 Oxide Computer Company
 */

use super::sublude::*;

use anyhow::{Result, anyhow, bail};

use dropshot::Body;
use futures::TryStreamExt;
use hyper::{
    Response, StatusCode,
    body::Frame,
    header::{CONTENT_LENGTH, CONTENT_RANGE, RANGE},
};
use slog::{Logger, o};
use tokio::sync::mpsc;

const BUF_COUNT: usize = 10;

/**
 * Produce a download response for a given URL, essentially proxying the
 * request.
 *
 * If "head_only" is true, it will be a HEAD response without a body; otherwise,
 * a GET response with the file contents.  If a range is provided, it will be
 * assessed against the actual file size and a partial (206) response will be
 * returned, for both GET and HEAD.
 */
pub async fn stream_from_url(
    log: &Logger,
    info: String,
    client: &reqwest::Client,
    url: String,
    range: Option<PotentialRange>,
    head_only: bool,
    content_type: String,
    assume_file_size: Option<u64>,
) -> Result<Option<Response<Body>>> {
    let log = log.new(o!("download" => "url"));
    let file_size: u64;
    let (mut body, want_bytes, crange) = if let Some(range) = range {
        if let Some(assumed) = assume_file_size {
            file_size = assumed;
        } else {
            /*
             * If this is a range request, we first need to determine the total
             * file size.
             */
            let head = client.head(&url).send().await?;

            /*
             * Beware the content_length() method on the response: on a HEAD
             * request it is (unhelpfully!) zero, because the body of the
             * request should ultimately be empty.  Parse the actual
             * Content-Length value ourselves:
             */
            file_size = head
                .headers()
                .get(CONTENT_LENGTH)
                .and_then(|hv| hv.to_str().ok())
                .and_then(|cl| cl.parse::<u64>().ok())
                .ok_or_else(|| anyhow!("no content-length in response"))?;

            if head.status() == StatusCode::NOT_FOUND {
                /*
                 * If the backend returns 404, we want to be able to pass that
                 * on to the client.
                 */
                return Ok(None);
            }
        }

        match range.single_range(file_size) {
            Some(hr) => {
                if head_only {
                    return Ok(Some(make_head_response(
                        Some(hr),
                        file_size,
                        Some(content_type.as_str()),
                    )?));
                }

                /*
                 * Begin fetching the resource from the backend with an
                 * appropriate Range header so that we get the expected portion
                 * of the file:
                 */
                let res = client
                    .get(&url)
                    .header(RANGE, hr.to_range())
                    .send()
                    .await?;

                /*
                 * We need a HTTP 206 response from the backend.
                 */
                match res.status() {
                    StatusCode::PARTIAL_CONTENT => (),
                    StatusCode::NOT_FOUND => return Ok(None),
                    o => bail!("wanted HTTP 206 from backend, got {o}"),
                }

                /*
                 * Confirm that the Content-Range header we got back from the
                 * backend is the same as what we're going to pass on to the
                 * client:
                 */
                let got = res
                    .headers()
                    .get(CONTENT_RANGE)
                    .and_then(|hv| hv.to_str().ok());
                let want = Some(hr.to_content_range());
                if got != want.as_deref() {
                    bail!(
                        "backend content-range mismatch: \
                        want {want:?}, got {got:?}"
                    );
                }

                (res.bytes_stream(), hr.content_length(), Some(hr))
            }
            None => {
                /*
                 * Return a HTTP 416 Range Not Satisfiable to the client.
                 */
                return Ok(Some(bad_range_response(file_size)));
            }
        }
    } else {
        /*
         * If this is not a range request, just fetch the whole file.
         */
        let res = client.get(&url).send().await?;
        file_size = res
            .content_length()
            .ok_or_else(|| anyhow!("no content-length in response"))?;

        match res.status() {
            StatusCode::OK => (),
            StatusCode::NOT_FOUND => return Ok(None),
            o => bail!("wanted HTTP 200 from backend, got {o}"),
        }

        if head_only {
            return Ok(Some(make_head_response(
                None,
                file_size,
                Some(content_type.as_str()),
            )?));
        }

        (res.bytes_stream(), file_size, None)
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
             * Determine how many bytes are left to transfer:
             */
            let remaining = want_bytes.checked_sub(read_bytes).unwrap();

            match body.try_next().await {
                Ok(None) => {
                    /*
                     * We are careful to issue reads only when we need at least
                     * one byte of data, so hitting EOF is unexpected.
                     */
                    let e =
                        sw.fail(&log, "unexpected end of stream from backend");
                    tx.send(e).await.ok();
                    return;
                }
                Ok(Some(data)) => {
                    let len: u64 = data.len().try_into().unwrap();
                    if len > remaining {
                        let x = len - remaining;
                        let e = sw.fail(
                            &log,
                            &format!("overrun from backend ({x} bytes)"),
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
                    let e = sw.fail(&log, &format!("backend error: {e}"));

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
    Ok(Some(make_get_response(
        crange,
        file_size,
        Some(content_type.as_str()),
        rx,
    )?))
}
