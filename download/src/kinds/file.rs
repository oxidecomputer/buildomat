/*
 * Copyright 2024 Oxide Computer Company
 */

use super::sublude::*;

use std::fs::File;
use std::os::unix::fs::FileExt;

use anyhow::Result;
use bytes::BytesMut;
use dropshot::Body;
use hyper::{Response, body::Frame};
use slog::{Logger, o};
use tokio::sync::mpsc;

/*
 * Four lots of 256KB is 1MB, which seems like enough total buffering (in the
 * channel) per connection for now:
 */
const BUF_SIZE: usize = 256 * 1024;
const BUF_COUNT: usize = 4;

/**
 * Produce a download response for a given local file.
 *
 * If "head_only" is true, it will be a HEAD response without a body; otherwise,
 * a GET response with the file contents.  If a range is provided, it will be
 * assessed against the actual file size and a partial (206) response will be
 * returned, for both GET and HEAD.
 */
pub async fn stream_from_file(
    log: &Logger,
    info: String,
    f: File,
    range: Option<PotentialRange>,
    head_only: bool,
) -> Result<Response<Body>> {
    let log = log.new(o!("download" => "file"));
    let md = f.metadata()?;
    assert!(md.is_file());
    let file_size = md.len();

    let (tx, rx) = mpsc::channel(BUF_COUNT);

    let (start_at, want_bytes, crange) = if let Some(range) = range {
        match range.single_range(file_size) {
            Some(sr) => {
                if head_only {
                    return make_head_response(Some(sr), file_size, None);
                }

                (sr.start(), sr.content_length(), Some(sr))
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
         * If this is not a range request, just return the whole file.
         */
        if head_only {
            return make_head_response(None, file_size, None);
        }

        (0, file_size, None)
    };

    let mut sw = Stopwatch::start(info, start_at, want_bytes);
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
             * Determine how many bytes we need to read this time, and from
             * where we need to begin reading:
             */
            let remaining = want_bytes.checked_sub(read_bytes).unwrap();
            let read_size = remaining.min(BUF_SIZE.try_into().unwrap());
            let pos = start_at.checked_add(read_bytes).unwrap();

            /*
             * Allocate the buffer we need, then set the length to the target
             * read size.  At this point, the buffer will still be
             * uninitialised.
             *
             * We allocate the full buffer size every time to avoid creating
             * fragmentation with shorter reads at the end of files.
             */
            assert!(read_size > 0);
            let mut buf = BytesMut::with_capacity(BUF_SIZE);
            unsafe { buf.set_len(read_size.try_into().unwrap()) };

            match tokio::task::block_in_place(|| f.read_at(&mut buf, pos)) {
                Ok(0) => {
                    /*
                     * We are careful to issue reads only when we need at least
                     * one byte of data, so hitting EOF is unexpected.
                     */
                    let e = sw.fail(&log, "unexpected end of file");
                    tx.send(e).await.ok();
                    return;
                }
                Ok(sz) => {
                    /*
                     * Trim the buffer to span only what we actually read.  The
                     * readable portion of the buffer is now safely initialised.
                     */
                    unsafe { buf.set_len(sz) };

                    read_bytes =
                        read_bytes.checked_add(sz.try_into().unwrap()).unwrap();

                    /*
                     * Pass the read bytes onto the client.
                     */
                    let buf = buf.freeze();
                    sw.add_bytes(buf.len());
                    if tx.send(Ok(Frame::data(buf))).await.is_err() {
                        sw.fail(&log, "interrupted on client side").ok();
                        return;
                    }
                }
                Err(e) => {
                    let e = sw.fail(&log, &format!("local file error: {e}"));

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
