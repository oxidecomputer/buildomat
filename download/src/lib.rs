/*
 * Copyright 2024 Oxide Computer Company
 */

use anyhow::Result;
use bytes::Bytes;

use dropshot::Body;
use http_body_util::StreamBody;
use hyper::{
    Response, StatusCode,
    body::Frame,
    header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE},
};

use tokio::sync::mpsc;

mod kinds;
mod stopwatch;

pub use kinds::file::stream_from_file;
pub use kinds::s3::{stream_from_s3, unruin_content_length};
pub use kinds::url::stream_from_url;

fn bad_range_response(file_size: u64) -> Response<Body> {
    hyper::Response::builder()
        .status(StatusCode::RANGE_NOT_SATISFIABLE)
        .header(ACCEPT_RANGES, "bytes")
        .header(CONTENT_RANGE, format!("bytes */{file_size}"))
        .body(Body::wrap(http_body_util::Empty::new()))
        .unwrap()
}

/**
 * Generate a GET response, optionally for a HTTP range request.  The total
 * file length should be provided, whether or not the expected Content-Length
 * for a range request is shorter.
 */
fn make_get_response<E>(
    crange: Option<SingleRange>,
    file_length: u64,
    content_type: Option<&str>,
    rx: mpsc::Receiver<std::result::Result<Frame<Bytes>, E>>,
) -> Result<Response<Body>>
where
    E: Into<Box<dyn std::error::Error + Send + Sync + 'static>>
        + Send
        + Sync
        + 'static,
{
    Ok(make_response_common(crange, file_length, content_type)?.body(
        Body::wrap(StreamBody::new(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )),
    )?)
}

/**
 * Generate a HEAD response, optionally for a HTTP range request.  The total
 * file length should be provided, whether or not the expected Content-Length
 * for a range request is shorter.
 */
fn make_head_response(
    crange: Option<SingleRange>,
    file_length: u64,
    content_type: Option<&str>,
) -> Result<Response<Body>> {
    Ok(make_response_common(crange, file_length, content_type)?
        .body(Body::wrap(http_body_util::Empty::new()))?)
}

fn make_response_common(
    crange: Option<SingleRange>,
    file_length: u64,
    content_type: Option<&str>,
) -> Result<hyper::http::response::Builder> {
    let mut res = Response::builder();
    res = res.header(ACCEPT_RANGES, "bytes");
    res = res.header(
        CONTENT_TYPE,
        content_type.unwrap_or("application/octet-stream"),
    );

    if let Some(crange) = crange {
        res = res.header(CONTENT_LENGTH, crange.content_length().to_string());
        res = res.header(CONTENT_RANGE, crange.to_content_range());
        res = res.status(StatusCode::PARTIAL_CONTENT);
    } else {
        res = res.header(CONTENT_LENGTH, file_length.to_string());
        res = res.status(StatusCode::OK);
    }

    Ok(res)
}

pub struct PotentialRange(Vec<u8>);

impl PotentialRange {
    pub fn single_range(&self, len: u64) -> Option<SingleRange> {
        match http_range::HttpRange::parse_bytes(&self.0, len) {
            Ok(ranges) => {
                if ranges.len() != 1 || ranges[0].length < 1 {
                    /*
                     * Right now, we don't want to deal with encoding a
                     * response that has multiple ranges.
                     */
                    None
                } else {
                    Some(SingleRange(ranges[0], len))
                }
            }
            Err(_) => None,
        }
    }
}

pub struct SingleRange(http_range::HttpRange, u64);

impl SingleRange {
    /**
     * Return the first byte in this range for use in inclusive ranges.
     */
    pub fn start(&self) -> u64 {
        self.0.start
    }

    /**
     * Return the last byte in this range for use in inclusive ranges.
     */
    pub fn end(&self) -> u64 {
        assert!(self.0.length > 0);

        self.0.start.checked_add(self.0.length).unwrap().checked_sub(1).unwrap()
    }

    /**
     * Generate the Content-Range header for inclusion in a HTTP 206 partial
     * content response using this range.
     */
    pub fn to_content_range(&self) -> String {
        format!("bytes {}-{}/{}", self.0.start, self.end(), self.1)
    }

    /**
     * Generate a Range header for inclusion in another HTTP request; e.g.,
     * to a backend object store.
     */
    pub fn to_range(&self) -> String {
        format!("bytes={}-{}", self.0.start, self.end())
    }

    pub fn content_length(&self) -> u64 {
        assert!(self.0.length > 0);

        self.0.length
    }
}

pub trait RequestContextEx {
    fn range(&self) -> Option<PotentialRange>;
}

impl<T> RequestContextEx for dropshot::RequestContext<std::sync::Arc<T>>
where
    T: Send + Sync + 'static,
{
    /**
     * If there is a Range header, return it for processing during response
     * generation.
     */
    fn range(&self) -> Option<PotentialRange> {
        self.request
            .headers()
            .get(hyper::header::RANGE)
            .map(|hv| PotentialRange(hv.as_bytes().to_vec()))
    }
}
