/*
 * Copyright 2021 Oxide Computer Company
 */

use anyhow::{anyhow, bail, Result};
use chrono::prelude::*;
use dropshot::{
    endpoint, ConfigDropshot, HttpError, HttpResponseOk, RequestContext,
};
use schemars::JsonSchema;
#[allow(unused_imports)]
use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use slog::{debug, error, info, o, trace, Logger};
use std::collections::HashMap;
use std::result::Result as SResult;
use std::sync::Arc;
use wollongong_database::types::*;

use super::{variety, App};

fn sign(body: &[u8], secret: &str) -> String {
    let hmac = hmac_sha256::HMAC::mac(body, secret.as_bytes());
    let mut out = "sha256=".to_string();
    for b in hmac.iter() {
        out.push_str(&format!("{:<02x}", b));
    }
    out
}

fn interr<T>(log: &slog::Logger, msg: &str) -> SResult<T, dropshot::HttpError> {
    error!(log, "internal error: {}", msg);
    Err(dropshot::HttpError::for_internal_error(msg.to_string()))
}

trait ToHttpError<T> {
    fn to_500(self) -> SResult<T, HttpError>;
}

impl<T> ToHttpError<T> for SResult<T, wollongong_database::DatabaseError> {
    fn to_500(self) -> SResult<T, HttpError> {
        self.map_err(|e| {
            let msg = format!("internal error: {}", e);
            HttpError::for_internal_error(msg)
        })
    }
}

impl<T> ToHttpError<T> for Result<T> {
    fn to_500(self) -> SResult<T, HttpError> {
        self.map_err(|e| {
            let msg = format!("internal error: {}", e);
            HttpError::for_internal_error(msg)
        })
    }
}

impl<T> ToHttpError<T> for SResult<T, rusty_ulid::DecodingError> {
    fn to_500(self) -> SResult<T, HttpError> {
        self.map_err(|e| {
            let msg = format!("internal error: {}", e);
            HttpError::for_internal_error(msg)
        })
    }
}

#[derive(Deserialize, JsonSchema)]
struct ArtefactPath {
    pub check_suite: String,
    pub url_key: String,
    pub check_run: String,
    pub output: String,
    pub name: String,
}

impl ArtefactPath {
    fn check_suite(&self) -> SResult<CheckSuiteId, HttpError> {
        self.check_suite.parse::<CheckSuiteId>().to_500()
    }

    fn check_run(&self) -> SResult<CheckRunId, HttpError> {
        self.check_run.parse::<CheckRunId>().to_500()
    }
}

#[endpoint {
    method = GET,
    path = "/artefact/{check_suite}/{url_key}/{check_run}/{output}/{name}"
}]
async fn artefact(
    rc: Arc<RequestContext<Arc<App>>>,
    path: dropshot::Path<ArtefactPath>,
) -> SResult<hyper::Response<hyper::Body>, HttpError> {
    let app = rc.context();
    let path = path.into_inner();

    let cs = app.db.load_check_suite(&path.check_suite()?).to_500()?;
    let cr = app.db.load_check_run(&path.check_run()?).to_500()?;
    if cs.url_key != path.url_key {
        return interr(&rc.log, "url key mismatch");
    }

    let response = match cr.variety {
        CheckRunVariety::Basic => {
            variety::basic::artefact(app, &cs, &cr, &path.output, &path.name)
                .await
                .to_500()?
        }
        _ => None,
    };

    if let Some(response) = response {
        Ok(response)
    } else {
        let out = "<html><head><title>404 Not Found</title>\
            <body>Artefact not found!</body></html>";

        Ok(hyper::Response::builder()
            .status(hyper::StatusCode::NOT_FOUND)
            .header(hyper::header::CONTENT_TYPE, "text/html")
            .header(hyper::header::CONTENT_LENGTH, out.as_bytes().len())
            .body(hyper::Body::from(out))?)
    }
}

#[derive(Deserialize, JsonSchema)]
struct DetailsPath {
    pub check_suite: String,
    pub url_key: String,
    pub check_run: String,
}

impl DetailsPath {
    fn check_suite(&self) -> SResult<CheckSuiteId, HttpError> {
        self.check_suite.parse::<CheckSuiteId>().to_500()
    }

    fn check_run(&self) -> SResult<CheckRunId, HttpError> {
        self.check_run.parse::<CheckRunId>().to_500()
    }
}

#[endpoint {
    method = GET,
    path = "/details/{check_suite}/{url_key}/{check_run}",
}]
async fn details(
    rc: Arc<RequestContext<Arc<App>>>,
    path: dropshot::Path<DetailsPath>,
) -> SResult<hyper::Response<hyper::Body>, HttpError> {
    let app = rc.context();
    let path = path.into_inner();

    let cs = app.db.load_check_suite(&path.check_suite()?).to_500()?;
    let cr = app.db.load_check_run(&path.check_run()?).to_500()?;
    if cs.url_key != path.url_key {
        return interr(&rc.log, "url key mismatch");
    }

    let mut out = String::new();
    out += "<html>\n";
    out += &format!("<head><title>Check Run: {}</title></head>\n", cr.name);
    out += "<body>\n";
    out += &format!("<h1>{}: {}</h1>\n", cr.id, cr.name);

    match cr.variety {
        CheckRunVariety::Control => {
            let p: super::ControlPrivate = cr.get_private().to_500()?;
            out += &format!("<pre>{:#?}</pre>\n", p);
        }
        CheckRunVariety::FailFirst => {
            let p: super::FailFirstPrivate = cr.get_private().to_500()?;
            out += &format!("<pre>{:#?}</pre>\n", p);
        }
        CheckRunVariety::AlwaysPass => {
            let p: super::AlwaysPassPrivate = cr.get_private().to_500()?;
            out += &format!("<pre>{:#?}</pre>\n", p);
        }
        CheckRunVariety::Basic => {
            out += &variety::basic::details(app, &cs, &cr).await.to_500()?;
        }
    }

    out += "</body>\n";
    out += "</html>\n";

    Ok(hyper::Response::builder()
        .status(hyper::StatusCode::OK)
        .header(hyper::header::CONTENT_TYPE, "text/html")
        .header(hyper::header::CONTENT_LENGTH, out.as_bytes().len())
        .body(hyper::Body::from(out))?)
}

#[endpoint {
    method = POST,
    path = "/webhook",
}]
async fn webhook(
    rc: Arc<RequestContext<Arc<App>>>,
    body: dropshot::UntypedBody,
) -> SResult<HttpResponseOk<()>, HttpError> {
    let app = rc.context();
    let log = &rc.log;
    let req = rc.request.lock().await;

    /*
     * Locate the HMAC-256 signature of the body from Github.
     */
    let sig = {
        if let Some(h) = req.headers().get("x-hub-signature-256") {
            if let Ok(s) = h.to_str() {
                s.to_string()
            } else {
                return interr(log, "invalid signature header");
            }
        } else {
            return interr(log, "no signature header");
        }
    };

    /*
     * Fetch the body as raw bytes so that we can calculate the signature before
     * parsing it as JSON.
     */
    let buf = body.as_bytes();
    let oursig = sign(buf, &app.config.webhook_secret);

    if sig != oursig {
        error!(log, "signatures"; "theirs" => sig, "ours" => oursig);
        return interr(log, "signature mismatch");
    }

    let v: serde_json::Value = if let Ok(ok) = serde_json::from_slice(buf) {
        ok
    } else {
        return interr(log, "invalid JSON");
    };

    /*
     * Save the headers as well.
     */
    let mut headers = HashMap::new();
    for (k, v) in req.headers().iter() {
        trace!(log, "header: {} -> {:?}", k, v);
        headers.insert(k.to_string(), v.to_str().unwrap().to_string());
    }

    let uuid = if let Some(uuid) = headers.get("x-github-delivery") {
        uuid.as_str()
    } else {
        return interr(log, "missing delivery uuid");
    };
    let event = if let Some(event) = headers.get("x-github-event") {
        event.as_str()
    } else {
        return interr(log, "missing delivery event");
    };

    trace!(log, "from GitHub: {:#?}", v);

    let seq = app
        .db
        .store_delivery(uuid, event, &headers, &v, Utc::now())
        .to_500()?;

    info!(log, "stored as delivery seq {}", seq);

    Ok(HttpResponseOk(()))
}

pub(crate) async fn server(
    app: Arc<App>,
    bind_address: std::net::SocketAddr,
) -> Result<()> {
    let cd = ConfigDropshot {
        bind_address,
        request_body_max_bytes: 1024 * 1024,
        ..Default::default()
    };

    let mut api = dropshot::ApiDescription::new();
    api.register(webhook).unwrap();
    api.register(details).unwrap();
    api.register(artefact).unwrap();

    let log = app.log.clone();
    let s = dropshot::HttpServerStarter::new(&cd, api, app, &log)?;

    s.start().await.map_err(|e| anyhow!("HTTP server failure: {}", e))?;
    bail!("HTTP server exited unexpectedly");
}
