/*
 * Copyright 2021 Oxide Computer Company
 */

use anyhow::{anyhow, bail, Result};
use buildomat_common::*;
use chrono::prelude::*;
use dropshot::{
    endpoint, ConfigDropshot, HttpError, HttpResponseOk, RequestContext,
};
use rusty_ulid::Ulid;
use schemars::JsonSchema;
#[allow(unused_imports)]
use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use slog::{debug, error, info, o, trace, Logger};
use std::collections::{HashMap, HashSet};
use std::result::Result as SResult;
use std::sync::Arc;
use wollongong_database::types::*;

use super::{variety, App};

trait IdExt {
    fn id(&self) -> Result<Ulid>;
}

impl IdExt for buildomat_openapi::types::Worker {
    fn id(&self) -> Result<Ulid> {
        to_ulid(&self.id)
    }
}

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

#[endpoint {
    method = GET,
    path = "/status",
}]
async fn status(
    rc: Arc<RequestContext<Arc<App>>>,
) -> SResult<hyper::Response<hyper::Body>, HttpError> {
    let app = rc.context();
    let b = app.buildomat_admin();

    let mut out = String::new();
    out += "<html>\n";
    out += "<head><title>Buildomat Status</title></head>\n";
    out += "<body>\n";
    out += "<h1>Buildomat Status</h1>\n";

    /*
     * Load active jobs, recently completed jobs, and active workers:
     */
    let jobs = b.admin_jobs_get(Some(true), None).await.to_500()?;
    let oldjobs = {
        let mut oldjobs = b.admin_jobs_get(None, Some(20)).await.to_500()?;
        /*
         * Display most recent job first by sorting the ID backwards; a ULID
         * begins with a timestamp prefix, so a lexicographical sort is ordered
         * by creation time.
         */
        oldjobs.sort_by(|a, b| b.id.cmp(&a.id));
        oldjobs
    };
    let workers = b.workers_list(Some(true)).await.to_500()?;

    fn github_url(tags: &HashMap<String, String>) -> Option<String> {
        let owner = tags.get("gong.repo.owner")?;
        let name = tags.get("gong.repo.name")?;
        let checkrun = tags.get("gong.run.github_id")?;

        let url =
            format!("https://github.com/{}/{}/runs/{}", owner, name, checkrun);

        Some(format!("<a href=\"{}\">{}</a>", url, url))
    }

    fn commit_url(tags: &HashMap<String, String>) -> Option<String> {
        let owner = tags.get("gong.repo.owner")?;
        let name = tags.get("gong.repo.name")?;
        let sha = tags.get("gong.head.sha")?;

        let url =
            format!("https://github.com/{}/{}/commit/{}", owner, name, sha);

        Some(format!("<a href=\"{}\">{}</a>", url, sha))
    }

    fn github_info(tags: &HashMap<String, String>) -> Option<String> {
        let owner = tags.get("gong.repo.owner")?;
        let name = tags.get("gong.repo.name")?;
        let title = tags.get("gong.name")?;

        let url = format!("https://github.com/{}/{}", owner, name);

        let mut out = format!("<a href=\"{}\">{}/{}</a>", url, owner, name);
        if let Some(branch) = tags.get("gong.head.branch") {
            out.push_str(&format!(" ({})", branch));
        }
        out.push_str(&format!(": {}", title));

        Some(out)
    }

    fn dump_info(tags: &HashMap<String, String>) -> String {
        let mut out = String::new();
        if let Some(info) = github_info(tags) {
            out += &format!("&nbsp;&nbsp;&nbsp;<b>{}</b><br>\n", info);
        }
        if let Some(url) = commit_url(tags) {
            out += &format!("&nbsp;&nbsp;&nbsp;<b>commit:</b> {}<br>\n", url);
        }
        if let Some(url) = github_url(tags) {
            out += &format!("&nbsp;&nbsp;&nbsp;<b>url:</b> {}<br>\n", url);
        }
        if !out.is_empty() {
            out = format!("<br>\n{}\n", out);
        }
        out
    }

    let mut seen = HashSet::new();

    if workers.workers.iter().any(|w| !w.deleted) {
        out += "<h2>Active Workers</h2>\n";
        out += "<ul>\n";

        for w in workers.workers.iter() {
            if w.deleted {
                continue;
            }

            out += "<li>";
            out += &w.id;
            if let Some(fp) = &w.factory_private {
                out += &format!(" ({})", fp);
            }
            out += &format!(
                " created {} ({}s ago)\n",
                w.id().to_500()?.creation(),
                w.id().to_500()?.age().as_secs(),
            );

            if !w.jobs.is_empty() {
                out += "<ul>\n";

                for job in w.jobs.iter() {
                    seen.insert(job.id.to_string());

                    let owner = b.user_get(&job.owner).await.to_500()?;

                    out += "<li>";
                    out += &format!("job {} user {}", job.id, owner.name);
                    out += &dump_info(&job.tags);
                    out += "<br>\n";
                }

                out += "</ul>\n";
            }
        }

        out += "</ul>\n";
    }

    if jobs.iter().any(|job| !seen.contains(&job.id)) {
        out += "<h2>Queued Jobs</h2>\n";
        out += "<ul>\n";
        for job in jobs.iter() {
            if seen.contains(&job.id) {
                continue;
            }

            if job.state == "completed" || job.state == "failed" {
                continue;
            }

            let owner = b.user_get(&job.owner).await.to_500()?;

            out += "<li>";
            out += &format!("{} user {}", job.id, owner.name);
            out += &dump_info(&job.tags);
            out += "<br>\n";
        }
        out += "</ul>\n";
    }

    out += "<h2>Recently Completed Jobs</h2>\n";
    out += "<ul>\n";
    for job in oldjobs.iter() {
        if seen.contains(&job.id) {
            continue;
        }

        let owner = b.user_get(&job.owner).await.to_500()?;

        out += "<li>";
        out += &format!("{} user {}", job.id, owner.name);
        if job.state == "failed" {
            out += " <span style=\"background-color: #f29494\">[FAIL]</span>";
        } else {
            out += " <span style=\"background-color: #97f294\">[OK]</span>";
        }
        out += &dump_info(&job.tags);
        out += "<br>\n";
    }
    out += "</ul>\n";

    out += "</body>\n";
    out += "</html>\n";

    Ok(hyper::Response::builder()
        .status(hyper::StatusCode::OK)
        .header(hyper::header::CONTENT_TYPE, "text/html; charset=utf-8")
        .header(hyper::header::CONTENT_LENGTH, out.as_bytes().len())
        .body(hyper::Body::from(out))?)
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
    api.register(status).unwrap();

    let log = app.log.clone();
    let s = dropshot::HttpServerStarter::new(&cd, api, app, &log)
        .map_err(|e| anyhow!("server starter error: {:?}", e))?;

    s.start().await.map_err(|e| anyhow!("HTTP server failure: {}", e))?;
    bail!("HTTP server exited unexpectedly");
}
