/*
 * Copyright 2025 Oxide Computer Company
 */

use anyhow::{anyhow, bail, Result};
use buildomat_client::prelude::*;
use buildomat_common::*;
use buildomat_download::RequestContextEx;
use buildomat_github_database::types::*;
use buildomat_sse::HeaderMapEx;
use chrono::prelude::*;
use dropshot::{
    endpoint, Body, ConfigDropshot, HttpError, HttpResponseOk, RequestContext,
};
use futures::TryStreamExt;
use schemars::JsonSchema;
use serde::Deserialize;
#[allow(unused_imports)]
use slog::{debug, error, info, o, trace, warn, Logger};
use std::collections::{HashMap, HashSet};
use std::result::Result as SResult;
use std::sync::Arc;
use std::time::Duration;

use super::{variety, App};

fn sign(body: &[u8], secret: &str) -> String {
    let hmac = hmac_sha256::HMAC::mac(body, secret.as_bytes());
    let mut out = "sha256=".to_string();
    for b in hmac.iter() {
        out.push_str(&format!("{:<02x}", b));
    }
    out
}

/**
 * Log and return a machine-readable internal error when something goes wrong
 * and we'd like the client to have another go.  This is only appropriate for
 * endpoints made for software (e.g., webhook delivery) not for people (e.g.,
 * status pages and log output).
 */
fn interr<T>(log: &slog::Logger, msg: &str) -> SResult<T, dropshot::HttpError> {
    error!(log, "internal error: {}", msg);
    Err(dropshot::HttpError::for_internal_error(msg.to_string()))
}

/**
 * Return a 404 error HTML page for a browser user.
 */
fn html_404(head_only: bool) -> SResult<hyper::Response<Body>, HttpError> {
    let body = include_bytes!("../../../www/notfound.html").as_slice();

    let res = hyper::Response::builder()
        .status(hyper::StatusCode::NOT_FOUND)
        .header(hyper::header::CONTENT_TYPE, "text/html; charset=utf-8")
        .header(hyper::header::CONTENT_LENGTH, body.len());

    Ok(if head_only {
        res.body(Body::empty())?
    } else {
        res.body(body.to_vec().into())?
    })
}

/**
 * Return a generic 500 error HTML page for a browser user, including the
 * request ID of the failed request in case they want to report the failure.
 */
fn html_500(
    req_id: &str,
    head_only: bool,
) -> SResult<hyper::Response<Body>, HttpError> {
    let body = include_str!("../../../www/error.html")
        .replace(
            "<!-- %EXTRA% -->",
            &format!(
                "<div class=\"second\">Your request ID was &nbsp; \
                <b>{req_id}</b></div>\n",
            ),
        )
        .into_bytes();

    let res = hyper::Response::builder()
        .status(hyper::StatusCode::NOT_FOUND)
        .header(hyper::header::CONTENT_TYPE, "text/html; charset=utf-8")
        .header(hyper::header::CONTENT_LENGTH, body.len());

    Ok(if head_only {
        res.body(Body::empty())?
    } else {
        res.body(body.into())?
    })
}

trait ToHttpError<T> {
    fn to_500(self) -> SResult<T, HttpError>;
}

impl<T> ToHttpError<T> for Result<T> {
    fn to_500(self) -> SResult<T, HttpError> {
        self.map_err(|e| {
            let msg = format!("internal error: {}", e);
            HttpError::for_internal_error(msg)
        })
    }
}

/**
 * This trait is implemented by the path objects for endpoints that operate on a
 * particular check run.  These URLs contain a "secret" key to make them less
 * easily to guess, without going as far as having a full authentication and
 * authorisation system, and to allow people to paste these URLs into chat
 * messages or issues to expose them to other people.
 */
trait LoadCheckSuiteAndRun {
    fn parsed_check_suite_id(&self) -> Option<CheckSuiteId>;
    fn parsed_check_run_id(&self) -> Option<CheckRunId>;
    fn url_key(&self) -> &str;

    /**
     * Load the check suite and check run from the URL and check that the secret
     * key matches the one we have on file.  This routine will return Ok(None)
     * if the suite or run are not found, or if the key does not match (to
     * prevent enumeration).  Any error should be returned to the client as a
     * server error; i.e., a 500-series error.
     */
    fn load(
        &self,
        rc: &RequestContext<Arc<App>>,
    ) -> Result<Option<CheckSuiteAndRun>> {
        let Some((csid, crid)) =
            self.parsed_check_suite_id().zip(self.parsed_check_run_id())
        else {
            /*
             * If the IDs in the path do not parse correctly, we do not even
             * need to look them up to know they do not exist.
             */
            return Ok(None);
        };

        let app = rc.context();
        let cs = match app.db.load_check_suite_opt(csid) {
            Ok(Some(cs)) => cs,
            Ok(None) => return Ok(None),
            Err(e) => bail!("database error: check suite {csid}: {e}"),
        };
        let cr = match app.db.load_check_run_opt(crid) {
            Ok(Some(cr)) => cr,
            Ok(None) => return Ok(None),
            Err(e) => bail!("database error: check run {crid}: {e}"),
        };

        if cs.url_key == self.url_key() {
            Ok(Some(CheckSuiteAndRun { cs, cr }))
        } else {
            Ok(None)
        }
    }
}

struct CheckSuiteAndRun {
    cs: CheckSuite,
    cr: CheckRun,
}

#[derive(Deserialize, JsonSchema)]
struct ArtefactPath {
    pub check_suite: String,
    pub url_key: String,
    pub check_run: String,
    pub output: String,
    pub name: String,
}

impl LoadCheckSuiteAndRun for ArtefactPath {
    fn parsed_check_suite_id(&self) -> Option<CheckSuiteId> {
        self.check_suite.parse().ok()
    }

    fn parsed_check_run_id(&self) -> Option<CheckRunId> {
        self.check_run.parse().ok()
    }

    fn url_key(&self) -> &str {
        &self.url_key
    }
}

#[derive(Deserialize, JsonSchema)]
struct ArtefactQuery {
    pub format: Option<String>,
}

#[endpoint {
    method = GET,
    path = "/artefact/{check_suite}/{url_key}/{check_run}/{output}/{name}"
}]
async fn artefact(
    rc: RequestContext<Arc<App>>,
    path: dropshot::Path<ArtefactPath>,
    query: dropshot::Query<ArtefactQuery>,
) -> SResult<hyper::Response<Body>, HttpError> {
    let app = rc.context();
    let path = path.into_inner();
    let query = query.into_inner();
    let pr = rc.range();

    let load = match path.load(&rc) {
        Ok(Some(load)) => load,
        Ok(None) => return html_404(false),
        Err(e) => {
            error!(rc.log, "artefact: load: {e}");
            return html_500(&rc.request_id, false);
        }
    };

    let res = match load.cr.variety {
        CheckRunVariety::Basic => {
            variety::basic::artefact(
                app,
                &load.cs,
                &load.cr,
                &path.output,
                &path.name,
                query.format.as_deref(),
                pr,
            )
            .await
        }
        /*
         * No other variety exposes output artefacts:
         */
        _ => Ok(None),
    };

    match res {
        Ok(Some(res)) => {
            /*
             * Pass the response on from the variety-specific code as-is.  It is
             * likely a large streamed response, from either a local file or an
             * object store.
             */
            Ok(res)
        }
        Ok(None) => html_404(false),
        Err(e) => {
            error!(rc.log, "artefact: variety: {e}");
            html_500(&rc.request_id, false)
        }
    }
}

#[derive(Deserialize, JsonSchema)]
struct DetailsPath {
    pub check_suite: String,
    pub url_key: String,
    pub check_run: String,
}

impl LoadCheckSuiteAndRun for DetailsPath {
    fn parsed_check_suite_id(&self) -> Option<CheckSuiteId> {
        self.check_suite.parse().ok()
    }

    fn parsed_check_run_id(&self) -> Option<CheckRunId> {
        self.check_run.parse().ok()
    }

    fn url_key(&self) -> &str {
        &self.url_key
    }
}

#[derive(Deserialize, JsonSchema)]
struct DetailsQuery {
    pub ts: Option<String>,
}

#[endpoint {
    method = GET,
    path = "/details/{check_suite}/{url_key}/{check_run}",
}]
async fn details(
    rc: RequestContext<Arc<App>>,
    path: dropshot::Path<DetailsPath>,
    query: dropshot::Query<DetailsQuery>,
) -> SResult<hyper::Response<Body>, HttpError> {
    let app = rc.context();
    let path = path.into_inner();

    let query = query.into_inner();
    let local_time = query.ts.as_deref() == Some("all");

    let load = match path.load(&rc) {
        Ok(Some(load)) => load,
        Ok(None) => return html_404(false),
        Err(e) => {
            error!(rc.log, "details: load: {e}");
            return html_500(&rc.request_id, false);
        }
    };

    let mut out = String::new();
    out += "<!doctype html>\n<html>\n";
    out += "<head>\n";
    out += &format!("<title>Check Run: {}</title>\n", load.cr.name);
    if matches!(load.cr.variety, CheckRunVariety::Basic) {
        /*
         * The <style> tag needs to appear inside the <head>:
         */
        out += "<style>\n";
        out += &app.templates.load("variety/basic/www/style.css")?;
        out += "</style>\n";
    }
    out += "</head>\n";
    if matches!(load.cr.variety, CheckRunVariety::Basic) {
        out += "<body onload=\"basic_onload()\">\n";
    } else {
        out += "<body>\n";
    }
    out += &format!("<h1>{}: {}</h1>\n", load.cr.id, load.cr.name);

    out += &match load.cr.variety {
        CheckRunVariety::Control => {
            match variety::control::details(app, &load.cs, &load.cr, local_time)
                .await
            {
                Ok(s) => s,
                Err(e) => {
                    error!(rc.log, "details: control: {e}");
                    return html_500(&rc.request_id, false);
                }
            }
        }
        CheckRunVariety::FailFirst => {
            let p: super::FailFirstPrivate = load.cr.get_private().to_500()?;
            format!("<pre>{:#?}</pre>\n", p)
        }
        CheckRunVariety::AlwaysPass => {
            let p: super::AlwaysPassPrivate = load.cr.get_private().to_500()?;
            format!("<pre>{:#?}</pre>\n", p)
        }
        CheckRunVariety::Basic => {
            match variety::basic::details(app, &load.cs, &load.cr, local_time)
                .await
            {
                Ok(s) => s,
                Err(e) => {
                    error!(rc.log, "details: basic: {e}");
                    return html_500(&rc.request_id, false);
                }
            }
        }
    };

    out += "</body>\n";
    out += "</html>\n";

    Ok(hyper::Response::builder()
        .status(hyper::StatusCode::OK)
        .header(hyper::header::CONTENT_TYPE, "text/html; charset=utf-8")
        .header(hyper::header::CONTENT_LENGTH, out.as_bytes().len())
        .body(Body::from(out))?)
}

#[derive(Deserialize, JsonSchema)]
struct DetailsLiveQuery {
    pub minseq: Option<u32>,
}

#[endpoint {
    method = GET,
    path = "/details/{check_suite}/{url_key}/{check_run}/live",
}]
async fn details_live(
    rc: RequestContext<Arc<App>>,
    path: dropshot::Path<DetailsPath>,
    query: dropshot::Query<DetailsLiveQuery>,
) -> SResult<hyper::Response<Body>, HttpError> {
    let app = rc.context();
    let path = path.into_inner();
    let query = query.into_inner();

    let load = match path.load(&rc) {
        Ok(Some(load)) => load,
        Ok(None) => return html_404(false),
        Err(e) => {
            error!(rc.log, "details: load: {e}");
            return html_500(&rc.request_id, false);
        }
    };

    let res = match load.cr.variety {
        CheckRunVariety::Basic => {
            variety::basic::live(
                app,
                &load.cs,
                &load.cr,
                rc.request.headers().last_event_id(),
                query.minseq,
            )
            .await
        }
        /*
         * No other variety exposes live details:
         */
        _ => Ok(None),
    };

    match res {
        Ok(Some(res)) => Ok(res),
        Ok(None) => html_404(false),
        Err(e) => {
            error!(rc.log, "details live: {e}");
            html_500(&rc.request_id, false)
        }
    }
}

#[endpoint {
    method = POST,
    path = "/webhook",
}]
async fn webhook(
    rc: RequestContext<Arc<App>>,
    body: dropshot::UntypedBody,
) -> SResult<HttpResponseOk<()>, HttpError> {
    let app = rc.context();
    let log = &rc.log;

    /*
     * Locate the HMAC-256 signature of the body from Github.
     */
    let sig = {
        if let Some(h) = rc.request.headers().get("x-hub-signature-256") {
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
    for (k, v) in rc.request.headers().iter() {
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

    let then = Utc::now();

    let (seq, new_delivery) = loop {
        match app.db.store_delivery(uuid, event, &headers, &v, then) {
            Ok(del) => break del,
            Err(e) if e.is_locked_database() => {
                /*
                 * Clients under our control will retry on failures, but
                 * generally GitHub will not retry a failed delivery.  If the
                 * database is locked by another process, sleep and try again
                 * until we succeed.
                 */
                warn!(log, "delivery uuid {uuid} sleeping for lock..");
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
            Err(e) => return interr(log, &format!("storing delivery: {e}")),
        }
    };

    if new_delivery {
        info!(log, "stored as delivery seq {seq} uuid {uuid}");
    } else {
        warn!(log, "replayed delivery seq {seq} uuid {uuid}");
    }

    Ok(HttpResponseOk(()))
}

#[endpoint {
    method = GET,
    path = "/status",
}]
async fn status(
    rc: RequestContext<Arc<App>>,
) -> SResult<hyper::Response<Body>, HttpError> {
    match status_impl(&rc).await {
        Ok(res) => Ok(res),
        Err(e) => {
            error!(rc.log, "status page: {e:?}");
            html_500(&rc.request_id, false)
        }
    }
}

async fn status_impl(
    rc: &RequestContext<Arc<App>>,
) -> Result<hyper::Response<Body>> {
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
    let jobs = b.admin_jobs_get().active(true).send().await?;
    let oldjobs = {
        let mut oldjobs = b.admin_jobs_get().completed(100).send().await?;
        /*
         * Display most recent job first by sorting the ID backwards; a ULID
         * begins with a timestamp prefix, so a lexicographical sort is ordered
         * by creation time.
         */
        oldjobs.sort_by(|a, b| b.id.cmp(&a.id));
        oldjobs
    };
    let workers =
        b.workers_list().active(true).stream().try_collect::<Vec<_>>().await?;
    let targets = b
        .targets_list()
        .send()
        .await?
        .into_inner()
        .into_iter()
        .map(|t| (t.id, t.name))
        .collect::<HashMap<_, _>>();
    let mut users: HashMap<String, String> = Default::default();

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

    fn dump_info(job: &buildomat_client::types::Job) -> String {
        let tags = &job.tags;

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
        if job.target == job.target_real {
            out += &format!(
                "&nbsp;&nbsp;&nbsp;<b>target:</b> {}<br>\n",
                job.target
            );
        } else {
            out += &format!(
                "&nbsp;&nbsp;&nbsp;<b>target:</b> {} &rarr; {}<br>\n",
                job.target, job.target_real
            );
        }

        if let Some(t) = job.times.get("complete") {
            out += &format!(
                "&nbsp;&nbsp;&nbsp;<b>completed at:</b> {} ({} ago)<br>\n",
                t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                t.age().render(),
            );
        } else if let Some(t) = job.times.get("submit") {
            out += &format!(
                "&nbsp;&nbsp;&nbsp;<b>submitted at:</b> {} ({} ago)<br>\n",
                t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                t.age().render(),
            );
        } else if let Ok(id) = job.id() {
            let t = id.creation();
            out += &format!(
                "&nbsp;&nbsp;&nbsp;<b>submitted at:</b> {} ({} ago)<br>\n",
                t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                t.age().render(),
            );
        }

        let mut times = Vec::new();
        if let Some(t) = job.duration("submit", "ready") {
            times.push(format!("waited {}", t.render()));
        }
        if let Some(t) = job.duration("ready", "assigned") {
            times.push(format!("queued {}", t.render()));
        }
        if let Some(t) = job.duration("assigned", "complete") {
            times.push(format!("ran for {}", t.render()));
        }
        if !times.is_empty() {
            out += &format!(
                "&nbsp;&nbsp;&nbsp;<b>times:</b> {}<br>\n",
                times.join(", ")
            );
        }

        if !out.is_empty() {
            out = format!("<br>\n{}\n", out);
        }
        out
    }

    let mut seen = HashSet::new();

    if workers.iter().any(|w| !w.deleted) {
        out += "<h2>Active Workers</h2>\n";
        out += "<ul>\n";

        for w in workers.iter() {
            if w.deleted || w.hold.is_some() {
                continue;
            }

            out += "<li>";
            out += &w.id;
            let mut things = Vec::new();
            if let Some(t) = targets.get(&w.target) {
                things.push(t.to_string());
            }
            if let Some(fp) = &w.factory_private {
                things.push(fp.to_string());
            }
            if !things.is_empty() {
                out += &format!(" ({})", things.join(", "));
            }
            out += &format!(
                " created {} ({} ago)\n",
                w.id()?
                    .creation()
                    .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                w.id()?.age().render(),
            );

            if !w.jobs.is_empty() {
                out += "<ul>\n";

                for job in w.jobs.iter() {
                    seen.insert(job.id.to_string());

                    if !users.contains_key(&job.owner) {
                        let owner =
                            b.user_get().user(&job.owner).send().await?;
                        users.insert(job.owner.clone(), owner.name.to_string());
                    }

                    out += "<li>";
                    out += &format!(
                        "job {} user {}",
                        job.id,
                        users.get(&job.owner).unwrap()
                    );
                    if let Some(job) = jobs.iter().find(|j| j.id == job.id) {
                        out += &dump_info(job);
                    }
                    out += "<br>\n";
                }

                out += "</ul>\n";
            }
        }

        out += "</ul>\n";
    }

    for (heading, state) in [
        ("Queued Jobs (waiting for capacity)", Some("queued")),
        ("Waiting Jobs (waiting for a dependency)", Some("waiting")),
        ("Other Jobs", None),
    ] {
        let mut did_heading = false;

        for job in jobs.iter() {
            if seen.contains(&job.id) {
                continue;
            }

            let display = if job.state == "completed" || job.state == "failed" {
                /*
                 * Completed jobs will be displayed in a later section.
                 */
                false
            } else if let Some(state) = state {
                /*
                 * This round, we are displaying jobs of a particular status.
                 */
                state == job.state
            } else {
                /*
                 * Catch all the stragglers.
                 */
                true
            };

            if !display {
                continue;
            }

            seen.insert(job.id.to_string());

            if !did_heading {
                did_heading = true;
                out += &format!("<h2>{}</h2>\n", heading);
                out += "<ul>\n";
            }

            if !users.contains_key(&job.owner) {
                let owner = b.user_get().user(&job.owner).send().await?;
                users.insert(job.owner.clone(), owner.name.to_string());
            }

            out += "<li>";
            out +=
                &format!("{} user {}", job.id, users.get(&job.owner).unwrap());
            out += &dump_info(job);
            out += "<br>\n";
        }

        if did_heading {
            out += "</ul>\n";
        }
    }

    out += "<h2>Recently Completed Jobs</h2>\n";
    out += "<ul>\n";
    for job in oldjobs.iter() {
        if seen.contains(&job.id) {
            continue;
        }

        if !users.contains_key(&job.owner) {
            let owner = b.user_get().user(&job.owner).send().await?;
            users.insert(job.owner.clone(), owner.name.to_string());
        }

        out += "<li>";
        out += &format!("{} user {}", job.id, users.get(&job.owner).unwrap());
        let (colour, word) = if job.state == "failed" {
            if job.cancelled {
                ("dabea6", "CANCEL")
            } else {
                ("f29494", "FAIL")
            }
        } else {
            ("97f294", "OK")
        };
        out += &format!(
            " <span style=\"background-color: #{}\">[{}]</span>",
            colour, word
        );
        out += &dump_info(job);
        out += "<br>\n";
    }
    out += "</ul>\n";

    out += "</body>\n";
    out += "</html>\n";

    Ok(hyper::Response::builder()
        .status(hyper::StatusCode::OK)
        .header(hyper::header::CONTENT_TYPE, "text/html; charset=utf-8")
        .header(hyper::header::CONTENT_LENGTH, out.as_bytes().len())
        .body(Body::from(out))?)
}

#[derive(Deserialize, JsonSchema)]
struct PublishedFilePath {
    pub owner: String,
    pub repo: String,
    pub series: String,
    pub version: String,
    pub name: String,
}

#[endpoint {
    method = HEAD,
    path = "/public/file/{owner}/{repo}/{series}/{version}/{name}",
}]
async fn published_file_head(
    rc: RequestContext<Arc<App>>,
    path: dropshot::Path<PublishedFilePath>,
) -> SResult<hyper::Response<Body>, HttpError> {
    published_file_common(rc, path, true).await
}

#[endpoint {
    method = GET,
    path = "/public/file/{owner}/{repo}/{series}/{version}/{name}",
}]
async fn published_file(
    rc: RequestContext<Arc<App>>,
    path: dropshot::Path<PublishedFilePath>,
) -> SResult<hyper::Response<Body>, HttpError> {
    published_file_common(rc, path, false).await
}

async fn published_file_common(
    rc: RequestContext<Arc<App>>,
    path: dropshot::Path<PublishedFilePath>,
    head_only: bool,
) -> SResult<hyper::Response<Body>, HttpError> {
    let app = rc.context();
    let path = path.into_inner();
    let pr = rc.range();

    /*
     * Determine the buildomat username for this GitHub owner/repository:
     */
    let bmu = match app.db.lookup_repository(&path.owner, &path.repo) {
        Ok(Some(repo)) => app.buildomat_username(&repo),
        Ok(None) => return html_404(head_only),
        Err(e) => {
            error!(rc.log, "published file: lookup: {e:?}");
            return html_500(&rc.request_id, head_only);
        }
    };

    /*
     * XXX There is not currently a good way to perform a range request using
     * the progenitor client.  We'll borrow the underlying reqwest client and
     * use it directly to make this request.
     */
    let Ok((client, baseurl)) = app.buildomat_raw() else {
        return html_500(&rc.request_id, head_only);
    };

    let url = format!(
        "{baseurl}/0/public/file/{bmu}/{}/{}/{}",
        path.series, path.version, path.name,
    );

    let res = match buildomat_download::stream_from_url(
        &rc.log,
        format!(
            "published file: owner {}/{} series {} version {} name {}",
            path.owner, path.repo, path.series, path.version, path.name,
        ),
        &client,
        url,
        pr,
        head_only,
        guess_mime_type(&path.name),
        None,
    )
    .await
    {
        Ok(Some(res)) => {
            /*
             * Pass the response on from the buildomat API as-is.  It is likely
             * a large streamed response, from either a local file or an object
             * store.
             */
            res
        }
        Ok(None) => {
            /*
             * The backend believes this published file does not
             * exist at all.
             */
            return html_404(head_only);
        }
        Err(e) => {
            error!(rc.log, "published file: buildomat: {e:?}");
            return html_500(&rc.request_id, head_only);
        }
    };

    Ok(res)
}

#[derive(Deserialize, JsonSchema)]
struct BranchToCommitPath {
    pub owner: String,
    pub repo: String,
    pub branch: String,
}

#[endpoint {
    method = GET,
    path = "/public/branch/{owner}/{repo}/{branch}",
}]
async fn branch_to_commit(
    rc: RequestContext<Arc<App>>,
    path: dropshot::Path<BranchToCommitPath>,
) -> SResult<hyper::Response<Body>, HttpError> {
    let log = &rc.log;
    let app = rc.context();
    let path = path.into_inner();

    /*
     * Make sure we know about this repository before we even bother to look it
     * up on GitHub.
     */
    let repo = match app.db.lookup_repository(&path.owner, &path.repo) {
        Ok(Some(repo)) => repo,
        Ok(None) => return html_404(false),
        Err(e) => {
            error!(log, "branch to commit: {e:?}");
            return html_500(&rc.request_id, false);
        }
    };

    /*
     * We need to use the credentials for the installation owned by the user
     * that owns this repo:
     */
    let install = match app.db.repo_to_install(&repo) {
        Ok(install) => install,
        Err(_) => {
            /*
             * We may know about a repository (e.g., from pull requests made
             * from forks from outside an organisation) but not actually have a
             * GitHub App installation that we can use for credentials.  Treat
             * this as if we could not locate the repository.
             */
            return html_404(false);
        }
    };

    let branch = match app
        .install_client(install.id)
        .repos()
        .get_branch(&repo.owner, &repo.name, &path.branch)
        .await
    {
        Ok(branch) => branch,
        Err(e) => {
            /*
             * The GitHub client we are using does not have fantastically
             * nuanced error handling available.  Ideally we would not arrive
             * here, anyway, as we've confirmed that (we believe!) the
             * repository exists above.
             */
            error!(
                log,
                "install {} getting branch ({:?}, {:?}, {:?}): {e}",
                install.id,
                repo.owner,
                repo.name,
                path.branch,
            );
            return html_500(&rc.request_id, false);
        }
    };

    let body = format!("{}\n", branch.commit.sha);

    Ok(hyper::Response::builder()
        .status(hyper::StatusCode::OK)
        .header(hyper::header::CONTENT_TYPE, "text/plain")
        .header(hyper::header::CONTENT_LENGTH, body.as_bytes().len())
        .body(body.into())?)
}

#[derive(Deserialize, JsonSchema)]
struct StaticPath {
    pub name: String,
}

#[endpoint {
    method = GET,
    path = "/static/{name}",
}]
async fn static_file(
    _rc: RequestContext<Arc<App>>,
    path: dropshot::Path<StaticPath>,
) -> SResult<hyper::Response<Body>, HttpError> {
    let path = path.into_inner();

    let (code, ctype, bytes) = match path.name.as_str() {
        "index.html" => (
            hyper::StatusCode::OK,
            "text/html; charset=utf-8",
            include_bytes!("../../../www/index.html").as_slice(),
        ),
        "buildomat.png" => (
            hyper::StatusCode::OK,
            "image/png",
            include_bytes!("../../../www/buildomat.png").as_slice(),
        ),
        "favicon.ico" => (
            hyper::StatusCode::OK,
            "image/png",
            include_bytes!("../../../www/favicon.ico").as_slice(),
        ),
        "notfound.html" => (
            /*
             * If you specifically request the 404 error page, we'll return that
             * with a 200 code.
             */
            hyper::StatusCode::OK,
            "text/html; charset=utf-8",
            include_bytes!("../../../www/notfound.html").as_slice(),
        ),
        "error.html" => (
            /*
             * If you specifically request the generic error page, we'll return
             * that with a 200 code.
             */
            hyper::StatusCode::OK,
            "text/html; charset=utf-8",
            include_bytes!("../../../www/error.html").as_slice(),
        ),
        _ => (
            hyper::StatusCode::NOT_FOUND,
            "text/html; charset=utf-8",
            include_bytes!("../../../www/notfound.html").as_slice(),
        ),
    };

    Ok(hyper::Response::builder()
        .status(code)
        .header(hyper::header::CONTENT_TYPE, ctype)
        .header(hyper::header::CONTENT_LENGTH, bytes.len())
        .body(bytes.to_vec().into())?)
}

pub(crate) async fn server(
    app: Arc<App>,
    bind_address: std::net::SocketAddr,
) -> Result<()> {
    let cd = ConfigDropshot {
        bind_address,
        default_request_body_max_bytes: 1024 * 1024,
        log_headers: vec!["X-Forwarded-For".into()],
        ..Default::default()
    };

    let mut api = dropshot::ApiDescription::new();
    api.register(webhook)?;
    api.register(details)?;
    api.register(details_live)?;
    api.register(artefact)?;
    api.register(status)?;
    api.register(published_file)?;
    api.register(published_file_head)?;
    api.register(branch_to_commit)?;
    api.register(static_file)?;

    let log = app.log.clone();
    let s = dropshot::HttpServerStarter::new(&cd, api, app, &log)
        .map_err(|e| anyhow!("server starter error: {:?}", e))?;

    s.start().await.map_err(|e| anyhow!("HTTP server failure: {}", e))?;
    bail!("HTTP server exited unexpectedly");
}
