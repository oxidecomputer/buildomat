/*
 * Copyright 2025 Oxide Computer Company
 */

use crate::{App, FlushOut, FlushState};
use anyhow::{bail, Result};
use buildomat_client::types::{DependSubmit, JobOutput};
use buildomat_common::*;
use buildomat_download::PotentialRange;
use buildomat_github_database::{types::*, Database};
use buildomat_jobsh::variety::basic::{output_sse, output_table, BasicConfig};
use chrono::SecondsFormat;
use dropshot::Body;
use futures::{FutureExt, StreamExt, TryStreamExt};
use http_body_util::StreamBody;
use hyper::body::Frame;
use hyper::Response;
use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use slog::{debug, error, info, o, trace, warn, Logger};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

const KILOBYTE: f64 = 1024.0;
const MEGABYTE: f64 = 1024.0 * KILOBYTE;
const GIGABYTE: f64 = 1024.0 * MEGABYTE;

const MAX_OUTPUTS: usize = 25;
const MAX_TAIL_LINES: usize = 20;
const MAX_LINE_LENGTH: usize = 90;

const MAX_RENDERED_LOG: u64 = 100 * 1024 * 1024;

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BasicPrivate {
    #[serde(default)]
    complete: bool,
    job_state: Option<String>,
    buildomat_id: Option<String>,
    error: Option<String>,
    #[serde(default)]
    cancelled: bool,

    #[serde(default)]
    events_tail: VecDeque<(Option<String>, String)>,
    #[serde(default)]
    event_minseq: u32,
    #[serde(default)]
    event_last_redraw_time: u64,
    #[serde(default)]
    event_tail_headers: VecDeque<(String, String)>,
    #[serde(default)]
    job_outputs: Vec<BasicOutput>,
    #[serde(default)]
    job_outputs_extra: usize,

    #[serde(default)]
    extra_repo_ids: Vec<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BasicOutput {
    path: String,
    href: String,
    size: String,
}

impl BasicOutput {
    fn new(
        app: &Arc<App>,
        cs: &CheckSuite,
        cr: &CheckRun,
        o: &JobOutput,
    ) -> BasicOutput {
        let name = o
            .path
            .chars()
            .rev()
            .take_while(|c| *c != '/')
            .collect::<String>()
            .chars()
            .rev()
            .collect::<String>();

        let href = app.make_url(&format!(
            "artefact/{}/{}/{}/{}/{}",
            cs.id, cs.url_key, cr.id, o.id, name
        ));

        let szf = o.size as f64;
        let size = if szf > GIGABYTE {
            format!("{:<.2}GiB", szf / GIGABYTE)
        } else if szf > MEGABYTE {
            format!("{:<.2}MiB", szf / MEGABYTE)
        } else if szf > KILOBYTE {
            format!("{:<.2}KiB", szf / KILOBYTE)
        } else {
            format!("{}B", szf)
        };

        BasicOutput { path: o.path.to_string(), href, size }
    }
}

pub(crate) async fn flush(
    app: &Arc<App>,
    cs: &CheckSuite,
    cr: &mut CheckRun,
    _repo: &Repository,
) -> Result<FlushOut> {
    let p: BasicPrivate = cr.get_private()?;

    /*
     * Construct a sort of "tail -f"-like view of the job output for the details
     * display.
     */
    let mut detail = String::new();

    if !p.event_tail_headers.is_empty() {
        detail += "```\n";
        let mut last: Option<String> = None;
        for (tag, msg) in p.event_tail_headers.iter() {
            if let Some(prevtag) = &last {
                if prevtag != tag {
                    detail += "...\n";
                    last = Some(tag.to_string());
                }
            } else {
                last = Some(tag.to_string());
            }
            detail += &format!("{}\n", msg);
        }
        if p.events_tail.is_empty() {
            detail += "```\n";
        }
    }
    if !p.events_tail.is_empty() {
        if p.event_tail_headers.is_empty() {
            detail += "```\n";
        } else {
            detail += "...\n";
        }
        for l in p.events_tail.iter() {
            detail += &format!("{}\n", l.1);
        }
        if !p.complete {
            detail += "...\n";
        }
        detail += "```\n";
    }

    let mut summary = String::new();
    if let Some(id) = &p.buildomat_id {
        summary += &format!(
            "The buildomat job ID is `{}`.  \
            [Click here]({}) for more detailed status.\n\n",
            id,
            app.make_details_url(cs, cr)
        );
    }

    if p.cancelled {
        summary += "The job was cancelled by a user.\n\n";
    }

    if !p.job_outputs.is_empty() {
        summary += "The job produced the following artefacts:\n";
        for bo in p.job_outputs.iter() {
            summary += &format!("* [`{}`]({}) ({})", bo.path, bo.href, bo.size);
            if guess_is_log_path(&bo.path) {
                /*
                 * Add an additional link to view a pretty-printed copy of
                 * what might be a bunyan log:
                 */
                summary +=
                    &format!(" [\\[rendered\\]]({}?format=x-bunyan)", bo.href);
            }
            summary += "\n";
        }
        if p.job_outputs_extra > 0 {
            summary += &format!(
                "* ... and {} more not shown here.\n",
                p.job_outputs_extra
            );
        }
        summary += "\n\n";
    }

    let cancel = vec![
        buildomat_github_client::types::ChecksCreateRequestActions {
            description: "Cancel execution and fail the check.".into(),
            identifier: "cancel".into(),
            label: "Cancel this job".into(),
        },
        buildomat_github_client::types::ChecksCreateRequestActions {
            description: "Cancel all jobs and fail all checks.".into(),
            identifier: "cancel_all".into(),
            label: "Cancel all jobs".into(),
        },
    ];

    Ok(if p.complete {
        if let Some(e) = p.error.as_deref() {
            FlushOut {
                title: "Failure!".into(),
                summary: format!("{}Flagrant Error: {}", summary, e),
                detail,
                state: FlushState::Failure,
                actions: Default::default(),
            }
        } else if p.job_state.as_deref().unwrap() == "completed" {
            FlushOut {
                title: "Success!".into(),
                summary: format!("{}The requested job was completed.", summary),
                detail,
                state: FlushState::Success,
                actions: Default::default(),
            }
        } else {
            FlushOut {
                title: "Failure!".into(),
                summary: format!(
                    "{}Job ended in state {:?}",
                    summary, p.job_state,
                ),
                detail,
                state: FlushState::Failure,
                actions: Default::default(),
            }
        }
    } else if let Some(ts) = p.job_state.as_deref() {
        if ts == "queued" {
            FlushOut {
                title: "Waiting to execute...".into(),
                summary: format!("{}The job is in line to run.", summary),
                detail,
                state: FlushState::Queued,
                actions: cancel,
            }
        } else if ts == "waiting" {
            FlushOut {
                title: "Waiting for dependencies...".into(),
                summary: format!(
                    "{}This job depends on other jobs that have not \
                    yet completed.",
                    summary
                ),
                detail,
                state: FlushState::Queued,
                actions: cancel,
            }
        } else {
            FlushOut {
                title: "Running...".into(),
                summary: format!("{}The job is running now!", summary),
                detail,
                state: FlushState::Running,
                actions: cancel,
            }
        }
    } else {
        FlushOut {
            title: "Waiting to submit...".into(),
            summary: format!("{}The job is in line to run.", summary),
            detail,
            state: FlushState::Queued,
            actions: cancel,
        }
    })
}

/**
 * Perform whatever actions are required to advance the state of this check run.
 * Returns true if the function should be called again, or false if this check
 * run is over.
 */
pub(crate) async fn run(
    app: &Arc<App>,
    cs: &CheckSuite,
    cr: &mut CheckRun,
) -> Result<bool> {
    let db = &app.db;
    let repo = db.load_repository(cs.repo)?;
    let log = &app.log;

    let mut p: BasicPrivate = cr.get_private()?;
    if p.complete {
        return Ok(false);
    }

    let fatal = |p: &mut BasicPrivate,
                 cr: &mut CheckRun,
                 db: &Database,
                 msg: &str|
     -> Result<bool> {
        p.complete = true;
        p.error = Some(msg.into());
        cr.set_private(p)?;
        cr.flushed = false;
        db.update_check_run(cr)?;
        Ok(false)
    };

    let c: BasicConfig = match cr.get_config() {
        Ok(c) => c,
        Err(e) => {
            return fatal(&mut p, cr, db, &format!("TOML front matter: {e}"));
        }
    };

    let script = if let Some(p) = &cr.content {
        p.to_string()
    } else {
        return fatal(&mut p, cr, db, "No script provided by user.");
    };

    let b = app.buildomat(&repo);
    if let Some(jid) = &p.buildomat_id {
        /*
         * We have submitted the task to buildomat already, so just try
         * to update our state.
         */
        let bt = b.job_get().job(jid).send().await?.into_inner();
        let running = bt.state == "running";
        let complete = bt.state == "completed" || bt.state == "failed";
        let new_state = Some(bt.state);
        if new_state != p.job_state {
            cr.flushed = false;
            p.job_state = new_state;
        }

        if running {
            let store =
                b.job_store_get_all().job(jid).send().await?.into_inner();

            if !store.contains_key("GITHUB_TOKEN") {
                /*
                 * As has become something of a theme, the GitHub API with which
                 * applications can generate an ephemeral credential for access
                 * to private repositories presents considerable opportunity for
                 * improvement.  One cannot specify a retention period, so the
                 * tokens just expire after around one hour.  If this is not
                 * long enough for you, well, whose fault is that anyway?
                 *
                 * In order to provide the absolute freshest token that we can,
                 * we must generate the token only once the job begins running:
                 * we cannot control how long a job spends in the "queued"
                 * state, as it depends on how busy the system is; we also
                 * cannot control how long the job spends in the "waiting"
                 * state, as it depends on how long its dependencies take to
                 * complete execution before it.
                 */
                let token = app
                    .temp_access_token(
                        cs.install,
                        &repo,
                        Some(&p.extra_repo_ids),
                    )
                    .await?;

                /*
                 * Place the generated token in the property store for the job.
                 * This store can be updated while the job is running.  Values
                 * are obtained through the "bmat" control program inside the
                 * job.  Timing is important but not critical: the job will wait
                 * for the value to appear before continuing.
                 */
                b.job_store_put()
                    .job(jid)
                    .name("GITHUB_TOKEN")
                    .body_map(|body| {
                        /*
                         * Mark this value as a secret so that it will not be
                         * included in diagnostic output.
                         */
                        body.secret(true).value(token)
                    })
                    .send()
                    .await?;
            }
        }

        /*
         * We don't want to overwhelm GitHub with requests to update the screen,
         * so we will only update our "tail -f" view of build output at most
         * every 6 seconds.
         */
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        if now - p.event_last_redraw_time >= 6 || complete {
            let mut change = false;

            for ev in b
                .job_events_get()
                .job(jid)
                .minseq(p.event_minseq)
                .send()
                .await?
                .into_inner()
            {
                change = true;
                if ev.seq + 1 > p.event_minseq {
                    p.event_minseq = ev.seq + 1;
                }

                let stdio = ev.stream == "stdout" || ev.stream == "stderr";
                let console = ev.stream == "console";
                let worker = ev.stream == "worker";
                let bgproc = ev.stream.starts_with("bg.");

                if stdio || console {
                    /*
                     * Some commands, like "cargo build --verbose", generate
                     * exceptionally long output lines, running into the
                     * thousands of characters.  The long lines present two
                     * challenges: they are not readily visible without
                     * horizontal scrolling in the GitHub UI; the maximum status
                     * message length GitHub will accept is 64KB, and even a
                     * small number of long lines means our status update will
                     * not be accepted.
                     *
                     * If a line is longer than 90 characters, truncate it.
                     * Users will still be able to see the full output in our
                     * detailed view where we get to render the whole page.
                     */
                    let mut line =
                        if console { "|C| " } else { "| " }.to_string();

                    /*
                     * We support ANSI escapes in the log renderer, which means
                     * that tools will generate ANSI sequences.  That doesn't
                     * work in the GitHub renderer, so we need to strip them out
                     * entirely.
                     */
                    let payload = strip_ansi_escapes::strip_str(&ev.payload);
                    let mut chars = payload.chars();

                    for _ in 0..MAX_LINE_LENGTH {
                        if let Some(c) = chars.next() {
                            line.push(c);
                        } else {
                            break;
                        }
                    }
                    if chars.next().is_some() {
                        /*
                         * If any characters remain, the string was truncated.
                         */
                        line.push_str(" [...]");
                    }

                    p.events_tail.push_back((None, line));
                } else if worker {
                    /*
                     * A job may produce a large number of files.  We must not
                     * treat worker output (which is mostly about file uploads
                     * and so on) as headers.  They must be regular records that
                     * are discarded as they scroll off the top.
                     */
                    let line = format!("|W| {}", ev.payload);
                    p.events_tail.push_back((None, line));
                } else if !bgproc {
                    p.events_tail.push_back((
                        Some(format!("{}/{:?}", ev.stream, ev.task)),
                        format!("{}: {}", ev.stream, ev.payload),
                    ));
                }
            }

            while p.events_tail.len() > MAX_TAIL_LINES {
                change = true;
                let first = p.events_tail.pop_front().unwrap();
                if let (Some(tag), msg) = first {
                    p.event_tail_headers.push_back((tag, msg));
                }
            }

            p.event_last_redraw_time = now;
            if change {
                /*
                 * Only send to GitHub if we saw any new output.
                 */
                cr.flushed = false;
            }
        }

        if complete {
            /*
             * Collect the list of uploaded artefacts.  Keep at most 25 of them.
             */
            let outputs = b.job_outputs_get().job(jid).send().await?;
            if !outputs.is_empty() {
                cr.flushed = false;
            }
            for o in outputs.iter() {
                if p.job_outputs.len() < MAX_OUTPUTS {
                    p.job_outputs.push(BasicOutput::new(app, cs, cr, o));
                } else {
                    p.job_outputs_extra += 1;
                }
            }

            /*
             * Resolve any publishing directives.  For now, we do not handle
             * publish rules that did not match any output from the actual job.
             * We also do not yet correctly handle a failure to publish, which
             * will require more nuance in reported errors from Dropshot and
             * Progenitor.  This feature is broadly still experimental.
             */
            for p in c.publish.iter() {
                if let Some(o) =
                    outputs.iter().find(|o| o.path == p.from_output)
                {
                    b.job_output_publish()
                        .job(jid)
                        .output(&o.id)
                        .body_map(|body| {
                            body.series(&p.series)
                                .version(&cs.head_sha)
                                .name(&p.name)
                        })
                        .send()
                        .await
                        .ok();
                }
            }
        }
    } else if !cr.active {
        /*
         * This check run has been made inactive prior to creating any
         * backend resources.
         */
        return Ok(false);
    } else {
        let gh = app.install_client(cs.install);

        /*
         * Before we can create this job in the buildomat backend, we need the
         * buildomat job ID for any job on which it depends.  If the job IDs for
         * the other check runs we depend on are not yet available, we need to
         * wait.
         */
        let mut depends: HashMap<_, _> = Default::default();
        for (name, crd) in cr.get_dependencies()? {
            if let Some(ocr) =
                db.load_check_run_for_suite_by_name(cs.id, crd.job())?
            {
                if !matches!(ocr.variety, CheckRunVariety::Basic) {
                    let msg =
                        "Basic variety jobs can only depend on other Basic \
                        variety jobs.";

                    return fatal(&mut p, cr, db, msg);
                }

                let op: BasicPrivate = ocr.get_private()?;
                if let Some(jobid) = &op.buildomat_id {
                    /*
                     * Use the job ID for a buildomat-level dependency.
                     */
                    depends.insert(
                        name.to_string(),
                        DependSubmit {
                            copy_outputs: true,
                            on_completed: true,
                            on_failed: false,
                            prior_job: jobid.to_string(),
                        },
                    );
                    continue;
                }

                if op.complete || op.error.is_some() {
                    let msg = format!(
                        "Dependency \"{}\" did not start a buildomat job \
                            before finishing.",
                        crd.job()
                    );

                    return fatal(&mut p, cr, db, &msg);
                }
            }

            /*
             * Arriving here should be infrequent.  Dependency relationships are
             * validated as part of loading the plan, and a complete set of
             * check runs for the suite should have been created prior to the
             * CheckSuiteState::Running state.  Nonetheless, there are a few
             * edge cases where we set "active" to false on a check run; e.g.,
             * when a re-run is requested.  During those windows we would not be
             * able to locate the active check run by name.
             */
            return Ok(true);
        }

        /*
         * We will need to provide the user program with an access token that
         * allows them to check out what may well be a private repository,
         * whether the repository for the check run or one of the other
         * repositories to which the check needs access.
         */
        if !c.access_repos.is_empty() {
            /*
             * First, make sure this job is authorised by a member of the
             * organisation that owns the repository.
             */
            if cs.approved_by.is_none() {
                let msg =
                    "Use of \"access_repos\" requires authorisation from \
                    a member of the organisation that owns the repository.";

                return fatal(&mut p, cr, db, msg);
            }

            /*
             * We need to map the symbolic name of each repository to an ID that
             * can be included in an access token request.  Invalid repository
             * names should result in a job error that the user can then
             * correct.
             */
            for dep in &c.access_repos {
                let msg = if let Some((owner, name)) = dep.split_once('/') {
                    match gh.repos().get(owner, name).await {
                        Ok(fr) => {
                            if !p.extra_repo_ids.contains(&fr.id) {
                                p.extra_repo_ids.push(fr.id);
                            }
                            continue;
                        }
                        Err(e) => {
                            warn!(
                                log,
                                "check run {} could not map repository {:?}: \
                                {:?}",
                                cr.id,
                                dep,
                                e,
                            );
                            format!(
                                "The \"access_repos\" entry {:?} is not valid.",
                                dep,
                            )
                        }
                    }
                } else {
                    format!(
                        "The \"access_repos\" entry {:?} is not valid.  \
                        It should be the name of a GitHub repository in \
                        \"owner/name\" format.",
                        dep
                    )
                };

                /*
                 * If we could not resolve the extra repository to which we need
                 * to provide access, report it to the user and fail the check
                 * run.
                 */
                return fatal(&mut p, cr, db, &msg);
            }
        }

        /*
         * Create a series of tasks to configure the build environment
         * before handing control to the user program.
         */
        let mut tb = buildomat_jobsh::variety::basic::TasksBuilder::default();
        tb.use_github_token(true);
        tb.script(&script);

        /*
         * We pass several GitHub-specific environment variables to tasks in the
         * job:
         */
        tb.env("GITHUB_REPOSITORY", &format!("{}/{}", repo.owner, repo.name));
        tb.env("GITHUB_SHA", &cs.head_sha);
        if let Some(branch) = cs.head_branch.as_deref() {
            tb.env("GITHUB_BRANCH", branch);
            tb.env("GITHUB_REF", &format!("refs/heads/{}", branch));
        }

        let app0 = app.clone();
        let repo0 = repo.clone();
        let sha = cs.head_sha.to_string();
        tb.with_file_loader(Box::new(move |name: &str| {
            let app = app0.clone();
            let name = name.to_string();
            let gh = gh.clone();
            let repo = repo0.clone();
            let sha = sha.clone();

            async move { app.load_file(&gh, &repo, &sha, &name).await }.boxed()
        }));

        let tasks = tb.build(&c).await?;

        /*
         * Attach tags that allow us to more easily map the buildomat job back
         * to the related GitHub activity, without needing to add a
         * Wollongong-level lookup API.
         */
        let mut tags = HashMap::new();
        tags.insert("gong.name".to_string(), cr.name.to_string());
        tags.insert("gong.variety".to_string(), cr.variety.to_string());
        tags.insert("gong.repo.owner".to_string(), repo.owner.to_string());
        tags.insert("gong.repo.name".to_string(), repo.name.to_string());
        tags.insert("gong.repo.id".to_string(), repo.id.to_string());
        tags.insert("gong.run.id".to_string(), cr.id.to_string());
        if let Some(ghid) = &cr.github_id {
            tags.insert("gong.run.github_id".to_string(), ghid.to_string());
        }
        tags.insert("gong.suite.id".to_string(), cs.id.to_string());
        tags.insert(
            "gong.suite.github_id".to_string(),
            cs.github_id.to_string(),
        );
        tags.insert("gong.head.sha".to_string(), cs.head_sha.to_string());
        if let Some(branch) = &cs.head_branch {
            tags.insert("gong.head.branch".to_string(), branch.to_string());
        }
        if let Some(sha) = &cs.plan_sha {
            tags.insert("gong.plan.sha".to_string(), sha.to_string());
        }

        let body = buildomat_client::types::JobSubmit::builder()
            .name(format!("gong/{}", cr.id))
            .output_rules(c.output_rules.clone())
            .target(c.target.as_deref().unwrap_or("default"))
            .tasks(tasks)
            .tags(tags)
            .depends(depends);
        let jsr = match b.job_submit().body(body).send().await {
            Ok(rv) => rv.into_inner(),
            Err(buildomat_client::Error::ErrorResponse(rv))
                if rv.status().is_client_error() =>
            {
                /*
                 * We assume that a client error means that the job is invalid
                 * in some way that is not a transient issue.  Report it to the
                 * user so that they can take corrective action.
                 */
                info!(
                    log,
                    "check run {} could not submit buildomat job ({}): {}",
                    cr.id,
                    rv.status(),
                    rv.message,
                );
                let msg = format!("Could not submit job: {}", rv.message);
                return fatal(&mut p, cr, db, &msg);
            }
            Err(e) => bail!("job submit failure: {:?}", e),
        };

        p.buildomat_id = Some(jsr.id);
        cr.flushed = false;
    }

    match p.job_state.as_deref() {
        Some("completed") | Some("failed") => {
            p.complete = true;
            cr.flushed = false;
        }
        _ => (),
    }

    cr.set_private(p)?;
    db.update_check_run(cr)?;
    Ok(true)
}

async fn bunyan_to_html(
    f: &mut tokio::fs::File,
    dec: &mut buildomat_bunyan::BunyanDecoder,
    num: &mut usize,
) -> Result<()> {
    while let Some(bl) = dec.pop() {
        *num += 1;

        let cssclass = match &bl {
            buildomat_bunyan::BunyanLine::Entry(be) => match be.level() {
                buildomat_bunyan::BunyanLevel::Trace => "bunyan-trace",
                buildomat_bunyan::BunyanLevel::Debug => "bunyan-debug",
                buildomat_bunyan::BunyanLevel::Info => "bunyan-info",
                buildomat_bunyan::BunyanLevel::Warn => "bunyan-warn",
                buildomat_bunyan::BunyanLevel::Error => "bunyan-error",
                buildomat_bunyan::BunyanLevel::Fatal => "bunyan-fatal",
            },
            buildomat_bunyan::BunyanLine::Other(_) => "bunyan-other",
        };

        /*
         * The first column is a permalink with the line number.
         */
        let mut out = format!(
            "<tr class=\"{cssclass}\">\
            <td style=\"vertical-align: top; text-align: right; \">\
            <a id=\"L{num}\">\
            <a href=\"#L{num}\" \
            style=\"white-space: pre; \
            font-family: monospace; \
            text-decoration: none; \
            color: #111111; \
            \">{num}</a></a>\
            </td>",
        );

        match bl {
            buildomat_bunyan::BunyanLine::Entry(be) => {
                /*
                 * The second column is the event timestamp.
                 */
                out += &format!(
                    "<td style=\"vertical-align: top;\">\
                    <span style=\"white-space: pre; \
                    font-family: monospace; \
                    \">{}</span>\
                    </td>",
                    be.time().to_rfc3339_opts(SecondsFormat::Millis, true)
                );

                /*
                 * The third column is the log level.
                 */
                out += &format!(
                    "<td style=\"vertical-align: top;\">\
                    <span style=\"white-space: pre; \
                    font-family: monospace; \
                    \"><b>{}</b></span>\
                    </td>",
                    be.level().render(),
                );

                /*
                 * The fourth column is an attempt to render the rest of the
                 * content in a readable way.
                 */
                let mut n =
                    format!("<b>{}</b>", html_escape::encode_safe(be.name()));
                if let Some(c) = be.component() {
                    if c != be.name() {
                        n += &format!(" ({})", html_escape::encode_safe(c));
                    }
                }

                /*
                 * For multi-line messages, indent subsequent lines by 4 spaces,
                 * so that they are at least somewhat distinguishable from the
                 * next log message.
                 */
                let msg = be
                    .msg()
                    .lines()
                    .enumerate()
                    .map(|(i, l)| {
                        let mut s = if i > 0 { "    " } else { "" }.to_string();
                        s.push_str(&html_escape::encode_safe(l));
                        s
                    })
                    .collect::<Vec<String>>()
                    .join("\n");

                out += &format!(
                    "<td style=\"vertical-align: top;\">\
                    <span style=\"white-space: pre-wrap; \
                    word-wrap: break-word; \
                    font-family: monospace; \
                    \">{}: {}\n",
                    n, msg,
                );

                for (k, v) in be.extras() {
                    out += &format!(
                        "    <b>{}</b> = ",
                        html_escape::encode_safe(k.as_str())
                    );
                    out += &html_escape::encode_safe(&match v {
                        serde_json::Value::Null => "null".into(),
                        serde_json::Value::Bool(v) => format!("{}", v),
                        serde_json::Value::Number(n) => format!("{}", n),
                        serde_json::Value::String(s) => {
                            let mut out = String::new();
                            for c in s.chars() {
                                if c != '"' && c != '\'' {
                                    out.push_str(
                                        &c.escape_default().to_string(),
                                    );
                                } else {
                                    out.push(c);
                                }
                            }
                            out
                        }
                        serde_json::Value::Array(a) => format!("{:?}", a),
                        serde_json::Value::Object(o) => format!("{:?}", o),
                    });
                    out += "\n";
                }

                out += "</span></td>";
            }
            buildomat_bunyan::BunyanLine::Other(line) => {
                /*
                 * For regular lines, we do not have a timestamp or a level,
                 * so skip those:
                 */
                out += "<td colspan=\"2\">&nbsp;</td>";

                /*
                 * Output the entire line in the final column:
                 */
                out += &format!(
                    "<td style=\"vertical-align: top;\">\
                    <span style=\"white-space: pre-wrap; \
                    word-wrap: break-word; \
                    font-family: monospace; \
                    \">{}</span>\
                    </td>",
                    html_escape::encode_safe(line.trim()),
                );
            }
        }

        out += "</tr>\n";

        f.write_all(out.as_bytes()).await?;
    }
    Ok(())
}

pub(crate) async fn artefact(
    app: &Arc<App>,
    cs: &CheckSuite,
    cr: &CheckRun,
    output: &str,
    name: &str,
    format: Option<&str>,
    pr: Option<PotentialRange>,
) -> Result<Option<hyper::Response<Body>>> {
    let p: BasicPrivate = cr.get_private()?;

    let bunyan = match format {
        Some("x-bunyan") => true,
        None => false,
        Some(other) => bail!("invalid format {:?}", other),
    };

    if let Some(id) = &p.buildomat_id {
        let bm = app.buildomat(&app.db.load_repository(cs.repo)?);

        /*
         * To try and help out the browser in deciding whether to display or
         * immediately download a particular file, we'll try to guess the
         * content MIME type based on the file extension.  It's not perfect, but
         * it's all we have without actually looking inside the file.
         *
         * Note that the "name" argument we are given here is merely the name
         * the client sent to us.  We determine which artefact to return solely
         * based on the output ID in the path.  In this way, we provide an
         * escape hatch of sorts for unhelpful file extensions: put whatever you
         * want in the URL!
         */
        let ct = guess_mime_type(name);

        if bunyan {
            if ct != "text/plain" {
                bail!("cannot reformat a file that is not plain text");
            }

            let backend =
                bm.job_output_download().job(id).output(output).send().await?;
            let cl = backend.content_length().unwrap();

            if cl > MAX_RENDERED_LOG {
                bail!("file too large for reformat");
            }

            /*
             * Open an anonymous temporary file into which we will write the
             * reformatted data.
             */
            let mut tf = tokio::fs::File::from_std(tempfile::tempfile()?);

            tf.write_all(
                concat!(
                    "<!doctype html><html>\n\
                    <head>\n\
                    <meta charset=\"UTF-8\">\n\
                    <style>\n",
                    include_str!("../../www/bunyan.css"),
                    "</style>\n\
                    <script>\n",
                    include_str!("../../www/bunyan.js"),
                    "</script>\n\
                    </head>\n\
                    <body>\n\
                    Max level shown:\n\
                    <select id=\"select-max-level\" \
                    onchange=\"selectMaxLevel(this)\">\n\
                    <option value=\"bunyan-trace\" selected>TRCE</option>\n\
                    <option value=\"bunyan-debug\">DEBG</option>\n\
                    <option value=\"bunyan-info\">INFO</option>\n\
                    <option value=\"bunyan-warn\">WARN</option>\n\
                    <option value=\"bunyan-error\">ERRO</option>\n\
                    <option value=\"bunyan-fatal\">FATA</option>\n\
                    </select>\n\
                    <table style=\"border: none;\">\n"
                )
                .as_bytes(),
            )
            .await?;

            let mut data = backend.into_inner();

            let mut dec = buildomat_bunyan::BunyanDecoder::new();

            let mut num = 0;
            while let Some(ch) = data.next().await.transpose()? {
                dec.feed(&ch)?;
                bunyan_to_html(&mut tf, &mut dec, &mut num).await?;
            }
            dec.fin()?;
            bunyan_to_html(&mut tf, &mut dec, &mut num).await?;

            tf.write_all("</table>\n</body>\n</html>\n".as_bytes()).await?;
            tf.flush().await?;

            /*
             * Rewind the file to the beginning and use it to provide the
             * response to the client.
             */
            tf.seek(std::io::SeekFrom::Start(0)).await?;
            let md = tf.metadata().await?;

            let stream = tokio_util::io::ReaderStream::new(tf);

            return Ok(Some(
                hyper::Response::builder()
                    .status(hyper::StatusCode::OK)
                    .header(hyper::header::CONTENT_TYPE, "text/html")
                    .header(hyper::header::CONTENT_LENGTH, md.len())
                    .body(Body::wrap(StreamBody::new(
                        stream.map_ok(|b| Frame::data(b)),
                    )))?,
            ));
        }

        /*
         * To improve efficiency, we can try to fetch the file directly from the
         * object store instead of forwarding the request through the API
         * server.
         */
        let url = bm
            .job_output_signed_url()
            .job(id)
            .output(output)
            .body_map(|body| body.content_type(ct.clone()).expiry_seconds(120))
            .send()
            .await?;
        if url.available {
            let client = reqwest::ClientBuilder::new()
                .timeout(Duration::from_secs(3600))
                .tcp_keepalive(Duration::from_secs(60))
                .connect_timeout(Duration::from_secs(15))
                .build()?;

            /*
             * The file is available for streaming directly from the object
             * store.
             */
            match buildomat_download::stream_from_url(
                &app.log,
                format!("pre-signed job output: job {id}, output {output}"),
                &client,
                url.url.clone(),
                pr,
                false,
                ct,
                Some(url.size),
            )
            .await
            {
                Ok(res) => return Ok(res),
                Err(e) => {
                    bail!(
                        "pre-signed job output: \
                        job {id} output {output}: {e:?}"
                    );
                }
            }
        } else {
            /*
             * Fall back to a regular request through the core API server.
             * XXX There is currently no good way to pass the range request to
             * the backend here.
             */
            let backend =
                bm.job_output_download().job(id).output(output).send().await?;
            let cl = backend.content_length().unwrap();

            return Ok(Some(
                hyper::Response::builder()
                    .status(hyper::StatusCode::OK)
                    .header(hyper::header::CONTENT_TYPE, ct)
                    .header(hyper::header::CONTENT_LENGTH, cl)
                    .body(Body::wrap(StreamBody::new(
                        backend.into_inner_stream().map_ok(|b| Frame::data(b)),
                    )))?,
            ));
        }
    }

    Ok(None)
}

pub(crate) async fn live(
    app: &Arc<App>,
    cs: &CheckSuite,
    cr: &CheckRun,
    last_event_id: Option<String>,
    query_minseq: Option<u32>,
) -> Result<Option<Response<Body>>> {
    let p: BasicPrivate = cr.get_private()?;
    let id = if let Some(id) = &p.buildomat_id {
        id.to_string()
    } else {
        /*
         * If there is no buildomat job for this run, return a not found error.
         */
        return Ok(None);
    };

    Ok(Some(
        output_sse(
            &app.buildomat(&app.db.load_repository(cs.repo)?),
            id,
            last_event_id,
            query_minseq,
        )
        .await?,
    ))
}

pub(crate) async fn details(
    app: &Arc<App>,
    cs: &CheckSuite,
    cr: &CheckRun,
    local_time: bool,
) -> Result<String> {
    let mut out = String::new();

    let c: BasicConfig = cr.get_config()?;

    out += &format!(
        "<pre>{}</pre>\n",
        format!("{:#?}", c).replace('<', "&lt;").replace('>', "&gt;")
    );

    let p: BasicPrivate = cr.get_private()?;

    if let Some(jid) = p.buildomat_id.as_deref() {
        /*
         * Try to fetch the log output of the job itself.
         */
        let repo = app.db.load_repository(cs.repo)?;
        let bm = app.buildomat(&repo);
        let job = bm.job_get().job(jid).send().await?;
        let outputs = bm.job_outputs_get().job(jid).send().await?.into_inner();

        out += &format!("<h2>Buildomat Job: {}</h2>\n", jid);

        if !job.tags.is_empty() {
            out += "<h3>Tags:</h3>\n";
            out += "<ul>\n";

            let mut keys = job.tags.keys().collect::<Vec<_>>();
            keys.sort_unstable();

            for &n in keys.iter() {
                let v = job.tags.get(n).unwrap();
                let url = match n.as_str() {
                    "gong.head.sha" | "gong.plan.sha" => Some(format!(
                        "https://github.com/{}/{}/commit/{}",
                        html_escape::encode_quoted_attribute(&repo.owner),
                        html_escape::encode_quoted_attribute(&repo.name),
                        html_escape::encode_quoted_attribute(v),
                    )),
                    "gong.repo.name" => Some(format!(
                        "https://github.com/{}/{}",
                        html_escape::encode_quoted_attribute(&repo.owner),
                        html_escape::encode_quoted_attribute(&repo.name),
                    )),
                    "gong.repo.owner" => Some(format!(
                        "https://github.com/{}",
                        html_escape::encode_quoted_attribute(&repo.owner),
                    )),
                    "gong.run.github_id" => Some(format!(
                        "https://github.com/{}/{}/runs/{}",
                        html_escape::encode_quoted_attribute(&repo.owner),
                        html_escape::encode_quoted_attribute(&repo.name),
                        html_escape::encode_quoted_attribute(v),
                    )),
                    _ => None,
                };

                let value = if let Some(url) = url {
                    format!(
                        "<a href=\"{url}\">{}</a>",
                        html_escape::encode_safe(v),
                    )
                } else {
                    html_escape::encode_safe(v).to_string()
                };

                out += &format!(
                    "<li><b>{}:</b> {value}\n",
                    html_escape::encode_safe(n),
                );
            }
            out += "</ul>\n";
        }

        if !outputs.is_empty() {
            out += "<h3>Artefacts:</h3>\n";
            out += "<ul>\n";
            for o in outputs {
                let bo = BasicOutput::new(app, cs, cr, &o);
                out += &format!(
                    "<li><a href=\"{}\">{}</a> ({})\n",
                    bo.href,
                    html_escape::encode_safe(&bo.path),
                    bo.size,
                );
                if guess_is_log_path(&bo.path) && o.size < MAX_RENDERED_LOG {
                    /*
                     * If the file might be a bunyan log and is not larger than
                     * we are willing to render, add an additional link to view
                     * a pretty-printed copy:
                     */
                    out += &format!(
                        " <a href=\"{}?format=x-bunyan\">[rendered]</a>\n",
                        bo.href
                    );
                }
            }
            out += "</ul>\n";
        }

        out += "<h3>Output:</h3>\n";
        out += &output_table(
            &bm,
            &job,
            local_time,
            format!("./{}/live", cr.id.to_string()),
        )
        .await?;
    }

    Ok(out)
}

pub(crate) async fn cancel(
    app: &Arc<App>,
    cs: &CheckSuite,
    cr: &mut CheckRun,
) -> Result<()> {
    let db = &app.db;
    let repo = db.load_repository(cs.repo)?;
    let log = &app.log;

    let mut p: BasicPrivate = cr.get_private()?;
    if p.complete || p.cancelled {
        return Ok(());
    }

    if let Some(jid) = &p.buildomat_id {
        /*
         * If we already started the buildomat job, we need to cancel it.
         */
        let b = app.buildomat(&repo);
        let j = b.job_get().job(jid).send().await?;

        if j.state == "complete" || j.state == "failed" {
            /*
             * This job is already finished.
             */
            return Ok(());
        }

        info!(log, "cancelling backend buildomat job {}", jid);
        b.job_cancel().job(jid).send().await?;
    } else {
        /*
         * Otherwise, report the failure and halt check run processing.
         */
        p.error = Some("Job was cancelled before it began running.".into());
        p.complete = true;
    }

    p.cancelled = true;
    cr.flushed = false;

    cr.set_private(p)?;
    db.update_check_run(cr)?;
    Ok(())
}

#[cfg(test)]
pub mod test {
    use super::*;
    use buildomat_github_testdata::*;

    #[test]
    fn basic_parse_basic() -> Result<()> {
        let (path, content, _) = real0();

        let jf = JobFile::parse_content_at_path(&content, &path)?.unwrap();

        let c: BasicConfig = serde_json::from_value(jf.config)?;
        println!("basic config = {c:#?}");

        let expect = BasicConfig {
            output_rules: vec![
                "=/work/manifest.toml".into(),
                "=/work/repo.zip".into(),
                "=/work/repo.zip.sha256.txt".into(),
                "%/work/*.log".into(),
            ],
            rust_toolchain: Some(StringOrBool::String("1.78.0".into())),
            target: Some("helios-2.0".into()),
            access_repos: vec![
                "oxidecomputer/amd-apcb".into(),
                "oxidecomputer/amd-efs".into(),
                "oxidecomputer/amd-firmware".into(),
                "oxidecomputer/amd-flash".into(),
                "oxidecomputer/amd-host-image-builder".into(),
                "oxidecomputer/boot-image-tools".into(),
                "oxidecomputer/chelsio-t6-roms".into(),
                "oxidecomputer/compliance-pilot".into(),
                "oxidecomputer/facade".into(),
                "oxidecomputer/helios".into(),
                "oxidecomputer/helios-omicron-brand".into(),
                "oxidecomputer/helios-omnios-build".into(),
                "oxidecomputer/helios-omnios-extra".into(),
                "oxidecomputer/nanobl-rs".into(),
            ],
            publish: vec![
                BasicConfigPublish {
                    from_output: "/work/manifest.toml".into(),
                    series: "rot-all".into(),
                    name: "manifest.toml".into(),
                },
                BasicConfigPublish {
                    from_output: "/work/repo.zip".into(),
                    series: "rot-all".into(),
                    name: "repo.zip".into(),
                },
                BasicConfigPublish {
                    from_output: "/work/repo.zip.sha256.txt".into(),
                    series: "rot-all".into(),
                    name: "repo.zip.sha256.txt".into(),
                },
            ],
            skip_clone: false,
        };
        assert_eq!(c, expect);

        Ok(())
    }
}
