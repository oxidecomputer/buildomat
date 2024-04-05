/*
 * Copyright 2024 Oxide Computer Company
 */

use crate::{App, FlushOut, FlushState};
use anyhow::{bail, Result};
use buildomat_client::types::{DependSubmit, JobOutput};
use buildomat_common::*;
use buildomat_github_database::types::*;
use chrono::SecondsFormat;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use slog::{debug, error, info, o, trace, warn, Logger};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

const KILOBYTE: f64 = 1024.0;
const MEGABYTE: f64 = 1024.0 * KILOBYTE;
const GIGABYTE: f64 = 1024.0 * MEGABYTE;

const MAX_OUTPUTS: usize = 25;
const MAX_TAIL_LINES: usize = 20;
const MAX_LINE_LENGTH: usize = 90;

const MAX_RENDERED_LOG: u64 = 100 * 1024 * 1024;

#[derive(Debug, Serialize, Deserialize)]
struct BasicConfig {
    #[serde(default)]
    output_rules: Vec<String>,
    rust_toolchain: Option<String>,
    target: Option<String>,
    #[serde(default)]
    access_repos: Vec<String>,
    #[serde(default)]
    publish: Vec<BasicConfigPublish>,
    #[serde(default)]
    skip_clone: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct BasicConfigPublish {
    from_output: String,
    series: String,
    name: String,
}

#[derive(Debug, Serialize, Deserialize)]
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
            if bo.path.ends_with(".log") {
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

    let c: BasicConfig = cr.get_config()?;

    let mut p: BasicPrivate = cr.get_private()?;
    if p.complete {
        return Ok(false);
    }

    let script = if let Some(p) = &cr.content {
        p.to_string()
    } else {
        p.complete = true;
        p.error = Some("No script provided by user".into());
        cr.set_private(p)?;
        cr.flushed = false;
        db.update_check_run(cr)?;
        return Ok(false);
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
                    let mut chars = ev.payload.chars();
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
                    p.complete = true;
                    p.error = Some(
                        "Basic variety jobs can only depend on other Basic \
                        variety jobs."
                            .into(),
                    );
                    cr.set_private(p)?;
                    cr.flushed = false;
                    db.update_check_run(cr)?;
                    return Ok(false);
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
                    p.complete = true;
                    p.error = Some(format!(
                        "Dependency \"{}\" did not start a buildomat job \
                        before finishing.",
                        crd.job()
                    ));
                    cr.set_private(p)?;
                    cr.flushed = false;
                    db.update_check_run(cr)?;
                    return Ok(false);
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
                p.complete = true;
                p.error = Some(
                    "Use of \"access_repos\" requires authorisation from \
                    a member of the organisation that owns the repository."
                        .into(),
                );
                cr.set_private(p)?;
                cr.flushed = false;
                db.update_check_run(cr)?;
                return Ok(false);
            }

            /*
             * We need to map the symbolic name of each repository to an ID that
             * can be included in an access token request.  Invalid repository
             * names should result in a job error that the user can then
             * correct.
             */
            let gh = app.install_client(cs.install);

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
                p.complete = true;
                p.error = Some(msg);
                cr.set_private(p)?;
                cr.flushed = false;
                db.update_check_run(cr)?;
                return Ok(false);
            }
        }

        /*
         * Create a series of tasks to configure the build environment
         * before handing control to the user program.
         */
        let mut tasks = Vec::new();

        /*
         * Set up a non-root user with which to run the build job, with a work
         * area at "/work".  The user will have the right to escalate to root
         * privileges via pfexec(1).
         */
        tasks.push(buildomat_client::types::TaskSubmit {
            name: "setup".into(),
            env: Default::default(),
            env_clear: false,
            gid: None,
            uid: None,
            workdir: None,
            script: include_str!("../../scripts/variety/basic/setup.sh").into(),
        });

        /*
         * Create the base environment for tasks that will run as
         * the non-root build user:
         */
        let mut buildenv = HashMap::new();
        buildenv.insert("HOME".into(), "/home/build".into());
        buildenv.insert("USER".into(), "build".into());
        buildenv.insert("LOGNAME".into(), "build".into());
        buildenv.insert(
            "PATH".into(),
            "/home/build/.cargo/bin:\
            /usr/bin:/bin:/usr/sbin:/sbin:/opt/ooce/bin:/opt/ooce/sbin"
                .into(),
        );
        buildenv.insert(
            "GITHUB_REPOSITORY".to_string(),
            format!("{}/{}", repo.owner, repo.name),
        );
        buildenv.insert("GITHUB_SHA".to_string(), cs.head_sha.to_string());
        if let Some(branch) = cs.head_branch.as_deref() {
            buildenv.insert("GITHUB_BRANCH".to_string(), branch.to_string());
            buildenv.insert(
                "GITHUB_REF".to_string(),
                format!("refs/heads/{}", branch),
            );
        }

        /*
         * If a Rust toolchain is requested, install it using rustup.
         */
        if let Some(toolchain) = c.rust_toolchain.as_deref() {
            let mut buildenv = buildenv.clone();
            buildenv.insert("TOOLCHAIN".into(), toolchain.into());

            tasks.push(buildomat_client::types::TaskSubmit {
                name: "rust-toolchain".into(),
                env: buildenv,
                env_clear: false,
                gid: Some(12345),
                uid: Some(12345),
                workdir: Some("/home/build".into()),
                script: "\
                    #!/bin/bash\n\
                    set -o errexit\n\
                    set -o pipefail\n\
                    set -o xtrace\n\
                    curl --proto '=https' --tlsv1.2 -sSf \
                        https://sh.rustup.rs | /bin/bash -s - \
                        -y --no-modify-path \
                        --default-toolchain \"$TOOLCHAIN\" \
                        --profile default\n\
                    rustc --version\n\
                    "
                .into(),
            });
        }

        /*
         * Write the temporary access token which gives brief read-only access
         * to only this (potentially private) repository into the ~/.netrc file.
         * When git tries to access GitHub via HTTPS it does so using curl,
         * which knows to look in this file for credentials.  This way, the
         * token need not appear in the build environment or any commands that
         * are run.
         *
         * We also provide an entry for "api.github.com" in case the job needs
         * to use curl to access the GitHub API.
         */
        tasks.push(buildomat_client::types::TaskSubmit {
            name: "authentication".into(),
            env: buildenv.clone(),
            env_clear: false,
            gid: Some(12345),
            uid: Some(12345),
            workdir: Some("/home/build".into()),
            script: "\
                #!/bin/bash\n\
                \n\
                set -o errexit\n\
                set -o pipefail\n\
                \n\
                GITHUB_TOKEN=$(bmat store get GITHUB_TOKEN)\n\
                \n\
                cat >$HOME/.netrc <<EOF\n\
                machine github.com\n\
                login x-access-token\n\
                password $GITHUB_TOKEN\n\
                \n\
                machine api.github.com\n\
                login x-access-token\n\
                password $GITHUB_TOKEN\n\
                \n\
                EOF\n\
                "
            .into(),
        });

        /*
         * By default, we assume that the target provides toolchains and other
         * development tools like git.  While this makes sense for most jobs, in
         * some cases we intend to build artefacts in one job, then run those
         * binaries in a separated, limited environment where it is not
         * appropriate to try to clone the repository again.  If "skip_clone" is
         * set, we will not clone the repository.
         */
        if !c.skip_clone {
            tasks.push(buildomat_client::types::TaskSubmit {
                name: "clone repository".into(),
                env: buildenv.clone(),
                env_clear: false,
                gid: Some(12345),
                uid: Some(12345),
                workdir: Some("/home/build".into()),
                script: "\
                    #!/bin/bash\n\
                    set -o errexit\n\
                    set -o pipefail\n\
                    set -o xtrace\n\
                    mkdir -p \"/work/$GITHUB_REPOSITORY\"\n\
                    git clone \"https://github.com/$GITHUB_REPOSITORY\" \
                        \"/work/$GITHUB_REPOSITORY\"\n\
                    cd \"/work/$GITHUB_REPOSITORY\"\n\
                    git fetch origin \"$GITHUB_SHA\"\n\
                    if [[ -n $GITHUB_BRANCH ]]; then\n\
                        current=$(git branch --show-current)\n\
                        if [[ $current != $GITHUB_BRANCH ]]; then\n\
                            git branch -f \"$GITHUB_BRANCH\" \"$GITHUB_SHA\"\n\
                            git checkout -f \"$GITHUB_BRANCH\"\n\
                        fi\n\
                    fi\n\
                    git reset --hard \"$GITHUB_SHA\"\n\
                    "
                .into(),
            });
        }

        buildenv.insert("CI".to_string(), "true".to_string());

        let workdir = if !c.skip_clone {
            format!("/work/{}/{}", repo.owner, repo.name)
        } else {
            /*
             * If we skipped the clone, just use the top-level work area as the
             * working directory for the job.
             */
            "/work".into()
        };

        tasks.push(buildomat_client::types::TaskSubmit {
            name: "build".into(),
            env: buildenv,
            env_clear: false,
            gid: Some(12345),
            uid: Some(12345),
            workdir: Some(workdir),
            script,
        });

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
                p.complete = true;
                p.error = Some(format!("Could not submit job: {}", rv.message));
                cr.set_private(p)?;
                cr.flushed = false;
                db.update_check_run(cr)?;
                return Ok(false);
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
         * The first column ia a permalink with the line number.
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
) -> Result<Option<hyper::Response<hyper::Body>>> {
    let p: BasicPrivate = cr.get_private()?;

    let bunyan = match format {
        Some("x-bunyan") => true,
        None => false,
        Some(other) => bail!("invalid format {:?}", other),
    };

    if let Some(id) = &p.buildomat_id {
        let bm = app.buildomat(&app.db.load_repository(cs.repo)?);

        let backend =
            bm.job_output_download().job(id).output(output).send().await?;
        let cl = backend.content_length().unwrap();

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
                    .body(hyper::Body::wrap_stream(stream))?,
            ));
        }

        return Ok(Some(
            hyper::Response::builder()
                .status(hyper::StatusCode::OK)
                .header(hyper::header::CONTENT_TYPE, ct)
                .header(hyper::header::CONTENT_LENGTH, cl)
                .body(hyper::Body::wrap_stream(backend.into_inner_stream()))?,
        ));
    }

    Ok(None)
}

pub(crate) async fn live(
    app: &Arc<App>,
    cs: &CheckSuite,
    cr: &CheckRun,
) -> Result<Option<hyper::Response<hyper::Body>>> {
    let p: BasicPrivate = cr.get_private()?;

    if let Some(id) = &p.buildomat_id {
        let bm = app.buildomat(&app.db.load_repository(cs.repo)?);

        let backend = bm.job_watch().job(id).send().await?;

        return Ok(Some(
            hyper::Response::builder()
                .status(hyper::StatusCode::OK)
                .header("X-Accel-Buffering", "no")
                .header(hyper::header::CONTENT_TYPE, "text/event-stream")
                .header(hyper::header::CACHE_CONTROL, "no-store")
                .body(hyper::Body::wrap_stream(backend.into_inner_stream()))?,
        ));
    }

    Ok(None)
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
        let bm = app.buildomat(&app.db.load_repository(cs.repo)?);
        let job = bm.job_get().job(jid).send().await?;
        let outputs = bm.job_outputs_get().job(jid).send().await?.into_inner();

        out += &format!("<h2>Buildomat Job: {}</h2>\n", jid);

        if !job.tags.is_empty() {
            out += "<h3>Tags:</h3>\n";
            out += "<ul>\n";
            let mut keys = job.tags.keys().collect::<Vec<_>>();
            keys.sort_unstable();
            for &n in keys.iter() {
                out += &format!(
                    "<li><b>{}:</b> {}\n",
                    html_escape::encode_safe(n),
                    html_escape::encode_safe(job.tags.get(n).unwrap()),
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
                if bo.path.ends_with(".log") && o.size < MAX_RENDERED_LOG {
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
        out += "<table id=\"table_output\" style=\"border: none;\">\n";

        let mut last = None;

        out += "<tr>\n";
        out += "<td style=\"vertical-align: top; text-align: right; \">\
            <span style=\"white-space: pre; \
            font-family: monospace; \
            font-weight: bold; \
            \">SEQ</td>\n";
        out += "<td style=\"vertical-align: top; text-align: left; \">\
            <span style=\"white-space: pre; \
            font-family: monospace; \
            font-weight: bold; \
            \">GLOBAL TIME</td>\n";
        if local_time {
            out += "<td style=\"vertical-align: top; text-align: left; \">\
                <span style=\"white-space: pre; \
                font-family: monospace; \
                font-weight: bold; \
                \">LOCAL TIME</td>\n";
        }
        out += "<td style=\"vertical-align: top; text-align: left; \">\
            <span style=\"white-space: pre; \
            font-family: monospace; \
            font-weight: bold; \
            \">DETAILS</td>\n";
        out += "</tr>\n";

        /*
         * Job event streams have no definite limit in length.  Because we are
         * assembling this page in memory, we don't want to load and render too
         * many records.
         */
        let mut events = Vec::with_capacity(10_000);
        let mut payload_size = 0;
        let mut minseq = 0;
        const PAYLOAD_SIZE_MAX: usize = 15 * 1024 * 1024;
        let truncated = 'outer: loop {
            let page = bm
                .job_events_get()
                .job(jid)
                .minseq(minseq)
                .send()
                .await?
                .into_inner();
            if page.is_empty() {
                /*
                 * We reached the (current) end of the event stream.  Note that
                 * if the job is still executing there may be more records
                 * later.
                 */
                break false;
            }

            for ev in page {
                minseq = ev.seq + 1;
                payload_size += ev.payload.as_bytes().len();
                events.push(ev);

                if payload_size > PAYLOAD_SIZE_MAX {
                    break 'outer true;
                }
            }
        };

        for ev in events {
            if ev.task != last {
                let cols = if local_time { 4 } else { 3 };
                out += &format!("<tr><td colspan=\"{cols}\">&nbsp;</td></tr>");
            }
            last = ev.task;

            /*
             * Set row colour based on the stream to which this event belongs.
             */
            let colour = match ev.stream.as_str() {
                "stdout" => "#ffffff",
                "stderr" => "#ffd9da",
                "task" => "#add8e6",
                "worker" => "#fafad2",
                "control" => "#90ee90",
                "console" => "#e7d1ff",
                s if s.starts_with("bg.") => "#f79d65",
                _ => "#dddddd",
            };
            out += &format!("<tr style=\"background-color: {};\">", colour);

            /*
             * The first column is a permalink with the event sequence number.
             */
            out += &format!(
                "<td style=\"vertical-align: top; text-align: right; \">\
                    <a id=\"S{}\">\
                    <a href=\"#S{}\" \
                    style=\"white-space: pre; \
                    font-family: monospace; \
                    text-decoration: none; \
                    color: #111111; \
                    \">{}</a></a>\
                </td>",
                ev.seq, ev.seq, ev.seq,
            );

            /*
             * The second column is the event timestamp.
             */
            out += &format!(
                "<td style=\"vertical-align: top;\">\
                    <span style=\"white-space: pre; \
                    font-family: monospace; \
                    \">{}</span>\
                </td>",
                ev.time.to_rfc3339_opts(SecondsFormat::Millis, true),
            );

            if local_time {
                /*
                 * We may be asked to render the job-local time as well as the
                 * global (NTP) time.
                 */
                let t = if let Some(t) = ev.time_remote {
                    t.to_rfc3339_opts(SecondsFormat::Millis, true)
                } else {
                    /*
                     * Not every record has a remote time.  In that case, we
                     * render an empty column rather than make up something
                     * potentially misleading.
                     */
                    "&nbsp;".into()
                };
                out += &format!(
                    "<td style=\"vertical-align: top;\">\
                        <span style=\"white-space: pre; \
                        font-family: monospace; \
                        \">{t}</span>\
                    </td>",
                );
            }

            /*
             * The final column is the message payload for the event.
             */
            let pfx = ev
                .stream
                .strip_prefix("bg.")
                .and_then(|s| s.split_once('.'))
                .filter(|(_, s)| *s == "stdout" || *s == "stderr")
                .map(|(n, _)| format!("[{}] ", html_escape::encode_safe(n)));
            out += &format!(
                "<td style=\"vertical-align: top;\">\
                    <span style=\"white-space: pre-wrap; \
                    white-space: -moz-pre-wrap !important; \
                    font-family: monospace; \
                    \">{}{}</span>\
                </td>",
                pfx.as_deref().unwrap_or(""),
                html_escape::encode_safe(&ev.payload),
            );

            out += "</tr>";
        }
        out += "\n</table>\n";

        if truncated {
            /*
             * For now, at least report truncation.  In the future perhaps we
             * will do something pedestrian, like a "Next page" link, or
             * something fancy, like use Javascript to do infinite scroll.
             */
            out += &format!(
                "<br><b>NOTE:</b> This job exceeds {PAYLOAD_SIZE_MAX} bytes \
                of text output, and this page has been truncated!  The full \
                log is still available through the buildomat API."
            );
        }
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
