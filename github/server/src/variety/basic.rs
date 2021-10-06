/*
 * Copyright 2021 Oxide Computer Company
 */

use crate::{App, FlushOut, FlushState};
use anyhow::Result;
use chrono::SecondsFormat;
use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use slog::{debug, error, info, o, trace, warn, Logger};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

const KILOBYTE: f64 = 1024.0;
const MEGABYTE: f64 = 1024.0 * KILOBYTE;
const GIGABYTE: f64 = 1024.0 * MEGABYTE;

const MAX_OUTPUTS: usize = 25;

#[derive(Debug, Serialize, Deserialize)]
struct BasicConfig {
    #[serde(default)]
    output_rules: Vec<String>,
    rust_toolchain: Option<String>,
    target: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct BasicPrivate {
    #[serde(default)]
    complete: bool,
    job_state: Option<String>,
    buildomat_id: Option<String>,
    error: Option<String>,

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
        cs: &wollongong_database::CheckSuite,
        cr: &wollongong_database::CheckRun,
        o: &buildomat_openapi::types::JobOutput,
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
    cs: &wollongong_database::CheckSuite,
    cr: &mut wollongong_database::CheckRun,
    _repo: &wollongong_database::Repository,
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

    if !p.job_outputs.is_empty() {
        summary += "The job produced the following artefacts:\n";
        for bo in p.job_outputs.iter() {
            summary +=
                &format!("* [`{}`]({}) ({})\n", bo.path, bo.href, bo.size);
        }
        if p.job_outputs_extra > 0 {
            summary += &format!(
                "* ... and {} more not shown here.\n",
                p.job_outputs_extra
            );
        }
        summary += "\n\n";
    }

    Ok(if p.complete {
        if let Some(e) = p.error.as_deref() {
            FlushOut {
                title: "Failure!".into(),
                summary: format!("{}Flagrant Error: {}", summary, e),
                detail,
                state: FlushState::Failure,
            }
        } else if p.job_state.as_deref().unwrap() == "completed" {
            FlushOut {
                title: "Success!".into(),
                summary: format!("{}The requested job was completed.", summary),
                detail,
                state: FlushState::Success,
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
            }
        }
    } else if let Some(ts) = p.job_state.as_deref() {
        if ts == "queued" {
            FlushOut {
                title: "Waiting to execute...".into(),
                summary: format!("{}The job is in line to run.", summary),
                detail,
                state: FlushState::Queued,
            }
        } else {
            FlushOut {
                title: "Running...".into(),
                summary: format!("{}The job is running now!", summary),
                detail,
                state: FlushState::Running,
            }
        }
    } else {
        FlushOut {
            title: "Waiting to submit...".into(),
            summary: format!("{}The job is in line to run.", summary),
            detail,
            state: FlushState::Queued,
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
    cs: &wollongong_database::CheckSuite,
    cr: &mut wollongong_database::CheckRun,
) -> Result<bool> {
    let db = &app.db;

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

    let b = app.buildomat();
    if let Some(jid) = &p.buildomat_id {
        /*
         * We have submitted the task to buildomat already, so just try
         * to update our state.
         */
        let bt = b.job_get(jid).await?;
        let new_state = Some(bt.state);
        let complete = if let Some(state) = new_state.as_deref() {
            state == "completed" || state == "failed"
        } else {
            false
        };
        if new_state != p.job_state {
            cr.flushed = false;
            p.job_state = new_state;
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

            for ev in b.job_events_get(jid, Some(p.event_minseq)).await? {
                change = true;
                if ev.seq + 1 > p.event_minseq {
                    p.event_minseq = ev.seq + 1;
                }

                if ev.stream == "stdout" || ev.stream == "stderr" {
                    p.events_tail
                        .push_back((None, format!("| {}", ev.payload)));
                } else {
                    p.events_tail.push_back((
                        Some(format!("{}/{:?}", ev.stream, ev.task)),
                        format!("{}: {}", ev.stream, ev.payload),
                    ));
                }
            }

            while p.events_tail.len() > 16 {
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
            let outputs = b.job_outputs_get(jid).await?;
            if !outputs.is_empty() {
                cr.flushed = false;
            }
            for o in outputs {
                if p.job_outputs.len() < MAX_OUTPUTS {
                    p.job_outputs.push(BasicOutput::new(app, cs, cr, &o));
                } else {
                    p.job_outputs_extra += 1;
                }
            }
        }
    } else {
        let repo = db.load_repository(cs.repo)?;

        /*
         * We will need to provide the user program with an access token
         * that allows them to check out what may well be a private
         * repository.
         */
        let token = app.temp_access_token(cs.install, &repo).await?;

        /*
         * Create a series of tasks to configure the build environment
         * before handing control to the user program.
         */
        let mut tasks = Vec::new();
        tasks.push(buildomat_openapi::types::TaskSubmit {
            name: "setup".into(),
            env: Default::default(),
            env_clear: false,
            gid: None,
            uid: None,
            workdir: None,
            script: "\
                #!/bin/bash\n\
                set -o errexit\n\
                set -o pipefail\n\
                set -o xtrace\n\
                groupadd -g 12345 build\n\
                useradd -u 12345 -g build -d /home/work -s /bin/bash \
                -c 'build' -P 'Primary Administrator' build\n\
                zfs create -o mountpoint=/work rpool/work\n\
                mkdir -p /home/work\n\
                chown build:build /home/work /work\n\
                chmod 0700 /home/work /work\n\
                "
            .into(),
        });

        /*
         * Create the base environment for tasks that will run as
         * the non-root build user:
         */
        let mut buildenv = HashMap::new();
        buildenv.insert("HOME".into(), "/home/work".into());
        buildenv.insert("USER".into(), "work".into());
        buildenv.insert("LOGNAME".into(), "work".into());
        buildenv.insert(
            "PATH".into(),
            "/home/work/.cargo/bin:\
            /usr/bin:/usr/sbin:/sbin:/opt/ooce/bin:/opt/ooce/sbin"
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

            tasks.push(buildomat_openapi::types::TaskSubmit {
                name: "rust-toolchain".into(),
                env: buildenv,
                env_clear: false,
                gid: Some(12345),
                uid: Some(12345),
                workdir: Some("/home/work".into()),
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

        buildenv.insert("GITHUB_TOKEN".into(), token.clone());

        /*
         * Write the temporary access token which gives brief read-only
         * access to only this (potentially private) repository into the
         * ~/.netrc file.  When git tries to access GitHub via HTTPS it
         * does so using curl, which knows to look in this file for
         * credentials.  This way, the token need not appear in the
         * build environment or any commands that are run.
         */
        tasks.push(buildomat_openapi::types::TaskSubmit {
            name: "authentication".into(),
            env: buildenv.clone(),
            env_clear: false,
            gid: Some(12345),
            uid: Some(12345),
            workdir: Some("/home/work".into()),
            script: "\
                #!/bin/bash\n\
                set -o errexit\n\
                set -o pipefail\n\
                cat >$HOME/.netrc <<EOF\n\
                machine github.com\n\
                login x-access-token\n\
                password $GITHUB_TOKEN\n\
                EOF\n\
                "
            .into(),
        });

        buildenv.remove("GITHUB_TOKEN"); /* XXX */

        tasks.push(buildomat_openapi::types::TaskSubmit {
            name: "clone repository".into(),
            env: buildenv.clone(),
            env_clear: false,
            gid: Some(12345),
            uid: Some(12345),
            workdir: Some("/home/work".into()),
            script: "\
                #!/bin/bash\n\
                set -o errexit\n\
                set -o pipefail\n\
                set -o xtrace\n\
                mkdir -p \"/work/$GITHUB_REPOSITORY\"\n\
                git clone \"https://github.com/$GITHUB_REPOSITORY\" \
                    \"/work/$GITHUB_REPOSITORY\"\n\
                cd \"/work/$GITHUB_REPOSITORY\"\n\
                if [[ -n $GITHUB_BRANCH ]]; then\n\
                    git fetch origin \"$GITHUB_BRANCH\"\n\
                    git checkout -B \"$GITHUB_BRANCH\" \
                        \"remotes/origin/$GITHUB_BRANCH\"\n\
                else\n\
                    git fetch origin \"$GITHUB_SHA\"\n\
                fi\n\
                git reset --hard \"$GITHUB_SHA\"
                "
            .into(),
        });

        buildenv.insert("CI".to_string(), "true".to_string());
        //buildenv.insert("GITHUB_TOKEN".to_string(), token);

        let workdir = format!("/work/{}/{}", repo.owner, repo.name);

        tasks.push(buildomat_openapi::types::TaskSubmit {
            name: "build".into(),
            env: buildenv,
            env_clear: false,
            gid: Some(12345),
            uid: Some(12345),
            workdir: Some(workdir),
            script,
        });

        let body = &buildomat_openapi::types::JobSubmit {
            name: format!("gong/{}", cr.id),
            output_rules: c.output_rules.clone(),
            target: c.target.as_deref().unwrap_or("default").into(),
            tasks,
        };
        let jsr = b.job_submit(body).await?;

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

pub(crate) async fn artefact(
    app: &Arc<App>,
    _cs: &wollongong_database::CheckSuite,
    cr: &wollongong_database::CheckRun,
    output: &str,
    name: &str,
) -> Result<Option<hyper::Response<hyper::Body>>> {
    let p: BasicPrivate = cr.get_private()?;

    if let Some(id) = &p.buildomat_id {
        let bm = app.buildomat();

        let backend = bm.job_output_download(id, output).await?;

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
        let ct = if name == "Cargo.lock" {
            /*
             * This file may be TOML, but is almost certainly plain text.
             */
            "text/plain".to_string()
        } else {
            new_mime_guess::from_path(std::path::PathBuf::from(name))
                .first_or_octet_stream()
                .to_string()
        };

        return Ok(Some(
            hyper::Response::builder()
                .status(hyper::StatusCode::OK)
                .header(hyper::header::CONTENT_TYPE, ct)
                .header(
                    hyper::header::CONTENT_LENGTH,
                    backend.content_length().unwrap(),
                )
                .body(hyper::Body::wrap_stream(backend.bytes_stream()))?,
        ));
    }

    Ok(None)
}

pub(crate) async fn details(
    app: &Arc<App>,
    cs: &wollongong_database::CheckSuite,
    cr: &wollongong_database::CheckRun,
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
        let bm = app.buildomat();
        let outputs = bm.job_outputs_get(jid).await?;

        out += &format!("<h2>Buildomat Job: {}</h2>\n", jid);
        //out += &format!("<pre>{:#?}</pre>\n", job);

        if !outputs.is_empty() {
            out += "<h3>Artefacts:</h3>\n";
            out += "<ul>\n";
            for o in outputs {
                let bo = BasicOutput::new(app, cs, cr, &o);
                out += &format!(
                    "<li><a href=\"{}\">{}</a> ({})\n",
                    bo.href, bo.path, bo.size,
                );
            }
            out += "</ul>\n";
        }

        out += "<h3>Output:</h3>\n";
        out += "<table style=\"border: none;\">\n";

        let mut last = None;

        for ev in bm.job_events_get(jid, None).await? {
            if ev.task != last {
                out += "<tr><td colspan=\"3\">&nbsp;</td></tr>";
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
                _ => "#000000",
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

            /*
             * The third and final column is the message payload for the event.
             */
            out += &format!(
                "<td style=\"vertical-align: top;\">\
                    <span style=\"white-space: pre-wrap; \
                    white-space: -moz-pre-wrap !important; \
                    font-family: monospace; \
                    \">{}</span>\
                </td>",
                ev.payload,
            );

            out += "</tr>";
        }
        out += "\n</table>\n";
    }

    Ok(out)
}
