/*
 * Copyright 2024 Oxide Computer Company
 */

use std::collections::HashMap;

use anyhow::{bail, Result};
use buildomat_client::{types::TaskSubmit, ClientExt, EventOrState};
use buildomat_common::*;
use buildomat_sse::ServerSentEvents;
use futures::future::BoxFuture;
use hyper::{Body, Response};
use serde::{Deserialize, Serialize};

use crate::JobEventEx;

/*
 * We can use "deny_unknown_fields" here because the global frontmatter fields
 * were already removed in JobFile::parse_content_at_path().
 */
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BasicConfig {
    #[serde(default)]
    pub output_rules: Vec<String>,
    pub rust_toolchain: Option<StringOrBool>,
    pub target: Option<String>,
    #[serde(default)]
    pub access_repos: Vec<String>,
    #[serde(default)]
    pub publish: Vec<BasicConfigPublish>,
    #[serde(default)]
    pub skip_clone: bool,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BasicConfigPublish {
    pub from_output: String,
    pub series: String,
    pub name: String,
}

/**
 * Render an HTML table with the job output.  This table can be inserted into a
 * generated HTML page if care is taken not to perform HTML escaping a second
 * time.  The "evurl" string should be a URL path to the endpoint for the live
 * event stream without any query parameters.
 */
pub async fn output_table(
    bm: &buildomat_client::Client,
    job: &buildomat_client::types::Job,
    local_time: bool,
    evurl: String,
) -> Result<String> {
    let mut out = "<table id=\"table_output\">\n".to_string();

    let mut last = None;

    out += "<tr>\n";
    out += "<td class=\"num\"><span class=\"header\">SEQ</span></td>\n";
    out += "<td><span class=\"header\">GLOBAL TIME</span></td>\n";
    if local_time {
        out += "<td><span class=\"header\">LOCAL TIME</span></td>\n";
    }
    out += "<td><span class=\"header\">DETAILS</span></td>\n";
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
            .job(&job.id)
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
        /*
         * The rendering logic here is shared with the live version, so we
         * use the same pre-formatted object to build the table output:
         */
        let evr = ev.event_row();

        /*
         * If the task has changed, render a full-width blank row in the
         * table:
         */
        if evr.task != last {
            let cols = evr
                .fields
                .iter()
                .filter(|f| !f.local_time || local_time)
                .count();

            out += &format!("<tr><td colspan=\"{cols}\">&nbsp;</td></tr>");
        }
        last = evr.task;

        out += &format!("<tr class=\"{}\">\n", evr.css_class);

        for f in evr.fields.iter() {
            if f.local_time && !local_time {
                /*
                 * Skip the local time column if the user has not requested
                 * it.
                 */
                continue;
            }

            out += &if let Some(id) = &f.anchor {
                format!(
                    "<td class=\"{cls}\">\
                            <a id=\"{id}\"></a>\
                            <a class=\"{cls}link\" href=\"#{id}\">{value}</a>\
                        </td>",
                    cls = &f.css_class,
                    value = &f.value,
                )
            } else {
                format!(
                    "<td><span class=\"{cls}\">{value}</span></td>",
                    cls = &f.css_class,
                    value = &f.value,
                )
            };
        }

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

    let complete = job.state == "completed" || job.state == "failed";
    out += "<script>\n";
    if complete {
        /*
         * Provide an empty function so that the "onload" property we put in
         * <body> earlier does not cause an error.
         */
        out += "function basic_onload() {}\n";
    } else {
        out += &include_str!("../../../variety/basic/www/live.js")
            .replace("%LOCAL_TIME%", &local_time.to_string())
            .replace("%EVENTSOURCE%", &format!("{evurl}?minseq={minseq}"));
    }
    out += "</script>\n";

    Ok(out)
}

/**
 * Produce a server sent event (SSE) stream of job events that will be correctly
 * appended to the pre-rendered partial output table as the job continues.
 */
pub async fn output_sse(
    bm: &buildomat_client::Client,
    job_id: String,
    last_event_id: Option<String>,
    query_minseq: Option<u32>,
) -> Result<Response<Body>> {
    let bme = bm.extra();

    /*
     * The "Last-Event-ID" header will be sent by a browser when reconnecting,
     * with the "id" field of the last event it saw in the previous stream.  The
     * event stream for this endpoint is a mixture of job events, which have a
     * well-defined sequence number, and status change events, which do not.
     *
     * We include in each ID value the sequence number of the most recently sent
     * job event, so that we can always seek to the right point in the events
     * for the job.  This may lead to more than one event with the same sequence
     * number, but that doesn't appear to be a problem in practice.
     *
     * Note that this value must take precedence over the query parameter, as a
     * resumed stream from the browser will, each time it reconnects, include
     * the original query string we gave to the EventSource.  It will only
     * include the header on subsequent retries once it has seen at least one
     * event.
     */
    let mut minseq = None;
    if let Some(lei) = &last_event_id {
        if let Some(num) = lei.strip_prefix("seq-") {
            if let Ok(lei_seq) = num.parse::<u32>() {
                if let Some(lei_seq) = lei_seq.checked_add(1) {
                    /*
                     * Resume the event stream from this earlier point.
                     */
                    minseq = Some(lei_seq);
                }
            }
        }
    }

    if minseq.is_none() {
        minseq = query_minseq;
    }

    let mut sse = ServerSentEvents::default();
    let res = sse.to_response()?;

    tokio::task::spawn(async move {
        let mut rx = bme.watch_job(&job_id, minseq.unwrap_or(0));
        while let Some(eos) = rx.recv().await {
            match eos {
                Ok(EventOrState::State(_)) => (),
                Ok(EventOrState::Event(ev)) => {
                    if !sse
                        .build_event()
                        .id(&format!("seq-{}", ev.seq))
                        .event("row")
                        .data(&serde_json::to_string(&ev.event_row())?)
                        .send()
                        .await
                    {
                        return Ok(());
                    }
                }
                Ok(EventOrState::Done) => {
                    sse.build_event().event("end").data("-").send().await;
                    return Ok(());
                }
                Err(_) => return Ok(()),
            }
        }

        Ok::<(), anyhow::Error>(())
    });

    Ok(res)
}

struct RustToolchain {
    channel: String,
    profile: String,
}

type FileLoader = Box<
    dyn Fn(&str) -> BoxFuture<'static, Result<Option<String>>> + Send + Sync,
>;

async fn load_rust_toolchain_from_repo(
    file_loader: &FileLoader,
) -> Result<RustToolchain> {
    use rust_toolchain_file::{
        toml::ToolchainSection, ParseStrategy, Parser, ToolchainFile, Variant,
    };

    let Ok(Some(f)) = file_loader("rust-toolchain.toml").await else {
        bail!(
            "Your project must have a \"rust-toolchain.toml\" file if you \
            wish to use \"rust_toolchain = true\".",
        );
    };

    match Parser::new(&f, ParseStrategy::Only(Variant::Toml)).parse() {
        Ok(ToolchainFile::Toml(toml)) => match toml.toolchain() {
            ToolchainSection::Path(_) => {
                bail!(
                    "Use of \"path\" in \"rust-toolchain.toml\" file is not \
                    supported with \"rust_toolchain = true\".",
                );
            }
            ToolchainSection::Spec(spec) => {
                let channel = match spec.channel() {
                    Some(c) => c.name().to_string(),
                    None => {
                        bail!(
                            "You must specify \"channel\" in \
                            \"rust-toolchain.toml\" file when using
                            \"rust_toolchain = true\".",
                        );
                    }
                };

                let profile = match spec.profile() {
                    Some(p) => p.name().to_string(),
                    None => "default".to_string(),
                };

                Ok(RustToolchain { channel, profile })
            }
        },
        Ok(ToolchainFile::Legacy(_)) => {
            bail!(
                "You must use a TOML formatted \"rust-toolchain.toml\" file \
                with \"rust_toolchain = true\".",
            );
        }
        Err(e) => bail!("invalid \"rust-toolchain.toml\" file: {e}"),
    }
}

#[derive(Default)]
pub struct TasksBuilder {
    buildenv: HashMap<String, String>,
    use_github_token: bool,
    script: Option<String>,
    file_loader: Option<FileLoader>,
}

impl TasksBuilder {
    pub fn env<K, V>(&mut self, k: K, v: V) -> &mut Self
    where
        K: AsRef<str>,
        V: AsRef<str>,
    {
        self.buildenv.insert(k.as_ref().to_string(), v.as_ref().to_string());
        self
    }

    pub fn script<S>(&mut self, s: S) -> &mut Self
    where
        S: AsRef<str>,
    {
        self.script = Some(s.as_ref().to_string());
        self
    }

    pub fn use_github_token(&mut self, use_github_token: bool) -> &mut Self {
        self.use_github_token = use_github_token;
        self
    }

    pub fn with_file_loader(&mut self, file_loader: FileLoader) -> &mut Self {
        self.file_loader = Some(file_loader);
        self
    }

    pub async fn build(&mut self, c: &BasicConfig) -> Result<Vec<TaskSubmit>> {
        let Some(script) = &self.script else {
            bail!("must provide script");
        };
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
            script: include_str!("../../../variety/basic/scripts/setup.sh")
                .into(),
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
        buildenv.insert("CI".to_string(), "true".to_string());
        buildenv.extend(self.buildenv.clone());

        /*
         * If a Rust toolchain is requested, install it using rustup.
         */
        let toolchain = match c.rust_toolchain.as_ref() {
            Some(StringOrBool::String(s)) => Some(RustToolchain {
                channel: s.to_string(),
                profile: "default".into(),
            }),
            Some(StringOrBool::Bool(true)) => {
                /*
                 * We need to read and parse the "rust-toolchain.toml" file from
                 * the repository for the commit under test:
                 */
                if let Some(fload) = &self.file_loader {
                    Some(load_rust_toolchain_from_repo(fload).await?)
                } else {
                    bail!("no idea how to load rust toolchain file!");
                }
            }
            None | Some(StringOrBool::Bool(false)) => None,
        };

        if let Some(toolchain) = toolchain {
            let mut buildenv = buildenv.clone();
            buildenv.insert("TC_CHANNEL".into(), toolchain.channel);
            buildenv.insert("TC_PROFILE".into(), toolchain.profile);

            tasks.push(buildomat_client::types::TaskSubmit {
                name: "rust-toolchain".into(),
                env: buildenv,
                env_clear: false,
                gid: Some(12345),
                uid: Some(12345),
                workdir: Some("/home/build".into()),
                script: include_str!(
                    "../../../variety/basic/scripts/rustup.sh"
                )
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
        if self.use_github_token {
            tasks.push(buildomat_client::types::TaskSubmit {
                name: "authentication".into(),
                env: buildenv.clone(),
                env_clear: false,
                gid: Some(12345),
                uid: Some(12345),
                workdir: Some("/home/build".into()),
                script: include_str!(
                    "../../../variety/basic/scripts/github_token.sh"
                )
                .into(),
            });
        }

        /*
         * By default, we assume that the target provides toolchains and other
         * development tools like git.  While this makes sense for most jobs, in
         * some cases we intend to build artefacts in one job, then run those
         * binaries in a separated, limited environment where it is not
         * appropriate to try to clone the repository again.  If "skip_clone" is
         * set, we will not clone the repository.
         */
        let workdir = if c.skip_clone {
            /*
             * If we skipped the clone, just use the top-level work area as the
             * working directory for the job.
             */
            "/work".into()
        } else if let Some(repo) = buildenv.get("GITHUB_REPOSITORY") {
            tasks.push(buildomat_client::types::TaskSubmit {
                name: "clone repository".into(),
                env: buildenv.clone(),
                env_clear: false,
                gid: Some(12345),
                uid: Some(12345),
                workdir: Some("/home/build".into()),
                script: include_str!(
                    "../../../variety/basic/scripts/github_clone.sh"
                )
                .into(),
            });

            format!("/work/{repo}")
        } else {
            bail!("no idea how to clone repository for job");
        };

        tasks.push(buildomat_client::types::TaskSubmit {
            name: "build".into(),
            env: buildenv,
            env_clear: false,
            gid: Some(12345),
            uid: Some(12345),
            workdir: Some(workdir),
            script: script.to_string(),
        });

        Ok(tasks)
    }
}
