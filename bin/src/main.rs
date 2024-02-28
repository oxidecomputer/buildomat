/*
 * Copyright 2024 Oxide Computer Company
 */

#![allow(clippy::many_single_char_names)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Instant;

use anyhow::{anyhow, bail, Result};
use buildomat_client::prelude::*;
use buildomat_client::{types::*, ClientBuilder};
use buildomat_common::*;
use chrono::prelude::*;
use hiercmd::prelude::*;
use rusty_ulid::Ulid;

const WIDTH_ISODATE: usize = 20;
const WIDTH_ID: usize = 26;

mod config;

trait FlagsExt {
    fn add_flags(&mut self, name: &'static str) -> Flags;
}

impl FlagsExt for Row {
    #[must_use]
    fn add_flags(&mut self, name: &'static str) -> Flags {
        Flags { row: self, name, out: String::new() }
    }
}

struct Flags<'a> {
    row: &'a mut Row,
    name: &'static str,
    out: String,
}

impl<'a> Flags<'a> {
    #[must_use]
    fn flag(mut self, c: char, b: bool) -> Self {
        if b {
            self.out.push(c);
        } else {
            self.out.push('-');
        }
        self
    }

    fn build(self) {
        self.row.add_str(self.name, &self.out);
    }
}

trait ErrorWrapper<T, F> {
    fn wrap_with(self, makemsg: F) -> Result<T>
    where
        F: Fn() -> String;
}

impl<T, F> ErrorWrapper<T, F>
    for std::result::Result<
        buildomat_client::gen::ResponseValue<T>,
        buildomat_client::Error<buildomat_client::types::Error>,
    >
{
    fn wrap_with(self, makemsg: F) -> Result<T>
    where
        F: Fn() -> String,
    {
        Ok(self
            .map_err(|e| {
                let msg = match e {
                    buildomat_client::Error::ErrorResponse(ref e) => {
                        match e.status() {
                            StatusCode::NOT_FOUND => {
                                Some(format!("{} was not found", makemsg()))
                            }
                            _ => None,
                        }
                    }
                    buildomat_client::Error::InvalidResponsePayload(ref e) => {
                        match e.status() {
                            None if e.is_decode() => {
                                /*
                                 * If the buildomat backend service is offline,
                                 * nginx will return a HTML-formatted page
                                 * instead of a JSON-formatted message.  For
                                 * right now, assume that this means the server
                                 * is offline, rather than emitting a less
                                 * helpful "Invalid Response Payload: error
                                 * decoding response body: expected value at
                                 * line 1 column 1" message.
                                 */
                                Some(
                                    "invalid response: server may be offline!"
                                        .to_string(),
                                )
                            }
                            _ => None,
                        }
                    }
                    _ => None,
                };

                if let Some(msg) = msg {
                    anyhow!("{}", msg)
                } else {
                    anyhow!("{e}")
                }
            })?
            .into_inner())
    }
}

#[derive(Default)]
struct Stuff {
    client_user: Option<Client>,
    client_admin: Option<Client>,
    profile: Option<config::Profile>,
}

impl Stuff {
    fn user(&self) -> &buildomat_client::Client {
        self.client_user.as_ref().unwrap()
    }

    fn admin(&self) -> &buildomat_client::Client {
        /*
         * If the profile has an admin token configured, use it for all admin
         * tasks.  Otherwise, we will try our luck with our regular user
         * credentials, in the hope that we have been granted the required
         * privileges.
         */
        self.client_admin.as_ref().unwrap_or_else(|| self.user())
    }

    /**
     * Perform a fuzzy mapping of an argument string to a user ID.  If the
     * string is correctly formatted, we'll just pass it back.  If not, we'll
     * try and look it up as a username on the server.  Critically, just because
     * this function returns an ID does NOT mean a user with that ID exists:
     * merely that it _might_.
     */
    async fn user_to_id(&self, arg: &str) -> Result<String> {
        if looks_like_a_ulid(arg) {
            /*
             * If this _looks_ like a ULID, make sure it actually _is_ one.
             */
            match Ulid::from_str(arg) {
                Ok(ulid) => Ok(ulid.to_string()),
                Err(e) => bail!(
                    "argument {arg:?} looks like, but is not, a ULID: {e}"
                ),
            }
        } else {
            let name = arg.trim();
            if name.is_empty() {
                bail!("invalid username {arg:?}");
            }

            /*
             * If this is _not_ a ULID, attempt to locate the user ID by the
             * provided username.  Note that the user list did not always
             * support filtering by username in the query string, so we must
             * check the results match what we expected.
             */
            let users =
                self.admin().users_list().name(name).send().await?.into_inner();
            for user in users {
                if user.name == name {
                    return Ok(user.id);
                }
            }

            bail!("user {name:?} was not found");
        }
    }

    /**
     * Perform a fuzzy mapping of an argument string to a target ID.  If the
     * string is correctly formatted, we'll just pass it back.  If not, we'll
     * try and look it up as a target name on the server.  Critically, just
     * because this function returns an ID does NOT mean a target with that ID
     * exists: merely that it _might_.  Note that target redirection resolution
     * is NOT performed here.
     */
    async fn target_to_id(&self, arg: &str) -> Result<String> {
        if looks_like_a_ulid(arg) {
            /*
             * If this _looks_ like a ULID, make sure it actually _is_ one.
             */
            match Ulid::from_str(arg) {
                Ok(ulid) => Ok(ulid.to_string()),
                Err(e) => bail!(
                    "argument {arg:?} looks like, but is not, a ULID: {e}"
                ),
            }
        } else {
            let name = arg.trim();
            if name.is_empty() {
                bail!("invalid target name {arg:?}");
            }

            /*
             * If this is _not_ a ULID, attempt to locate the target ID by the
             * provided target name.
             */
            let targets =
                self.admin().targets_list().send().await?.into_inner();
            for t in targets {
                if t.name == name {
                    return Ok(t.id);
                }
            }

            bail!("could not locate target with name {name:?}");
        }
    }

    /**
     * Perform a fuzzy mapping of an argument string to a factory ID.  If the
     * string is correctly formatted, we'll just pass it back.  If not, we'll
     * try and look it up as a factory name on the server.  Critically, just
     * because this function returns an ID does NOT mean a factory with that ID
     * exists: merely that it _might_.
     */
    async fn factory_to_id(&self, arg: &str) -> Result<String> {
        if looks_like_a_ulid(arg) {
            /*
             * If this _looks_ like a ULID, make sure it actually _is_ one.
             */
            match Ulid::from_str(arg) {
                Ok(ulid) => Ok(ulid.to_string()),
                Err(e) => bail!(
                    "argument {arg:?} looks like, but is not, a ULID: {e}"
                ),
            }
        } else {
            let name = arg.trim();
            if name.is_empty() {
                bail!("invalid factory name {arg:?}");
            }

            /*
             * If this is _not_ a ULID, attempt to locate the factory ID by the
             * provided factory name.
             */
            let factories =
                self.admin().factories_list().send().await?.into_inner();
            for f in factories {
                if f.name == name {
                    return Ok(f.id);
                }
            }

            bail!("could not locate factory with name {name:?}");
        }
    }
}

async fn do_job_join(mut l: Level<Stuff>) -> Result<()> {
    l.usage_args(Some("JOB..."));

    l.optflag("v", "", "verbose output");

    let a = args!(l);
    let verbose = a.opts().opt_present("v");

    /*
     * Try to make sure the job IDs are actually job IDs before we get started
     * polling.
     */
    for id in a.args() {
        Ulid::from_str(id.as_str())?;
    }

    let mut last_state: HashMap<String, String> = Default::default();
    let mut waiting_for: HashSet<String> =
        a.args().iter().map(String::to_string).collect();

    loop {
        let mut failed = false;

        if waiting_for.is_empty() {
            break;
        }

        for jid in waiting_for.clone().into_iter() {
            match l.context().user().job_get().job(&jid).send().await {
                Ok(rv) => {
                    let job = rv.into_inner();

                    if verbose {
                        if let Some(old) = last_state.get(&job.id) {
                            if &job.state != old {
                                eprintln!(
                                    "INFO: job {} state change: {:?} -> {:?}",
                                    job.id, old, job.state,
                                );
                            }
                        } else if job.state != "completed" {
                            eprintln!(
                                "INFO: job {} in state {:?}",
                                job.id, job.state,
                            );
                        }
                    }
                    last_state
                        .insert(job.id.to_string(), job.state.to_string());

                    match job.state.as_str() {
                        "completed" => {
                            if verbose {
                                eprintln!(
                                    "job {} completed successfully",
                                    job.id
                                );
                            }
                            assert!(waiting_for.remove(&job.id));
                        }
                        "failed" | "cancelled" => {
                            eprintln!("ERROR: job {} failed", job.id);
                            failed = true;
                        }
                        _ => (),
                    }
                }
                Err(e) => {
                    eprintln!("WARNING: polling {jid:?} failed: {e}");
                }
            }

            if failed {
                bail!("some jobs failed to complete successfully");
            }
        }

        if waiting_for.is_empty() {
            break;
        }

        sleep_ms(5000).await;
    }

    if verbose {
        eprintln!("all specified jobs completed successfully");
    }

    Ok(())
}

async fn do_job_tail(mut l: Level<Stuff>) -> Result<()> {
    l.usage_args(Some("JOB"));

    l.optflag("j", "", "format output records as line-separated JSON");

    let a = args!(l);

    if a.args().len() != 1 {
        bad_args!(l, "specify a job");
    }

    poll_job(&l, &a.args()[0], a.opts().opt_present("j")).await
}

async fn do_job_run(mut l: Level<Stuff>) -> Result<()> {
    l.optflag("W", "no-wait", "do not wait for job to complete");
    l.optflag("E", "empty-env", "start with a completely empty environment");
    l.optmulti("e", "env", "environment variable", "KEY=VALUE");
    l.reqopt("n", "name", "job name", "NAME");
    l.optopt("c", "script", "bash script to run", "SCRIPT");
    l.optopt("C", "script-file", "bash program file to run", "FILE");
    l.optopt("t", "target", "target on which to run the job", "SCRIPT");
    l.optmulti(
        "O",
        "output-rule",
        "output rule to match files to save after the job completes",
        "GLOB",
    );
    l.optmulti("i", "input", "input file to pass to job", "[NAME=]FILE");
    l.optmulti("d", "depend-on", "depend on prior job", "NAME=JOB_ID");
    l.optmulti("T", "tag", "informational tag to identify job", "KEY=VALUE");
    l.optflag("v", "", "debugging output");

    l.mutually_exclusive(&[("c", "script"), ("C", "script-file")]);

    let a = no_args!(l);

    let nowait = a.opts().opt_present("no-wait");
    let name = a.opts().opt_str("name").unwrap();
    let target = a.opts().opt_str("target").unwrap_or_else(|| "default".into());
    let script = if let Some(script) = a.opts().opt_str("script") {
        script
    } else if let Some(path) = a.opts().opt_str("script-file") {
        let mut f = std::fs::File::open(path)?;
        let mut s = String::new();
        f.read_to_string(&mut s)?;
        s
    } else {
        bail!("must specify one of --script (-c) or --script-file (-C)");
    };
    let output_rules = a.opts().opt_strs("output-rule");
    let env_clear = a.opts().opt_present("empty-env");
    let env = a
        .opts()
        .opt_strs("env")
        .iter()
        .map(|val| {
            if let Some((k, v)) = val.split_once('=') {
                (k.to_string(), v.to_string())
            } else {
                bad_args!(
                    l,
                    "--env (-e) requires KEY=VALUE environment variables"
                );
            }
        })
        .collect::<HashMap<String, String>>();
    let tags = a
        .opts()
        .opt_strs("tag")
        .iter()
        .map(|val| {
            if let Some((k, v)) = val.split_once('=') {
                (k.to_string(), v.to_string())
            } else {
                bad_args!(
                    l,
                    "--tag (-t) requires KEY=VALUE tag specifications"
                );
            }
        })
        .collect::<HashMap<String, String>>();
    let inputs = a
        .opts()
        .opt_strs("input")
        .iter()
        .map(|val| {
            if let Some((name, path)) = val.split_once('=') {
                /*
                 * If the user provided a name for the input, use it as-is:
                 */
                Ok((name.to_string(), PathBuf::from(path)))
            } else {
                /*
                 * Otherwise, use basename (the name of the file) as the input
                 * name:
                 */
                let path = PathBuf::from(val);
                if !path.is_file() {
                    bail!("path {:?} is not a file", path);
                }
                if let Some(name) = path.file_name() {
                    if let Some(name) = name.to_str() {
                        Ok((name.to_string(), path))
                    } else {
                        bail!("path {:?} not a valid string", path);
                    }
                } else {
                    bail!("path {:?} not well-formed", path);
                }
            }
        })
        .collect::<Result<HashMap<String, PathBuf>>>()?;
    let depends = a
        .opts()
        .opt_strs("depend-on")
        .iter()
        .map(|val| {
            if let Some((name, job)) = val.split_once('=') {
                Ok((
                    name.to_string(),
                    DependSubmit {
                        copy_outputs: true,
                        on_completed: true,
                        on_failed: false,
                        prior_job: job.to_string(),
                    },
                ))
            } else {
                bail!("dependency {:?} not well-formed", val);
            }
        })
        .collect::<Result<HashMap<String, DependSubmit>>>()?;

    /*
     * Check that the set of input files will fit within any quota requirements
     * in place on the server.  The server will eventually reject our request if
     * it exceeds the quota anyway, but we can fail quickly and with a helpful
     * error message here.
     */
    if !inputs.is_empty() {
        let q = l.context().user().quota().send().await?.into_inner();

        for (_, p) in inputs.iter() {
            let md =
                p.metadata().map_err(|e| anyhow!("input file {p:?}: {e}"))?;

            if !md.is_file() {
                bail!("input file {p:?} is not a regular file");
            }

            if md.len() > q.max_bytes_per_input {
                bail!(
                    "input file {p:?} is {} bytes long, \
                    but the maximum input file size is {} bytes",
                    md.len(),
                    q.max_bytes_per_input,
                );
            }
        }
    }

    let mut w = Stopwatch::start(a.opts().opt_present("v"));

    /*
     * Create the job on the server.
     */
    let x = l
        .context()
        .user()
        .job_submit()
        .body(JobSubmit {
            name,
            target,
            output_rules,
            tasks: vec![TaskSubmit {
                name: "default".to_string(),
                script,
                env_clear,
                env,
                gid: None,
                uid: None,
                workdir: None,
            }],
            inputs: inputs.keys().cloned().collect(),
            tags,
            depends,
        })
        .send()
        .await?;
    w.lap("job submit");

    for (name, path) in inputs.iter() {
        let mut f = std::fs::File::open(path)?;

        /*
         * Read 5MB chunks of the file and upload them to the server.
         */
        let mut total = 0;
        let mut chunks = Vec::new();
        loop {
            let mut buf = bytes::BytesMut::new();
            buf.resize(5 * 1024 * 1024, 0);

            let buf = match f.read(&mut buf) {
                Ok(0) => break,
                Ok(sz) => {
                    buf.truncate(sz);
                    total += sz as u64;
                    buf.freeze()
                }
                Err(e) => {
                    bail!("failed to read from {:?}: {:?}", path, e);
                }
            };

            chunks.push(
                l.context()
                    .user()
                    .job_upload_chunk()
                    .job(&x.id)
                    .body(buf)
                    .send()
                    .await?
                    .into_inner()
                    .id,
            );

            w.lap(&format!("upload {} chunk {}", name, chunks.len()));
        }

        let commit_id = Ulid::generate();

        loop {
            match l
                .context()
                .user()
                .job_add_input()
                .job(&x.id)
                .body_map(|body| {
                    body.chunks(chunks.clone())
                        .name(name)
                        .size(total as i64)
                        .commit_id(commit_id.to_string())
                })
                .send()
                .await
            {
                Ok(jair) => {
                    if !jair.complete {
                        /*
                         * XXX For now, we poll on completion.  It would
                         * obviously better to be notified, e.g., through long
                         * polling.
                         */
                        sleep_ms(1000).await;
                        continue;
                    }

                    if let Some(e) = &jair.error {
                        bail!("input file error: {e}");
                    }

                    break;
                }
                Err(e) => bail!("input file error: {e}"),
            }
        }
        w.lap(&format!("add input {}", name));
    }

    if nowait {
        /*
         * In no-wait mode, just emit the job ID so that it can be used from a
         * shell script without additional parsing.
         */
        println!("{}", x.id);
        return Ok(());
    }

    println!("job {} submitted", x.id);
    poll_job(&l, &x.id, false).await
}

async fn poll_job(l: &Level<Stuff>, id: &str, json: bool) -> Result<()> {
    if !json {
        println!("polling for job output...");
    }

    let mut nextseq = 0;
    let mut exit_status = 0;
    let mut last_state = String::new();
    loop {
        let t = match l.context().user().job_get().job(id).send().await {
            Ok(t) => t,
            Err(_) => {
                sleep_ms(500).await;
                continue;
            }
        };

        if t.state == "failed" {
            exit_status = 1;
        }

        if t.state != last_state {
            if !json {
                println!("STATE CHANGE: {} -> {}", last_state, t.state);
            }
            last_state = t.state.to_string();
        }

        let terminal = t.state == "completed" || t.state == "failed";

        if let Ok(events) = l
            .context()
            .user()
            .job_events_get()
            .job(id)
            .minseq(nextseq)
            .send()
            .await
        {
            if events.is_empty() {
                if terminal {
                    /*
                     * EOF.
                     */
                    break;
                }
            } else {
                for e in events.iter() {
                    if json {
                        println!("{}", serde_json::to_string(&e)?);
                    } else if e.stream == "stdout" || e.stream == "stderr" {
                        println!("{}", e.payload);
                    } else if e.stream == "control" {
                        println!("|=| {}", e.payload);
                    } else if e.stream == "worker" {
                        println!("|W| {}", e.payload);
                    } else if e.stream == "task" {
                        println!("|T| {}", e.payload);
                    } else if e.stream == "console" {
                        println!("|C| {}", e.payload);
                    } else if e.stream.starts_with("bg.") {
                        let t = e.stream.split('.').collect::<Vec<_>>();
                        if t.len() == 3 {
                            if t[2] == "stdout" || t[2] == "stderr" {
                                println!("[{}] {}", t[1], e.payload);
                            } else {
                                println!("{:?}", e);
                            }
                        } else {
                            println!("{:?}", e);
                        }
                    } else {
                        println!("{:?}", e);
                    }
                    nextseq = e.seq + 1;
                }
            }
        }

        if !terminal {
            sleep_ms(250).await;
        }
    }

    if exit_status != 0 {
        std::process::exit(exit_status);
    } else {
        Ok(())
    }
}

async fn do_info(mut l: Level<Stuff>) -> Result<()> {
    no_args!(l);
    let whoami = l.context().user().whoami().send().await?;
    println!("{:#?}", whoami);
    Ok(())
}

async fn do_control_resume(mut l: Level<Stuff>) -> Result<()> {
    no_args!(l);
    println!("{:?}", l.context().admin().control_resume().send().await?);
    Ok(())
}

async fn do_control_hold(mut l: Level<Stuff>) -> Result<()> {
    no_args!(l);
    println!("{:?}", l.context().admin().control_hold().send().await?);
    Ok(())
}

async fn do_control_recycle(mut l: Level<Stuff>) -> Result<()> {
    no_args!(l);
    println!("{:?}", l.context().admin().workers_recycle().send().await?);
    Ok(())
}

async fn do_control(mut l: Level<Stuff>) -> Result<()> {
    l.cmd("hold", "hold new VM creation", cmd!(do_control_hold))?;
    l.cmd("resume", "resume new VM creation", cmd!(do_control_resume))?;
    l.cmd("recycle", "recycle all workers", cmd!(do_control_recycle))?;

    sel!(l).run().await
}

async fn do_job_cancel(mut l: Level<Stuff>) -> Result<()> {
    l.usage_args(Some("JOB"));

    let a = args!(l);

    if a.args().len() != 1 {
        bad_args!(l, "specify job ID");
    }

    l.context().user().job_cancel().job(a.args()[0].as_str()).send().await?;

    Ok(())
}

async fn do_job_outputs(mut l: Level<Stuff>) -> Result<()> {
    l.add_column("path", 68, true);
    l.add_column("size", 10, true);
    l.add_column("id", WIDTH_ID, false);

    l.usage_args(Some("JOB"));

    let a = args!(l);
    let mut t = a.table();

    if a.args().len() != 1 {
        bad_args!(l, "specify job ID");
    }
    let j = a.args()[0].as_str();

    for i in
        l.context().user().job_outputs_get().job(j).send().await?.into_inner()
    {
        let mut r = Row::default();
        r.add_str("id", &i.id);
        r.add_str("path", &i.path);
        r.add_bytes("size", i.size);
        t.add_row(r);
    }

    print!("{}", t.output()?);
    Ok(())
}

async fn do_job_list(mut l: Level<Stuff>) -> Result<()> {
    l.add_column("id", WIDTH_ID, true);
    l.add_column("age", 8, true);
    l.add_column("s", 1, true);
    l.add_column("name", 32, true);
    l.add_column("state", 15, false);

    l.optmulti("T", "", "job tag filter", "TAG=VALUE");
    l.optopt("F", "", "job state filter", "STATE");
    l.optflag("U", "", "do not sort locally; just stream results from server");

    let a = no_args!(l);
    let ftags = a
        .opts()
        .opt_strs("T")
        .iter()
        .map(|a| {
            a.split_once('=')
                .ok_or_else(|| anyhow!("invalid tag filter"))
                .map(|(k, v)| (k.to_string(), v.to_string()))
        })
        .collect::<Result<Vec<_>>>()?;

    let fstate = a.opts().opt_str("F");
    let unsorted = a.opts().opt_present("U");

    let mut t = a.table();

    if unsorted {
        print!("{}", t.output_unsorted_header()?);
    }

    let mut jobs = l.context().user().jobs_list();
    if !ftags.is_empty() {
        jobs = jobs.tag(
            ftags
                .into_iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join("|"),
        );
    }
    let mut jobs = jobs.recent_first(unsorted).stream();

    while let Some(job) = jobs.try_next().await? {
        if let Some(s) = &fstate {
            if s != &job.state {
                continue;
            }
        }

        let mut r = Row::default();
        r.add_str("id", &job.id);
        r.add_str("name", &job.name);
        r.add_age("age", job.id()?.age());
        if job.state == "failed" && job.cancelled {
            r.add_str("s", "X");
            r.add_str("state", "cancelled");
        } else {
            r.add_str(
                "s",
                match job.state.as_str() {
                    /*
                     * Terminal states in upper case:
                     */
                    "failed" => "F",
                    "completed" => "C",
                    /*
                     * Non-terminal states in lower case:
                     */
                    "running" => "r",
                    "waiting" => "w",
                    "queued" => "q",

                    _ => "?",
                },
            );
            r.add_str("state", &job.state);
        }

        if unsorted {
            print!("{}", t.output_unsorted(r)?);
        } else {
            t.add_row(r);
        }
    }

    if !unsorted {
        print!("{}", t.output()?);
    }
    Ok(())
}

async fn do_job_dump(mut l: Level<Stuff>) -> Result<()> {
    let a = args!(l);

    let c = l.context().user();

    for id in a.args() {
        let job = c.job_get().job(id).send().await?;

        println!("{:<26} {:<15} {}", job.id, job.state, job.name);
        println!("{:#?}", job);
        println!();
    }

    Ok(())
}

async fn do_job_timings(mut l: Level<Stuff>) -> Result<()> {
    l.optopt("I", "", "measure an interval", "FROM,TO");

    let a = args!(l);

    let measure = a
        .opts()
        .opt_str("I")
        .map(|a| {
            a.split_once(',')
                .ok_or_else(|| anyhow!("invalid interval specification"))
                .map(|(f, t)| (f.to_string(), t.to_string()))
        })
        .transpose()?;

    let c = l.context().user();

    for id in a.args() {
        let job = c.job_get().job(id).send().await?;

        if let Some((mfrom, mto)) = &measure {
            match (job.times.get(mfrom), job.times.get(mto)) {
                (Some(tfrom), Some(tto)) => {
                    let dur = tto.signed_duration_since(*tfrom);
                    println!(
                        "{} {} {}",
                        job.id,
                        tfrom
                            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                        dur.num_seconds()
                    );
                }
                _ => {
                    println!("{} missing", job.id);
                }
            }
        } else {
            let mut times = job.times.iter().collect::<Vec<_>>();
            times.sort_by(|a, b| a.1.cmp(b.1));

            for time in times {
                println!(
                    "{} {} {}",
                    job.id,
                    time.1.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                    time.0,
                );
            }
        }
    }

    Ok(())
}

async fn do_job_copy(mut l: Level<Stuff>) -> Result<()> {
    l.usage_args(Some("JOB SRC DST"));

    let a = args!(l);

    if a.args().len() != 3 {
        bad_args!(l, "specify a job, a job output path, and a local file name");
    }

    let job = a.args()[0].as_str();
    let src = a.args()[1].as_str();
    let dst = a.args()[2].as_str();

    let c = l.context().user();
    for o in c.job_outputs_get().job(job).send().await?.into_inner() {
        if o.path == src {
            eprintln!(
                "downloading {} -> {} ({}KB)",
                o.path,
                dst,
                o.size / 1024,
            );
            let mut res = c
                .job_output_download()
                .job(job)
                .output(&o.id)
                .send()
                .await?
                .into_inner();

            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(dst)?;

            while let Some(ch) = res.next().await.transpose()? {
                f.write_all(&ch)?;
            }
            f.flush()?;

            return Ok(());
        }
    }

    bail!("job {} does not have a file that matches {}", job, src);
}

async fn do_job_sign(mut l: Level<Stuff>) -> Result<()> {
    l.usage_args(Some("JOB SRC"));

    let a = args!(l);

    if a.args().len() != 2 {
        bad_args!(l, "specify a job and a job output path");
    }

    let job = a.args()[0].as_str();
    let src = a.args()[1].as_str();

    let c = l.context().user();
    for o in c.job_outputs_get().job(job).send().await?.into_inner() {
        if o.path != src {
            continue;
        }

        let su = c
            .job_output_signed_url()
            .job(job)
            .output(&o.id)
            .body_map(|body| {
                body.content_type("text/plain".to_string()).expiry_seconds(3600)
            })
            .send()
            .await?
            .into_inner();

        println!("{}", su.url);
        return Ok(());
    }

    bail!("job {} does not have a file that matches {}", job, src);
}

async fn do_job_publish(mut l: Level<Stuff>) -> Result<()> {
    l.usage_args(Some("JOB SRC SERIES VERSION NAME"));

    let a = args!(l);

    if a.args().len() != 5 {
        bad_args!(
            l,
            "specify a job, a job output path, and the name for \
            the published file: series, version, and file name"
        );
    }

    let job = a.args()[0].as_str();
    let src = a.args()[1].as_str();
    let series = a.args()[2].as_str();
    let version = a.args()[3].as_str();
    let name = a.args()[4].as_str();

    let c = l.context().user();
    for o in c.job_outputs_get().job(job).send().await?.into_inner() {
        if o.path == src {
            println!(
                "publishing {} -> {}/{}/{} ({}KB)",
                o.path,
                series,
                version,
                name,
                o.size / 1024
            );

            c.job_output_publish()
                .job(job)
                .output(&o.id)
                .body_map(|body| {
                    body.name(name).series(series).version(version)
                })
                .send()
                .await?;
            return Ok(());
        }
    }

    bail!("job {} does not have a file that matches {}", job, src);
}

async fn do_job_store_put(mut l: Level<Stuff>) -> Result<()> {
    l.usage_args(Some("JOB NAME [VALUE]"));
    l.optflag("s", "", "mark value as secret data");

    let a = args!(l);

    /*
     * Processing of the format of the input should be kept in sync with what
     * "bmat store put" does inside the job; see the "buildomat-agent" crate.
     */
    let value = match a.args().len() {
        2 => {
            let mut s = String::new();
            std::io::stdin().lock().read_to_string(&mut s)?;
            if let Some(suf) = s.strip_suffix('\n') {
                if suf.contains('\n') {
                    /*
                     * This is a multiline value, so leave it as-is.
                     */
                    s
                } else {
                    suf.to_string()
                }
            } else {
                s
            }
        }
        3 => a.args()[2].to_string(),
        _ => {
            bad_args!(l, "specify name of value, and value, to put in store");
        }
    };
    let job = a.args()[0].to_string();
    let name = a.args()[1].to_string();

    let secret = a.opts().opt_present("s");

    l.context()
        .user()
        .job_store_put()
        .job(&job)
        .name(&name)
        .body_map(|body| body.secret(secret).value(value))
        .send()
        .await?;

    Ok(())
}

async fn do_job_store_get(mut l: Level<Stuff>) -> Result<()> {
    l.usage_args(Some("JOB NAME"));

    let a = args!(l);

    if a.args().len() != 2 {
        bad_args!(l, "specify job ID and name of value to fetch from store");
    }

    let job = a.args()[0].to_string();
    let name = a.args()[1].to_string();

    let store = l
        .context()
        .user()
        .job_store_get_all()
        .job(&job)
        .send()
        .await?
        .into_inner();

    if let Some(ent) = store.get(&name) {
        if ent.secret {
            bail!(
                "{name:?} is a secret property; \
                cannot get value outside of job",
            );
        }

        let Some(value) = &ent.value else {
            /*
             * This should currently only happen for secret properties, which we
             * have handled above.
             */
            bail!("server would not give us the value of {name:?}");
        };

        /*
         * Output formatting here should be kept consistent with what "bmat
         * store get" does inside a job; see the "buildomat-agent" crate.
         */
        if value.ends_with('\n') {
            print!("{}", value);
        } else {
            println!("{}", value);
        }

        Ok(())
    } else {
        bail!("{name:?} was not found in the store for job {job}");
    }
}

async fn do_job_store_list(mut l: Level<Stuff>) -> Result<()> {
    l.usage_args(Some("JOB"));

    l.add_column("name", 16, true);
    l.add_column("flags", 5, true);
    l.add_column("age", 6, true);
    l.add_column("value", 50, true);
    l.add_column("source", 10, false);
    l.add_column("updated", WIDTH_ISODATE, false);

    let a = args!(l);

    if a.args().len() != 1 {
        bad_args!(l, "specify job ID");
    }

    let job = a.args()[0].to_string();

    let mut t = a.table();

    let store = l
        .context()
        .user()
        .job_store_get_all()
        .job(&job)
        .send()
        .await?
        .into_inner()
        .into_iter()
        .collect::<BTreeMap<_, _>>();

    for (name, ent) in store {
        let mut r = Row::default();

        r.add_str("name", &name);
        r.add_flags("flags").flag('S', ent.secret).build();
        r.add_str("value", ent.value.as_deref().unwrap_or("-"));
        r.add_str("source", &ent.source);
        r.add_age(
            "age",
            Utc::now().signed_duration_since(ent.time_update).to_std().unwrap(),
        );
        r.add_str(
            "updated",
            &ent.time_update.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        );

        t.add_row(r);
    }

    print!("{}", t.output()?);

    Ok(())
}

async fn do_job_store(mut l: Level<Stuff>) -> Result<()> {
    l.cmda("list", "ls", "list store contents", cmd!(do_job_store_list))?;
    l.cmd("get", "get a value from the job store", cmd!(do_job_store_get))?;
    l.cmd("put", "put a value into the job store", cmd!(do_job_store_put))?;

    sel!(l).run().await
}

async fn do_job(mut l: Level<Stuff>) -> Result<()> {
    l.cmda("list", "ls", "list jobs", cmd!(do_job_list))?;
    l.cmd("run", "run a job", cmd!(do_job_run))?;
    l.cmd("cancel", "cancel a job", cmd!(do_job_cancel))?;
    l.cmd("tail", "listen for events from a job", cmd!(do_job_tail))?;
    l.cmd("join", "wait for completion of jobs", cmd!(do_job_join))?;
    l.cmd("store", "manage the job store", cmd!(do_job_store))?;
    l.cmd("outputs", "list job outputs", cmd!(do_job_outputs))?;
    l.cmd("dump", "dump information about jobs", cmd!(do_job_dump))?;
    l.cmd("timings", "timing information about a job", cmd!(do_job_timings))?;
    l.cmda(
        "copy",
        "cp",
        "copy from job outputs to local files",
        cmd!(do_job_copy),
    )?;
    l.cmd("sign", "sign a download URL for a job output", cmd!(do_job_sign))?;
    l.cmd(
        "publish",
        "publish a job output for public consumption",
        cmd!(do_job_publish),
    )?;

    sel!(l).run().await
}

async fn do_user_create(mut l: Level<Stuff>) -> Result<()> {
    l.usage_args(Some("NAME"));

    let a = args!(l);

    if a.args().len() != 1 {
        bad_args!(l, "specify name of user");
    }
    let name = a.args()[0].to_string();

    let res = l
        .context()
        .admin()
        .user_create()
        .body_map(|body| body.name(name))
        .send()
        .await?;

    println!("{}", res.token);
    Ok(())
}

async fn do_user_grant(mut l: Level<Stuff>) -> Result<()> {
    l.usage_args(Some("USER_ID|USERNAME PRIVILEGE"));

    let a = args!(l);

    if a.args().len() != 2 {
        bad_args!(l, "specify name or ID of user and name of privilege");
    }
    let id = l.context().user_to_id(&a.args()[0]).await?;
    let privilege = a.args()[1].to_string();

    l.context()
        .admin()
        .user_privilege_grant()
        .user(&id)
        .privilege(&privilege)
        .send()
        .await?;
    Ok(())
}

async fn do_user_revoke(mut l: Level<Stuff>) -> Result<()> {
    l.usage_args(Some("USER_ID|USERNAME PRIVILEGE"));

    let a = args!(l);

    if a.args().len() != 2 {
        bad_args!(l, "specify name or ID of user and name of privilege");
    }
    let id = l.context().user_to_id(&a.args()[0]).await?;
    let privilege = a.args()[1].to_string();

    l.context()
        .admin()
        .user_privilege_revoke()
        .user(&id)
        .privilege(&privilege)
        .send()
        .await?;
    Ok(())
}

async fn do_user_list(mut l: Level<Stuff>) -> Result<()> {
    l.add_column("id", WIDTH_ID, true);
    l.add_column("name", 30, true);
    l.add_column("age", 8, true);
    l.add_column("creation", WIDTH_ISODATE, false);

    let a = no_args!(l);

    let mut t = a.table();

    for u in l.context().admin().users_list().send().await?.into_inner() {
        let mut r = Row::default();
        r.add_str("id", &u.id);
        r.add_str("name", &u.name);
        r.add_str(
            "creation",
            &u.time_create.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        );
        r.add_age("age", u.time_create.age());
        t.add_row(r);
    }

    print!("{}", t.output()?);
    Ok(())
}

async fn do_user_show(mut l: Level<Stuff>) -> Result<()> {
    l.usage_args(Some("USER_ID|USERNAME"));

    let a = args!(l);

    if a.args().len() != 1 {
        bad_args!(l, "specify name or ID of user");
    }
    let id = l.context().user_to_id(&a.args()[0]).await?;

    let res = l
        .context()
        .admin()
        .user_get()
        .user(&id)
        .send()
        .await
        .wrap_with(|| format!("user {:?}", a.args()[0]))?;

    println!("id:          {}", res.id);
    println!("name:        {}", res.name);
    println!("created at:  {}", res.time_create);
    if !res.privileges.is_empty() {
        println!("privileges:");
        for privilege in res.privileges.iter() {
            println!("    * {}", privilege);
        }
    }

    Ok(())
}

async fn do_user(mut l: Level<Stuff>) -> Result<()> {
    l.cmda("list", "ls", "list users", cmd!(do_user_list))?;
    l.cmd("create", "create a user", cmd!(do_user_create))?;
    l.cmd("show", "examine a particular user", cmd!(do_user_show))?;
    l.cmd("grant", "grant a privilege to a user", cmd!(do_user_grant))?;
    l.cmd("revoke", "revoke a privilege from a user", cmd!(do_user_revoke))?;

    sel!(l).run().await
}

async fn do_worker_list(mut l: Level<Stuff>) -> Result<()> {
    l.add_column("id", WIDTH_ID, true);
    l.add_column("flags", 5, true);
    l.add_column("creation", WIDTH_ISODATE, true);
    l.add_column("age", 8, true);
    l.add_column("info", 20, false);
    l.add_column("target", WIDTH_ID, false);
    l.add_column("hold_time", WIDTH_ISODATE, false);
    l.add_column("hold_reason", 32, false);

    l.optflag("A", "active", "display only workers not yet destroyed");
    l.optflag("h", "held", "display only workers that are marked on hold");
    l.optflag("U", "", "do not sort locally; just stream results from server");

    let a = no_args!(l);
    let active = a.opts().opt_present("active");
    let held = a.opts().opt_present("held");
    let unsorted = a.opts().opt_present("U");
    let c = l.context();

    let mut t = a.table();

    if unsorted {
        print!("{}", t.output_unsorted_header()?);
    }

    let mut workers = c.admin().workers_list().active(active).stream();
    while let Some(w) = workers.try_next().await? {
        if active && w.deleted {
            continue;
        }

        if held && (w.deleted || w.hold.is_none()) {
            continue;
        }

        let id = w.id()?;

        let mut r = Row::default();
        r.add_str("id", &w.id);
        r.add_str(
            "creation",
            id.creation().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        );
        r.add_age("age", id.age());
        r.add_flags("flags")
            .flag('B', w.bootstrap)
            .flag('J', !w.jobs.is_empty())
            .flag('R', w.recycle)
            .flag('D', w.deleted)
            .flag('H', w.hold.is_some())
            .build();
        r.add_str("info", w.factory_private.as_deref().unwrap_or("-"));
        r.add_str("target", &w.target);

        if let Some(hold) = &w.hold {
            r.add_str("hold_reason", &hold.reason);
            r.add_str(
                "hold_time",
                hold.time.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            );
        } else {
            r.add_str("hold_reason", "-");
            r.add_str("hold_time", "-");
        }

        if unsorted {
            print!("{}", t.output_unsorted(r)?);
        } else {
            t.add_row(r);
        }
    }

    if !unsorted {
        print!("{}", t.output()?);
    }
    Ok(())
}

async fn do_worker_recycle(mut l: Level<Stuff>) -> Result<()> {
    l.usage_args(Some("WORKER..."));

    let a = args!(l);
    if a.args().is_empty() {
        bad_args!(l, "specify a worker to recycle");
    }

    for arg in a.args() {
        if let Err(e) =
            l.context().admin().worker_recycle().worker(arg).send().await
        {
            bail!("ERROR: recycling {}: {:?}", arg, e);
        }
    }

    Ok(())
}

async fn do_worker_hold(mut l: Level<Stuff>) -> Result<()> {
    l.usage_args(Some("WORKER..."));
    l.reqopt("r", "", "specify the reason for the hold", "REASON");

    let a = args!(l);
    if a.args().is_empty() {
        bad_args!(l, "specify a worker to mark as held");
    }

    let reason = a.opts().opt_str("r").unwrap();
    if reason.trim().len() < 8 {
        bail!("please specify a more in depth reason for the hold");
    }

    for arg in a.args() {
        if let Err(e) = l
            .context()
            .admin()
            .worker_hold_mark()
            .worker(arg)
            .body_map(|b| b.reason(reason.trim()))
            .send()
            .await
        {
            bail!("ERROR: marking {arg} as held: {e:?}");
        }
    }

    Ok(())
}

async fn do_worker_release(mut l: Level<Stuff>) -> Result<()> {
    l.usage_args(Some("WORKER..."));

    let a = args!(l);
    if a.args().is_empty() {
        bad_args!(l, "specify a worker to release");
    }

    for arg in a.args() {
        if let Err(e) =
            l.context().admin().worker_hold_release().worker(arg).send().await
        {
            bail!("ERROR: releasing hold on {arg}: {e:?}");
        }
    }

    Ok(())
}

async fn do_worker(mut l: Level<Stuff>) -> Result<()> {
    l.cmda("list", "ls", "list workers", cmd!(do_worker_list))?;
    l.cmd("recycle", "recycle a worker", cmd!(do_worker_recycle))?;
    l.cmd("hold", "mark a worker as held", cmd!(do_worker_hold))?;
    l.cmd("release", "release a held worker", cmd!(do_worker_release))?;

    sel!(l).run().await
}

async fn do_check(mut l: Level<Stuff>) -> Result<()> {
    l.optflag("v", "verbose", "print details about profile");

    let a = no_args!(l);
    let s = l.context();

    let verbose = a.opts().opt_present("v");

    if let Some(p) = &s.profile {
        if verbose {
            println!("profile:");
            if let Some(name) = p.name.as_deref() {
                println!("    named {:?}, loaded from file", name);
            } else {
                println!("    from environment");
            }
            println!("    url: {:?}", p.url);
        }
    } else {
        bail!("no profile");
    }

    Ok(())
}

async fn do_factory_create(mut l: Level<Stuff>) -> Result<()> {
    l.usage_args(Some("NAME"));

    let a = args!(l);

    if a.args().len() != 1 {
        bad_args!(l, "specify name of factory");
    }
    let name = a.args()[0].to_string();

    let res = l
        .context()
        .admin()
        .factory_create()
        .body_map(|body| body.name(name))
        .send()
        .await?;

    println!("{}", res.token);
    Ok(())
}

async fn do_factory_enable(mut l: Level<Stuff>) -> Result<()> {
    l.usage_args(Some("FACTORY_ID|NAME"));
    l.optflag("h", "", "mark new workers created by this factory as held");

    let a = args!(l);
    let hold_workers = a.opts().opt_present("h");

    if a.args().len() != 1 {
        bad_args!(l, "specify name or ID of factory");
    }
    let id = l.context().factory_to_id(&a.args()[0]).await?;

    l.context()
        .admin()
        .factory_enable()
        .factory(&id)
        .body_map(|b| b.hold_workers(hold_workers))
        .send()
        .await?;
    Ok(())
}

async fn do_factory_disable(mut l: Level<Stuff>) -> Result<()> {
    l.usage_args(Some("FACTORY_ID|NAME"));

    let a = args!(l);

    if a.args().len() != 1 {
        bad_args!(l, "specify name or ID of factory");
    }
    let id = l.context().factory_to_id(&a.args()[0]).await?;

    l.context().admin().factory_disable().factory(&id).send().await?;
    Ok(())
}

async fn do_factory_drain(mut l: Level<Stuff>) -> Result<()> {
    l.usage_args(Some("FACTORY_ID|NAME"));

    let a = args!(l);

    if a.args().len() != 1 {
        bad_args!(l, "specify name or ID of factory");
    }
    let id = l.context().factory_to_id(&a.args()[0]).await?;

    println!(" * disabling factory {id}...");
    l.context().admin().factory_disable().factory(&id).send().await?;

    let mut last_count = None;
    loop {
        /*
         * Determine the current count of workers outstanding for this factory.
         */
        let workers = l
            .context()
            .admin()
            .workers_list()
            .active(true)
            .factory(&id)
            .stream()
            .try_collect::<Vec<_>>()
            .await?;
        let new_count = workers.len();

        if new_count == 0 {
            break;
        }

        if last_count != Some(new_count) {
            println!(" * waiting for {new_count} workers to complete...");
            last_count = Some(new_count);
        }

        sleep_ms(3000).await;
    }

    println!("factory {id} is disabled and has no active workers");
    Ok(())
}

async fn do_factory_list(mut l: Level<Stuff>) -> Result<()> {
    l.add_column("id", WIDTH_ID, true);
    l.add_column("name", 32, true);
    l.add_column("flags", 5, true);
    l.add_column("ping", 4, true);
    l.add_column("last_ping", WIDTH_ISODATE, false);

    let a = no_args!(l);

    let mut t = a.table();

    for f in l.context().admin().factories_list().send().await?.into_inner() {
        let mut r = Row::default();
        r.add_str("id", &f.id);
        r.add_str("name", &f.name);
        r.add_str(
            "last_ping",
            f.last_ping
                .map(|d| d.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
                .as_deref()
                .unwrap_or("-"),
        );
        r.add_str(
            "ping",
            f.last_ping
                .map(|d| d.age().as_secs().to_string())
                .unwrap_or_else(|| "-".to_string()),
        );
        r.add_flags("flags")
            .flag('E', f.enable)
            .flag('H', f.hold_workers)
            .build();
        t.add_row(r);
    }

    print!("{}", t.output()?);
    Ok(())
}

async fn do_factory(mut l: Level<Stuff>) -> Result<()> {
    l.cmda("list", "ls", "list factories", cmd!(do_factory_list))?;
    l.cmd("create", "create a factory", cmd!(do_factory_create))?;
    l.cmd("enable", "enable a factory", cmd!(do_factory_enable))?;
    l.cmd("disable", "disable a factory", cmd!(do_factory_disable))?;
    l.cmd("drain", "disable and drain a factory", cmd!(do_factory_drain))?;

    sel!(l).run().await
}

async fn do_target_create(mut l: Level<Stuff>) -> Result<()> {
    l.usage_args(Some("NAME"));
    l.reqopt("d", "desc", "target description", "DESC");

    let a = args!(l);

    if a.args().len() != 1 {
        bad_args!(l, "specify name of target");
    }
    let name = a.args()[0].to_string();
    let desc = a.opts().opt_str("d").unwrap();

    let res = l
        .context()
        .admin()
        .target_create()
        .body_map(|body| body.name(name).desc(desc))
        .send()
        .await?;

    println!("{}", res.id);
    Ok(())
}

async fn do_target_rename(mut l: Level<Stuff>) -> Result<()> {
    l.usage_args(Some("TARGET_ID|NAME NAME"));
    l.reqopt("d", "desc", "signpost target description", "DESC");

    let a = args!(l);

    if a.args().len() != 2 {
        bad_args!(
            l,
            "specify name or ID of existing target \
            and new name of target",
        );
    }
    let id = l.context().target_to_id(&a.args()[0]).await?;
    let new_name = a.args()[1].to_string();
    let signpost_description = a.opts().opt_str("d").unwrap();

    let res = l
        .context()
        .admin()
        .target_rename()
        .target(&id)
        .body_map(|body| {
            body.new_name(new_name).signpost_description(signpost_description)
        })
        .send()
        .await?;

    println!("{}", res.id);
    Ok(())
}

async fn do_target_list(mut l: Level<Stuff>) -> Result<()> {
    l.add_column("id", WIDTH_ID, true);
    l.add_column("name", 20, true);
    l.add_column("description", 38, false);
    l.add_column("redirect_id", WIDTH_ID, false);
    l.add_column("redirect", 20, true);
    l.add_column("privilege", 14, true);

    let a = no_args!(l);

    let mut t = a.table();

    let targs = l.context().admin().targets_list().send().await?.into_inner();

    for targ in targs.iter() {
        let mut r = Row::default();
        r.add_str("id", &targ.id);
        r.add_str("name", &targ.name);
        r.add_str("description", &targ.desc);
        if let Some(redir) = targ.redirect.as_deref() {
            r.add_str("redirect_id", redir);
            if let Some(rt) = targs.iter().find(|t| t.id == redir) {
                r.add_str("redirect", &rt.name);
            } else {
                /*
                 * This should not happen.  It implies a target has an invalid
                 * redirect to a target that does not exist.
                 */
                r.add_str("redirect", "?MISSING");
            }
        } else {
            r.add_str("redirect_id", "-");
            r.add_str("redirect", "-");
        }
        r.add_str("privilege", targ.privilege.as_deref().unwrap_or("-"));
        t.add_row(r);
    }

    print!("{}", t.output()?);
    Ok(())
}

async fn do_target_restrict(mut l: Level<Stuff>) -> Result<()> {
    l.usage_args(Some("TARGET_ID|NAME PRIVILEGE"));

    let a = args!(l);

    if a.args().len() != 2 {
        bad_args!(l, "specify name or ID of target and name of privilege");
    }
    let id = l.context().target_to_id(&a.args()[0]).await?;
    let privilege = a.args()[1].to_string();

    l.context()
        .admin()
        .target_require_privilege()
        .target(&id)
        .privilege(&privilege)
        .send()
        .await?;
    Ok(())
}

async fn do_target_unrestrict(mut l: Level<Stuff>) -> Result<()> {
    l.usage_args(Some("TARGET_ID|NAME"));

    let a = args!(l);

    if a.args().len() != 1 {
        bad_args!(l, "specify name or ID of target");
    }
    let id = l.context().target_to_id(&a.args()[0]).await?;

    l.context()
        .admin()
        .target_require_no_privilege()
        .target(&id)
        .send()
        .await?;
    Ok(())
}

async fn do_target_redirect(mut l: Level<Stuff>) -> Result<()> {
    l.usage_args(Some("TARGET_ID|NAME REDIRECT_TO_TARGET_ID|NAME"));

    let a = args!(l);

    if a.args().len() != 2 {
        bad_args!(
            l,
            "specify name or ID of target to redirect, \
            and name or ID of target to which it should be redirected",
        );
    }
    let id = l.context().target_to_id(&a.args()[0]).await?;
    let redirect = l.context().target_to_id(&a.args()[1]).await?;

    l.context()
        .admin()
        .target_redirect()
        .target(&id)
        .body_map(|body| body.redirect(redirect))
        .send()
        .await?;
    Ok(())
}

async fn do_target_unredirect(mut l: Level<Stuff>) -> Result<()> {
    l.usage_args(Some("TARGET_ID|NAME"));

    let a = args!(l);

    if a.args().len() != 1 {
        bad_args!(
            l,
            "specify name or ID of target \
            for which redirection should be disabled",
        );
    }
    let id = l.context().target_to_id(&a.args()[0].to_string()).await?;

    /*
     * By omitting the body altogether, we clear the redirect property on the
     * server.
     */
    l.context().admin().target_redirect().target(id).send().await?;
    Ok(())
}

async fn do_target(mut l: Level<Stuff>) -> Result<()> {
    l.cmda("list", "ls", "list targets", cmd!(do_target_list))?;
    l.cmd("create", "create a target", cmd!(do_target_create))?;
    l.cmd(
        "restrict",
        "require a privilege to use this target",
        cmd!(do_target_restrict),
    )?;
    l.cmd(
        "unrestrict",
        "require no privileges to use this target",
        cmd!(do_target_unrestrict),
    )?;
    l.cmd(
        "redirect",
        "redirect a target to another target",
        cmd!(do_target_redirect),
    )?;
    l.cmd(
        "unredirect",
        "disable redirection for a target",
        cmd!(do_target_unredirect),
    )?;
    l.cmd(
        "rename",
        "rename a target and leave a redirecting target in its place",
        cmd!(do_target_rename),
    )?;

    sel!(l).run().await
}

struct Stopwatch {
    enable: bool,
    last: Instant,
}

impl Stopwatch {
    fn start(enable: bool) -> Stopwatch {
        Stopwatch { enable, last: Instant::now() }
    }

    fn lap(&mut self, n: &str) {
        if !self.enable {
            return;
        }

        let now = Instant::now();
        let delta = now.checked_duration_since(self.last).unwrap();
        self.last = now;
        eprintln!("WATCH: {} ({} s)", n, delta.as_secs_f64());
    }
}

async fn do_dash(mut l: Level<Stuff>) -> Result<()> {
    l.optflag("v", "", "debugging output");
    l.optopt("r", "", "number of recently completed jobs to display", "COUNT");

    let a = no_args!(l);
    let nrc = if let Some(arg) = a.opts().opt_str("r") {
        let nrc = arg.parse::<u64>()?;
        if nrc == 0 {
            None
        } else {
            Some(nrc)
        }
    } else {
        Some(10)
    };

    let s = l.context();

    let mut w = Stopwatch::start(a.opts().opt_present("v"));

    let users = s
        .admin()
        .users_list()
        .send()
        .await?
        .into_inner()
        .into_iter()
        .map(|u| (u.id, u.name))
        .collect::<HashMap<String, String>>();
    w.lap("users_list");

    let targets = s
        .admin()
        .targets_list()
        .send()
        .await?
        .into_inner()
        .into_iter()
        .map(|t| (t.id, t.name))
        .collect::<HashMap<String, String>>();
    w.lap("targets_list");

    /*
     * Load active jobs:
     */
    let jobs =
        s.admin().admin_jobs_get().active(true).send().await?.into_inner();
    w.lap("admin_jobs_get active");

    /*
     * Load some of recently completed jobs:
     */
    let oldjobs = if let Some(nrc) = nrc {
        s.admin().admin_jobs_get().completed(nrc).send().await?.into_inner()
    } else {
        Default::default()
    };
    w.lap("admin_jobs_get completed");

    /*
     * Load active workers:
     */
    let workers = s
        .admin()
        .workers_list()
        .active(true)
        .stream()
        .try_collect::<Vec<_>>()
        .await?;
    w.lap("workers_list");

    fn github_url(tags: &HashMap<String, String>) -> Option<String> {
        let owner = tags.get("gong.repo.owner")?;
        let name = tags.get("gong.repo.name")?;
        let checkrun = tags.get("gong.run.github_id")?;

        Some(format!("https://github.com/{}/{}/runs/{}", owner, name, checkrun))
    }

    fn github_info(tags: &HashMap<String, String>) -> Option<String> {
        let owner = tags.get("gong.repo.owner")?;
        let name = tags.get("gong.repo.name")?;
        let title = tags.get("gong.name")?;

        let mut out = format!("{}/{}", owner, name);
        if let Some(branch) = tags.get("gong.head.branch") {
            out.push_str(&format!(" ({})", branch));
        }
        out.push_str(&format!(": {}", title));

        Some(out)
    }

    fn dump_info(job: &Job) {
        let tags = &job.tags;

        if let Some(info) = github_info(tags) {
            println!("    {}", info);
        }
        if let Some(sha) = tags.get("gong.head.sha") {
            println!("    commit: {}", sha);
        }
        if let Some(url) = github_url(tags) {
            println!("    url: {}", url);
        }
        if job.target == job.target_real {
            println!("    target: {}", job.target);
        } else {
            println!("    target: {} -> {}", job.target, job.target_real);
        }
        if let Some(t) = job.times.get("complete") {
            println!(
                "    completed at: {} ({} ago)",
                t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                t.age().render(),
            );
        } else if let Some(t) = job.times.get("submit") {
            println!(
                "    submitted at: {} ({} ago)",
                t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                t.age().render(),
            );
        } else if let Ok(id) = job.id() {
            let t = id.creation();
            println!(
                "    submitted at: {} ({} ago)",
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
            println!("    times: {}", times.join(", "));
        }
    }

    /*
     * Display each worker, and its associated job if there is one:
     */
    let mut seen = HashSet::new();
    for w in workers.iter() {
        if w.deleted {
            continue;
        }

        println!(
            "== worker {} ({}, {})\n    created {} ({}s ago)",
            w.id,
            targets.get(&w.target).map(|s| s.as_str()).unwrap_or("?"),
            w.factory_private.as_deref().unwrap_or("?"),
            w.id()?.creation(),
            w.id()?.age().as_secs(),
        );
        for job in w.jobs.iter() {
            seen.insert(job.id.to_string());

            let owner = users.get(&job.owner).unwrap_or(&job.owner);
            println!("    job: {} (user {})", job.id, owner);

            if let Some(job) = jobs.iter().find(|j| j.id == job.id) {
                dump_info(job);
            }
        }
        println!();
    }
    w.lap("worker display");

    /*
     * Display all active jobs that have not been displayed already, and which
     * are not complete.
     */
    for state in [Some("queued"), Some("waiting"), None] {
        for job in jobs.iter() {
            if seen.contains(&job.id) {
                continue;
            }

            let display = if job.state == "completed" || job.state == "failed" {
                /*
                 * Completed jobs will be displayed later.
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

            let owner = users.get(&job.owner).unwrap_or(&job.owner);

            println!("~~ {} job {} (user {})", job.state, job.id, owner);
            dump_info(job);
            println!();
        }
    }
    w.lap("job display");

    /*
     * Display recently completed jobs:
     */
    for job in oldjobs.iter() {
        if seen.contains(&job.id) {
            continue;
        }

        let owner = users.get(&job.owner).unwrap_or(&job.owner);

        println!("~~ recent completed job {} (user {})", job.id, owner);
        dump_info(job);
        println!();
    }
    w.lap("completed job display");

    Ok(())
}

async fn do_admin_job_dump(mut l: Level<Stuff>) -> Result<()> {
    l.usage_args(Some("JOB..."));

    let a = args!(l);

    let c = l.context().admin();

    for id in a.args() {
        let job = c.admin_job_get().job(id).send().await?.into_inner();

        println!("{:<26} {:<15} {}", job.id, job.state, job.name);
        println!("{:#?}", job);
        println!();
    }

    Ok(())
}

async fn do_admin_job_archive(mut l: Level<Stuff>) -> Result<()> {
    l.usage_args(Some("JOB..."));

    let a = args!(l);
    if a.args().is_empty() {
        bad_args!(l, "specify a job to archive");
    }

    for arg in a.args() {
        if let Err(e) = l
            .context()
            .admin()
            .admin_job_archive_request()
            .job(arg)
            .send()
            .await
        {
            bail!("ERROR: archiving {}: {:?}", arg, e);
        }
    }

    Ok(())
}

async fn do_admin_job_list(mut l: Level<Stuff>) -> Result<()> {
    l.add_column("id", WIDTH_ID, true);
    l.add_column("age", 8, true);
    l.add_column("s", 1, true);
    l.add_column("name", 32, true);
    l.add_column("state", 15, false);
    l.add_column("user", 30, true);
    l.add_column("target", 20, true);
    l.add_column("creation", WIDTH_ISODATE, false);

    l.optmulti("T", "", "job tag filter", "TAG=VALUE");
    l.optopt("F", "", "job state filter", "STATE");
    l.optflag("U", "", "do not sort locally; just stream results from server");

    let a = no_args!(l);
    let ftags = a
        .opts()
        .opt_strs("T")
        .iter()
        .map(|a| {
            a.split_once('=')
                .ok_or_else(|| anyhow!("invalid tag filter"))
                .map(|(k, v)| (k.to_string(), v.to_string()))
        })
        .collect::<Result<Vec<_>>>()?;

    let fstate = a.opts().opt_str("F");
    let unsorted = a.opts().opt_present("U");

    let mut t = a.table();

    if unsorted {
        print!("{}", t.output_unsorted_header()?);
    }

    /*
     * Get a list of targets first so that we can print resolved target names in
     * a column.
     */
    let targs = l.context().admin().targets_list().send().await?.into_inner();
    let users = l.context().admin().users_list().send().await?.into_inner();

    let mut jobs = l.context().admin().admin_jobs_list();
    if !ftags.is_empty() {
        jobs = jobs.tag(
            ftags
                .into_iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join("|"),
        );
    }
    let mut jobs = jobs.recent_first(unsorted).stream();

    while let Some(job) = jobs.try_next().await? {
        if let Some(s) = &fstate {
            if s != &job.state {
                continue;
            }
        }

        let id = job.id()?;

        let mut r = Row::default();
        r.add_str("id", &job.id);
        r.add_str("name", &job.name);
        r.add_str(
            "creation",
            id.creation().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        );
        r.add_age("age", id.age());
        if job.state == "failed" && job.cancelled {
            r.add_str("s", "X");
            r.add_str("state", "cancelled");
        } else {
            r.add_str(
                "s",
                match job.state.as_str() {
                    /*
                     * Terminal states in upper case:
                     */
                    "failed" => "F",
                    "completed" => "C",
                    /*
                     * Non-terminal states in lower case:
                     */
                    "running" => "r",
                    "waiting" => "w",
                    "queued" => "q",

                    _ => "?",
                },
            );
            r.add_str("state", &job.state);
        }
        if let Some(t) = targs.iter().find(|t| t.id == job.target_id) {
            r.add_str("target", &t.name);
        } else {
            /*
             * This should not happen.  It implies a target was used for a job
             * in the past, but is now missing from the system.
             */
            r.add_str("target", "?MISSING");
        }
        if let Some(u) = users.iter().find(|u| u.id == job.owner) {
            r.add_str("user", &u.name);
        } else {
            /*
             * This should not happen.  It implies a user ran this job, but is
             * now missing.
             */
            r.add_str("user", "?MISSING");
        }

        if unsorted {
            print!("{}", t.output_unsorted(r)?);
        } else {
            t.add_row(r);
        }
    }

    if !unsorted {
        print!("{}", t.output()?);
    }
    Ok(())
}
async fn do_admin_job(mut l: Level<Stuff>) -> Result<()> {
    l.cmda("list", "ls", "list jobs", cmd!(do_admin_job_list))?;
    l.cmd("archive", "request archive of a job", cmd!(do_admin_job_archive))?;
    l.cmd("dump", "dump information about jobs", cmd!(do_admin_job_dump))?;

    sel!(l).run().await
}

async fn do_admin(mut l: Level<Stuff>) -> Result<()> {
    l.cmd("user", "user management", cmd!(do_user))?;
    l.cmd("factory", "factory management", cmd!(do_factory))?;
    l.cmd("target", "target management", cmd!(do_target))?;
    l.cmda("dashboard", "dash", "summarise system state", cmd!(do_dash))?;
    l.cmd("control", "server control functions", cmd!(do_control))?;
    l.cmd("worker", "worker management", cmd!(do_worker))?;
    l.cmd("job", "job management", cmd!(do_admin_job))?;

    sel!(l).run().await
}

#[tokio::main]
async fn main() -> Result<()> {
    sigpipe::reset();

    let mut l = Level::new("buildomat", Stuff::default());
    l.optopt("p", "profile", "authentication and server profile", "PROFILE");
    l.optopt("D", "delegate", "act as another user account", "USERNAME");

    l.hcmd("check", "confirm profile is valid", cmd!(do_check))?;
    l.cmd(
        "info",
        "get information about server and user account",
        cmd!(do_info),
    )?;
    l.cmd("job", "job management", cmd!(do_job))?;
    l.cmda("admin", "a", "administrative functions", cmd!(do_admin))?;
    l.hcmd("control", "server control functions", cmd!(do_control))?;
    l.hcmd("worker", "worker management", cmd!(do_worker))?;

    let a = args!(l);

    let profile = config::load(a.opts().opt_str("p").as_deref())?;

    if let Some(admin_token) = profile.admin_token.as_deref() {
        l.context_mut().client_admin = Some(
            ClientBuilder::new(&profile.url)
                .bearer_token(admin_token)
                .build()?,
        );
    };

    l.context_mut().client_user = {
        let mut cb = ClientBuilder::new(&profile.url);
        cb.bearer_token(profile.secret.as_str());
        if let Some(delegate) = a.opts().opt_str("D") {
            cb.delegated_user(&delegate);
        }
        Some(cb.build()?)
    };

    l.context_mut().profile = Some(profile);

    sel!(l).run().await
}
