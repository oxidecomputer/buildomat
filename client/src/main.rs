/*
 * Copyright 2021 Oxide Computer Company
 */

#![allow(unused_imports)]
#![allow(clippy::many_single_char_names)]

use std::collections::HashMap;
use std::env::{args, var};
use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io::{ErrorKind, Read, Write};
use std::path::PathBuf;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};
use buildomat_openapi::{types::*, Client};
use chrono::prelude::*;
use hiercmd::prelude::*;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use reqwest::ClientBuilder;
use rusty_ulid::Ulid;

const WIDTH_ISODATE: usize = 20;

mod config;

fn to_ulid(id: &str) -> Result<Ulid> {
    Ok(Ulid::from_str(id)?)
}

async fn sleep_ms(ms: u64) {
    tokio::time::sleep(Duration::from_millis(ms)).await;
    // std::thread::sleep(std::time::Duration::from_millis(ms));
}

#[derive(Default)]
struct Stuff {
    client_user: Option<Client>,
    client_admin: Option<Client>,
    //profile: Option<config::Profile>,
}

impl Stuff {
    fn user(&self) -> &buildomat_openapi::Client {
        self.client_user.as_ref().unwrap()
    }

    fn admin(&self) -> Result<&buildomat_openapi::Client> {
        self.client_admin.as_ref().ok_or_else(|| anyhow!("need admin token"))
    }
}

async fn do_job_tail(mut l: Level<Stuff>) -> Result<()> {
    l.usage_args(Some("JOB"));

    let a = args!(l);

    if a.args().len() != 1 {
        bad_args!(l, "specify a job");
    }

    poll_job(&l, &a.args()[0]).await
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

    l.mutually_exclusive(&[("c", "script"), ("C", "script-file")]);

    let a = no_args!(l);

    let name = a.opts().opt_str("name").unwrap();
    let target = a.opts().opt_str("target").unwrap_or_else(|| "default".into());
    let script = if let Some(script) = a.opts().opt_str("script") {
        script
    } else if let Some(path) = a.opts().opt_str("script-file") {
        let mut f = std::fs::File::open(&path)?;
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

    /*
     * Create the job on the server.
     */
    let t = TaskSubmit {
        name: "default".to_string(),
        script,
        env_clear,
        env,
        gid: None,
        uid: None,
        workdir: None,
    };
    let j = JobSubmit { name, target, output_rules, tasks: vec![t] };
    let x = l.context().user().job_submit(&j).await?;
    println!("job {} submitted", x.id);

    if a.opts().opt_present("no-wait") {
        return Ok(());
    }

    poll_job(&l, &x.id).await
}

async fn poll_job(l: &Level<Stuff>, id: &str) -> Result<()> {
    println!("polling for job output...");

    let mut nextseq = 0;
    let mut exit_status = 0;
    let mut last_state = String::new();
    loop {
        let t = match l.context().user().job_get(id).await {
            Ok(t) => t,
            Err(_) => {
                sleep_ms(1000).await;
                continue;
            }
        };

        if t.state == "failed" {
            exit_status = 1;
        }

        if t.state != last_state {
            println!("STATE CHANGE: {} -> {}", last_state, t.state);
            last_state = t.state.to_string();
        }

        if let Ok(events) =
            l.context().user().job_events_get(id, Some(nextseq)).await
        {
            if events.is_empty() {
                if t.state == "completed" || t.state == "failed" {
                    /*
                     * EOF.
                     */
                    break;
                }
            } else {
                for e in events.iter() {
                    if e.stream == "stdout" || e.stream == "stderr" {
                        println!("{}", e.payload);
                    } else {
                        println!("{:?}", e);
                    }
                    nextseq = e.seq + 1;
                }
            }
        }

        sleep_ms(1000).await;
    }

    if exit_status != 0 {
        std::process::exit(exit_status);
    } else {
        Ok(())
    }
}

async fn do_info(mut l: Level<Stuff>) -> Result<()> {
    no_args!(l);
    let whoami = l.context().user().whoami().await?;
    println!("{:#?}", whoami);
    Ok(())
}

async fn do_control_resume(mut l: Level<Stuff>) -> Result<()> {
    no_args!(l);
    println!("{:?}", l.context().admin()?.control_resume().await?);
    Ok(())
}

async fn do_control_hold(mut l: Level<Stuff>) -> Result<()> {
    no_args!(l);
    println!("{:?}", l.context().admin()?.control_hold().await?);
    Ok(())
}

async fn do_control_recycle(mut l: Level<Stuff>) -> Result<()> {
    no_args!(l);
    println!("{:?}", l.context().admin()?.workers_recycle().await?);
    Ok(())
}

async fn do_control(mut l: Level<Stuff>) -> Result<()> {
    l.cmd("hold", "hold new VM creation", cmd!(do_control_hold))?;
    l.cmd("resume", "resume new VM creation", cmd!(do_control_resume))?;
    l.cmd("recycle", "recycle all workers", cmd!(do_control_recycle))?;

    sel!(l).run().await
}

async fn do_job_outputs(mut l: Level<Stuff>) -> Result<()> {
    l.add_column("path", 68, true);
    l.add_column("size", 10, true);
    l.add_column("id", 26, false);

    l.usage_args(Some("JOB"));

    let a = args!(l);
    let mut t = a.table();

    if a.args().len() != 1 {
        bad_args!(l, "specify job ID");
    }

    for i in l.context().user().job_outputs_get(a.args()[0].as_str()).await? {
        let mut r = Row::default();
        r.add_str("id", &i.id);
        r.add_str("path", &i.path);
        r.add_bytes("size", i.size as u64);
        t.add_row(r);
    }

    print!("{}", t.output()?);
    Ok(())
}

async fn do_job_list(mut l: Level<Stuff>) -> Result<()> {
    l.add_column("id", 26, true);
    l.add_column("name", 32, true);
    l.add_column("state", 15, true);

    let a = no_args!(l);

    let mut t = a.table();

    for job in l.context().user().jobs_get().await? {
        let mut r = Row::default();
        r.add_str("id", &job.id);
        r.add_str("name", &job.name);
        r.add_str("state", &job.state);
        t.add_row(r);
    }

    print!("{}", t.output()?);
    Ok(())
}

async fn do_job_dump(mut l: Level<Stuff>) -> Result<()> {
    no_args!(l);

    let c = l.context().user();

    for job in c.jobs_get().await? {
        println!("{:<26} {:<15} {}", job.id, job.state, job.name);
        println!("{:#?}", job);
        println!();
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
    for o in c.job_outputs_get(job).await? {
        if o.path == src {
            println!("downloading {} -> {} ({}KB)", o.path, dst, o.size / 1024);
            let mut res = c.job_output_download(job, &o.id).await?;

            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&dst)?;

            while let Some(ch) = res.chunk().await? {
                f.write_all(&ch)?;
            }
            f.flush()?;

            return Ok(());
        }
    }

    bail!("job {} does not have a file that matches {}", job, src);
}

async fn do_job(mut l: Level<Stuff>) -> Result<()> {
    l.cmda("list", "ls", "list jobs", cmd!(do_job_list))?;
    l.cmd("run", "run a job", cmd!(do_job_run))?;
    l.cmd("tail", "listen for events from a job", cmd!(do_job_tail))?;
    l.cmd("outputs", "list job outputs", cmd!(do_job_outputs))?;
    l.cmd("dump", "dump information about jobs", cmd!(do_job_dump))?;
    l.cmda(
        "copy",
        "cp",
        "copy from job outputs to local files",
        cmd!(do_job_copy),
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

    let res = l.context().admin()?.user_create(&UserCreate { name }).await?;

    println!("{}", res.token);
    Ok(())
}

async fn do_user_list(mut l: Level<Stuff>) -> Result<()> {
    l.add_column("id", 26, true);
    l.add_column("name", 30, true);
    l.add_column("creation", WIDTH_ISODATE, true);

    let a = no_args!(l);

    let mut t = a.table();

    for u in l.context().admin()?.users_list().await? {
        let mut r = Row::default();
        r.add_str("id", &u.id);
        r.add_str("name", &u.name);
        r.add_str(
            "creation",
            &u.time_create.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        );
        t.add_row(r);
    }

    print!("{}", t.output()?);
    Ok(())
}

async fn do_user(mut l: Level<Stuff>) -> Result<()> {
    l.cmda("list", "ls", "list users", cmd!(do_user_list))?;
    l.cmd("create", "create a user", cmd!(do_user_create))?;

    sel!(l).run().await
}

async fn do_worker_list(mut l: Level<Stuff>) -> Result<()> {
    l.add_column("id", 26, true);
    l.add_column("flags", 5, true);
    l.add_column("creation", WIDTH_ISODATE, true);
    l.add_column("age", 8, true);

    l.optflag("A", "active", "display only workers not yet destroyed");

    let a = no_args!(l);
    let active = a.opts().opt_present("active");

    let mut t = a.table();

    for w in l.context().admin()?.workers_list().await?.workers {
        if active && w.deleted {
            continue;
        }

        let id = to_ulid(&w.id)?;
        let creation = Utc.timestamp_millis(id.timestamp() as i64);

        let when = std::time::UNIX_EPOCH
            .checked_add(Duration::from_millis(id.timestamp()))
            .unwrap();
        let age = std::time::SystemTime::now().duration_since(when).unwrap();

        let flags = format!(
            "{}{}",
            if w.recycle { "R" } else { "-" },
            if w.deleted { "D" } else { "-" },
        );

        let mut r = Row::default();
        r.add_str("id", &w.id);
        r.add_str(
            "creation",
            creation.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        );
        r.add_age("age", age);
        r.add_str("flags", flags);
        t.add_row(r);
    }

    print!("{}", t.output()?);
    Ok(())
}

async fn do_worker(mut l: Level<Stuff>) -> Result<()> {
    l.cmda("list", "ls", "list workers", cmd!(do_worker_list))?;

    sel!(l).run().await
}

fn bearer_client(token: &str) -> Result<reqwest::Client> {
    let mut dh = HeaderMap::new();
    dh.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {}", token)).unwrap(),
    );

    Ok(ClientBuilder::new()
        .timeout(Duration::from_secs(15))
        .connect_timeout(Duration::from_secs(15))
        .default_headers(dh)
        .build()?)
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut l = Level::new("buildomat", Stuff::default());
    l.optopt("p", "profile", "authentication and server profile", "PROFILE");

    l.cmd(
        "info",
        "get information about server and user account",
        cmd!(do_info),
    )?;
    l.cmd("control", "server control functions", cmd!(do_control))?;
    l.cmd("job", "job management", cmd!(do_job))?;
    l.cmd("user", "user management", cmd!(do_user))?;
    l.cmd("worker", "worker management", cmd!(do_worker))?;

    let a = args!(l);

    let profile = config::load(a.opts().opt_str("p").as_deref())?;

    if let Some(admin_token) = profile.admin_token.as_deref() {
        l.context_mut().client_admin = Some(Client::new_with_client(
            &profile.url,
            bearer_client(admin_token)?,
        ));
    };

    l.context_mut().client_user = Some(Client::new_with_client(
        &profile.url,
        bearer_client(profile.secret.as_str())?,
    ));

    sel!(l).run().await
}
