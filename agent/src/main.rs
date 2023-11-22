/*
 * Copyright 2023 Oxide Computer Company
 */

#![allow(clippy::many_single_char_names)]

use std::collections::VecDeque;
use std::env;
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind::NotFound, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use anyhow::{bail, Result};
use chrono::prelude::*;
use futures::StreamExt;
use hiercmd::prelude::*;
use rusty_ulid::Ulid;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;

use buildomat_client::types::*;
use buildomat_common::*;
use buildomat_types::*;

mod control;
mod download;
mod exec;
#[cfg(target_os = "illumos")]
mod uadmin;
mod upload;

use control::protocol::Payload;
use exec::ExitDetails;

const CONFIG_PATH: &str = "/opt/buildomat/etc/agent.json";
const AGENT: &str = "/opt/buildomat/lib/agent";
const INPUT_PATH: &str = "/input";
const CONTROL_PROGRAM: &str = "bmat";
#[cfg(target_os = "illumos")]
mod os_constants {
    pub const METHOD: &str = "/opt/buildomat/lib/start.sh";
    pub const MANIFEST: &str = "/var/svc/manifest/site/buildomat-agent.xml";
    pub const INPUT_DATASET: &str = "rpool/input";
}
#[cfg(target_os = "linux")]
mod os_constants {
    pub const UNIT: &str = "/etc/systemd/system/buildomat-agent.service";
}
use os_constants::*;

use crate::control::protocol::StoreEntry;

#[derive(Serialize, Deserialize)]
struct ConfigFile {
    baseurl: String,
    bootstrap: String,
    token: String,
}

impl ConfigFile {
    fn make_client(&self) -> ClientWrap {
        ClientWrap {
            client: buildomat_client::ClientBuilder::new(&self.baseurl)
                .bearer_token(&self.token)
                .build()
                .expect("new client"),
            job: None,
        }
    }
}

struct OutputRecord {
    stream: String,
    time: DateTime<Utc>,
    msg: String,
}

impl OutputRecord {
    fn new(stream: &str, msg: &str) -> OutputRecord {
        OutputRecord {
            stream: stream.to_string(),
            time: Utc::now(),
            msg: msg.to_string(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct ClientWrap {
    client: buildomat_client::Client,
    job: Option<WorkerPingJob>,
}

impl ClientWrap {
    async fn append(&self, rec: &OutputRecord) {
        let job = self.job.as_ref().unwrap();

        loop {
            match self
                .client
                .worker_job_append()
                .job(&job.id)
                .body_map(|body| {
                    body.stream(&rec.stream).time(rec.time).payload(&rec.msg)
                })
                .send()
                .await
            {
                Ok(_) => return,
                Err(e) => {
                    println!("ERROR: append: {:?}", e);
                    sleep_ms(1000).await;
                }
            }
        }
    }

    async fn append_msg(&self, msg: &str) {
        self.append(&OutputRecord::new("worker", msg)).await;
    }

    async fn append_task(&self, task: &WorkerPingTask, rec: &OutputRecord) {
        let job = self.job.as_ref().unwrap();

        loop {
            match self
                .client
                .worker_task_append()
                .job(&job.id)
                .task(task.id)
                .body_map(|body| {
                    body.stream(&rec.stream).time(rec.time).payload(&rec.msg)
                })
                .send()
                .await
            {
                Ok(_) => return,
                Err(e) => {
                    println!("ERROR: append: {:?}", e);
                    sleep_ms(1000).await;
                }
            }
        }
    }

    async fn append_task_msg(&self, task: &WorkerPingTask, msg: &str) {
        self.append_task(task, &OutputRecord::new("task", msg)).await;
    }

    async fn task_complete(&self, task: &WorkerPingTask, failed: bool) {
        let job = self.job.as_ref().unwrap();

        loop {
            match self
                .client
                .worker_task_complete()
                .job(&job.id)
                .task(task.id)
                .body_map(|body| body.failed(failed))
                .send()
                .await
            {
                Ok(_) => return,
                Err(e) => {
                    println!("ERROR: complete: {:?}", e);
                    sleep_ms(1000).await;
                }
            }
        }
    }

    async fn job_complete(&self, failed: bool) {
        let job = self.job.as_ref().unwrap();

        loop {
            match self
                .client
                .worker_job_complete()
                .job(&job.id)
                .body_map(|body| body.failed(failed))
                .send()
                .await
            {
                Ok(_) => return,
                Err(e) => {
                    println!("ERROR: complete: {:?}", e);
                    sleep_ms(1000).await;
                }
            }
        }
    }

    async fn chunk(&self, buf: bytes::Bytes) -> String {
        let job = self.job.as_ref().unwrap();

        loop {
            match self
                .client
                .worker_job_upload_chunk()
                .job(&job.id)
                .body(buf.clone())
                .send()
                .await
            {
                Ok(uc) => {
                    return uc.into_inner().id;
                }
                Err(e) => {
                    println!("ERROR: chunk upload: {:?}", e);
                    sleep_ms(1000).await;
                }
            }
        }
    }

    async fn output(
        &self,
        path: &Path,
        size: u64,
        chunks: &[String],
    ) -> Option<String> {
        let job = self.job.as_ref().unwrap();
        let commit_id = Ulid::generate();
        let wao = WorkerAddOutput {
            chunks: chunks.to_vec(),
            path: path.to_str().unwrap().to_string(),
            size,
            commit_id: commit_id.to_string(),
        };

        loop {
            match self
                .client
                .worker_job_add_output()
                .job(&job.id)
                .body(&wao)
                .send()
                .await
            {
                Ok(waor) => {
                    if !waor.complete {
                        /*
                         * XXX For now, we poll on completion.  It would
                         * obviously be better to be notified, e.g., through
                         * long polling.
                         */
                        sleep_ms(1000).await;
                        continue;
                    }

                    if let Some(e) = &waor.error {
                        println!("ERROR: output file error: {:?}", e);
                        return Some(e.to_string());
                    } else {
                        return None;
                    }
                }
                Err(e) => {
                    println!("ERROR: add output: {:?}", e);
                    sleep_ms(1000).await;
                }
            }
        }
    }

    async fn input(&self, id: &str, path: &Path) {
        let job = self.job.as_ref().unwrap();

        'outer: loop {
            match self
                .client
                .worker_job_input_download()
                .job(&job.id)
                .input(id)
                .send()
                .await
            {
                Ok(res) => {
                    let mut body = res.into_inner();

                    let mut f = match tokio::fs::File::create(path).await {
                        Ok(f) => f,
                        Err(e) => {
                            println!("ERROR: input: {:?}", e);
                            sleep_ms(1000).await;
                            continue 'outer;
                        }
                    };

                    loop {
                        match body.next().await.transpose() {
                            Ok(None) => return,
                            Ok(Some(mut ch)) => {
                                if let Err(e) = f.write_all_buf(&mut ch).await {
                                    println!("ERROR: input: {:?}", e);
                                    sleep_ms(1000).await;
                                    continue 'outer;
                                }
                            }
                            Err(e) => {
                                println!("ERROR: input: {:?}", e);
                                sleep_ms(1000).await;
                                continue 'outer;
                            }
                        }
                    }
                }
                Err(e) => {
                    println!("ERROR: input: {:?}", e);
                    sleep_ms(1000).await;
                }
            }
        }
    }

    async fn quota(&self) -> WorkerJobQuota {
        let job = self.job.as_ref().unwrap();

        loop {
            match self.client.worker_job_quota().job(&job.id).send().await {
                Ok(wq) => {
                    return wq.into_inner();
                }
                Err(e) => {
                    println!("ERROR: asking for quota: {:?}", e);
                    sleep_ms(1000).await;
                }
            }
        }
    }
}

fn load<P, T>(p: P) -> Result<T>
where
    P: AsRef<Path>,
    for<'de> T: Deserialize<'de>,
{
    let p = p.as_ref();
    let f = File::open(p)?;
    Ok(serde_json::from_reader(f)?)
}

fn store<P, T>(p: P, t: &T) -> Result<()>
where
    P: AsRef<Path>,
    T: Serialize,
{
    let p = p.as_ref();
    let mut s = serde_json::to_string_pretty(t)?;
    s.push('\n');
    let mut f = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o640)
        .open(p)?;
    let b = s.as_bytes();
    f.write_all(b)?;
    f.flush()?;
    Ok(())
}

fn rmfile<P: AsRef<Path>>(p: P) -> Result<()> {
    let p = p.as_ref();
    if let Err(e) = std::fs::remove_file(p) {
        if e.kind() != NotFound {
            bail!("removing file {:?}: {:?}", p, e);
        }
    }
    Ok(())
}

fn write_text<P: AsRef<Path>>(p: P, text: &str) -> Result<()> {
    let p = p.as_ref();
    let mut f =
        OpenOptions::new().write(true).create(true).truncate(true).open(p)?;
    f.write_all(text.as_bytes())?;
    f.flush()?;
    Ok(())
}

fn make_executable<P: AsRef<Path>>(p: P) -> Result<()> {
    const MODE: u32 = 0o755;

    let p = p.as_ref();
    let md = std::fs::metadata(p)?;

    let mut perms = md.permissions();
    if perms.mode() != MODE {
        perms.set_mode(MODE);
        std::fs::set_permissions(p, perms)?;
    }

    Ok(())
}

fn make_dirs_for<P: AsRef<Path>>(p: P) -> Result<()> {
    let mut p = p.as_ref().to_path_buf();
    if p.pop() {
        /*
         * Create any missing parent directories if this path is not a bare
         * filename.  If not, do nothing.
         */
        std::fs::create_dir_all(p)?;
    }
    Ok(())
}

fn write_script(script: &str) -> Result<String> {
    let targ = format!("/tmp/{}.sh", genkey(16));
    let mut f = OpenOptions::new().create_new(true).write(true).open(&targ)?;
    let mut data = script.to_string();
    if !data.ends_with('\n') {
        data.push('\n');
    }
    f.write_all(data.as_bytes())?;
    f.flush()?;
    Ok(targ)
}

fn hard_reset() -> Result<()> {
    /*
     * For whatever reason, attempting to power off the AWS guest does not
     * result in a speedy termination.  Doing an immediate reset without even
     * bothering to sync file systems, however, seems to make destruction much
     * quicker!
     */
    #[cfg(target_os = "illumos")]
    {
        use anyhow::Context;

        uadmin::uadmin(uadmin::Action::Reboot(uadmin::Next::Boot))
            .context("hard_reset: uadmin")?;
    }

    Ok(())
}

enum Stage {
    Ready,
    Download(mpsc::Receiver<download::Activity>),
    NextTask,
    Child(mpsc::Receiver<exec::Activity>, WorkerPingTask, Option<bool>),
    Upload(mpsc::Receiver<upload::Activity>),
    Complete,
}

async fn cmd_install(mut l: Level<()>) -> Result<()> {
    l.usage_args(Some("BASEURL BOOTSTRAP_TOKEN"));

    let a = args!(l);

    if a.args().len() < 2 {
        bad_args!(l, "specify base URL and bootstrap token value");
    }

    /*
     * The server will have provided these parameters in the userscript
     * used to inject the agent into this build VM.
     */
    let baseurl = a.args()[0].to_string();
    let bootstrap = a.args()[1].to_string();

    /*
     * Write /opt/buildomat/etc/agent.json with this configuration.
     */
    make_dirs_for(CONFIG_PATH)?;
    rmfile(CONFIG_PATH)?;
    let cf = ConfigFile { baseurl, bootstrap, token: genkey(64) };
    store(CONFIG_PATH, &cf)?;

    /*
     * Copy the agent binary into a permanent home.
     */
    let exe = env::current_exe()?;
    make_dirs_for(AGENT)?;
    rmfile(AGENT)?;
    std::fs::copy(&exe, AGENT)?;
    make_executable(AGENT)?;

    /*
     * Install the agent binary with the control program name in a location in
     * the default PATH so that job programs can find it.
     */
    let cprog = format!("/usr/bin/{CONTROL_PROGRAM}");
    rmfile(&cprog)?;
    std::fs::copy(&exe, &cprog)?;
    make_executable(&cprog)?;

    #[cfg(target_os = "illumos")]
    {
        /*
         * Copy SMF method script and manifest into place.
         */
        let method = include_str!("../smf/start.sh");
        make_dirs_for(METHOD)?;
        rmfile(METHOD)?;
        write_text(METHOD, method)?;
        make_executable(METHOD)?;

        let manifest = include_str!("../smf/agent.xml");
        rmfile(MANIFEST)?;
        write_text(MANIFEST, manifest)?;

        /*
         * Create the input directory.
         */
        let status = Command::new("/sbin/zfs")
            .arg("create")
            .arg("-o")
            .arg(&format!("mountpoint={}", INPUT_PATH))
            .arg(INPUT_DATASET)
            .env_clear()
            .current_dir("/")
            .status();
        match status {
            Ok(o) if o.success() => (),
            Ok(o) => bail!("zfs create failure: {:?}", o),
            Err(e) => bail!("could not execute zfs create: {:?}", e),
        }

        /*
         * Import SMF service.
         */
        let status = Command::new("/usr/sbin/svccfg")
            .arg("import")
            .arg(MANIFEST)
            .env_clear()
            .current_dir("/")
            .status();
        match status {
            Ok(o) if o.success() => (),
            Ok(o) => bail!("svccfg import failure: {:?}", o),
            Err(e) => bail!("could not execute svccfg import: {:?}", e),
        }
    }

    #[cfg(target_os = "linux")]
    {
        /*
         * Create the input directory.
         */
        std::fs::create_dir_all(INPUT_PATH)?;

        /*
         * Write a systemd unit file for the agent service.
         */
        let unit = include_str!("../systemd/agent.service");
        make_dirs_for(UNIT)?;
        rmfile(UNIT)?;
        write_text(UNIT, unit)?;

        /*
         * Ask systemd to load the unit file we just wrote.
         */
        let status = Command::new("/bin/systemctl")
            .arg("daemon-reload")
            .env_clear()
            .current_dir("/")
            .status();
        match status {
            Ok(o) if o.success() => {}
            Ok(o) => bail!("systemd reload failure: {:?}", o),
            Err(e) => bail!("could not execute daemon-reload: {:?}", e),
        }

        /*
         * Attempt to start the service:
         */
        let status = Command::new("/bin/systemctl")
            .arg("start")
            .arg("buildomat-agent.service")
            .env_clear()
            .current_dir("/")
            .status();
        match status {
            Ok(o) if o.success() => {}
            Ok(o) => bail!("systemd start failure: {:?}", o),
            Err(e) => bail!("could not execute systemctl: {:?}", e),
        }

        /*
         * Ubuntu 18.04 had a genuine pre-war separate /bin directory!
         */
        let binmd = std::fs::symlink_metadata("/bin")?;
        if binmd.is_dir() {
            std::os::unix::fs::symlink(
                &format!("../usr/bin/{CONTROL_PROGRAM}"),
                &format!("/bin/{CONTROL_PROGRAM}"),
            )?;
        }
    }

    Ok(())
}

async fn cmd_run(mut l: Level<()>) -> Result<()> {
    no_args!(l);

    let cf = load::<_, ConfigFile>(CONFIG_PATH)?;

    println!("agent starting, using {}", cf.baseurl);
    let mut cw = cf.make_client();

    let res = loop {
        let res = cw
            .client
            .worker_bootstrap()
            .body_map(|body| body.bootstrap(&cf.bootstrap).token(&cf.token))
            .send()
            .await;

        match res {
            Ok(res) => break res,
            Err(e) => println!("ERROR: bootstrap: {:?}", e),
        }

        sleep_ms(1000).await;
    };

    let wid = res.into_inner().id;
    println!("bootstrapped as worker {}", wid);

    let mut tasks: VecDeque<WorkerPingTask> = VecDeque::new();
    let mut stage = Stage::Ready;
    let mut exit_details: Vec<ExitDetails> = Vec::new();
    let mut upload_errors = false;

    let mut pingfreq = tokio::time::interval(Duration::from_secs(5));
    pingfreq.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let mut control = control::server::listen()?;
    let mut creq: Option<control::server::Request> = None;
    let mut bgprocs = exec::BackgroundProcesses::new();

    let mut metadata: Option<metadata::FactoryMetadata> = None;

    let mut do_ping = true;
    loop {
        if do_ping {
            match cw.client.worker_ping().send().await {
                Err(e) => {
                    println!("PING ERROR: {e}");
                    sleep_ms(1000).await;
                }
                Ok(p) => {
                    let p = p.into_inner();

                    if p.poweroff {
                        println!("powering off at server request");
                        if let Err(e) = hard_reset() {
                            println!("ERROR: {:?}", e);
                        }
                        sleep_ms(1000).await;
                        continue;
                    }

                    /*
                     * If we have not yet been assigned a task, check for one:
                     */
                    if cw.job.is_none() {
                        assert!(matches!(stage, Stage::Ready));

                        if let Some(j) = &p.job {
                            /*
                             * Adopt the job, which may contain several tasks to
                             * execute in sequence.
                             */
                            println!(
                                "adopted job {} with {} tasks",
                                j.id,
                                j.tasks.len()
                            );
                            tasks.clear();
                            tasks.extend(j.tasks.iter().cloned());
                            cw.job = Some(j.clone());
                            pingfreq.reset();
                            stage = Stage::Download(download::download(
                                cw.clone(),
                                j.inputs.clone(),
                                PathBuf::from(INPUT_PATH),
                            ));
                        }
                    }

                    do_ping = false;

                    if let Some(md) = p.factory_metadata {
                        metadata = Some(md);
                    }
                }
            }
            continue;
        }

        if let Some(req) = creq.take() {
            /*
             * Handle requests from the control program.
             */
            let reply = match req.payload() {
                Payload::StoreGet(name) => {
                    let job = cw.job.as_ref().unwrap();

                    match cw
                        .client
                        .worker_job_store_get()
                        .job(&job.id)
                        .name(name)
                        .send()
                        .await
                    {
                        Ok(res) => Payload::StoreGetResult(
                            res.into_inner().value.map(|v| StoreEntry {
                                name: name.to_string(),
                                value: v.value,
                                secret: v.secret,
                            }),
                        ),
                        Err(e) => Payload::Error(e.to_string()),
                    }
                }
                Payload::StorePut(name, value, secret) => {
                    let job = cw.job.as_ref().unwrap();

                    match cw
                        .client
                        .worker_job_store_put()
                        .job(&job.id)
                        .name(name)
                        .body_map(|body| body.secret(*secret).value(value))
                        .send()
                        .await
                    {
                        Ok(..) => Payload::Ack,
                        Err(e) => Payload::Error(e.to_string()),
                    }
                }
                Payload::MetadataAddresses => {
                    Payload::MetadataAddressesResult(match metadata.as_ref() {
                        Some(metadata::FactoryMetadata::V1(md)) => {
                            md.addresses.clone()
                        }
                        _ => Default::default(),
                    })
                }
                Payload::ProcessStart {
                    name,
                    cmd,
                    args,
                    env,
                    pwd,
                    uid,
                    gid,
                } => {
                    match bgprocs.start(name, cmd, args, env, pwd, *uid, *gid) {
                        Ok(_) => Payload::Ack,
                        Err(e) => Payload::Error(e.to_string()),
                    }
                }
                _ => Payload::Error(format!("unexpected message type")),
            };

            req.reply(reply).await;
        }

        match &mut stage {
            Stage::Ready | Stage::Complete => {
                /*
                 * If we are not working on something, just sleep for a
                 * second and ping again in case there is a new directive.
                 */
                do_ping = true;
                sleep_ms(1000).await;
                continue;
            }
            Stage::NextTask => {
                let job = cw.job.as_ref().unwrap();

                /*
                 * If any task fails, we will not execute subsequent tasks.
                 * In case it is useful for diagnostic purposes, we will
                 * still attempt to upload output files before we complete
                 * the job.
                 */
                let failures = exit_details.iter().any(|ex| ex.failed());
                if failures {
                    println!("aborting after failed task");
                }

                if failures || tasks.is_empty() {
                    /*
                     * There are no more tasks to complete, so move on to
                     * uploading outputs.
                     */
                    println!("no more tasks for job {}", job.id);

                    stage = Stage::Upload(upload::upload(
                        cw.clone(),
                        job.output_rules.clone(),
                    ));
                    continue;
                }

                let t = tasks.pop_front().unwrap();

                /*
                 * Emit an event that we can use to visually separate tasks
                 * in the output.
                 */
                let msg = format!("starting task {}: \"{}\"", t.id, t.name);
                cw.append_task_msg(&t, &msg).await;

                /*
                 * Write the submitted script to a file.
                 */
                let s = write_script(&t.script)?;

                let mut cmd = Command::new("/bin/bash");
                cmd.arg(&s);

                /*
                 * The user task should have a pristine and reproducible
                 * environment that does not leak in artefacts of the
                 * agent.
                 */
                cmd.env_clear();
                if !t.env_clear {
                    /*
                     * Absent a request for an entirely clean slate, we
                     * set a few specific environment variables.
                     * XXX HOME/USER/LOGNAME should probably respect "uid"
                     */
                    cmd.env("HOME", "/root");
                    cmd.env("USER", "root");
                    cmd.env("LOGNAME", "root");
                    cmd.env("TZ", "UTC");
                    cmd.env(
                        "PATH",
                        "/usr/bin:/bin:/usr/sbin:/sbin:\
                            /opt/ooce/bin:/opt/ooce/sbin",
                    );
                    cmd.env("LANG", "en_US.UTF-8");
                    cmd.env("LC_ALL", "en_US.UTF-8");
                    cmd.env("BUILDOMAT_JOB_ID", &job.id);
                    cmd.env("BUILDOMAT_TASK_ID", t.id.to_string());
                }
                for (k, v) in t.env.iter() {
                    /*
                     * Overlay the user-provided environment onto what
                     * we have so far, thus allowing them to replace
                     * some or all of what we would otherwise provide.
                     */
                    cmd.env(k, v);
                }

                /*
                 * Each task may be expected to run under a different user
                 * account or with a different working directory.
                 */
                cmd.current_dir(&t.workdir);
                cmd.uid(t.uid as u32);
                cmd.gid(t.gid as u32);

                match exec::run(cmd) {
                    Ok(c) => {
                        stage = Stage::Child(c, t, None);
                    }
                    Err(e) => {
                        /*
                         * Try to post the error we would have reported
                         * to the server, but don't try too hard.
                         */
                        cw.client
                            .worker_job_append()
                            .job(&job.id)
                            .body(
                                WorkerAppendJob::builder()
                                    .stream("agent")
                                    .time(Utc::now())
                                    .payload(format!("ERROR: exec: {:?}", e)),
                            )
                            .send()
                            .await
                            .ok();
                    }
                }
            }
            Stage::Child(ch, t, failed) => {
                let a = tokio::select! {
                    _ = pingfreq.tick() => {
                        do_ping = true;
                        continue;
                    }
                    req = control.recv() => {
                        creq = req;
                        continue;
                    }
                    a = ch.recv() => a,
                    a = bgprocs.recv() => a,
                };

                match a {
                    Some(exec::Activity::Output(o)) => {
                        cw.append_task(t, &o.to_record()).await;
                    }
                    Some(exec::Activity::Exit(ex)) => {
                        cw.append_task(t, &ex.to_record()).await;

                        /*
                         * Preserve the exit status for when we record task
                         * completion, and so that we can determine whether
                         * to execute any subsequent tasks.
                         */
                        *failed = Some(ex.failed());
                        exit_details.push(ex);
                    }
                    Some(exec::Activity::Complete) => {
                        /*
                         * Bring down any background processes started by this
                         * task before we mark it as completed.
                         */
                        for a in bgprocs.killall().await {
                            if let exec::Activity::Output(o) = a {
                                cw.append_task(t, &o.to_record()).await;
                            }
                        }

                        /*
                         * Record completion of this task within the job.
                         */
                        cw.task_complete(t, failed.unwrap()).await;

                        stage = Stage::NextTask;
                    }
                    None => {
                        for a in bgprocs.killall().await {
                            if let exec::Activity::Output(o) = a {
                                cw.append_task(t, &o.to_record()).await;
                            }
                        }

                        cw.append_msg("channel disconnected").await;
                        cw.job_complete(true).await;

                        stage = Stage::Complete;
                    }
                }
            }
            Stage::Download(ch) => {
                let a = tokio::select! {
                    _ = pingfreq.tick() => {
                        do_ping = true;
                        continue;
                    }
                    a = ch.recv() => a,
                };

                match a {
                    Some(download::Activity::Downloading(p)) => {
                        cw.append_msg(&format!(
                            "downloading input: {}",
                            p.display(),
                        ))
                        .await;
                    }
                    Some(download::Activity::Downloaded(p)) => {
                        cw.append_msg(&format!(
                            "downloaded input: {}",
                            p.display(),
                        ))
                        .await;
                    }
                    Some(download::Activity::Complete) => {
                        stage = Stage::NextTask;
                    }
                    None => {
                        cw.append_msg("download channel disconnected").await;
                        cw.job_complete(true).await;
                        stage = Stage::Complete;
                    }
                }
            }
            Stage::Upload(ch) => {
                let a = tokio::select! {
                    _ = pingfreq.tick() => {
                        do_ping = true;
                        continue;
                    }
                    a = ch.recv() => a,
                };

                match a {
                    Some(upload::Activity::Scanned(count)) => {
                        cw.append_msg(&format!("found {} output files", count))
                            .await;
                    }
                    Some(upload::Activity::Uploading(p, sz)) => {
                        cw.append_msg(&format!(
                            "uploading: {} ({} bytes)",
                            p.display(),
                            sz
                        ))
                        .await;
                    }
                    Some(upload::Activity::Uploaded(p)) => {
                        cw.append_msg(&format!("uploaded: {}", p.display()))
                            .await;
                    }
                    Some(upload::Activity::Complete) => {
                        let failed = upload_errors
                            || exit_details.iter().any(|ex| ex.failed());
                        cw.job_complete(failed).await;
                        stage = Stage::Complete;
                    }
                    Some(upload::Activity::Error(s)) => {
                        cw.append_msg(&format!("upload error: {}", s)).await;
                        upload_errors = true;
                    }
                    Some(upload::Activity::Warning(s)) => {
                        cw.append_msg(&format!("upload warning: {}", s)).await;
                    }
                    None => {
                        cw.append_msg("upload channel disconnected").await;
                        cw.job_complete(true).await;
                        stage = Stage::Complete;
                    }
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cmdname = std::env::args()
        .next()
        .as_deref()
        .map(|s| {
            let path = PathBuf::from(s);
            path.file_name()
                .map(|s| s.to_str())
                .flatten()
                .map(|s| Some(s.to_string()))
                .flatten()
        })
        .flatten();

    match cmdname.as_deref() {
        None => bail!("could not determine executable name?"),
        Some(CONTROL_PROGRAM) => {
            /*
             * This is the in-job control entrypoint to be invoked by job
             * programs.
             */
            control::main().await
        }
        _ => {
            /*
             * XXX For now, assume that any name other than the name for
             * the control entrypoint is the regular agent.
             */
            let mut l = Level::new("buildomat-agent", ());

            l.cmd("install", "install the agent", cmd!(cmd_install))?;
            l.cmd("run", "run the agent", cmd!(cmd_run))?;

            sel!(l).run().await
        }
    }
}
