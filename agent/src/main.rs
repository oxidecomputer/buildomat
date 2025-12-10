/*
 * Copyright 2024 Oxide Computer Company
 */

#![allow(clippy::many_single_char_names)]

use std::collections::VecDeque;
use std::env;
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind::NotFound, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Result};
use chrono::prelude::*;
use futures::StreamExt;
use hiercmd::prelude::*;
use rusty_ulid::Ulid;
use serde::{Deserialize, Serialize};
use slog::{crit, error, info, o, Logger};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::time::MissedTickBehavior;

use buildomat_client::types::*;
use buildomat_common::*;
use buildomat_types::*;

mod control;
mod download;
mod exec;
mod shadow;
mod upload;

use control::protocol::{FactoryInfo, Payload};
use exec::ExitDetails;

struct Agent {
    log: Logger,
}

const CONFIG_PATH: &str = "/opt/buildomat/etc/agent.json";
const JOB_PATH: &str = "/opt/buildomat/etc/job.json";
const AGENT: &str = "/opt/buildomat/lib/agent";
const INPUT_PATH: &str = "/input";
const CONTROL_PROGRAM: &str = "bmat";
const SHADOW: &str = "/etc/shadow";
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
    fn make_client(&self, log: Logger) -> ClientWrap {
        let client = buildomat_client::ClientBuilder::new(&self.baseurl)
            .bearer_token(&self.token)
            .build()
            .expect("new client");

        /*
         * Start the task responsible for uploading per-worker events to the
         * server.  We may have events for this stream prior to the assignment
         * of a job.
         */
        let (worker_tx, rx) = mpsc::channel(256);
        tokio::task::spawn(append_worker_worker(
            log.new(o!("component" => "append_worker")),
            client.clone(),
            rx,
        ));

        ClientWrap { log, client, job: None, tx: None, worker_tx }
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

enum AppendJobEntry {
    JobEvent(OutputRecord),
    TaskEvent(u32, OutputRecord),
    FlushBarrier(oneshot::Sender<()>),
}

async fn append_job_worker(
    log: Logger,
    client: buildomat_client::Client,
    job: String,
    mut rx: mpsc::Receiver<AppendJobEntry>,
) {
    let mut barrier: Option<oneshot::Sender<()>> = None;

    loop {
        if let Some(barrier) = barrier.take() {
            barrier.send(()).unwrap();
        }

        /*
         * Build a batch of events to send to the server.
         */
        let mut events: Vec<WorkerAppendJobOrTask> = Default::default();
        while events.len() < 100 {
            let ae = if events.is_empty() {
                /*
                 * Block and wait for the first event in the batch.
                 */
                if let Some(ae) = rx.recv().await {
                    ae
                } else {
                    return;
                }
            } else {
                /*
                 * Grab more events if they are available, without blocking.
                 */
                if let Ok(ae) = rx.try_recv() {
                    ae
                } else {
                    break;
                }
            };

            events.push(match ae {
                AppendJobEntry::JobEvent(rec) => WorkerAppendJobOrTask {
                    task: None,
                    stream: rec.stream,
                    payload: rec.msg,
                    time: rec.time,
                },
                AppendJobEntry::TaskEvent(task, rec) => WorkerAppendJobOrTask {
                    task: Some(task),
                    stream: rec.stream,
                    payload: rec.msg,
                    time: rec.time,
                },
                AppendJobEntry::FlushBarrier(fb) => {
                    /*
                     * If we get a flush request, push out the current batch so
                     * we can acknowledge it in the correct order.
                     */
                    assert!(barrier.is_none());
                    barrier = Some(fb);
                    break;
                }
            });
        }

        if events.is_empty() {
            continue;
        }

        loop {
            match client
                .worker_job_append()
                .job(&job)
                .body(events.clone())
                .send()
                .await
            {
                Ok(_) => break,
                Err(e) => {
                    error!(log, "append job: {e:?}");
                    sleep_ms(1000).await;
                }
            }
        }
    }
}

enum AppendWorkerEntry {
    WorkerEvent(OutputRecord),
    FlushBarrier(oneshot::Sender<()>),
}

async fn append_worker_worker(
    log: Logger,
    client: buildomat_client::Client,
    mut rx: mpsc::Receiver<AppendWorkerEntry>,
) {
    let mut barrier: Option<oneshot::Sender<()>> = None;

    loop {
        if let Some(barrier) = barrier.take() {
            barrier.send(()).unwrap();
        }

        /*
         * Build a batch of events to send to the server.
         */
        let mut events: Vec<WorkerAppend> = Default::default();
        while events.len() < 100 {
            let ae = if events.is_empty() {
                /*
                 * Block and wait for the first event in the batch.
                 */
                if let Some(ae) = rx.recv().await {
                    ae
                } else {
                    return;
                }
            } else {
                /*
                 * Grab more events if they are available, without blocking.
                 */
                if let Ok(ae) = rx.try_recv() {
                    ae
                } else {
                    break;
                }
            };

            events.push(match ae {
                AppendWorkerEntry::WorkerEvent(rec) => WorkerAppend {
                    stream: rec.stream,
                    payload: rec.msg,
                    time: rec.time,
                },
                AppendWorkerEntry::FlushBarrier(fb) => {
                    /*
                     * If we get a flush request, push out the current batch so
                     * we can acknowledge it in the correct order.
                     */
                    assert!(barrier.is_none());
                    barrier = Some(fb);
                    break;
                }
            });
        }

        if events.is_empty() {
            continue;
        }

        loop {
            match client.worker_append().body(events.clone()).send().await {
                Ok(_) => break,
                Err(e) => {
                    error!(log, "append worker: {:?}", e);
                    sleep_ms(1000).await;
                }
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct ClientWrap {
    log: Logger,
    client: buildomat_client::Client,
    job: Option<Arc<WorkerPingJob>>,
    tx: Option<mpsc::Sender<AppendJobEntry>>,
    worker_tx: mpsc::Sender<AppendWorkerEntry>,
}

impl ClientWrap {
    fn job_id(&self) -> Option<&str> {
        self.job.as_ref().map(|job| job.id.as_str())
    }

    async fn start_job(&mut self, job: WorkerPingJob) {
        let job_id = job.id.to_string();
        assert!(self.job.is_none());
        self.job = Some(Arc::new(job));

        let (tx, rx) = mpsc::channel(256);
        self.tx = Some(tx);

        tokio::task::spawn(append_job_worker(
            self.log.new(o!("component" => "append_job")),
            self.client.clone(),
            job_id,
            rx,
        ));
    }

    async fn append_worker_msg(&self, name: &str, msg: &str) {
        self.append_worker(OutputRecord::new(&format!("diag.{name}"), msg))
            .await
    }

    async fn append_worker(&self, rec: OutputRecord) {
        self.worker_tx.send(AppendWorkerEntry::WorkerEvent(rec)).await.unwrap();
    }

    async fn flush_worker_barrier(&self) {
        let (tx, rx) = oneshot::channel::<()>();

        self.worker_tx.send(AppendWorkerEntry::FlushBarrier(tx)).await.unwrap();

        rx.await.unwrap();
    }

    async fn append(&self, rec: OutputRecord) {
        self.tx
            .as_ref()
            .unwrap()
            .send(AppendJobEntry::JobEvent(rec))
            .await
            .unwrap();
    }

    async fn append_msg(&self, msg: &str) {
        self.append(OutputRecord::new("worker", msg)).await;
    }

    async fn append_task(&self, task: &WorkerPingTask, rec: OutputRecord) {
        self.tx
            .as_ref()
            .unwrap()
            .send(AppendJobEntry::TaskEvent(task.id, rec))
            .await
            .unwrap();
    }

    async fn append_task_msg(&self, task: &WorkerPingTask, msg: &str) {
        self.append_task(task, OutputRecord::new("task", msg)).await;
    }

    async fn flush_job_barrier(&self) {
        let (tx, rx) = oneshot::channel::<()>();

        self.tx
            .as_ref()
            .unwrap()
            .send(AppendJobEntry::FlushBarrier(tx))
            .await
            .unwrap();

        rx.await.unwrap();
    }

    async fn task_complete(&self, task: &WorkerPingTask, failed: bool) {
        /*
         * Make sure any previously enqueued event log events have gone out to
         * the server before we complete the task.
         */
        self.flush_job_barrier().await;

        loop {
            match self
                .client
                .worker_task_complete()
                .job(self.job_id().unwrap())
                .task(task.id)
                .body_map(|body| body.failed(failed))
                .send()
                .await
            {
                Ok(_) => return,
                Err(e) => {
                    error!(self.log, "task complete: {e:?}");
                    sleep_ms(1000).await;
                }
            }
        }
    }

    async fn job_complete(&self, failed: bool) {
        /*
         * Make sure any previously enqueued event log events have gone out to
         * the server before we complete the job.
         */
        self.flush_job_barrier().await;

        loop {
            match self
                .client
                .worker_job_complete()
                .job(self.job_id().unwrap())
                .body_map(|body| body.failed(failed))
                .send()
                .await
            {
                Ok(_) => return,
                Err(e) => {
                    error!(self.log, "job complete: {e:?}");
                    sleep_ms(1000).await;
                }
            }
        }
    }

    async fn diagnostics_enable(&self) {
        loop {
            match self.client.worker_diagnostics_enable().send().await {
                Ok(_) => return,
                Err(e) => {
                    error!(self.log, "diagnostics enable: {e:?}");
                    sleep_ms(1000).await;
                }
            }
        }
    }

    async fn diagnostics_complete(&self, hold: bool) {
        /*
         * Make sure any previously enqueued event log events have gone out to
         * the server before we complete diagnostics.
         */
        self.flush_worker_barrier().await;

        loop {
            match self
                .client
                .worker_diagnostics_complete()
                .body_map(|body| body.hold(hold))
                .send()
                .await
            {
                Ok(_) => return,
                Err(e) => {
                    error!(self.log, "diagnostics complete: {e:?}");
                    sleep_ms(1000).await;
                }
            }
        }
    }

    async fn report_failure(&self, reason: Option<&str>) {
        loop {
            match self
                .client
                .worker_fail()
                .body_map(|body| body.reason(reason.map(str::to_string)))
                .send()
                .await
            {
                Ok(_) => return,
                Err(e) => {
                    error!(self.log, "report failure: {e:?}");
                    sleep_ms(1000).await;
                }
            }
        }
    }

    async fn chunk(&self, buf: bytes::Bytes) -> String {
        loop {
            match self
                .client
                .worker_job_upload_chunk()
                .job(self.job_id().unwrap())
                .body(buf.clone())
                .send()
                .await
            {
                Ok(uc) => {
                    return uc.into_inner().id;
                }
                Err(e) => {
                    error!(self.log, "chunk upload: {e:?}");
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
                .job(self.job_id().unwrap())
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
                        error!(self.log, "output file error: {e:?}");
                        return Some(e.to_string());
                    } else {
                        return None;
                    }
                }
                Err(e) => {
                    error!(self.log, "add output: {e:?}");
                    sleep_ms(1000).await;
                }
            }
        }
    }

    async fn input(&self, id: &str, path: &Path) {
        'outer: loop {
            match self
                .client
                .worker_job_input_download()
                .job(self.job_id().unwrap())
                .input(id)
                .send()
                .await
            {
                Ok(res) => {
                    let mut body = res.into_inner();

                    let mut f = match tokio::fs::File::create(path).await {
                        Ok(f) => f,
                        Err(e) => {
                            error!(self.log, "input: {e:?}");
                            sleep_ms(1000).await;
                            continue 'outer;
                        }
                    };

                    loop {
                        match body.next().await.transpose() {
                            Ok(None) => return,
                            Ok(Some(mut ch)) => {
                                if let Err(e) = f.write_all_buf(&mut ch).await {
                                    error!(self.log, "input: {e:?}");
                                    sleep_ms(1000).await;
                                    continue 'outer;
                                }
                            }
                            Err(e) => {
                                error!(self.log, "input: {e:?}");
                                sleep_ms(1000).await;
                                continue 'outer;
                            }
                        }
                    }
                }
                Err(e) => {
                    error!(self.log, "input: {e:?}");
                    sleep_ms(1000).await;
                }
            }
        }
    }

    async fn quota(&self) -> WorkerJobQuota {
        loop {
            match self
                .client
                .worker_job_quota()
                .job(self.job_id().unwrap())
                .send()
                .await
            {
                Ok(wq) => {
                    return wq.into_inner();
                }
                Err(e) => {
                    error!(self.log, "asking for quota: {e:?}");
                    sleep_ms(1000).await;
                }
            }
        }
    }

    /**
     * Start a pre- or post-job diagnostic script.  Returns a channel with
     * script output and termination status so that it can be forwarded to the
     * server.
     */
    async fn start_diag_script(
        &self,
        name: &str,
        script: &str,
    ) -> Option<mpsc::Receiver<exec::Activity>> {
        self.append_worker_msg(
            name,
            &format!("starting {name}-job diagnostics"),
        )
        .await;

        /*
         * Write the submitted script to a file.
         */
        let s = match write_script(script) {
            Ok(s) => s,
            Err(e) => {
                error!(self.log, "writing {name}-job diagnostic script: {e}");
                return None;
            }
        };

        let mut cmd = Command::new("/bin/bash");
        cmd.arg(&s);

        /*
         * The diagnostic task should have a pristine and reproducible
         * environment that does not leak in artefacts of the agent.
         */
        cmd.env_clear();

        cmd.env("HOME", "/root");
        cmd.env("USER", "root");
        cmd.env("LOGNAME", "root");
        cmd.env("TZ", "UTC");
        cmd.env(
            "PATH",
            "/usr/bin:/bin:/usr/sbin:/sbin:/opt/ooce/bin:/opt/ooce/sbin",
        );
        cmd.env("LANG", "en_US.UTF-8");
        cmd.env("LC_ALL", "en_US.UTF-8");

        /*
         * Run the diagnostic script as root:
         */
        cmd.current_dir("/");
        cmd.uid(0);
        cmd.gid(0);

        match exec::run_diagnostic(cmd, name) {
            Ok(c) => Some(c),
            Err(e) => {
                /*
                 * Try to post the error we would have reported to the server,
                 * but don't try too hard.
                 */
                self.client
                    .worker_append()
                    .body(vec![WorkerAppend {
                        stream: "agent".into(),
                        time: Utc::now(),
                        payload: format!("ERROR: diag.{name} exec: {:?}", e),
                    }])
                    .send()
                    .await
                    .ok();

                None
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

fn write_script(script: &str) -> Result<PathBuf> {
    let targ = format!("/tmp/{}.sh", genkey(16)).into();
    let mut f = OpenOptions::new().create_new(true).write(true).open(&targ)?;
    let mut data = script.to_string();
    if !data.ends_with('\n') {
        data.push('\n');
    }
    f.write_all(data.as_bytes())?;
    f.flush()?;
    Ok(targ)
}

fn set_root_password_hash(log: &Logger, hash: &str) -> Result<()> {
    /*
     * Install the provided root password hash into shadow(5) so that console
     * logins are possible.
     */
    let mut f = shadow::ShadowFile::load(SHADOW)?;
    f.password_set("root", hash)?;
    f.write(SHADOW)?;

    info!(log, "root password hash was set!");
    Ok(())
}

fn set_root_authorized_keys(_log: &Logger, keys: &str) -> Result<()> {
    /*
     * Install the provided root "authorized_keys" file.  Note that the
     * permissions are important, in order for sshd to actually use the file.
     */
    let dir = PathBuf::from("/root/.ssh");
    if !dir.exists() {
        std::fs::DirBuilder::new().mode(0o700).create(&dir)?;
    }
    let file = dir.join("authorized_keys");
    write_text(&file, keys)?;
    Ok(())
}

#[cfg(target_os = "illumos")]
fn set_nodename(name: &str) -> Result<()> {
    write_text("/etc/nodename", &format!("{name}\n"))?;
    write_text(
        "/etc/inet/hosts",
        &format!(
            concat!(
                "#\n",
                "# Internet host table\n",
                "#\n",
                "::1            localhost\n",
                "127.0.0.1      localhost loghost {}\n",
            ),
            name
        ),
    )?;

    let out = Command::new("/bin/uname")
        .args(["-S", name])
        .env_clear()
        .current_dir("/")
        .output()?;

    if !out.status.success() {
        bail!("uname -S {name:?}: {}", out.info());
    }

    Ok(())
}

#[cfg(target_os = "linux")]
fn set_nodename(name: &str) -> Result<()> {
    write_text("/etc/hostname", &format!("{name}\n"))?;
    write_text(
        "/etc/hosts",
        &format!(
            concat!(
                "127.0.0.1       localhost\n",
                "127.0.1.1       {}\n",
                "\n",
                "# The following lines are desirable for IPv6 capable hosts\n",
                "::1     ip6-localhost ip6-loopback\n",
                "fe00::0 ip6-localnet\n",
                "ff00::0 ip6-mcastprefix\n",
                "ff02::1 ip6-allnodes\n",
                "ff02::2 ip6-allrouters\n",
            ),
            name
        ),
    )?;

    let out = Command::new("/bin/hostname")
        .arg(name)
        .env_clear()
        .current_dir("/")
        .output()?;

    if !out.status.success() {
        bail!("hostname {name:?}: {}", out.info());
    }

    Ok(())
}

fn zfs_exists(dataset: &str) -> Result<bool> {
    let out = Command::new("/sbin/zfs")
        .arg("get")
        .args(["-Ho", "value"])
        .arg("type")
        .arg(dataset)
        .env_clear()
        .current_dir("/")
        .output()?;

    if !out.status.success() {
        let e = String::from_utf8_lossy(&out.stderr);
        if e.contains("dataset does not exist") {
            return Ok(false);
        }

        bail!("could not check for dataset {dataset:?}: {}", out.info());
    }

    Ok(true)
}

fn zfs_get(dataset: &str, prop: &str) -> Result<String> {
    let out = Command::new("/sbin/zfs")
        .arg("get")
        .args(["-Ho", "value"])
        .arg(prop)
        .arg(dataset)
        .env_clear()
        .current_dir("/")
        .output()?;

    if !out.status.success() {
        bail!("zfs get {prop:?} {dataset:?}: {}", out.info());
    }

    let o = String::from_utf8(out.stdout)?;
    let l = o.lines().collect::<Vec<_>>();
    if l.len() != 1 {
        bail!("zfs get {prop:?} {dataset:?}: weird output {l:?}");
    }

    Ok(l[0].to_string())
}

fn zfs_set(dataset: &str, prop: &str, value: &str) -> Result<()> {
    let out = Command::new("/sbin/zfs")
        .arg("set")
        .arg(format!("{prop}={value}"))
        .arg(dataset)
        .env_clear()
        .current_dir("/")
        .output()?;

    if !out.status.success() {
        bail!("zfs set {prop:?}={value:?} {dataset:?}: {}", out.info());
    }

    Ok(())
}

fn zfs_create_volume(dataset: &str, size_mb: u32) -> Result<()> {
    let out = Command::new("/sbin/zfs")
        .arg("create")
        .args(["-V", &format!("{size_mb}M")])
        .arg(dataset)
        .env_clear()
        .current_dir("/")
        .output()?;

    if !out.status.success() {
        bail!("zfs create {dataset:?}: {}", out.info());
    }

    Ok(())
}

fn ensure_dump_device(log: &Logger, mbsz: u32) -> Result<()> {
    /*
     * First, make sure the "rpool/dump" zvol exists.
     */
    if !zfs_exists("rpool/dump")? {
        zfs_create_volume("rpool/dump", mbsz)?;
    } else if zfs_get("rpool/dump", "type")? != "volume" {
        bail!("rpool/dump is not a volume?!");
    }

    let out = Command::new("/usr/sbin/dumpadm")
        .arg("-y")
        .args(["-z", "on"])
        .args(["-d", "/dev/zvol/dsk/rpool/dump"])
        .env_clear()
        .current_dir("/")
        .output()?;

    if !out.status.success() {
        bail!("dumpadm: {}", out.info());
    }

    /*
     * The boot archive contains the dump configuration, and savecore(8) does
     * not always notice the dump after a reboot if we have not refreshed the
     * archive with the new configuration prior to panicking.
     */
    let out = Command::new("/sbin/bootadm")
        .arg("update-archive")
        .env_clear()
        .current_dir("/")
        .output()?;

    if !out.status.success() {
        bail!("bootadm update-archive: {}", out.info());
    }

    info!(log, "dump device was configured!");
    Ok(())
}

#[cfg(target_os = "illumos")]
fn rpool_disable_sync(log: &Logger) -> Result<()> {
    if !zfs_exists("rpool")? {
        /*
         * If the system does not have a root pool, there is no point trying to
         * disable sync.  Note that technically a system could boot from a pool
         * with any name, not just "rpool"; in practice the images we use on
         * buildomat systems all use "rpool" today.
         */
        info!(log, "skipping sync disable as system has no rpool");
    } else {
        zfs_set("rpool", "sync", "disabled")?;

        info!(log, "sync was disabled on rpool!");
    }

    Ok(())
}

#[cfg(not(target_os = "illumos"))]
fn rpool_disable_sync(_log: &Logger) -> Result<()> {
    /*
     * Sure, whatever you say!
     */
    Ok(())
}

enum Stage {
    Ready,
    Download(mpsc::Receiver<download::Activity>),
    NextTask,
    Child(mpsc::Receiver<exec::Activity>, WorkerPingTask, Option<bool>),
    Upload(mpsc::Receiver<upload::Activity>),
    StartPreDiagnostics(String),
    PreDiagnostics(mpsc::Receiver<exec::Activity>, Option<bool>),
    StartPostDiagnostics,
    PostDiagnostics(mpsc::Receiver<exec::Activity>, Option<bool>),
    Complete,
    Broken,
}

async fn cmd_install(mut l: Level<Agent>) -> Result<()> {
    l.usage_args(Some("BASEURL BOOTSTRAP_TOKEN"));
    l.optopt("N", "", "set nodename of machine", "NODENAME");

    let a = args!(l);
    let log = l.context().log.clone();

    if a.args().len() < 2 {
        bad_args!(l, "specify base URL and bootstrap token value");
    }

    /*
     * The server will have provided these parameters in the userscript
     * used to inject the agent into this build VM.
     */
    let baseurl = a.args()[0].to_string();
    let bootstrap = a.args()[1].to_string();

    if let Some(nodename) = a.opts().opt_str("N") {
        if let Err(e) = set_nodename(&nodename) {
            error!(log, "setting nodename: {e}");
        }
    }

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
            .arg(format!("mountpoint={INPUT_PATH}"))
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

async fn cmd_run(mut l: Level<Agent>) -> Result<()> {
    no_args!(l);

    let cf = load::<_, ConfigFile>(CONFIG_PATH)?;
    let log = l.context().log.clone();

    info!(log, "agent starting"; "baseurl" => &cf.baseurl);
    let mut cw = cf.make_client(log.clone());

    let res = loop {
        let res = cw
            .client
            .worker_bootstrap()
            .body_map(|body| body.bootstrap(&cf.bootstrap).token(&cf.token))
            .send()
            .await;

        match res {
            Ok(res) => break res,
            Err(e) => error!(log, "bootstrap: {e:?}"),
        }

        sleep_ms(1000).await;
    };

    let wid = res.into_inner().id;
    info!(log, "bootstrapped as worker {wid}");

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
    let mut factory: Option<WorkerPingFactoryInfo> = None;
    let mut set_root_password = false;
    let mut set_root_keys = false;
    let mut dump_device_configured = false;
    let mut post_diag_script: Option<String> = None;
    let mut pre_diag_done = false;
    let mut sync_disabled = false;

    /*
     * Check for a file containing a description of the job we are running.  If
     * this file exists, it must have been written by a previous agent process
     * inside this same worker.  Neither the agent process nor the job model in
     * general can really handle tasks being started a second time.  If we find
     * any evidence that we've done this before, we must report it to the
     * central server and do nothing else.
     */
    if PathBuf::from(JOB_PATH).try_exists()? {
        error!(log, "found previously assigned job; reporting failure");

        /*
         * Report this condition to the server:
         */
        cw.report_failure(None).await;

        /*
         * We need to park and do nothing other than continue to ping the server
         * waiting to be told to power off the instance.
         */
        stage = Stage::Broken;
    }

    let mut do_ping = true;
    loop {
        if let Some(md) = &metadata {
            if !set_root_password {
                if let Some(rph) = md.root_password_hash() {
                    if let Err(e) = set_root_password_hash(&log, rph) {
                        error!(log, "setting root password: {e}");
                    }

                    /*
                     * This facility is just for diagnostic purposes.  If for
                     * some reason it does not work out, don't try again and
                     * don't interrupt the other operation of the agent.
                     */
                    set_root_password = true;
                }
            }

            if !set_root_keys {
                if let Some(rak) = md.root_authorized_keys() {
                    if let Err(e) = set_root_authorized_keys(&log, rak) {
                        error!(log, "setting root SSH keys: {e}");
                    }

                    set_root_keys = true;
                }
            }

            if !dump_device_configured {
                if let Some(mbsz) = md.dump_to_rpool() {
                    if let Err(e) = ensure_dump_device(&log, mbsz) {
                        error!(log, "ensuring dump device: {e}");
                    }

                    dump_device_configured = true;
                }
            }

            if !sync_disabled && md.rpool_disable_sync() {
                if let Err(e) = rpool_disable_sync(&log) {
                    error!(log, "disabling sync on rpool: {e}");
                }

                sync_disabled = true;
            }
        }

        if do_ping {
            match cw.client.worker_ping().send().await {
                Err(e) => {
                    error!(log, "ping error: {e}");
                    sleep_ms(1000).await;
                }
                Ok(p) => {
                    let p = p.into_inner();

                    if p.poweroff {
                        /*
                         * If we're being recycled, just hold here and wait.
                         */
                        sleep_ms(1000).await;
                        continue;
                    }

                    if let Some(fi) = &p.factory_info {
                        factory = Some(fi.clone());
                    }

                    if let Some(md) = &p.factory_metadata {
                        /*
                         * If the factory has given us a post-job diagnostic
                         * script to run, we need to report that the server
                         * should wait for it before recycling us.
                         */
                        if let Some(script) = md.post_job_diagnostic_script() {
                            if post_diag_script.is_none() {
                                cw.diagnostics_enable().await;
                                post_diag_script = Some(script.into());
                            }
                        }

                        /*
                         * If there is a pre-diagnostic script we need to invoke
                         * it prior to starting the job.
                         */
                        if let Some(script) = md.pre_job_diagnostic_script() {
                            if !pre_diag_done && matches!(stage, Stage::Ready) {
                                stage =
                                    Stage::StartPreDiagnostics(script.into());
                                pre_diag_done = true;
                            }
                        }

                        metadata = Some(md.clone());
                    }

                    /*
                     * If we have not yet been assigned a task, check for one:
                     */
                    if matches!(stage, Stage::Ready) && cw.job_id().is_none() {
                        if let Some(j) = &p.job {
                            /*
                             * Adopt the job, which may contain several tasks to
                             * execute in sequence.
                             */
                            info!(
                                log,
                                "adopted job {} with {} tasks",
                                j.id,
                                j.tasks.len()
                            );

                            /*
                             * Write the job assignment to a file for diagnostic
                             * purposes, and to serve as a marker file in case
                             * of agent or system restart.
                             */
                            {
                                let diag = serde_json::to_vec_pretty(&p)?;
                                let mut jf = std::fs::OpenOptions::new()
                                    .create_new(true)
                                    .create(false)
                                    .write(true)
                                    .open(JOB_PATH)?;
                                jf.write_all(&diag)?;
                                jf.flush()?;
                                jf.sync_all()?;
                            }

                            tasks.clear();
                            tasks.extend(j.tasks.iter().cloned());
                            cw.start_job(j.clone()).await;
                            pingfreq.reset();
                            stage = Stage::Download(download::download(
                                cw.clone(),
                                j.inputs.clone(),
                                PathBuf::from(INPUT_PATH),
                            ));
                        }
                    }

                    do_ping = false;
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
                    match cw
                        .client
                        .worker_job_store_get()
                        .job(cw.job_id().unwrap())
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
                    match cw
                        .client
                        .worker_job_store_put()
                        .job(cw.job_id().unwrap())
                        .name(name)
                        .body_map(|body| body.secret(*secret).value(value))
                        .send()
                        .await
                    {
                        Ok(..) => Payload::Ack,
                        Err(e) => Payload::Error(e.to_string()),
                    }
                }
                Payload::MetadataAddresses => Payload::MetadataAddressesResult(
                    metadata
                        .as_ref()
                        .map(|md| md.addresses().to_vec())
                        .unwrap_or_default(),
                ),
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
                Payload::FactoryInfo => {
                    if let Some(f) = &factory {
                        Payload::FactoryInfoResult(FactoryInfo {
                            id: f.id.to_string(),
                            name: f.name.to_string(),
                            private: f.private.clone(),
                        })
                    } else {
                        Payload::Error("factory info not available".into())
                    }
                }
                _ => Payload::Error("unexpected message type".to_string()),
            };

            req.reply(reply).await;
        }

        match &mut stage {
            Stage::Ready | Stage::Complete | Stage::Broken => {
                /*
                 * If we are not working on something, just sleep for a
                 * second and ping again in case there is a new directive.
                 */
                do_ping = true;
                sleep_ms(1000).await;
                continue;
            }
            Stage::StartPreDiagnostics(script) => {
                if let Some(c) =
                    cw.start_diag_script("pre", script.as_str()).await
                {
                    stage = Stage::PreDiagnostics(c, None);
                } else {
                    stage = Stage::Ready;
                }
            }
            Stage::PreDiagnostics(ch, failed) => {
                let a = tokio::select! {
                    _ = pingfreq.tick() => {
                        do_ping = true;
                        continue;
                    }
                    a = ch.recv() => a,
                };

                match a {
                    Some(exec::Activity::Output(o)) => {
                        cw.append_worker(o.to_record()).await;
                    }
                    Some(exec::Activity::Exit(ex)) => {
                        cw.append_worker(ex.to_record()).await;

                        /*
                         * Preserve the exit status for when we record task
                         * completion, and so that we can determine whether to
                         * mark the worker as failed.
                         */
                        *failed = Some(ex.failed());
                        exit_details.push(ex);
                    }
                    Some(exec::Activity::Complete) if failed.unwrap() => {
                        /*
                         * If the pre-job diagnostic script fails, mark the
                         * worker as broken prior to accepting a job from
                         * the server.
                         */
                        error!(log, "pre-diagnostics failed; aborting");
                        cw.report_failure(Some(
                            "pre-job diagnostics reported a fault",
                        ))
                        .await;
                        stage = Stage::Broken;
                    }
                    Some(exec::Activity::Complete) => {
                        info!(log, "pre-job diagnostics complete");
                        stage = Stage::Ready;
                    }
                    None => {
                        cw.append_worker_msg("pre", "channel disconnected")
                            .await;
                        error!(log, "pre-diagnostics failed; aborting");
                        cw.report_failure(None).await;
                        stage = Stage::Broken;
                    }
                }
            }
            Stage::NextTask => {
                /*
                 * If any task fails, we will not execute subsequent tasks.
                 * In case it is useful for diagnostic purposes, we will
                 * still attempt to upload output files before we complete
                 * the job.
                 */
                let failures = exit_details.iter().any(|ex| ex.failed());
                if failures {
                    error!(log, "aborting after failed task");
                }

                if failures || tasks.is_empty() {
                    /*
                     * There are no more tasks to complete, so move on to
                     * uploading outputs.
                     */
                    info!(
                        log,
                        "no more tasks for job {}",
                        cw.job_id().unwrap(),
                    );

                    stage = Stage::Upload(upload::upload(
                        cw.clone(),
                        cw.job.as_ref().unwrap().output_rules.clone(),
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
                    cmd.env("BUILDOMAT_JOB_ID", cw.job_id().unwrap());
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
                cmd.uid(t.uid);
                cmd.gid(t.gid);

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
                            .job(cw.job_id().unwrap())
                            .body(vec![WorkerAppendJobOrTask {
                                task: None,
                                stream: "agent".into(),
                                time: Utc::now(),
                                payload: format!("ERROR: exec: {:?}", e),
                            }])
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
                        cw.append_task(t, o.to_record()).await;
                    }
                    Some(exec::Activity::Exit(ex)) => {
                        cw.append_task(t, ex.to_record()).await;

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
                                cw.append_task(t, o.to_record()).await;
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
                                cw.append_task(t, o.to_record()).await;
                            }
                        }

                        cw.append_msg("channel disconnected").await;
                        cw.job_complete(true).await;

                        stage = Stage::StartPostDiagnostics;
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
                        stage = Stage::StartPostDiagnostics;
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
                        stage = Stage::StartPostDiagnostics;
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
                        stage = Stage::StartPostDiagnostics;
                    }
                }
            }
            Stage::StartPostDiagnostics => {
                if let Some(script) = post_diag_script.as_deref() {
                    /*
                     * The factory gave us a post-diagnostic script to run
                     * and the server will wait for us to run it.
                     */
                    if let Some(c) = cw.start_diag_script("post", script).await
                    {
                        stage = Stage::PostDiagnostics(c, None);
                    } else {
                        cw.diagnostics_complete(false).await;
                        stage = Stage::Complete;
                    }
                } else {
                    stage = Stage::Complete;
                    continue;
                }
            }
            Stage::PostDiagnostics(ch, failed) => {
                let a = tokio::select! {
                    _ = pingfreq.tick() => {
                        do_ping = true;
                        continue;
                    }
                    a = ch.recv() => a,
                };

                match a {
                    Some(exec::Activity::Output(o)) => {
                        cw.append_worker(o.to_record()).await;
                    }
                    Some(exec::Activity::Exit(ex)) => {
                        cw.append_worker(ex.to_record()).await;

                        /*
                         * Preserve the exit status for when we record task
                         * completion, and so that we can determine whether to
                         * mark the worker as held.
                         */
                        *failed = Some(ex.failed());
                        exit_details.push(ex);
                    }
                    Some(exec::Activity::Complete) => {
                        /*
                         * Record completion of diagnostics so that the server
                         * can proceed to recycle this instance.
                         */
                        cw.diagnostics_complete(failed.unwrap()).await;

                        stage = Stage::Complete;
                    }
                    None => {
                        cw.append_worker_msg("post", "channel disconnected")
                            .await;
                        cw.diagnostics_complete(false).await;

                        stage = Stage::Complete;
                    }
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cmdname = std::env::args().next().as_deref().and_then(|s| {
        let path = PathBuf::from(s);
        path.file_name().and_then(|s| s.to_str()).map(|s| s.to_string())
    });

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
             * Assume that any name other than the name for the control
             * entrypoint is the regular agent.
             */
            let log = make_log("buildomat-agent");
            let mut l =
                Level::new("buildomat-agent", Agent { log: log.clone() });

            l.cmd("install", "install the agent", cmd!(cmd_install))?;
            l.cmd("run", "run the agent", cmd!(cmd_run))?;

            let res = sel!(l).run().await;
            if let Err(e) = &res {
                crit!(log, "agent failure: {e}");
            }
            res
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_path_value() {
        assert_eq!(CONFIG_PATH, "/opt/buildomat/etc/agent.json");
    }

    #[test]
    fn test_job_path_value() {
        assert_eq!(JOB_PATH, "/opt/buildomat/etc/job.json");
    }

    #[test]
    fn test_agent_path_value() {
        assert_eq!(AGENT, "/opt/buildomat/lib/agent");
    }

    #[cfg(target_os = "illumos")]
    mod illumos_tests {
        use super::*;

        #[test]
        fn test_method_path_value() {
            assert_eq!(METHOD, "/opt/buildomat/lib/start.sh");
        }

        #[test]
        fn test_manifest_path_value() {
            assert_eq!(MANIFEST, "/var/svc/manifest/site/buildomat-agent.xml");
        }
    }
}
