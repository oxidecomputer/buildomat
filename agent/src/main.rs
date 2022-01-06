/*
 * Copyright 2021 Oxide Computer Company
 */

#![allow(clippy::many_single_char_names)]

use std::collections::VecDeque;
use std::env;
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use chrono::prelude::*;
use serde::{Deserialize, Serialize};
use ErrorKind::NotFound;

use buildomat_common::*;
use buildomat_openapi::types::*;

mod download;
mod exec;
mod uadmin;
mod upload;

use exec::{Activity, ExitDetails};
use tokio::io::AsyncWriteExt;

const CONFIG_PATH: &str = "/opt/buildomat/etc/agent.json";
const AGENT: &str = "/opt/buildomat/lib/agent";
const METHOD: &str = "/opt/buildomat/lib/start.sh";
const MANIFEST: &str = "/var/svc/manifest/site/buildomat-agent.xml";
const INPUT_DATASET: &str = "rpool/input";
const INPUT_PATH: &str = "/input";

#[derive(Serialize, Deserialize)]
struct ConfigFile {
    baseurl: String,
    bootstrap: String,
    token: String,
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
    client: buildomat_openapi::Client,
    job: Option<WorkerPingJob>,
}

impl ClientWrap {
    async fn append(&self, rec: &OutputRecord) {
        let job = self.job.as_ref().unwrap();

        let wat = WorkerAppendJob {
            stream: rec.stream.to_string(),
            time: rec.time,
            payload: rec.msg.to_string(),
        };

        loop {
            match self.client.worker_job_append(job.id.as_str(), &wat).await {
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

        let wat = WorkerAppendJob {
            stream: rec.stream.to_string(),
            time: rec.time,
            payload: rec.msg.to_string(),
        };

        loop {
            match self
                .client
                .worker_task_append(job.id.as_str(), task.id, &wat)
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
        let wac = WorkerCompleteTask { failed };

        loop {
            match self
                .client
                .worker_task_complete(job.id.as_str(), task.id, &wac)
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
        let wac = WorkerCompleteJob { failed };

        loop {
            match self.client.worker_job_complete(job.id.as_str(), &wac).await {
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
                .worker_job_upload_chunk(&job.id, buf.clone())
                .await
            {
                Ok(uc) => {
                    return uc.id;
                }
                Err(e) => {
                    println!("ERROR: chunk upload: {:?}", e);
                    sleep_ms(1000).await;
                }
            }
        }
    }

    async fn output(&self, path: &Path, size: u64, chunks: &[String]) {
        let job = self.job.as_ref().unwrap();
        let wao = WorkerAddOutput {
            chunks: chunks.to_vec(),
            path: path.to_str().unwrap().to_string(),
            size: size as i64,
        };

        loop {
            match self.client.worker_job_add_output(&job.id, &wao).await {
                Ok(_) => return,
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
            match self.client.worker_job_input_download(&job.id, id).await {
                Ok(mut res) => {
                    let mut f = match tokio::fs::File::create(path).await {
                        Ok(f) => f,
                        Err(e) => {
                            println!("ERROR: input: {:?}", e);
                            sleep_ms(1000).await;
                            continue 'outer;
                        }
                    };

                    loop {
                        match res.chunk().await {
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
    p.pop();
    std::fs::create_dir_all(p)?;
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
    uadmin::uadmin(uadmin::Action::Reboot(uadmin::Next::Boot))
        .context("hard_reset: uadmin")?;

    Ok(())
}

fn argn(n: usize, name: &str) -> Result<String> {
    env::args().nth(n).ok_or_else(|| anyhow!("no {} argument?", name))
}

fn make_client(cf: &ConfigFile) -> ClientWrap {
    let client = bearer_client(&cf.token).expect("new client");

    ClientWrap {
        client: buildomat_openapi::Client::new_with_client(&cf.baseurl, client),
        job: None,
    }
}

enum Stage {
    Ready,
    Download(mpsc::Receiver<download::Activity>),
    NextTask,
    Child(mpsc::Receiver<exec::Activity>, WorkerPingTask, Option<bool>),
    Upload(mpsc::Receiver<upload::Activity>),
    Complete,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cf = match argn(1, "command")?.as_str() {
        "install" => {
            /*
             * The server will have provided these parameters in the userscript
             * used to inject the agent into this build VM.
             */
            let baseurl = argn(2, "baseurl")?;
            let bootstrap = argn(3, "bootstrap")?;

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

            return Ok(());
        }
        "run" => load::<_, ConfigFile>(CONFIG_PATH)?,
        cmd => {
            bail!("unknown command: {}", cmd);
        }
    };

    println!("agent starting, using {}", cf.baseurl);
    let mut cw = make_client(&cf);

    let res = loop {
        let res = cw
            .client
            .worker_bootstrap(&WorkerBootstrap {
                bootstrap: cf.bootstrap.to_string(),
                token: cf.token.to_string(),
            })
            .await;

        match res {
            Ok(res) => break res,
            Err(e) => println!("ERROR: bootstrap: {:?}", e),
        }

        sleep_ms(1000).await;
    };

    let wid = res.id;
    println!("bootstrapped as worker {}", wid);

    let mut tasks: VecDeque<WorkerPingTask> = VecDeque::new();
    let mut stage = Stage::Ready;
    let mut exit_details: Vec<ExitDetails> = Vec::new();
    let mut upload_errors = false;

    loop {
        /*
         * First, take care of submitting our current status to the server.  The
         * ping will contain instructions if we need to start executing a job,
         * etc.
         */
        match cw.client.worker_ping().await {
            Ok(p) => {
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
                        stage = Stage::Download(download::download(
                            cw.clone(),
                            j.inputs.clone(),
                            PathBuf::from(INPUT_PATH),
                        ));
                    }
                }
            }
            Err(e) => {
                println!("ERROR: ping: {:?}", e);
            }
        }

        /*
         * Don't process events for more than 5 seconds at a time:
         */
        let end = Instant::now().checked_add(Duration::from_secs(5)).unwrap();
        loop {
            let rem = end.saturating_duration_since(Instant::now());
            if rem.as_millis() == 0 {
                break;
            }

            match &mut stage {
                Stage::Ready | Stage::Complete => {
                    /*
                     * If we are not working on something, just sleep for a
                     * second and ping again in case there is a new directive.
                     */
                    sleep_ms(1000).await;
                    break;
                }
                Stage::NextTask => {
                    let job = cw.job.as_ref().unwrap();

                    /*
                     * If any task fails, we will not execute subsequent tasks.
                     * In case it is useful for diagnostic purposes, we will
                     * still attempt to upload output files before we complete
                     * the job.
                     */
                    let failures = exit_details.iter().any(|ex| ex.code != 0);
                    if failures {
                        println!("aborting after failed task");
                    }

                    if failures || tasks.is_empty() {
                        /*
                         * There are no more tasks to complete, so move on to
                         * uploading outputs.
                         */
                        println!("no more tasks for for job {}", job.id);

                        stage = Stage::Upload(upload::upload(
                            cw.clone(),
                            job.output_rules.clone(),
                        ));
                        break;
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
                            "/usr/sbin:/usr/bin:/sbin:/opt/ooce/bin:\
                            /opt/ooce/sbin",
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
                                .worker_job_append(
                                    job.id.as_str(),
                                    &WorkerAppendJob {
                                        stream: "agent".into(),
                                        time: Utc::now(),
                                        payload: format!(
                                            "ERROR: exec: {:?}",
                                            e
                                        ),
                                    },
                                )
                                .await
                                .ok();
                        }
                    }
                }
                Stage::Child(ch, t, failed) => {
                    match ch.recv_timeout(rem) {
                        Ok(Activity::Output(o)) => {
                            cw.append_task(t, &o.to_record()).await;
                        }
                        Ok(Activity::Exit(ex)) => {
                            let msg = format!(
                                "process exited: \
                                    duration {} ms, exit code {}",
                                ex.duration_ms, ex.code
                            );
                            let rec = OutputRecord {
                                stream: "task".to_string(),
                                msg,
                                time: ex.when,
                            };
                            cw.append_task(t, &rec).await;

                            /*
                             * Preserve the exit status for when we record task
                             * completion, and so that we can determine whether
                             * to execute any subsequent tasks.
                             */
                            *failed = Some(ex.code != 0);
                            exit_details.push(ex);

                            break;
                        }
                        Ok(Activity::Complete) => {
                            /*
                             * Record completion of this task within the job.
                             */
                            cw.task_complete(t, failed.unwrap()).await;
                            stage = Stage::NextTask;
                            break;
                        }
                        Err(mpsc::RecvTimeoutError::Timeout) => {
                            break;
                        }
                        Err(mpsc::RecvTimeoutError::Disconnected) => {
                            stage = Stage::Complete;
                            cw.append_msg("channel disconnected").await;

                            cw.job_complete(true).await;
                            break;
                        }
                    }
                }
                Stage::Download(ch) => match ch.recv_timeout(rem) {
                    Ok(download::Activity::Downloading(p)) => {
                        cw.append_msg(&format!(
                            "downloading input: {}",
                            p.display(),
                        ))
                        .await;
                    }
                    Ok(download::Activity::Downloaded(p)) => {
                        cw.append_msg(&format!(
                            "downloaded input: {}",
                            p.display(),
                        ))
                        .await;
                    }
                    Ok(download::Activity::Complete) => {
                        stage = Stage::NextTask;
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        break;
                    }
                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                        cw.append_msg("download channel disconnected").await;
                        cw.job_complete(true).await;
                        stage = Stage::Complete;
                        break;
                    }
                },
                Stage::Upload(ch) => match ch.recv_timeout(rem) {
                    Ok(upload::Activity::Scanned(count)) => {
                        cw.append_msg(&format!("found {} output files", count))
                            .await;
                    }
                    Ok(upload::Activity::Uploading(p, sz)) => {
                        cw.append_msg(&format!(
                            "uploading: {} ({} bytes)",
                            p.display(),
                            sz
                        ))
                        .await;
                    }
                    Ok(upload::Activity::Uploaded(p)) => {
                        cw.append_msg(&format!("uploaded: {}", p.display()))
                            .await;
                    }
                    Ok(upload::Activity::Complete) => {
                        let failed = upload_errors
                            || exit_details.iter().any(|ex| ex.code != 0);
                        cw.job_complete(failed).await;
                        stage = Stage::Complete;
                        break;
                    }
                    Ok(upload::Activity::Error(s)) => {
                        cw.append_msg(&format!("upload error: {:?}", s)).await;
                        upload_errors = true;
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        break;
                    }
                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                        cw.append_msg("upload channel disconnected").await;
                        cw.job_complete(true).await;
                        stage = Stage::Complete;
                        break;
                    }
                },
            }
        }
    }
}
