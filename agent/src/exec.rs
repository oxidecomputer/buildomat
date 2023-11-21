/*
 * Copyright 2023 Oxide Computer Company
 */

use std::collections::HashMap;
use std::ffi::OsString;
use std::io::{BufRead, BufReader, Read};
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{channel, Receiver, Sender};

use anyhow::{anyhow, bail, Result};
use chrono::prelude::*;

use super::OutputRecord;

fn spawn_reader<T>(
    tx: Sender<Activity>,
    name: String,
    stream: Option<T>,
) -> Option<std::thread::JoinHandle<()>>
where
    T: Read + Send + 'static,
{
    let stream = match stream {
        Some(stream) => stream,
        None => return None,
    };

    Some(std::thread::spawn(move || {
        let mut r = BufReader::new(stream);

        loop {
            let mut buf: Vec<u8> = Vec::new();

            /*
             * We have no particular control over the output from the child
             * processes we run, so we read until a newline character without
             * relying on totally valid UTF-8 output.
             */
            match r.read_until(b'\n', &mut buf) {
                Ok(0) => {
                    /*
                     * EOF.
                     */
                    return;
                }
                Ok(_) => {
                    let s = String::from_utf8_lossy(&buf);

                    if tx
                        .blocking_send(Activity::msg(&name, s.trim_end()))
                        .is_err()
                    {
                        /*
                         * If the channel is not available, give up and close
                         * the stream.
                         */
                        return;
                    }
                }
                Err(e) => {
                    /*
                     * Try to report whatever error we experienced to the
                     * server, but don't panic if we cannot.
                     */
                    tx.blocking_send(Activity::msg(
                        "error",
                        &format!("failed to read {}: {:?}", name, e),
                    ))
                    .ok();
                    return;
                }
            }
        }
    }))
}

#[derive(Debug)]
pub struct ExitDetails {
    stream: String,
    duration_ms: u64,
    when: DateTime<Utc>,
    code: i32,
}

impl ExitDetails {
    pub(crate) fn to_record(&self) -> OutputRecord {
        OutputRecord {
            stream: self.stream.to_string(),
            msg: format!(
                "process exited: duration {} ms, exit code {}",
                self.duration_ms, self.code
            ),
            time: self.when,
        }
    }

    pub fn failed(&self) -> bool {
        self.code != 0
    }
}

#[derive(Clone, Debug)]
pub struct OutputDetails {
    stream: String,
    msg: String,
    time: DateTime<Utc>,
}

impl OutputDetails {
    pub(crate) fn to_record(&self) -> OutputRecord {
        OutputRecord {
            stream: self.stream.to_string(),
            msg: self.msg.to_string(),
            time: self.time,
        }
    }
}

#[derive(Debug)]
pub enum Activity {
    Output(OutputDetails),
    Exit(ExitDetails),
    Complete,
}

struct ActivityBuilder {
    error_stream: String,
    exit_stream: String,
    bgproc: Option<String>,
}

impl ActivityBuilder {
    fn exit(&self, start: &Instant, end: &Instant, code: i32) -> Activity {
        Activity::Exit(ExitDetails {
            stream: self.exit_stream.to_string(),
            duration_ms: end.duration_since(*start).as_millis() as u64,
            when: Utc::now(),
            code,
        })
    }

    fn stdout_stream(&self) -> String {
        if let Some(n) = &self.bgproc {
            format!("bg.{n}.stdout")
        } else {
            "stdout".to_string()
        }
    }

    fn stderr_stream(&self) -> String {
        if let Some(n) = &self.bgproc {
            format!("bg.{n}.stderr")
        } else {
            "stderr".to_string()
        }
    }

    fn errmsg(&self, pfx: &str, msg: &str) -> String {
        let mut s = format!("{pfx}: ");
        if let Some(bg) = &self.bgproc {
            s += &format!("background process {bg:?}: ");
        }
        s += ": ";
        s += msg;
        s
    }

    fn err(&self, msg: &str) -> Activity {
        Activity::Output(OutputDetails {
            stream: self.error_stream.to_string(),
            msg: self.errmsg("exec error", msg),
            time: Utc::now(),
        })
    }

    fn warn(&self, msg: &str) -> Activity {
        Activity::Output(OutputDetails {
            stream: self.error_stream.to_string(),
            msg: self.errmsg("exec warning", msg),
            time: Utc::now(),
        })
    }
}

impl Activity {
    fn msg(stream: &str, msg: &str) -> Activity {
        Activity::Output(OutputDetails {
            stream: stream.to_string(),
            msg: msg.to_string(),
            time: Utc::now(),
        })
    }
}

pub fn thread_done(
    t: &mut Option<std::thread::JoinHandle<()>>,
    name: &str,
    until: Instant,
) -> bool {
    loop {
        if t.is_none() {
            /*
             * Nothing left to wait for!
             */
            return true;
        }

        if Instant::now() > until {
            /*
             * We have waited as long as we can, but the thread is still
             * running.
             */
            return false;
        }

        if !t.as_ref().map(|t| t.is_finished()).unwrap_or(false) {
            /*
             * Keep waiting.
             */
            std::thread::sleep(Duration::from_millis(50));
            continue;
        }

        t.take().unwrap().join().expect(&format!("join {} thread", name));
        return true;
    }
}

pub fn run(cmd: Command) -> Result<Receiver<Activity>> {
    let (tx, rx) = channel::<Activity>(64);

    run_common(
        cmd,
        ActivityBuilder {
            error_stream: "worker".to_string(),
            exit_stream: "task".to_string(),
            bgproc: None,
        },
        tx,
    )?;

    Ok(rx)
}

fn run_common(
    mut cmd: Command,
    ab: ActivityBuilder,
    tx: Sender<Activity>,
) -> Result<u32> {
    cmd.stdin(Stdio::null());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let start = Instant::now();
    let mut child = cmd.spawn()?;
    let pid = child.id();

    let mut readout =
        spawn_reader(tx.clone(), ab.stdout_stream(), child.stdout.take());
    let mut readerr =
        spawn_reader(tx.clone(), ab.stderr_stream(), child.stderr.take());

    std::thread::spawn(move || {
        let wait = child.wait();
        let end = Instant::now();
        let stdio_warning = match wait {
            Err(e) => {
                tx.blocking_send(
                    ab.err(&format!("child wait failed: {:?}", e)),
                )
                .unwrap();

                /*
                 * Only send an exit notification if this is the primary task
                 * process.
                 */
                if ab.bgproc.is_none() {
                    tx.blocking_send(ab.exit(&start, &end, std::i32::MAX))
                        .unwrap();
                }

                false
            }
            Ok(es) => {
                /*
                 * Wait up to five seconds for stdio threads to flush out before
                 * we report exit status.  In general we expect this to complete
                 * within milliseconds.  This is really just cosmetic; we would
                 * like, when possible, the final log output of a command to
                 * appear prior to its exit status.
                 */
                let until =
                    Instant::now().checked_add(Duration::from_secs(5)).unwrap();
                let stdio_warning = !thread_done(&mut readout, "stdout", until)
                    | !thread_done(&mut readerr, "stderr", until);

                if ab.bgproc.is_some() {
                    /*
                     * No further notifications are required for background
                     * processes.
                     */
                }

                if let Some(sig) = es.signal() {
                    tx.blocking_send(
                        ab.warn(&format!("child terminated by signal {}", sig)),
                    )
                    .unwrap();
                }
                let code = if let Some(code) = es.code() {
                    code
                } else {
                    std::i32::MAX
                };
                tx.blocking_send(ab.exit(&start, &end, code)).unwrap();
                stdio_warning
            }
        };

        assert!(ab.bgproc.is_none());

        if stdio_warning {
            tx.blocking_send(ab.warn(&format!(
                "stdio descriptors remain open after task exit; \
                waiting 60 seconds for them to close",
            )))
            .unwrap();
        }

        let until =
            Instant::now().checked_add(Duration::from_secs(60)).unwrap();
        if !thread_done(&mut readout, "stdout", until) {
            tx.blocking_send(ab.warn(
                "stdout descriptor may be held open by a background process; \
                giving up!",
            ))
            .unwrap();
        }
        if !thread_done(&mut readerr, "stderr", until) {
            tx.blocking_send(ab.warn(
                "stderr descriptor may be held open by a background process; \
                giving up!",
            ))
            .unwrap();
        }

        tx.blocking_send(Activity::Complete).unwrap();
    });

    Ok(pid)
}

pub struct BackgroundProcesses {
    rx: Receiver<Activity>,
    tx: Sender<Activity>,
    procs: HashMap<String, Proc>,
}

struct Proc {
    pid: u32,
}

impl BackgroundProcesses {
    pub fn new() -> Self {
        let (tx, rx) = channel::<Activity>(64);

        BackgroundProcesses { rx, tx, procs: Default::default() }
    }

    pub fn start(
        &mut self,
        name: &str,
        cmd: &str,
        args: &Vec<String>,
        env: &Vec<(OsString, OsString)>,
        pwd: &str,
        uid: u32,
        gid: u32,
    ) -> Result<u32> {
        /*
         * Process name must be unique within the task.
         */
        if self.procs.contains_key(name) {
            bail!("background process {name:?} is already running");
        }

        #[cfg(target_os = "illumos")]
        let mut c = {
            /*
             * Run the process under a new contract using ctrun(1).
             */
            let mut c = Command::new("/usr/bin/ctrun");
            /*
             * The contract is marked noorphan so that when we kill the ctrun
             * child, the whole contract is torn down:
             */
            c.arg("-o").arg("noorphan");
            /*
             * Set the lifetime to child, so that ctrun exits when its immediate
             * child terminates, tearing down the rest of the children:
             */
            c.arg("-l").arg("child");
            c.arg(cmd);
            c
        };

        #[cfg(not(target_os = "illumos"))]
        let mut c = {
            /*
             * Regrettably other operating systems do not have contracts.  For
             * now, just start the program.
             */
            Command::new(cmd)
        };

        for a in args.iter() {
            c.arg(a);
        }

        /*
         * Use the environment, working directory, and credentials passed to us
         * by the control program, not our own:
         */
        c.current_dir(pwd);
        c.env_clear();
        for (k, v) in env.iter() {
            c.env(k, v);
        }
        c.uid(uid);
        c.gid(gid);

        let pid = run_common(
            c,
            ActivityBuilder {
                error_stream: format!("bg.{name}"),
                exit_stream: format!("bg.{name}"),
                bgproc: Some(name.to_string()),
            },
            self.tx.clone(),
        )
        .map_err(|e| anyhow!("starting background process {name:?}: {e}"))?;

        self.procs.insert(name.to_string(), Proc { pid });

        Ok(pid)
    }

    pub async fn recv(&mut self) -> Option<Activity> {
        self.rx.recv().await
    }

    pub async fn killall(&mut self) -> Vec<Activity> {
        if self.procs.is_empty() {
            return Default::default();
        }

        /*
         * Allow a short grace period for background tasks to emit a few final
         * messages prior to sending the shutdown signals.
         */
        std::thread::sleep(Duration::from_secs(2));

        for (_, p) in self.procs.drain() {
            let pid = p.pid.try_into().unwrap();

            /*
             * Ask the child process to exit.
             */
            unsafe { libc::kill(pid, libc::SIGTERM) };

            /*
             * Give each process ten seconds to exit after SIGTERM hits.
             */
            let deadline =
                Instant::now().checked_add(Duration::from_secs(10)).unwrap();
            loop {
                if unsafe { libc::kill(pid, 0) } != 0 {
                    break;
                }

                if deadline < Instant::now() {
                    /*
                     * I'm afraid we really must ask you to leave:
                     */
                    unsafe { libc::kill(pid, libc::SIGKILL) };
                    break;
                }

                std::thread::sleep(Duration::from_millis(100));
            }
        }

        assert!(self.procs.is_empty());

        /*
         * One last sleep to let the stdio threads push data into the event
         * append queue.
         */
        std::thread::sleep(Duration::from_secs(2));

        let mut lastwords = Vec::new();
        self.rx.close();
        while let Some(a) = self.rx.recv().await {
            if let Activity::Output(o) = &a {
                if o.stream.ends_with("stdout") || o.stream.ends_with("stderr")
                {
                    lastwords.push(a);
                }
            }
        }

        /*
         * Replace the channels with new channels for the next task.
         */
        let (tx, rx) = channel::<Activity>(100);
        self.rx = rx;
        self.tx = tx;

        lastwords
    }
}
