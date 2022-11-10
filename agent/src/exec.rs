/*
 * Copyright 2021 Oxide Computer Company
 */

use std::io::{BufRead, BufReader, Read};
use std::os::unix::process::ExitStatusExt;
use std::process::{Command, Stdio};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::time::{Duration, Instant};

use anyhow::Result;
use chrono::prelude::*;

use super::OutputRecord;

fn spawn_reader<T>(
    tx: Sender<Activity>,
    name: &str,
    stream: Option<T>,
) -> Option<std::thread::JoinHandle<()>>
where
    T: Read + Send + 'static,
{
    let name = name.to_string();
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

                    if tx.send(Activity::msg(&name, s.trim_end())).is_err() {
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
                    tx.send(Activity::msg(
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

pub struct ExitDetails {
    pub duration_ms: u64,
    pub when: DateTime<Utc>,
    pub code: i32,
}

#[derive(Clone)]
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

pub enum Activity {
    Output(OutputDetails),
    Exit(ExitDetails),
    Complete,
}

impl Activity {
    fn exit(start: &Instant, end: &Instant, code: i32) -> Activity {
        Activity::Exit(ExitDetails {
            duration_ms: end.duration_since(*start).as_millis() as u64,
            when: Utc::now(),
            code,
        })
    }

    fn msg(stream: &str, msg: &str) -> Activity {
        Activity::Output(OutputDetails {
            stream: stream.to_string(),
            msg: msg.to_string(),
            time: Utc::now(),
        })
    }

    fn err(msg: &str) -> Activity {
        Activity::Output(OutputDetails {
            stream: "worker".to_string(),
            msg: format!("exec error: {}", msg),
            time: Utc::now(),
        })
    }

    fn warn(msg: &str) -> Activity {
        Activity::Output(OutputDetails {
            stream: "worker".to_string(),
            msg: format!("exec warning: {}", msg),
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

pub fn run(mut cmd: Command) -> Result<Receiver<Activity>> {
    let (tx, rx) = channel::<Activity>();

    cmd.stdin(Stdio::null());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let start = Instant::now();
    let mut child = cmd.spawn()?;

    let mut readout = spawn_reader(tx.clone(), "stdout", child.stdout.take());
    let mut readerr = spawn_reader(tx.clone(), "stderr", child.stderr.take());

    std::thread::spawn(move || {
        let wait = child.wait();
        let end = Instant::now();
        let stdio_warning = match wait {
            Err(e) => {
                tx.send(Activity::err(&format!("child wait failed: {:?}", e)))
                    .unwrap();
                tx.send(Activity::exit(&start, &end, std::i32::MAX)).unwrap();
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

                if let Some(sig) = es.signal() {
                    tx.send(Activity::warn(&format!(
                        "child terminated by signal {}",
                        sig
                    )))
                    .unwrap();
                }
                let code = if let Some(code) = es.code() {
                    code
                } else {
                    std::i32::MAX
                };
                tx.send(Activity::exit(&start, &end, code)).unwrap();
                stdio_warning
            }
        };

        if stdio_warning {
            tx.send(Activity::warn(&format!(
                "stdio descriptors remain open after task exit; \
                waiting 60 seconds for them to close",
            )))
            .unwrap();
        }

        let until =
            Instant::now().checked_add(Duration::from_secs(60)).unwrap();
        if !thread_done(&mut readout, "stdout", until) {
            tx.send(Activity::warn(
                "stdout descriptor may be held open by a background process; \
                giving up!",
            ))
            .unwrap();
        }
        if !thread_done(&mut readerr, "stderr", until) {
            tx.send(Activity::warn(
                "stderr descriptor may be held open by a background process; \
                giving up!",
            ))
            .unwrap();
        }

        tx.send(Activity::Complete).unwrap();
    });

    Ok(rx)
}
