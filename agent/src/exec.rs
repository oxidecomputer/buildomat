/*
 * Copyright 2021 Oxide Computer Company
 */

use std::io::{BufRead, BufReader, Read};
use std::os::unix::process::ExitStatusExt;
use std::process::{Command, Stdio};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::time::Instant;

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

                    tx.send(Activity::msg(&name, s.trim_end())).unwrap();
                }
                Err(e) => {
                    /*
                     * Try to report whatever error we experienced to the
                     * server:
                     */
                    tx.send(Activity::msg(
                        "error",
                        &format!("failed to read {}: {:?}", name, e),
                    ))
                    .unwrap();
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
            stream: "error".to_string(),
            msg: msg.to_string(),
            time: Utc::now(),
        })
    }
}

pub fn run(mut cmd: Command) -> Result<Receiver<Activity>> {
    let (tx, rx) = channel::<Activity>();

    cmd.stdin(Stdio::null());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let start = Instant::now();
    let mut child = cmd.spawn()?;

    let readout = spawn_reader(tx.clone(), "stdout", child.stdout.take());
    let readerr = spawn_reader(tx.clone(), "stderr", child.stderr.take());

    std::thread::spawn(move || {
        /*
         * Note that we explicitly join the stdout and stderr threads here
         * ensure that all I/O messages are passed to the channel before the
         * process error or exit message.
         */
        if let Some(t) = readout {
            t.join().expect("join stdout thread");
        }
        if let Some(t) = readerr {
            t.join().expect("join stderr thread");
        }

        let wait = child.wait();
        let end = Instant::now();
        match wait {
            Err(e) => {
                tx.send(Activity::err(&format!("child wait error: {:?}", e)))
                    .unwrap();
                tx.send(Activity::exit(&start, &end, std::i32::MAX)).unwrap();
            }
            Ok(es) => {
                if let Some(sig) = es.signal() {
                    tx.send(Activity::err(&format!(
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
            }
        }

        tx.send(Activity::Complete).unwrap();
    });

    Ok(rx)
}
