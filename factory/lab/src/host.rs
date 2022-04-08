use std::io::{BufRead, Read};
use std::os::unix::prelude::FromRawFd;
use std::os::unix::process::CommandExt;
use std::process::Command;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Error, Result};
use buildomat_common::*;
use serde::Deserialize;
use slog::{debug, error, info, trace, warn};

use super::{Activity, Central, Message};

fn ipmitool(config: &super::config::ConfigFileHost) -> Command {
    let mut cmd = Command::new("/usr/sbin/ipmitool");
    cmd.env_clear();
    cmd.arg("-C");
    cmd.arg("3");
    cmd.arg("-I");
    cmd.arg("lanplus");
    cmd.arg("-H");
    cmd.arg(&config.lom_ip);
    cmd.arg("-U");
    cmd.arg(&config.lom_username);
    cmd.arg("-P");
    cmd.arg(&config.lom_password);
    cmd
}

fn spawn_thread_reader<T>(
    nodename: &str,
    tx: &mpsc::Sender<Activity>,
    error: bool,
    stream: Option<T>,
) -> Option<thread::JoinHandle<()>>
where
    T: Read + Send + 'static,
{
    let nodename = nodename.to_string();
    let tx = tx.clone();
    let stream = if let Some(stream) = stream {
        stream
    } else {
        return None;
    };

    let t = thread::Builder::new()
        .name(format!("read-{}", nodename))
        .spawn(move || {
            let mut r = std::io::BufReader::new(stream);
            let mut linebuf: Vec<u8> = Vec::new();
            let a = super::ActivityBuilder { nodename: nodename.to_string() };

            loop {
                let mut buf = [0u8; 1];

                /*
                 * We have no particular control over the output from the
                 * child processes we run, so we read until a newline or
                 * carriage return character without relying on totally
                 * valid UTF-8 output.
                 */
                match r.read(&mut buf) {
                    Ok(0) => {
                        /*
                         * EOF.
                         */
                        return;
                    }
                    Ok(1)
                        if buf[0] == b'\n'
                            || buf[0] == b'\r'
                            || (linebuf.contains(&b'\x1b')
                                && linebuf.len() > 64) =>
                    {
                        /*
                         * New line or carriage return.
                         */
                        let s = String::from_utf8_lossy(&linebuf);
                        let s = s.replace('\x1b', "^[");
                        let s = s.trim();
                        if !s.is_empty() {
                            if error {
                                tx.send(a.status(&format!(
                                    "ipmitool stderr: {:?}",
                                    s
                                )))
                                .unwrap();
                            } else {
                                tx.send(a.serial(s)).unwrap();
                            }
                        }
                        linebuf.clear();
                    }
                    Ok(1) => {
                        linebuf.push(buf[0]);
                    }
                    Ok(n) => {
                        eprintln!("read was {}, too long!", n);
                        std::process::exit(100);
                    }
                    Err(e) => {
                        eprintln!("failed to read: {}", e);
                        std::process::exit(100);
                    }
                }
            }
        })
        .unwrap();

    Some(t)
}

fn thread_serial(
    hc: &super::config::ConfigFileHost,
    tx: &mpsc::Sender<Activity>,
) -> Result<()> {
    /*
     * First, attempt to deactivate any existing SOL session.
     */
    let mut cmd = ipmitool(hc);
    cmd.arg("sol");
    cmd.arg("deactivate");
    let res = cmd.output().context("sol deactivate")?;
    if !res.status.success() {
        let err = String::from_utf8_lossy(&res.stderr);

        /*
         * If there was not already an active session, this command will fail
         * but we can ignore the failure.
         */
        if !err.contains("SOL payload already de-activated") {
            bail!("sol deactivate failure: {}", res.info());
        }
    }

    /*
     * Then, we want to spawn a child to receive SOL data.
     */
    let mut cmd = ipmitool(hc);
    cmd.arg("sol");
    cmd.arg("activate");

    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(std::process::Stdio::null());
    cmd.stderr(std::process::Stdio::null());

    let mut pty = crate::pty::Pty::new()?;
    let sub = pty.subsidiary();

    unsafe {
        cmd.pre_exec(move || {
            //unsafe {
            libc::setsid();
            libc::ioctl(sub, libc::TIOCSCTTY, 0);
            libc::dup2(sub, 0);
            libc::dup2(sub, 1);
            libc::dup2(sub, 2);
            libc::close(sub);
            //}
            Ok(())
        })
    };

    let mut child = cmd.spawn().context("sol activate")?;

    spawn_thread_reader(&hc.nodename, tx, false, Some(pty.manager()))
        .expect("start reader thread")
        .join()
        .expect("join reader thread");

    match child.wait() {
        Err(e) => bail!("child wait failure: {:?}", e),
        Ok(st) => {
            if !st.success() {
                bail!("ipmitool sol activate failure: {:?}", st);
            }

            /*
             * Really the tool should not exit at all, but if it does we will
             * restart it.
             * XXX make a status message
             */
            Ok(())
        }
    }
}

pub(crate) fn thread_manager(
    c: &Central,
    rx: &mpsc::Receiver<super::Activity>,
) -> Result<()> {
    let log = c.log.clone();

    loop {
        /*
         * Process events for at most one second at a time:
         */
        let end = Instant::now().checked_add(Duration::from_secs(1)).unwrap();
        loop {
            let rem = end.saturating_duration_since(Instant::now());
            if rem.as_millis() == 0 {
                break;
            }

            match rx.recv_timeout(rem) {
                Ok(a) => {
                    if let Some(i) = c.db.instance_for_host(&a.nodename)? {
                        match a.message {
                            Message::Status(v) => {
                                info!(log, "[{}] status: {:?}", i.id(), v);
                            }
                            Message::SerialOutput(v) => {
                                info!(log, "[{}] serial: {:?}", i.id(), v);
                                let dialtone = v.contains(super::MARKER_HOLD);

                                if dialtone {
                                    info!(
                                        log,
                                        "dialtone from host {}", a.nodename
                                    );
                                    c.hosts
                                        .get(&a.nodename)
                                        .unwrap()
                                        .state
                                        .lock()
                                        .unwrap()
                                        .record_dialtone();
                                }

                                if i.should_teardown() {
                                    /*
                                     * If we are tearing down, we are just
                                     * waiting for the dialtone.
                                     */
                                    continue;
                                }

                                if !i.is_preboot() && dialtone {
                                    warn!(
                                        log,
                                        "dialtone for {}; destroying",
                                        i.id()
                                    );
                                    c.hosts
                                        .get(&a.nodename)
                                        .unwrap()
                                        .state
                                        .lock()
                                        .unwrap()
                                        .reset();
                                    c.db.instance_destroy(&i)?;
                                    continue;
                                }

                                c.db.instance_append(
                                    &i, "console", &v, a.time,
                                )?;
                            }
                        }
                    } else {
                        match a.message {
                            Message::Status(v) => {
                                info!(log, "[{}] status: {:?}", a.nodename, v);
                            }
                            Message::SerialOutput(v) => {
                                info!(log, "[{}] serial: {:?}", a.nodename, v);
                                if v.contains(super::MARKER_HOLD) {
                                    info!(
                                        log,
                                        "dialtone from host {}", a.nodename
                                    );

                                    c.hosts
                                        .get(&a.nodename)
                                        .unwrap()
                                        .state
                                        .lock()
                                        .unwrap()
                                        .record_dialtone();
                                }
                            }
                        }
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    bail!("rx channel disconnected?");
                }
            }
        }

        trace!(log, "instance housekeeping");
        for i in c.db.active_instances()? {
            match i.state {
                crate::db::InstanceState::Preboot => {}
                crate::db::InstanceState::Booted => {}
                crate::db::InstanceState::Destroying => {
                    if c.db.instance_next_event_to_upload(&i)?.is_some() {
                        /*
                         * If we still have events to upload, we need to wait
                         * for that to complete before marking the instance as
                         * destroyed.
                         */
                        continue;
                    }

                    let mut h =
                        c.hosts.get(&i.nodename).unwrap().state.lock().unwrap();
                    if h.has_dialtone() {
                        /*
                         * If we have the dialtone, we are all done.
                         */
                        info!(log, "host {} back to idle pool", i.nodename);
                        c.db.instance_mark_destroyed(&i)?;
                        continue;
                    }

                    /*
                     * Otherwise, wait for the most recent issued reset to
                     * complete.  It is hard to know how long to wait, but if we
                     * do not see a dialtone in five minutes it is worth kicking
                     * the server again.
                     */
                    if !h.need_cycle()
                        && h.since_last_cycle().as_secs() > 5 * 60
                    {
                        warn!(log, "host {} awol for 5 minutes?", i.nodename);
                        h.reset();
                        continue;
                    }
                }
                crate::db::InstanceState::Destroyed => {}
            }
        }

        trace!(log, "power control housekeeping");
        for h in c.hosts.iter() {
            let mut st = h.1.state.lock().unwrap();

            if st.need_cycle() {
                info!(log, "need to reset host {}", h.0);

                /*
                 * First, just in case someone has turned off the server, try to
                 * make sure it is indeed powered on:
                 */
                let mut cmd = ipmitool(&h.1.config);
                cmd.arg("power");
                cmd.arg("on");
                let res = cmd.output().context("power on")?;
                if !res.status.success() {
                    bail!("power on failure: {}", res.info());
                }

                let mut cmd = ipmitool(&h.1.config);
                cmd.arg("power");
                cmd.arg("cycle");
                let res = cmd.output().context("power cycle")?;
                if !res.status.success() {
                    bail!("power cycle failure: {}", res.info());
                }

                info!(log, "power cycled host {}", h.0);
                st.record_cycle();
            }
        }
    }
}

pub(crate) fn start_manager(
    c: Arc<Central>,
    rx: mpsc::Receiver<super::Activity>,
) {
    /*
     * Create the thread that will receive messages from all of the
     * IPMI serial-over-LAN threads, and perform house-keeping for unallocated
     * hosts.
     */
    let c0 = Arc::clone(&c);
    thread::Builder::new()
        .name("host-manager".into())
        .spawn(move || loop {
            let log = c0.log.clone();

            if let Err(e) = thread_manager(&c0, &rx) {
                error!(log, "host manager thread error: {:?}", e);
            }

            thread::sleep(Duration::from_secs(1));
        })
        .unwrap();

    /*
     * For each host, start an IPMI serial-over-LAN listener.
     */
    for (nodename, host) in &c.hosts {
        let log = c.log.clone();
        let nodename = nodename.to_string();
        let hc = host.config.clone();
        let tx = c.tx.lock().unwrap().clone();
        thread::Builder::new()
            .name(format!("serial-{}", nodename))
            .spawn(move || loop {
                if let Err(e) = thread_serial(&hc, &tx) {
                    error!(
                        log,
                        "host {} serial thread error: {:?}", nodename, e
                    );
                }

                thread::sleep(Duration::from_secs(1));
            })
            .unwrap();
    }
}
