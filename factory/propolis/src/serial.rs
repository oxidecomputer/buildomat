/*
 * Copyright 2023 Oxide Computer Company
 */

use std::{
    collections::HashMap,
    ffi::CString,
    os::{fd::AsRawFd, unix::prelude::PermissionsExt},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::ucred::PeerUCred;
use anyhow::{anyhow, bail, Result};
use libc::zoneid_t;
use slog::{debug, error, info, o, trace, warn, Logger};
use tokio::net::UnixListener;
use tokio::{io::Interest, net::UnixStream};

#[derive(Debug)]
pub enum SerialData {
    Line(String),
}

pub struct SerialForZone {
    name: String,
    zoneid: zoneid_t,
    rx: tokio::sync::mpsc::Receiver<SerialData>,
    shutdown: tokio::sync::watch::Sender<bool>,
    inner: Arc<Inner>,
    cancelled: bool,
}

impl SerialForZone {
    pub fn cancel(&mut self) {
        if self.cancelled {
            return;
        }

        /*
         * Remove the zone from the active set and shut down any socket tasks.
         */
        let mut zones = self.inner.zones.lock().unwrap();
        let removed = zones.remove(&self.zoneid).unwrap();
        assert_eq!(removed.name, self.name);
        assert_eq!(removed.zoneid, self.zoneid);
        self.shutdown.send(true).ok();
        self.cancelled = true;
    }

    pub async fn recv(&mut self) -> Result<SerialData> {
        self.rx
            .recv()
            .await
            .ok_or_else(|| anyhow!("tx side of channel closed?"))
    }
}

impl Drop for SerialForZone {
    fn drop(&mut self) {
        self.cancel();
    }
}

#[derive(Clone)]
pub struct Serial(Arc<Inner>);

#[derive(Clone)]
struct Zone {
    name: String,
    zoneid: zoneid_t,
    tx: tokio::sync::mpsc::Sender<SerialData>,
    shutdown: tokio::sync::watch::Receiver<bool>,
}

pub struct Inner {
    zones: Mutex<HashMap<zoneid_t, Zone>>,
}

impl Serial {
    pub fn new(log: Logger, socketdir: PathBuf) -> Result<Serial> {
        let sockpath = socketdir.join("sock");

        /*
         * Always remove the socket as there might be a stale one from prior
         * restarted processes.
         */
        std::fs::remove_file(&sockpath).ok();

        /*
         * Bind a new listen socket.
         */
        let listen = tokio::net::UnixListener::bind(&sockpath)?;

        /*
         * Allow everyone to connect:
         */
        let mut perm = std::fs::metadata(&sockpath)?.permissions();
        perm.set_mode(0o777);
        std::fs::set_permissions(&sockpath, perm)?;

        let s0 =
            Serial(Arc::new(Inner { zones: Mutex::new(Default::default()) }));

        /*
         * Start accept task.
         */
        let s = s0.clone();
        tokio::spawn(async move {
            s.accept_task(log, listen).await;
        });

        Ok(s0)
    }

    async fn accept_task(&self, log: Logger, listen: UnixListener) -> ! {
        loop {
            let (sock, zoneid) = match listen.accept().await {
                Ok((sock, _)) => {
                    match sock.peer_ucred() {
                        Ok(uc) => {
                            let Some(zoneid) = uc.zoneid() else {
                                error!(log, "could not get zone ID for sock");
                                continue;
                            };

                            if zoneid == 0 {
                                /*
                                 * We never want connections from the global
                                 * zone.
                                 */
                                error!(log, "incoming socket from GZ?!");
                                continue;
                            }

                            (sock, zoneid)
                        }
                        Err(e) => {
                            error!(log, "incoming socket: {e}");
                            continue;
                        }
                    }

                    /*
                     * Then we need to determine if this is a zone that we
                     * actually want to hear from or not.
                     *
                     * If it is, start a task that shovels serial data from the
                     * zone side to our channel for that zone.
                     */
                }
                Err(e) => {
                    error!(log, "accept error: {e}");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };

            debug!(log, "incoming socket connection from zone ID {zoneid}");

            let zones = self.0.zones.lock().unwrap();
            let Some(z) = zones.get(&zoneid) else {
                error!(
                    log,
                    "incoming socket connection from \
                    zone ID {zoneid} which is not known to us",
                );
                continue;
            };

            let zlog = log.new(o!(
                "sock" => format!("{}/fd:{}", z.name, sock.as_raw_fd()),
            ));
            let z = z.clone();
            tokio::spawn(async move {
                zone_serial_task(zlog, z, sock).await;
            });
        }
    }

    pub fn zone_add(&self, name: &str) -> Result<SerialForZone> {
        let zoneid = zone_name_to_id(name)?;

        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let (shut_tx, shut_rx) = tokio::sync::watch::channel(false);

        /*
         * Make sure this zone is not already added.
         */
        let mut zones = self.0.zones.lock().unwrap();
        if zones.contains_key(&zoneid) {
            bail!("zone {name:?} (id {zoneid} already listening for serial");
        }
        zones.insert(
            zoneid,
            Zone { name: name.to_string(), zoneid, tx, shutdown: shut_rx },
        );

        /*
         * Return the handle that allows the caller to listen for serial output.
         */
        let z = SerialForZone {
            name: name.to_string(),
            zoneid,
            rx,
            shutdown: shut_tx,
            inner: Arc::clone(&self.0),
            cancelled: false,
        };

        Ok(z)
    }
}

#[link(name = "c")]
extern "C" {
    fn getzoneidbyname(name: *const libc::c_char) -> zoneid_t;
}

fn zone_name_to_id(name: &str) -> Result<zoneid_t> {
    let cs = CString::new(name)?;

    let id = unsafe { getzoneidbyname(cs.as_ptr()) };
    if id < 0 {
        let e = std::io::Error::last_os_error();
        bail!("getzoneidbyname({name}): {e}");
    }

    Ok(id)
}

fn clean_line(linebuf: &[u8]) -> Option<String> {
    let s = String::from_utf8_lossy(linebuf);
    let s = s.replace('\x1b', "^[");
    let s = s.trim();
    if !s.is_empty() {
        Some(s.to_string())
    } else {
        None
    }
}

async fn zone_serial_task(log: Logger, mut z: Zone, sock: UnixStream) {
    if *z.shutdown.borrow() {
        warn!(log, "zst shutdown before getting started!");
    }

    let mut linebuf: Vec<u8> = Default::default();

    loop {
        let ready = tokio::select! {
            _ = z.shutdown.wait_for(|shutdown| *shutdown) => {
                None
            }
            ready = sock.ready(Interest::READABLE) => {
                Some(ready)
            }
        };

        match ready {
            None => {
                info!(log, "zst shutdown");
                if let Some(s) = clean_line(&linebuf) {
                    z.tx.send(SerialData::Line(s.to_string())).await.ok();
                }
                return;
            }
            Some(Err(e)) => {
                error!(log, "zst sock ready error: {e}");
                return;
            }
            Some(Ok(_)) => (),
        }

        /*
         * We have no particular control over the output from the guest, so we
         * read until a newline or carriage return character without relying on
         * totally valid UTF-8 output.
         */
        let mut buf = [0u8; 1];
        match sock.try_read(&mut buf) {
            Ok(0) => {
                /*
                 * EOF.
                 */
                trace!(log, "zst EOF");
                return;
            }
            Ok(1) if buf[0] == 0x08 => {
                trace!(log, "zst backspace byte {:?}", buf[0]);

                /*
                 * We assume that the intent of a backspace character is to
                 * remove and ovewrite the last character on the line.
                 */
                if !linebuf.is_empty() {
                    linebuf.truncate(linebuf.len().checked_sub(1).unwrap());
                }
            }
            Ok(1)
                if buf[0] == b'\n'
                    || buf[0] == b'\r'
                    || (linebuf.contains(&b'\x1b') && linebuf.len() > 64) =>
            {
                trace!(log, "zst newline byte {:?}", buf[0]);

                /*
                 * New line or carriage return.
                 */
                if let Some(s) = clean_line(&linebuf) {
                    if let Err(e) =
                        z.tx.send(SerialData::Line(s.to_string())).await
                    {
                        warn!(log, "zst send error, shutting down: {e}");
                        return;
                    }
                }
                linebuf.clear();
            }
            Ok(1) => {
                trace!(log, "zst byte {:?}", buf[0]);

                linebuf.push(buf[0]);
            }
            Ok(_) => unreachable!(),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => (),
            Err(e) => {
                error!(log, "zst read error: {e}");
                return;
            }
        }
    }
}
