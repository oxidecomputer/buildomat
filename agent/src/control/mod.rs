/*
 * Copyright 2024 Oxide Computer Company
 */

use std::{io::Read, ops::Range, time::Duration};

use anyhow::{bail, Result};
use bytes::BytesMut;
use hiercmd::prelude::*;
use ipnet::IpAdd;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
};

use protocol::{Decoder, FactoryInfo, Message, Payload};

pub(crate) mod protocol;
pub(crate) mod server;

/// Default socket path.
const DEFAULT_SOCKET_PATH: &str = "/var/run/buildomat.sock";

/// Get the control socket path for client (bmat) connections.
/// Uses BUILDOMAT_SOCKET env var if set (set by agent when running jobs),
/// otherwise falls back to the default path.
fn socket_path() -> String {
    std::env::var("BUILDOMAT_SOCKET").unwrap_or_else(|_| DEFAULT_SOCKET_PATH.to_string())
}

struct Stuff {
    us: Option<UnixStream>,
    dec: Option<Decoder>,
    ids: Range<u64>,
}

impl Stuff {
    async fn connect(&mut self) -> Result<()> {
        self.us = Some(UnixStream::connect(socket_path()).await?);
        self.dec = Some(Decoder::new());
        Ok(())
    }

    async fn send_and_recv(&mut self, mout: &Message) -> Result<Message> {
        let us = self.us.as_mut().unwrap();
        let dec = self.dec.as_mut().unwrap();

        let buf = mout.pack()?;
        us.write_all(&buf).await?;

        let mut buf = BytesMut::with_capacity(4096);
        loop {
            buf.clear();
            let sz = us.read_buf(&mut buf).await?;
            if sz > 0 {
                dec.ingest_bytes(&buf);
            } else {
                dec.ingest_eof();
            }

            if dec.ended() {
                /*
                 * We have reached the end of the stream without a message.
                 */
                bail!("stream ended before reply");
            }

            if let Some(min) = dec.take()? {
                if min.id != mout.id {
                    bail!(
                        "sent ID {} but got reply with ID {}",
                        mout.id,
                        min.id,
                    );
                }
                return Ok(min);
            }
        }
    }
}

pub async fn main() -> Result<()> {
    let s = Stuff { us: None, dec: None, ids: (1u64..u64::MAX) };

    let mut l = Level::new("bmat", s);

    l.cmd("store", "access the job store", cmd!(cmd_store))?;
    l.cmd("address", "manage IP addresses for this job", cmd!(cmd_address))?;
    l.cmd("process", "manage background processes", cmd!(cmd_process))?;
    l.cmd("factory", "factory information for this worker", cmd!(cmd_factory))?;
    l.hcmd("eng", "for working on and testing buildomat", cmd!(cmd_eng))?;

    sel!(l).run().await
}

async fn cmd_address(mut l: Level<Stuff>) -> Result<()> {
    l.context_mut().connect().await?;

    l.cmda("list", "ls", "list IP addresses", cmd!(cmd_address_list))?;

    sel!(l).run().await
}

async fn cmd_address_list(mut l: Level<Stuff>) -> Result<()> {
    l.add_column("name", 16, true);
    l.add_column("cidr", 18, true);
    l.add_column("first", 15, true);
    l.add_column("last", 15, true);
    l.add_column("count", 5, true);
    l.add_column("family", 6, false);
    l.add_column("network", 15, false);
    l.add_column("mask", 15, false);
    l.add_column("routed", 6, false);
    l.add_column("gateway", 15, false);

    l.optopt("f", "", "find exactly one match for this name or fail", "NAME");

    let a = no_args!(l);

    let filter = a.opts().opt_str("f");

    let addrs = {
        let mout = Message {
            id: l.context_mut().ids.next().unwrap(),
            payload: Payload::MetadataAddresses,
        };

        match l.context_mut().send_and_recv(&mout).await {
            Ok(min) => match min.payload {
                Payload::Error(e) => {
                    bail!("WARNING: control request failure: {e}");
                }
                Payload::MetadataAddressesResult(addrs) => addrs,
                other => bail!("unexpected response: {other:?}"),
            },
            Err(e) => {
                /*
                 * Requests to the agent are relatively simple and over a UNIX
                 * socket; they should not fail.  This implies something has
                 * gone seriously wrong and it is unlikely that it will be fixed
                 * without intervention.  Don't retry.
                 */
                bail!("could not talk to the agent: {e}");
            }
        }
    };

    let mut t = a.table();
    let mut count = 0;

    for addr in addrs {
        if let Some(filter) = filter.as_deref() {
            if filter != addr.name {
                continue;
            }
        }

        let Ok(first) = addr.first.parse::<std::net::IpAddr>() else {
            continue;
        };
        let Ok(net) = addr.cidr.parse::<ipnet::IpNet>() else {
            continue;
        };
        if addr.count < 1 {
            continue;
        }

        match (net, first) {
            (ipnet::IpNet::V4(net), std::net::IpAddr::V4(ip)) => {
                if !net.contains(&ip)
                    || net.network() == ip && net.broadcast() == ip
                {
                    continue;
                }

                let last =
                    ip.saturating_add(addr.count.checked_sub(1).unwrap());

                if !net.contains(&last)
                    || net.network() == last
                    || net.broadcast() == last
                {
                    continue;
                }

                let mut r = Row::default();

                r.add_str("name", &addr.name);
                r.add_str("cidr", net.to_string());
                r.add_str("first", first.to_string());
                r.add_str("last", last.to_string());
                r.add_u64("count", addr.count.into());
                r.add_str("family", "inet");
                r.add_str("network", net.network().to_string());
                r.add_str("mask", net.netmask().to_string());
                r.add_str("routed", if addr.routed { "yes" } else { "no" });
                r.add_str("gateway", addr.gateway.as_deref().unwrap_or("-"));

                t.add_row(r);
                count += 1;
            }
            (ipnet::IpNet::V6(_), std::net::IpAddr::V6(_)) => {
                /*
                 * No IPv6 support at the moment.
                 */
                continue;
            }
            _ => {
                /*
                 * Weird combination?
                 */
                continue;
            }
        }
    }

    match (filter.as_deref(), count) {
        (None, _) | (Some(_), 1) => (),
        (Some(filter), 0) => {
            bail!("IP address range {filter:?} not found");
        }
        (Some(filter), n) => {
            bail!("{n} IP address ranges matched name {filter:?}");
        }
    }

    print!("{}", t.output()?);
    Ok(())
}

async fn cmd_eng(mut l: Level<Stuff>) -> Result<()> {
    l.context_mut().connect().await?;

    l.cmd("metadata", "dump the factory metadata", cmd!(cmd_eng_metadata))?;

    sel!(l).run().await
}

async fn cmd_eng_metadata(mut l: Level<Stuff>) -> Result<()> {
    let _ = no_args!(l);

    let mout = Message {
        id: l.context_mut().ids.next().unwrap(),
        payload: Payload::MetadataAddresses,
    };

    match l.context_mut().send_and_recv(&mout).await {
        Ok(min) => match min.payload {
            Payload::Error(e) => {
                bail!("WARNING: control request failure: {e}");
            }
            Payload::MetadataAddressesResult(addrs) => {
                println!("addrs = {addrs:#?}");
                Ok(())
            }
            other => bail!("unexpected response: {other:?}"),
        },
        Err(e) => {
            /*
             * Requests to the agent are relatively simple and over a UNIX
             * socket; they should not fail.  This implies something has
             * gone seriously wrong and it is unlikely that it will be fixed
             * without intervention.  Don't retry.
             */
            bail!("could not talk to the agent: {e}");
        }
    }
}

async fn cmd_store(mut l: Level<Stuff>) -> Result<()> {
    l.context_mut().connect().await?;

    l.cmd("get", "get a value from the job store", cmd!(cmd_store_get))?;
    l.cmd("put", "put a value in the job store", cmd!(cmd_store_put))?;

    sel!(l).run().await
}

async fn cmd_store_get(mut l: Level<Stuff>) -> Result<()> {
    l.usage_args(Some("NAME"));
    l.optflag("W", "", "do not wait for the value to exist in the store");

    let a = args!(l);

    if a.args().len() != 1 {
        bad_args!(l, "specify name of value to fetch from store");
    }

    let name = a.args()[0].to_string();
    let no_wait = a.opts().opt_present("W");
    let mut printed_wait = false;

    loop {
        let mout = Message {
            id: l.context_mut().ids.next().unwrap(),
            payload: Payload::StoreGet(name.clone()),
        };

        match l.context_mut().send_and_recv(&mout).await {
            Ok(min) => {
                match min.payload {
                    Payload::Error(e) => {
                        /*
                         * Requests to the buildomat core are allowed to fail
                         * intermittently.  We need to retry until we are able
                         * to get a successful response of some kind back from
                         * the server.
                         */
                        eprintln!(
                            "WARNING: control request failure (retrying): {e}"
                        );
                        tokio::time::sleep(Duration::from_secs(2)).await;
                        continue;
                    }
                    Payload::StoreGetResult(Some(ent)) => {
                        /*
                         * Output formatting here should be kept consistent with
                         * what "buildomat job store get" does outside a job;
                         * see the "buildomat" crate.
                         */
                        if ent.value.ends_with('\n') {
                            print!("{}", ent.value);
                        } else {
                            println!("{}", ent.value);
                        }
                        break Ok(());
                    }
                    Payload::StoreGetResult(None) => {
                        if no_wait {
                            bail!("the store has no value for {name:?}");
                        }

                        if !printed_wait {
                            eprintln!(
                                "WARNING: job store has no value \
                                for {name:?}; waiting for a value..."
                            );
                            printed_wait = true;
                        }

                        tokio::time::sleep(Duration::from_secs(2)).await;
                        continue;
                    }
                    other => bail!("unexpected response: {other:?}"),
                }
            }
            Err(e) => {
                /*
                 * Requests to the agent are relatively simple and over a UNIX
                 * socket; they should not fail.  This implies something has
                 * gone seriously wrong and it is unlikely that it will be fixed
                 * without intervention.  Don't retry.
                 */
                bail!("could not talk to the agent: {e}");
            }
        }
    }
}

async fn cmd_store_put(mut l: Level<Stuff>) -> Result<()> {
    l.usage_args(Some("NAME [VALUE]"));
    l.optflag("s", "", "mark value as secret data");

    let a = args!(l);

    /*
     * Processing of the format of the input should be kept in sync with what
     * "buildomat job store put" does outside the job; see the "buildomat"
     * crate.
     */
    let value = match a.args().len() {
        1 => {
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
        2 => a.args()[1].to_string(),
        _ => {
            bad_args!(l, "specify name of value, and value, to put in store");
        }
    };

    let secret = a.opts().opt_present("s");

    loop {
        let mout = Message {
            id: l.context_mut().ids.next().unwrap(),
            payload: Payload::StorePut(
                a.args()[0].to_string(),
                value.clone(),
                secret,
            ),
        };

        match l.context_mut().send_and_recv(&mout).await {
            Ok(min) => {
                match min.payload {
                    Payload::Error(e) => {
                        /*
                         * Requests to the buildomat core are allowed to fail
                         * intermittently.  We need to retry until we are able
                         * to get a successful response of some kind back from
                         * the server.
                         */
                        eprintln!(
                            "WARNING: control request failure (retrying): {e}"
                        );
                        tokio::time::sleep(Duration::from_secs(2)).await;
                        continue;
                    }
                    Payload::Ack => break Ok(()),
                    other => bail!("unexpected response: {other:?}"),
                }
            }
            Err(e) => {
                /*
                 * Requests to the agent are relatively simple and over a UNIX
                 * socket; they should not fail.  This implies something has
                 * gone seriously wrong and it is unlikely that it will be fixed
                 * without intervention.  Don't retry.
                 */
                bail!("could not talk to the agent: {e}");
            }
        }
    }
}

async fn cmd_process(mut l: Level<Stuff>) -> Result<()> {
    l.context_mut().connect().await?;

    l.cmd("start", "start a background process", cmd!(cmd_process_start))?;

    sel!(l).run().await
}

async fn cmd_process_start(mut l: Level<Stuff>) -> Result<()> {
    l.usage_args(Some("NAME COMMAND [ARGS...]"));

    let a = args!(l);

    if a.args().len() < 2 {
        bad_args!(l, "specify at least a process name and a command to run");
    }

    let mout = Message {
        id: l.context_mut().ids.next().unwrap(),
        payload: Payload::ProcessStart {
            name: a.args()[0].to_string(),
            cmd: a.args()[1].to_string(),
            args: a.args().iter().skip(2).cloned().collect::<Vec<_>>(),

            /*
             * The process will actually be spawned by the agent, which is
             * running under service management.  To aid the user, we want
             * to forward the environment and current directory so that the
             * process can be started as if it were run from the job program
             * itself.
             */
            env: std::env::vars_os().collect::<Vec<_>>(),
            pwd: std::env::current_dir()?.to_str().unwrap().to_string(),

            uid: unsafe { libc::geteuid() },
            gid: unsafe { libc::getegid() },
        },
    };

    match l.context_mut().send_and_recv(&mout).await {
        Ok(min) => {
            match min.payload {
                Payload::Error(e) => {
                    /*
                     * This request is purely local to the agent, so an
                     * error is not something we should retry indefinitely.
                     */
                    bail!("could not start process: {e}");
                }
                Payload::Ack => Ok(()),
                other => bail!("unexpected response: {other:?}"),
            }
        }
        Err(e) => {
            /*
             * Requests to the agent are relatively simple and over a UNIX
             * socket; they should not fail.  This implies something has
             * gone seriously wrong and it is unlikely that it will be fixed
             * without intervention.  Don't retry.
             */
            bail!("could not talk to the agent: {e}");
        }
    }
}

async fn factory_info(s: &mut Stuff) -> Result<FactoryInfo> {
    let mout =
        Message { id: s.ids.next().unwrap(), payload: Payload::FactoryInfo };

    match s.send_and_recv(&mout).await {
        Ok(min) => {
            match min.payload {
                Payload::Error(e) => {
                    /*
                     * This request is purely local to the agent, so an
                     * error is not something we should retry indefinitely.
                     */
                    bail!("could not get factory info: {e}");
                }
                Payload::FactoryInfoResult(fi) => Ok(fi),
                other => bail!("unexpected response: {other:?}"),
            }
        }
        Err(e) => {
            /*
             * Requests to the agent are relatively simple and over a UNIX
             * socket; they should not fail.  This implies something has
             * gone seriously wrong and it is unlikely that it will be fixed
             * without intervention.  Don't retry.
             */
            bail!("could not talk to the agent: {e}");
        }
    }
}

async fn cmd_factory(mut l: Level<Stuff>) -> Result<()> {
    l.context_mut().connect().await?;

    l.cmd(
        "name",
        "print the name of the factory that produced this worker",
        cmd!(cmd_factory_name),
    )?;
    l.cmd(
        "id",
        "print the unique ID of the factory that produced this worker",
        cmd!(cmd_factory_id),
    )?;
    l.cmd(
        "private",
        "print the factory-specific identifier for the underlying resource",
        cmd!(cmd_factory_private),
    )?;

    sel!(l).run().await
}

async fn cmd_factory_id(mut l: Level<Stuff>) -> Result<()> {
    no_args!(l);

    let fi = factory_info(l.context_mut()).await?;

    println!("{}", fi.id);

    Ok(())
}

async fn cmd_factory_name(mut l: Level<Stuff>) -> Result<()> {
    no_args!(l);

    let fi = factory_info(l.context_mut()).await?;

    println!("{}", fi.name);

    Ok(())
}

async fn cmd_factory_private(mut l: Level<Stuff>) -> Result<()> {
    no_args!(l);

    let fi = factory_info(l.context_mut()).await?;

    let Some(fp) = &fi.private else {
        bail!("factory private info not available");
    };

    println!("{fp}");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_socket_path_default() {
        std::env::remove_var("BUILDOMAT_SOCKET");
        assert_eq!(socket_path(), "/var/run/buildomat.sock");
    }

    #[test]
    fn test_socket_path_from_env() {
        std::env::set_var("BUILDOMAT_SOCKET", "/custom/path/buildomat.sock");
        assert_eq!(socket_path(), "/custom/path/buildomat.sock");
        std::env::remove_var("BUILDOMAT_SOCKET");
    }
}
