/*
 * Copyright 2023 Oxide Computer Company
 */

use std::{io::Read, ops::Range, time::Duration};

use anyhow::{bail, Result};
use bytes::BytesMut;
use hiercmd::prelude::*;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
};

use protocol::{Decoder, Message, Payload};

pub(crate) mod protocol;
pub(crate) mod server;

pub const SOCKET_PATH: &str = "/var/run/buildomat.sock";

struct Stuff {
    us: Option<UnixStream>,
    dec: Option<Decoder>,
    ids: Range<u64>,
}

impl Stuff {
    async fn connect(&mut self) -> Result<()> {
        self.us = Some(UnixStream::connect(SOCKET_PATH).await?);
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
    let s = Stuff { us: None, dec: None, ids: (1u64..u64::MAX).into_iter() };

    let mut l = Level::new("bmat", s);

    l.cmd("store", "access the job store", cmd!(cmd_store))?;

    sel!(l).run().await
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
                        if ent.value.ends_with("\n") {
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
