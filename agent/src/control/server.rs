/*
 * Copyright 2023 Oxide Computer Company
 */

use std::{io::ErrorKind, os::unix::prelude::PermissionsExt, sync::Arc};

use anyhow::{bail, Result};
use bytes::{Buf, BufMut, BytesMut};
use tokio::{
    io::Interest,
    net::{UnixListener, UnixStream},
    sync::{
        mpsc::{channel, Receiver, Sender},
        Mutex, Notify,
    },
};

use super::{
    protocol::{Decoder, Message, Payload},
    SOCKET_PATH,
};

#[derive(Debug)]
pub struct Request {
    id: u64,
    payload: Payload,
    conn: Arc<Connection>,
}

impl Request {
    pub async fn reply(self, payload: Payload) {
        let m = Message { id: self.id, payload }.pack().unwrap();

        /*
         * Put the serialised message on the write queue for the socket from
         * whence it came and wake the I/O task:
         */
        let mut ci = self.conn.inner.lock().await;
        ci.writeq.put_slice(&m);
        self.conn.notify.notify_one();
    }

    pub fn payload(&self) -> &Payload {
        &self.payload
    }
}

pub fn listen() -> Result<Receiver<Request>> {
    /*
     * Create the UNIX socket that the control program will use to contact the
     * agent.
     */
    std::fs::remove_file(SOCKET_PATH).ok();
    let ul = UnixListener::bind(SOCKET_PATH)?;

    /*
     * Allow everyone to connect:
     */
    let mut perm = std::fs::metadata(SOCKET_PATH)?.permissions();
    perm.set_mode(0o777);
    std::fs::set_permissions(SOCKET_PATH, perm)?;

    /*
     * Create channel to hand requests back to the main loop.
     */
    let (tx, rx) = channel::<Request>(64);

    tokio::spawn(async move {
        loop {
            match ul.accept().await {
                Ok((stream, _addr)) => {
                    /*
                     * Create new client connection.
                     */
                    let conn = Connection::new();
                    let tx = tx.clone();
                    tokio::spawn(async move {
                        handle_client(conn, stream, tx).await
                    });
                }
                Err(e) => println!("CONTROL ERROR: accept: {e}"),
            }
        }
    });

    Ok(rx)
}

#[derive(Debug)]
enum ClientState {
    Running,
}

#[derive(Debug)]
struct ConnectionInner {
    writeq: BytesMut,
    state: ClientState,
    decoder: Decoder,
}

#[derive(Debug)]
struct Connection {
    notify: Notify,
    inner: Mutex<ConnectionInner>,
}

impl Connection {
    fn new() -> Arc<Connection> {
        Arc::new(Connection {
            notify: Notify::new(),
            inner: Mutex::new(ConnectionInner {
                writeq: Default::default(),
                state: ClientState::Running,
                decoder: Decoder::new(),
            }),
        })
    }
}

async fn handle_client(
    conn: Arc<Connection>,
    us: UnixStream,
    tx: Sender<Request>,
) {
    /*
     * This loop runs until a client connection is terminated, on purpose or
     * otherwise:
     */
    loop {
        match handle_client_turn(&conn, &us, &tx).await {
            Ok(fin) => {
                if fin {
                    break;
                }
            }
            Err(e) => {
                println!("client error (closing connection): {:?}", e);
                break;
            }
        };

        tokio::task::yield_now().await;
    }
}

async fn handle_client_turn(
    conn: &Arc<Connection>,
    us: &UnixStream,
    tx: &Sender<Request>,
) -> Result<bool> {
    /*
     * Create our notified listener prior to checking if the writeq is
     * empty:
     */
    let notified = conn.notify.notified();
    let dowrite = !conn.inner.lock().await.writeq.is_empty();

    let mut i = Interest::READABLE;
    if dowrite {
        i = i.add(Interest::WRITABLE);
    }

    /*
     * Wait for incoming data, or for the socket to be ready to send
     * outbound data from our queue.
     */
    let r = tokio::select! {
        _ = notified => {
            /*
             * If we have been notified, there was probably an outside
             * change to the write queue and we should go for another turn.
             */
            return Ok(false);
        }
        r = us.ready(i) => r,
    }?;

    let mut ci = conn.inner.lock().await;

    if r.is_writable() && !ci.writeq.is_empty() {
        match us.try_write(&ci.writeq) {
            Ok(n) => ci.writeq.advance(n),
            Err(e) if e.kind() == ErrorKind::WouldBlock => (),
            Err(e) => bail!("write error: {:?}", e),
        }
    }

    if r.is_readable() {
        let mut data = vec![0; 1024];
        match us.try_read(&mut data) {
            Ok(0) => ci.decoder.ingest_eof(),
            Ok(n) => ci.decoder.ingest_bytes(&data[0..n]),
            Err(e) if e.kind() == ErrorKind::WouldBlock => (),
            Err(e) => bail!("read error: {:?}", e),
        }
    }

    /*
     * Process messages we have read from the connection, if any:
     */
    while let Some(msg) = ci.decoder.take()? {
        match ci.state {
            ClientState::Running => match &msg.payload {
                Payload::StoreGet(..)
                | Payload::StorePut(..)
                | Payload::MetadataAddresses
                | Payload::ProcessStart { .. } => {
                    /*
                     * These are requests from the control program.  Pass them
                     * on to the main loop.
                     */
                    let req = Request {
                        id: msg.id,
                        payload: msg.payload.clone(),
                        conn: Arc::clone(conn),
                    };

                    tx.send(req).await.unwrap();
                }
                other => {
                    bail!("unexpected message {:?}", other);
                }
            },
        }
    }

    Ok(ci.decoder.ended())
}
