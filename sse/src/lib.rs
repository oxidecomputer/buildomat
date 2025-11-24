use std::{result::Result as SResult, time::Duration};

use anyhow::{Result, anyhow};
use bytes::Bytes;
use dropshot::Body;
use http::{
    Response, StatusCode,
    header::{CACHE_CONTROL, CONTENT_TYPE},
};
use http_body_util::StreamBody;
use hyper::body::Frame;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

pub struct ServerSentEvents {
    tx: mpsc::Sender<String>,
    brx: Option<mpsc::Receiver<SResult<Frame<Bytes>, std::io::Error>>>,
}

const PING_INTERVAL_SECONDS: u64 = 5;
const SEND_TIMEOUT: u64 = 15;

impl Default for ServerSentEvents {
    fn default() -> Self {
        let (btx, brx) =
            mpsc::channel::<SResult<Frame<Bytes>, std::io::Error>>(1);
        let (tx, mut rx) = mpsc::channel::<String>(64);

        /*
         * Spawn a task to format and send messages out.  If no messages are
         * sent for a while, send a ping record to keep the connection alive.
         */
        tokio::task::spawn(async move {
            let mut next_ping = tokio::time::Instant::now()
                .checked_add(Duration::from_secs(PING_INTERVAL_SECONDS))
                .unwrap();

            loop {
                enum Act {
                    Ping,
                    Send(String),
                }

                let act = tokio::select! {
                    _ = tokio::time::sleep_until(next_ping) => Act::Ping,
                    ev = rx.recv() => if let Some(ev) = ev {
                        Act::Send(ev)
                    } else {
                        return;
                    },
                };

                let output = Frame::data(
                    match act {
                        Act::Ping => ": nothing happens\n".as_bytes().to_vec(),
                        Act::Send(ev) => ev.into_bytes(),
                    }
                    .into(),
                );

                match btx
                    .send_timeout(Ok(output), Duration::from_secs(SEND_TIMEOUT))
                    .await
                {
                    Ok(_) => {
                        /*
                         * We have just sent something, so delay the next ping
                         * for a full interval:
                         */
                        next_ping = tokio::time::Instant::now()
                            .checked_add(Duration::from_secs(
                                PING_INTERVAL_SECONDS,
                            ))
                            .unwrap();
                    }
                    Err(_) => {
                        /*
                         * If the send to the channel times out, it must have
                         * been full for some time.  This implies that our
                         * messages and heartbeats are not getting through to
                         * the remote peer.
                         */
                        return;
                    }
                }
            }
        });

        ServerSentEvents { brx: Some(brx), tx }
    }
}

impl ServerSentEvents {
    pub fn to_response(&mut self) -> Result<Response<Body>> {
        Ok(Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "text/event-stream")
            /*
             * In case we are behind a reverse proxy, tell nginx not to buffer
             * the response at all, just pass it straight on to the client:
             */
            .header("X-Accel-Buffering", "no")
            /*
             * No client or proxy should cache these responses:
             */
            .header(CACHE_CONTROL, "no-store")
            .body(Body::wrap(StreamBody::new(ReceiverStream::new(
                self.brx.take().ok_or_else(|| {
                    anyhow!("body already created for stream")
                })?,
            ))))?)
    }

    pub fn build_event(&self) -> EventBuilder<'_> {
        EventBuilder { sse: self, id: None, event: None, data: None }
    }

    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }
}

pub struct EventBuilder<'a> {
    sse: &'a ServerSentEvents,
    id: Option<&'a str>,
    event: Option<&'a str>,
    data: Option<&'a str>,
}

impl<'a> EventBuilder<'a> {
    pub fn id(mut self, id: &'a str) -> Self {
        self.id = Some(id);
        self
    }

    pub fn event(mut self, event: &'a str) -> Self {
        self.event = Some(event);
        self
    }

    pub fn data(mut self, data: &'a str) -> Self {
        self.data = Some(data);
        self
    }

    pub async fn send(self) -> bool {
        let mut ev = String::new();
        if let Some(event) = self.event {
            ev += &format!("event: {event}\n");
        }
        if let Some(id) = self.id {
            ev += &format!("id: {id}\n");
        }
        if let Some(data) = self.data {
            ev += &format!("data: {data}\n");
        }
        ev += "\n";

        self.sse.tx.send(ev).await.is_ok()
    }
}

pub trait HeaderMapEx {
    fn last_event_id(&self) -> Option<String>;
}

impl HeaderMapEx for http::HeaderMap<http::HeaderValue> {
    fn last_event_id(&self) -> Option<String> {
        let mut hvs = self.get_all("Last-Event-ID").into_iter();

        let hv = hvs.next()?;

        if hvs.next().is_some() {
            /*
             * It is not clear what we should do if there is more than one
             * header value.
             */
            return None;
        }

        hv.to_str().ok().map(str::to_string)
    }
}
