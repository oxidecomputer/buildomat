/*
 * Copyright 2024 Oxide Computer Company
 */

use std::time::Duration;

use anyhow::{bail, Result};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};

pub mod events;
pub mod ext;

pub mod gen {
    progenitor::generate_api!(
        spec = "openapi.json",
        interface = Builder,
        replace = {
            FactoryMetadata = buildomat_types::metadata::FactoryMetadata,
        },
    );
}

pub mod prelude {
    pub use super::ext::*;
    pub use super::gen::prelude::*;
    pub use super::{ClientExt, ClientExtra};
    pub use futures::{StreamExt, TryStreamExt};
    pub use reqwest::StatusCode;
}
pub use gen::{types, Client, Error};

pub struct ClientBuilder {
    url: String,
    token: Option<String>,
    delegate_user: Option<String>,
}

impl ClientBuilder {
    pub fn new(url: &str) -> ClientBuilder {
        ClientBuilder { url: url.to_string(), token: None, delegate_user: None }
    }

    pub fn bearer_token<S: AsRef<str>>(&mut self, token: S) -> &mut Self {
        self.token = Some(token.as_ref().to_string());
        self
    }

    pub fn delegated_user<S: AsRef<str>>(&mut self, user: S) -> &mut Self {
        self.delegate_user = Some(user.as_ref().to_string());
        self
    }

    pub fn build(&mut self) -> Result<Client> {
        let mut dh = HeaderMap::new();

        if let Some(user) = self.delegate_user.as_deref() {
            if self.token.is_none() {
                bail!("delegated authentication requires a bearer token");
            }

            dh.insert(
                "X-Buildomat-Delegate",
                HeaderValue::from_str(user).unwrap(),
            );
        }

        if let Some(token) = self.token.as_deref() {
            dh.insert(
                AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {}", token)).unwrap(),
            );
        }

        let client = reqwest::ClientBuilder::new()
            .timeout(Duration::from_secs(3600))
            .tcp_keepalive(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(15))
            .default_headers(dh)
            .build()?;

        Ok(Client::new_with_client(&self.url, client))
    }
}

pub trait ClientExt {
    fn extra(&self) -> ClientExtra;
}

impl ClientExt for Client {
    fn extra(&self) -> ClientExtra {
        ClientExtra(self.clone())
    }
}

pub struct ClientExtra(Client);

pub enum EventOrState {
    Event(types::JobEvent),
    State(String),
    Done,
}

impl ClientExtra {
    pub fn watch_job(
        &self,
        id: &str,
    ) -> tokio::sync::mpsc::Receiver<std::result::Result<EventOrState, String>>
    {
        let (tx, rx) = tokio::sync::mpsc::channel(1);

        let c = self.0.clone();
        let id = id.to_string();
        tokio::task::spawn(async move {
            let mut prev_seq = 0;
            let mut done = false;
            let mut prev_state = "".to_string();

            'outer: loop {
                let mut chan = match c.job_watch().job(&id).send().await {
                    Ok(rvbs) => events::attach(rvbs),
                    Err(e) => {
                        if let Some(status) = e.status() {
                            if status.as_u16() == 404 || status.as_u16() == 403
                            {
                                /*
                                 * This job does not exist, or is not visible to
                                 * us.
                                 */
                                tx.send(Err(format!("job {id} not found")))
                                    .await
                                    .ok();
                                return;
                            }
                        }

                        /*
                         * Sleep and try again.
                         */
                        tokio::time::sleep(Duration::from_secs(2)).await;
                        continue;
                    }
                };

                loop {
                    let send = match chan.recv().await {
                        Some(Ok(ser)) => {
                            match ser.event().as_str() {
                                "check" => {
                                    /*
                                     * We cannot subscribe to the job.  It may
                                     * have completed already.
                                     */
                                    break;
                                }
                                "state" => {
                                    /*
                                     * The job state may have changed.
                                     */
                                    let new_state =
                                        ser.data().trim().to_string();

                                    if prev_state == new_state {
                                        continue;
                                    }
                                    prev_state = new_state;

                                    EventOrState::State(prev_state.clone())
                                }
                                "job" => {
                                    /*
                                     * A job event record.
                                     */
                                    let je: types::JobEvent =
                                        match serde_json::from_str(&ser.data())
                                        {
                                            Ok(je) => je,
                                            Err(e) => {
                                                tx.send(Err(e.to_string()))
                                                    .await
                                                    .ok();
                                                return;
                                            }
                                        };

                                    if je.seq > prev_seq + 1 {
                                        /*
                                         * If we reconnect to the event stream
                                         * we might have missed some records.
                                         * Fetch them now using the regular
                                         * paginated interface:
                                         */
                                        prev_seq = record_catchup(
                                            &c,
                                            &id,
                                            &tx,
                                            prev_seq,
                                            Some(je.seq),
                                        )
                                        .await;
                                    }

                                    if je.seq <= prev_seq {
                                        continue;
                                    }
                                    prev_seq = je.seq;

                                    EventOrState::Event(je)
                                }
                                "complete" => {
                                    /*
                                     * The job event stream is complete.
                                     */
                                    done = true;
                                    EventOrState::Done
                                }
                                other => {
                                    tx.send(Err(format!(
                                        "unknown event {other:?}",
                                    )))
                                    .await
                                    .ok();
                                    return;
                                }
                            }
                        }
                        None | Some(Err(_)) => {
                            /*
                             * Early end of stream.  Connect again.
                             */
                            continue 'outer;
                        }
                    };

                    if tx.send(Ok(send)).await.is_err() {
                        return;
                    }

                    if done {
                        return;
                    }
                }

                /*
                 * Check the job state with a regular request.
                 */
                match c.job_get().job(&id).send().await {
                    Ok(j) => {
                        if prev_state != j.state {
                            /*
                             * Pass that on to the consumer:
                             */
                            if tx
                                .send(Ok(EventOrState::State(j.state.clone())))
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }

                        if j.state == "completed" || j.state == "failed" {
                            /*
                             * The job has reached a terminal state.
                             */
                            break 'outer;
                        }

                        /*
                         * Sleep for a bit, waiting for the job subscription to
                         * be available in the server.
                         */
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                    Err(_) => {
                        /*
                         * Sleep and then try to subscribe again.
                         */
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                }
            }

            /*
             * Dump out any remaining event records.
             */
            record_catchup(&c, &id, &tx, prev_seq, None).await;
        });

        rx
    }
}

async fn record_catchup(
    c: &Client,
    id: &str,
    tx: &tokio::sync::mpsc::Sender<std::result::Result<EventOrState, String>>,
    mut prev_seq: u32,
    stop_seq: Option<u32>,
) -> u32 {
    loop {
        if tx.is_closed() {
            return prev_seq;
        }

        match c
            .job_events_get()
            .job(id)
            .minseq(prev_seq.checked_add(1).unwrap())
            .send()
            .await
        {
            Ok(events) if events.is_empty() => {
                if stop_seq.is_some() {
                    /*
                     * We're still running, but have hit the end of the record
                     * stream without hitting the event we're trying to catch up
                     * to.  This really shouldn't happen, as the event stream
                     * for a job is append-only.
                     */
                    return prev_seq;
                }

                tx.send(Ok(EventOrState::Done)).await.ok();
                return prev_seq;
            }
            Ok(events) => {
                for ev in events.into_inner() {
                    if let Some(stop_seq) = stop_seq {
                        if ev.seq >= stop_seq {
                            /*
                             * We've caught up to the point in the stream where
                             * live events are showing up.
                             */
                            return prev_seq;
                        }
                    }

                    if ev.seq > prev_seq {
                        prev_seq = ev.seq;
                        if tx.send(Ok(EventOrState::Event(ev))).await.is_err() {
                            return prev_seq;
                        }
                    }
                }
            }
            Err(_) => {
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
}
