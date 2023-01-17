use std::{
    collections::{BTreeMap, VecDeque},
    pin::Pin,
    sync::Arc,
    task::Poll,
};

use crate::{Error, ErrorExt, Result};

use futures_core::{Future, Stream};
use serde::Deserialize;

pub(crate) struct ObjectStreamConfig {
    #[allow(unused)]
    pub(crate) name: &'static str,
    pub(crate) url: String,
    pub(crate) creds: Option<super::Creds>,
    pub(crate) query_base: Vec<(String, String)>,
}

pub struct ObjectStream<O: Unpin + Send + 'static>
where
    for<'de> O: Deserialize<'de>,
{
    pub(crate) client: reqwest::Client,
    pub(crate) config: ObjectStreamConfig,
    pub(crate) fin: bool,
    pub(crate) one_shot: bool,
    pub(crate) start_at: usize,
    pub(crate) q: VecDeque<O>,
    pub(crate) fetch:
        Option<Pin<Box<(dyn Future<Output = Result<Vec<O>>> + Send)>>>,
}

impl<O: Unpin + Send + 'static> ObjectStream<O>
where
    for<'de> O: Deserialize<'de>,
{
    pub(crate) fn new(
        client: &reqwest::Client,
        config: ObjectStreamConfig,
    ) -> ObjectStream<O> {
        ObjectStream {
            client: client.clone(),
            config,
            fin: false,
            one_shot: false,
            start_at: 0,
            q: Default::default(),
            fetch: None,
        }
    }

    pub(crate) fn new_one_shot(
        client: &reqwest::Client,
        config: ObjectStreamConfig,
    ) -> ObjectStream<O> {
        ObjectStream {
            client: client.clone(),
            config,
            fin: false,
            one_shot: true,
            start_at: 0,
            q: Default::default(),
            fetch: None,
        }
    }
}

impl<O: Unpin + Send + 'static> Stream for ObjectStream<O>
where
    for<'de> O: Deserialize<'de>,
{
    type Item = Result<O>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            /*
             * If there is something in the queue already, we can just return
             * it.
             */
            if let Some(o) = self.q.pop_front() {
                return Poll::Ready(Some(Ok(o)));
            }

            /*
             * If we have already read the last page, there is nothing left to
             * do.
             */
            if self.fin {
                return Poll::Ready(None);
            }

            /*
             * At this point, we either need to spawn a task to load the next
             * page from the server, or if we already have a running task we
             * need to wait for it to be finished.
             */
            if let Some(fetch) = self.fetch.as_mut() {
                let pin = Pin::new(fetch);

                match pin.poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(olist)) => {
                        self.fetch = None;

                        if olist.is_empty() {
                            /*
                             * An empty request means we're finished.
                             */
                            self.fin = true;
                        } else {
                            self.start_at =
                                self.start_at.checked_add(olist.len()).unwrap();
                            for o in olist {
                                self.q.push_back(o);
                            }
                        }

                        if self.one_shot {
                            /*
                             * This request only returns one page.
                             */
                            self.fin = true;
                        }
                    }
                    Poll::Ready(Err(e)) => {
                        self.fetch = None;

                        return Poll::Ready(Some(Err(e)));
                    }
                }
            } else {
                /*
                 * No fetch was in progress.  Start one.
                 */
                let mut req = self
                    .client
                    .get(&self.config.url)
                    .query(&self.config.query_base);

                if let Some(creds) = &self.config.creds {
                    req =
                        req.basic_auth(&creds.username, Some(&creds.password));
                }

                if self.start_at > 0 {
                    req = req.query(&[("S", &self.start_at.to_string())]);
                }

                self.fetch = Some(Box::pin(fetch_page(self.config.name, req)));
            }
        }
    }
}

async fn fetch_page<O: Unpin + Send + 'static>(
    name: &'static str,
    req: reqwest::RequestBuilder,
) -> Result<Vec<O>>
where
    for<'de> O: Deserialize<'de>,
{
    let res = req
        .send()
        .await
        .map_err(|e| e.for_req(name))?
        .error_for_status()
        .map_err(|e| e.for_req(name))?;

    Ok(super::parseres(name, res).await?)
}

pub struct NamedObjectStream<O: Unpin + Send + 'static>
where
    for<'de> O: Deserialize<'de>,
{
    pub(crate) client: reqwest::Client,
    pub(crate) config: ObjectStreamConfig,
    pub(crate) fin: bool,
    pub(crate) start_at: usize,
    pub(crate) q: VecDeque<(String, O)>,
    pub(crate) fetch: Option<
        Pin<Box<(dyn Future<Output = Result<Vec<(String, O)>>> + Send)>>,
    >,
}

impl<O: Unpin + Send + 'static> NamedObjectStream<O>
where
    for<'de> O: Deserialize<'de>,
{
    pub(crate) fn new(
        client: &reqwest::Client,
        config: ObjectStreamConfig,
    ) -> NamedObjectStream<O> {
        NamedObjectStream {
            client: client.clone(),
            config,
            fin: false,
            start_at: 0,
            q: Default::default(),
            fetch: None,
        }
    }
}

impl<O: Unpin + Send + 'static> Stream for NamedObjectStream<O>
where
    for<'de> O: Deserialize<'de>,
{
    type Item = Result<(String, O)>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            /*
             * If there is something in the queue already, we can just return
             * it.
             */
            if let Some(o) = self.q.pop_front() {
                return Poll::Ready(Some(Ok(o)));
            }

            /*
             * If we have already read the last page, there is nothing left to
             * do.
             */
            if self.fin {
                return Poll::Ready(None);
            }

            /*
             * At this point, we either need to spawn a task to load the next
             * page from the server, or if we already have a running task we
             * need to wait for it to be finished.
             */
            if let Some(fetch) = self.fetch.as_mut() {
                let pin = Pin::new(fetch);

                match pin.poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(olist)) => {
                        self.fetch = None;

                        if olist.is_empty() {
                            /*
                             * An empty request means we're finished.
                             */
                            self.fin = true;
                        } else {
                            self.start_at =
                                self.start_at.checked_add(olist.len()).unwrap();
                            for o in olist {
                                self.q.push_back(o);
                            }
                        }
                    }
                    Poll::Ready(Err(e)) => {
                        self.fetch = None;

                        return Poll::Ready(Some(Err(e)));
                    }
                }
            } else {
                /*
                 * No fetch was in progress.  Start one.
                 */
                let mut req = self
                    .client
                    .get(&self.config.url)
                    .query(&self.config.query_base);

                if let Some(creds) = &self.config.creds {
                    req =
                        req.basic_auth(&creds.username, Some(&creds.password));
                }

                if self.start_at > 0 {
                    req = req.query(&[("S", &self.start_at.to_string())]);
                }

                self.fetch =
                    Some(Box::pin(fetch_named_page(self.config.name, req)));
            }
        }
    }
}

async fn fetch_named_page<O: Unpin + Send + 'static>(
    name: &'static str,
    req: reqwest::RequestBuilder,
) -> Result<Vec<(String, O)>>
where
    for<'de> O: Deserialize<'de>,
{
    let res = req
        .send()
        .await
        .map_err(|e| e.for_req(name))?
        .error_for_status()
        .map_err(|e| e.for_req(name))?;

    let set: BTreeMap<String, O> = super::parseres(name, res).await?;

    let mut out = Vec::new();
    for (name, o) in set {
        out.push((name, o));
    }

    Ok(out)
}
