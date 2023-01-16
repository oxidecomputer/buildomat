use anyhow::Result;
use futures_core::{Future, Stream};
use std::{collections::VecDeque, pin::Pin, sync::Arc, task::Poll};

use serde::Deserialize;

pub(crate) struct ObjectStreamConfig {
    #[allow(unused)]
    pub(crate) name: &'static str,
    pub(crate) url: String,
    pub(crate) creds: Option<super::Creds>,
    pub(crate) query_base: Vec<(String, String)>,
}

pub struct ObjectStream<O: Unpin>
where
    for<'de> O: Deserialize<'de>,
{
    pub(crate) client: reqwest::Client,
    pub(crate) config: ObjectStreamConfig,
    pub(crate) fin: bool,
    pub(crate) start_at: usize,
    pub(crate) q: VecDeque<O>,
    pub(crate) fetch: Option<Pin<Box<(dyn Future<Output = Result<Vec<O>>>)>>>,
}

impl<O: Unpin> ObjectStream<O>
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
            start_at: 0,
            q: Default::default(),
            fetch: None,
        }
    }
}

impl<O: Unpin + 'static> Stream for ObjectStream<O>
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

                self.fetch = Some(Box::pin(fetch_page(req)));
            }
        }
    }
}

async fn fetch_page<O: Unpin>(req: reqwest::RequestBuilder) -> Result<Vec<O>>
where
    for<'de> O: Deserialize<'de>,
{
    let res = req.send().await?.error_for_status()?;

    Ok(super::parseres(res).await?)
}
