use futures::TryStreamExt;

#[derive(Debug)]
#[allow(dead_code)]
enum ServerEventLine {
    Event(String),
    Data(String),
    Id(String),
    Retry(String),
}

#[derive(Debug)]
pub struct ServerEventRecord(Vec<ServerEventLine>);

impl ServerEventRecord {
    pub fn data(&self) -> String {
        self.0
            .iter()
            .filter_map(|ev| match ev {
                ServerEventLine::Data(da) => Some(da.to_string()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    pub fn event(&self) -> String {
        self.0
            .iter()
            .filter_map(|ev| match ev {
                ServerEventLine::Event(ev) => Some(ev.to_string()),
                _ => None,
            })
            .next()
            .unwrap_or_else(String::new)
    }
}

fn process_line(l: &str) -> Option<ServerEventLine> {
    if l.starts_with(':') {
        /*
         * Treat this as a comment.
         */
        return None;
    }

    let (name, value) = if let Some((name, value)) = l.split_once(':') {
        (name, value)
    } else {
        (l, "")
    };

    /*
     * According to the whatwg specification, we should discard a single space
     * after the colon if there was one.
     */
    let value = value.strip_prefix(' ').unwrap_or(value);

    Some(match name.trim() {
        "event" => ServerEventLine::Event(value.to_string()),
        "data" => ServerEventLine::Data(value.to_string()),
        "id" => ServerEventLine::Id(value.to_string()),
        "retry" => ServerEventLine::Retry(value.to_string()),
        _ => return None,
    })
}

pub fn attach(
    rv: progenitor::progenitor_client::ResponseValue<
        progenitor::progenitor_client::ByteStream,
    >,
) -> tokio::sync::mpsc::Receiver<std::result::Result<ServerEventRecord, String>>
{
    let (tx, rx) = tokio::sync::mpsc::channel(128);

    tokio::task::spawn(async move {
        let mut stream = rv.into_inner_stream();

        let mut line = Vec::new();
        let mut fields = Vec::new();
        loop {
            let buf = match stream.try_next().await {
                Ok(Some(buf)) => buf,
                Ok(None) => {
                    /*
                     * We have reached the end of the stream.
                     */
                    if !fields.is_empty() {
                        tx.send(Ok(ServerEventRecord(std::mem::take(
                            &mut fields,
                        ))))
                        .await
                        .ok();
                    }
                    return;
                }
                Err(e) => {
                    tx.send(Err(e.to_string())).await.ok();
                    return;
                }
            };

            for b in buf {
                if b == b'\n' {
                    if line.is_empty() {
                        /*
                         * This is the end of a record.
                         */
                        if fields.is_empty() {
                            continue;
                        }

                        if tx
                            .send(Ok(ServerEventRecord(std::mem::take(
                                &mut fields,
                            ))))
                            .await
                            .is_err()
                        {
                            return;
                        }
                        continue;
                    }

                    /*
                     * Process line.
                     */
                    let ls = match std::str::from_utf8(&line) {
                        Ok(ls) => ls,
                        Err(e) => {
                            tx.send(Err(e.to_string())).await.ok();
                            return;
                        }
                    };

                    if let Some(f) = process_line(ls) {
                        fields.push(f);
                    }
                    line.clear();
                    continue;
                } else {
                    line.push(b);
                }
            }
        }
    });

    rx
}
