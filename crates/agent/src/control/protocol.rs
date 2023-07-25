/*
 * Copyright 2023 Oxide Computer Company
 */

use anyhow::{bail, Result};
use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct StoreEntry {
    pub name: String,
    pub value: String,
    pub secret: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum Payload {
    Ack,
    Error(String),

    StoreGet(String),
    StoreGetResult(Option<StoreEntry>),

    StorePut(String, String, bool),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message {
    pub id: u64,
    pub payload: Payload,
}

impl Message {
    pub fn pack(&self) -> Result<BytesMut> {
        let buf = serde_json::to_vec(self)?;
        let mut out = BytesMut::with_capacity(buf.len() + 2 * 4);
        out.put_u32_le(1);
        out.put_u32_le(buf.len().try_into().unwrap());
        out.put_slice(&buf);
        Ok(out)
    }
}

#[derive(Clone, Debug)]
enum State {
    Rest,
    Version,
    Header { length: usize },
    Error(String),
}

#[derive(Debug)]
pub struct Decoder {
    fin: bool,
    readq: BytesMut,
    state: State,
}

impl Decoder {
    pub fn new() -> Decoder {
        Decoder { readq: Default::default(), fin: false, state: State::Rest }
    }

    pub fn ingest_bytes(&mut self, input: &[u8]) {
        assert!(!self.fin);
        self.readq.put_slice(input);
    }

    pub fn ingest_eof(&mut self) {
        self.fin = true;
    }

    pub fn take(&mut self) -> Result<Option<Message>> {
        loop {
            match self.state.clone() {
                State::Rest => {
                    if self.fin && self.readq.is_empty() {
                        /*
                         * The stream is complete and we have no spare bytes.
                         */
                        return Ok(None);
                    }

                    if self.readq.remaining() < 4 {
                        /*
                         * Wait for a version number.
                         */
                        break;
                    }

                    let version = self.readq.get_u32_le();
                    if version != 1 {
                        self.state =
                            State::Error(format!("version {} != 1", version));
                    } else {
                        self.state = State::Version;
                    }
                    continue;
                }
                State::Version => {
                    if self.readq.remaining() < 4 {
                        /*
                         * Wait for a frame length.
                         */
                        break;
                    }

                    let length: usize =
                        self.readq.get_u32_le().try_into().unwrap();
                    if length > 1024 * 1024 * 1024 {
                        self.state = State::Error(format!(
                            "outrageous packet length {}",
                            length,
                        ));
                    } else {
                        self.state = State::Header { length };
                    }
                    continue;
                }
                State::Header { length } => {
                    if self.readq.remaining() < length {
                        /*
                         * Wait for the whole frame to arrive.
                         */
                        break;
                    }

                    let frame = serde_json::from_slice::<Message>(
                        &self.readq[0..length],
                    );
                    self.readq.advance(length);
                    match frame {
                        Ok(msg) => {
                            self.state = State::Rest;
                            return Ok(Some(msg));
                        }
                        Err(e) => {
                            self.state = State::Error(format!("{:?}", e));
                            continue;
                        }
                    }
                }
                State::Error(e) => bail!("decode error: {}", e),
            }
            #[allow(unreachable_code)]
            {
                panic!("should not be here");
            }
        }

        /*
         * If we got out of the loop this way, it's because we do not yet
         * have enough bytes for a full frame.  Make sure that more bytes
         * are on the way:
         */
        if self.fin {
            bail!("abrupt end of stream");
        } else {
            return Ok(None);
        }
    }

    pub fn ended(&self) -> bool {
        self.fin && self.readq.is_empty()
    }
}
