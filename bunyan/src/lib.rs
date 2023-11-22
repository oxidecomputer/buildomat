use anyhow::Result;
use bytes::{Buf, Bytes, BytesMut};
use chrono::prelude::*;
use serde::{Deserialize, Serialize};
use serde_repr::Deserialize_repr;
use std::collections::{BTreeMap, VecDeque};

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
pub struct BunyanEntry {
    v: i64,
    level: BunyanLevel,
    name: String,
    hostname: String,
    pid: u64,
    time: DateTime<Utc>,
    msg: String,

    /*
     * This is not a part of the base specification, but is widely used:
     */
    component: Option<String>,

    #[serde(flatten)]
    extra: BTreeMap<String, serde_json::Value>,
}

impl BunyanEntry {
    pub fn level(&self) -> BunyanLevel {
        self.level
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn hostname(&self) -> &str {
        &self.hostname
    }

    pub fn pid(&self) -> u64 {
        self.pid
    }

    pub fn time(&self) -> DateTime<Utc> {
        self.time
    }

    pub fn msg(&self) -> &str {
        &self.msg
    }

    pub fn component(&self) -> Option<&str> {
        self.component.as_deref()
    }

    pub fn extras(
        &self,
    ) -> Box<dyn Iterator<Item = (String, serde_json::Value)>> {
        Box::new(self.extra.clone().into_iter())
    }
}

#[derive(Deserialize_repr, Debug, Clone, Copy)]
#[repr(u8)]
pub enum BunyanLevel {
    Fatal = 60,
    Error = 50,
    Warn = 40,
    Info = 30,
    Debug = 20,
    Trace = 10,
}

impl BunyanLevel {
    pub fn render(&self) -> &'static str {
        match self {
            BunyanLevel::Fatal => "FATA",
            BunyanLevel::Error => "ERRO",
            BunyanLevel::Warn => "WARN",
            BunyanLevel::Info => "INFO",
            BunyanLevel::Debug => "DEBG",
            BunyanLevel::Trace => "TRAC",
        }
    }
}

#[derive(Debug)]
pub enum BunyanLine {
    Entry(BunyanEntry),
    Other(String),
}

#[derive(Default)]
pub struct BunyanDecoder {
    accum: BytesMut,
    q: VecDeque<BunyanLine>,
}

impl BunyanDecoder {
    pub fn new() -> BunyanDecoder {
        BunyanDecoder::default()
    }

    fn append_other(&mut self, len: usize) {
        self.q.push_back(BunyanLine::Other(
            String::from_utf8_lossy(&self.accum[0..len])
                .trim_end_matches('\n')
                .to_string(),
        ));
    }

    fn process(&mut self, len: usize) {
        /*
         * Steel yourselves; this is unfortunate.
         *
         * Serde takes a somewhat fundamentalist position on parsing JSON
         * with duplicate keys directly into a struct.  This is not wholly
         * unreasonable: there is no defined interpretation for a document
         * with objects with duplicate keys.
         *
         * Unfortunately, we have to be pragmatic.  Some bunyan logs will have
         * duplicate keys.  There does not seem to be a way to tell serde to
         * ignore the problem when targetting a struct, so, we create a struct
         * that will flatten everything into a map, using a custom deserialiser
         * that just keeps the first value we see.
         *
         * We would use serde_json::Value instead of BTreeMap here, but of
         * course serde_with does not implement maps_first_key_wins for anything
         * but the standard maps.
         */
        #[derive(Serialize, Deserialize)]
        struct Sigh {
            #[serde(flatten, with = "::serde_with::rust::maps_first_key_wins")]
            everything: BTreeMap<String, serde_json::Value>,
        }

        /*
         * You can't deserialise into a more specific struct directly from a
         * BTreeMap as far as I can tell, so we first convert that map back to a
         * serde_json::Value to then deserialise it again into our target
         * struct.
         */
        let ok = match serde_json::from_slice::<Sigh>(&self.accum[0..len]) {
            Ok(sigh) => match serde_json::to_value(sigh) {
                Ok(j) => match serde_json::from_value::<BunyanEntry>(j) {
                    Ok(be) if be.v == 0 => {
                        /*
                         * We were able to parse so we have the minimum set of
                         * properties, and we have the correct version.
                         */
                        self.q.push_back(BunyanLine::Entry(be));
                        true
                    }
                    _ => false,
                },
                Err(_) => false,
            },
            Err(_) => false,
        };

        if !ok {
            /*
             * We could not process this line for some reason.  Emit it
             * as-is:
             */
            self.append_other(len);
        }

        /*
         * Discard the data we've just used.
         */
        self.accum.advance(len);
    }

    pub fn fin(&mut self) -> Result<()> {
        assert!(!self.accum.contains(&b'\n'));

        if !self.accum.is_empty() {
            self.process(self.accum.len());
        }

        Ok(())
    }

    pub fn feed(&mut self, input: &Bytes) -> Result<()> {
        self.accum.extend_from_slice(input);

        loop {
            if let Some(i) = self.accum.iter().position(|&a| a == b'\n') {
                self.process(i + 1);
            } else {
                /*
                 * No newlines yet.
                 */
                return Ok(());
            }
        }
    }

    pub fn pop(&mut self) -> Option<BunyanLine> {
        self.q.pop_front()
    }
}
