/*
 * Copyright 2025 Oxide Computer Company
 */

use std::{collections::HashMap, process::Command};

use anyhow::{anyhow, bail, Result};

use buildomat_common::*;

mod humility;
mod pipe;

struct System {
    serial: String,
    probe: String,
    archive: String,
}

impl System {
    fn open(slot: &str) -> Result<Self> {
        let output =
            Command::new("/usr/bin/gimlets").env_clear().arg(&slot).output()?;
        if !output.status.success() {
            bail!("could not get status for slot {slot}: {}", output.info());
        }

        let out = String::from_utf8(output.stdout)?;
        let mut info = out
            .lines()
            .filter_map(|l| {
                if let Some(t) = l.strip_prefix("export ") {
                    let kv = t.split('=').collect::<Vec<_>>();
                    if kv.len() == 2 {
                        if let Some(v) = kv[1]
                            .strip_prefix('\'')
                            .and_then(|v| v.strip_suffix('\''))
                        {
                            return Some((kv[0].to_string(), v.to_string()));
                        }
                    }
                }

                None
            })
            .collect::<HashMap<String, String>>();

        let probe = info
            .remove("HUMILITY_PROBE")
            .ok_or_else(|| anyhow!("no HUMILITY_PROBE from gimlets?"))?;
        let archive = info
            .remove("HUMILITY_ARCHIVE")
            .ok_or_else(|| anyhow!("no HUMILITY_ARCHIVE from gimlets?"))?;

        /*
         * Attempt to raise the Gimlet and get its serial number.
         */
        todo!()
    }
}

fn main() -> Result<()> {
    let a = getopts::Options::new()
        .parsing_style(getopts::ParsingStyle::FloatingFrees)
        .parse(std::env::args_os().skip(1))?;

    if a.free.len() != 1 {
        bail!("which slot?");
    }

    let slot = a.free[0].to_string();

    let sys = System::open(&slot)?;

    //println!("info = {info:#?}");

    Ok(())
}
