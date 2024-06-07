/*
 * Copyright 2024 Oxide Computer Company
 */

use std::fs::OpenOptions;
use std::io::Read;
use std::path::Path;

use anyhow::Result;
use serde::Deserialize;
use buildomat_common::*;

#[derive(Deserialize)]
pub struct Config {
    pub id: u64,
    pub secret: String,
}

pub fn load_bytes<P: AsRef<Path>>(p: P) -> Result<Vec<u8>> {
    let p = p.as_ref();
    let mut f = OpenOptions::new().read(true).open(p)?;
    let mut d = Vec::new();
    f.read_to_end(&mut d)?;
    Ok(d)
}

pub fn load_config<P: AsRef<Path>>(p: P) -> Result<Config> {
    read_toml(p)
}
