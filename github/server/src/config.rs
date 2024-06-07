/*
 * Copyright 2024 Oxide Computer Company
 */

use std::fs::OpenOptions;
use std::io::Read;
use std::path::Path;

use anyhow::Result;
use buildomat_common::*;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct Sqlite {
    #[serde(default)]
    pub cache_kb: Option<u32>,
}

#[derive(Deserialize)]
pub struct Buildomat {
    pub token: String,
    pub url: String,
}

#[derive(Deserialize)]
pub struct Config {
    pub id: u64,
    pub secret: String,
    pub webhook_secret: String,
    pub base_url: String,
    pub confroot: String,
    pub buildomat: Buildomat,
    pub allow_owners: Vec<String>,
    pub sqlite: Sqlite,
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
