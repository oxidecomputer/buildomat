use anyhow::Result;
use serde::Deserialize;
use std::fs::OpenOptions;
use std::io::Read;
use std::path::Path;

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
}

pub fn load_toml<T, P: AsRef<Path>>(p: P) -> Result<T>
where
    for<'de> T: Deserialize<'de>,
{
    let d = load_bytes(p)?;
    Ok(toml::from_slice(d.as_slice())?)
}

pub fn load_bytes<P: AsRef<Path>>(p: P) -> Result<Vec<u8>> {
    let p = p.as_ref();
    let mut f = OpenOptions::new().read(true).open(p)?;
    let mut d = Vec::new();
    f.read_to_end(&mut d)?;
    Ok(d)
}

pub fn load_config<P: AsRef<Path>>(p: P) -> Result<Config> {
    load_toml(p)
}
