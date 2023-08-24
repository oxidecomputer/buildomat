/*
 * Copyright 2023 Oxide Computer Company
 */

use std::path::{Path};

use anyhow::{Result};
use serde::Deserialize;
#[allow(unused_imports)]
use slog::{error, info, o, warn, Logger};
use buildomat_common::*;

#[derive(Deserialize, Debug)]
pub struct ConfigFile {
    pub admin: ConfigFileAdmin,
    #[allow(dead_code)]
    pub general: ConfigFileGeneral,
    pub storage: ConfigFileStorage,
    pub sqlite: ConfigFileSqlite,
    pub job: ConfigFileJob,
}

#[derive(Deserialize, Debug)]
pub struct ConfigFileGeneral {
    #[allow(dead_code)]
    pub baseurl: String,
}

#[derive(Deserialize, Debug)]
pub struct ConfigFileJob {
    pub max_runtime: u64,
}

#[derive(Deserialize, Debug)]
pub struct ConfigFileSqlite {
    #[serde(default)]
    pub cache_kb: Option<u32>,
}

#[derive(Deserialize, Debug)]
pub struct ConfigFileAdmin {
    pub token: String,
    /**
     * Should we hold off on new VM creation by default at startup?
     */
    pub hold: bool,
}

#[derive(Deserialize, Debug)]
pub struct ConfigFileStorage {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub bucket: String,
    pub prefix: String,
    pub region: String,
}

impl ConfigFileStorage {
    pub fn creds(&self) -> aws_credential_types::Credentials {
        aws_credential_types::Credentials::new(
            &self.access_key_id,
            &self.secret_access_key,
            None,
            None,
            "buildomat",
        )
    }

    pub fn region(&self) -> aws_types::region::Region {
        aws_types::region::Region::new(self.region.to_string())
    }
}

pub fn load<P: AsRef<Path>>(path: P) -> Result<ConfigFile> {
    read_toml(path.as_ref())
}
