/*
 * Copyright 2025 Oxide Computer Company
 */

use std::path::Path;

use anyhow::Result;
use buildomat_common::*;
use serde::Deserialize;
#[allow(unused_imports)]
use slog::{error, info, o, warn, Logger};

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct ConfigFile {
    pub admin: ConfigFileAdmin,
    #[allow(dead_code)]
    pub general: ConfigFileGeneral,
    pub storage: ConfigFileStorage,
    pub sqlite: ConfigFileSqlite,
    pub job: ConfigFileJob,
    #[serde(default)]
    pub file: ConfigFileFile,
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct ConfigFileGeneral {
    #[allow(dead_code)]
    pub baseurl: String,
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct ConfigFileJob {
    pub max_runtime: u64,
    #[serde(default = "default_max_size_per_file_mb")]
    pub max_size_per_file_mb: u64,
    #[serde(default)]
    pub auto_archive: bool,
    #[serde(default)]
    pub auto_purge: bool,
    #[serde(default = "default_purge_delay_msec")]
    pub purge_delay_msec: u64,
}

impl ConfigFileJob {
    pub fn max_bytes_per_output(&self) -> u64 {
        self.max_size_per_file_mb.saturating_mul(1024 * 1024)
    }

    pub fn max_bytes_per_input(&self) -> u64 {
        self.max_size_per_file_mb.saturating_mul(1024 * 1024)
    }
}

fn default_max_size_per_file_mb() -> u64 {
    /*
     * By default, allow 1GB files to be uploaded:
     */
    1024
}

fn default_purge_delay_msec() -> u64 {
    /*
     * By default, wait half a second after a successful purge before purging
     * another job.  When there are a lot of jobs to purge, this can help to
     * keep the system responsive to active jobs.
     */
    500
}

#[derive(Deserialize, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct ConfigFileFile {
    pub auto_archive: Option<bool>,
}

impl ConfigFileFile {
    pub fn auto_archive(&self) -> bool {
        self.auto_archive.unwrap_or(true)
    }
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct ConfigFileSqlite {
    #[serde(default)]
    pub cache_kb: Option<u32>,
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct ConfigFileAdmin {
    pub token: String,
    /**
     * Should we hold off on new VM creation by default at startup?
     */
    pub hold: bool,
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
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
