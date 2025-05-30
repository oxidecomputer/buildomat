/*
 * Copyright 2025 Oxide Computer Company
 */

use iddqd::{id_upcast, IdHashItem, IdHashMap};
use std::{collections::HashMap, path::Path};

use anyhow::{bail, Result};
use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConfigFile {
    pub general: ConfigFileGeneral,
    pub factory: ConfigFileFactory,
    pub host: HashMap<String, ConfigFileHost>,
    pub target: HashMap<String, ConfigFileTarget>,
    pub tools: ConfigFileTools,
    // #[serde(default)]
    // pub diag: ConfigFileDiag,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConfigFileGeneral {
    pub baseurl: String,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConfigFileFactory {
    pub token: String,
}

#[derive(Deserialize, Debug, Clone)]
pub(crate) struct ConfigFileTarget {
    pub hosts: Vec<String>,
    pub os_dir: String,
}

#[derive(Deserialize, Debug, Clone)]
pub(crate) struct ConfigFileHost {
    pub slot: String,

    pub serial: String,
    pub partno: String,
    pub rev: u64,

    pub ip: String,
    pub mac: String,
    pub control_nic: String,

    pub cleaning_os_dir: String,

    // XXX debug_*?
    // XXX extra_ips
}

#[derive(Deserialize, Debug, Clone)]
pub(crate) struct ConfigFileTools {
    pub humility: String,
    pub bootserver: String,
    pub gimlets: String,
}

#[derive(Debug, Clone)]
pub struct ConfigHost {
    pub id: crate::HostId,
    pub config: ConfigFileHost,
}

impl IdHashItem for ConfigHost {
    type Key<'a> = &'a crate::HostId;

    fn key(&self) -> Self::Key<'_> {
        &self.id
    }

    id_upcast!();
}

pub struct Config {
    pub general: ConfigFileGeneral,
    pub factory: ConfigFileFactory,
    pub tools: ConfigFileTools,
    pub hosts: IdHashMap<ConfigHost>,
}

pub fn load<P: AsRef<Path>>(path: P) -> Result<Config> {
    let p = path.as_ref();
    let ConfigFile { general, factory, host, target, tools } =
        buildomat_common::read_toml(p)?;

    let mut hosts: IdHashMap<ConfigHost> = Default::default();
    for (id, hc) in host {
        hosts
            .insert_unique(ConfigHost { id: id.parse()?, config: hc })
            .unwrap();
    }

    Ok(Config { general, factory, tools, hosts })
}
