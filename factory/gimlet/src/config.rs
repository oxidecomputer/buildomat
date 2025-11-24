/*
 * Copyright 2025 Oxide Computer Company
 */

use iddqd::{IdHashItem, IdHashMap, id_upcast};
use std::{collections::HashMap, path::Path};

use anyhow::Result;
use buildomat_types::config::{ConfigFileDiag, ConfigFileExtraIps};
use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConfigFile {
    pub general: ConfigFileGeneral,
    pub factory: ConfigFileFactory,
    pub host: HashMap<String, ConfigFileHost>,
    pub target: HashMap<String, ConfigFileTarget>,
    pub tools: ConfigFileTools,
    #[serde(default)]
    pub diag: ConfigFileDiag,
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
    pub os_dir: String,
    #[serde(default)]
    pub diag: ConfigFileDiag,
}

#[derive(Deserialize, Debug, Clone)]
pub(crate) struct ConfigFileHost {
    pub slot: String,

    pub serial: String,
    pub partno: String,
    pub rev: u64,

    pub ip: String,
    pub gateway: String,
    pub extra_ips: Option<ConfigFileExtraIps>,
    pub mac: String,
    pub control_nic: String,

    pub cleaning_os_dir: String,
}

#[derive(Deserialize, Debug, Clone)]
pub(crate) struct ConfigFileTools {
    pub humility: String,
    pub bootserver: String,
    pub gimlets: String,
}

#[derive(Debug, Clone)]
pub struct ConfigTarget {
    pub id: String,
    pub os_dir: String,
    pub diag: ConfigFileDiag,
}

impl IdHashItem for ConfigTarget {
    type Key<'a> = &'a String;

    fn key(&self) -> Self::Key<'_> {
        &self.id
    }

    id_upcast!();
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
    pub targets: IdHashMap<ConfigTarget>,
    pub diag: ConfigFileDiag,
}

pub fn load<P: AsRef<Path>>(path: P) -> Result<Config> {
    let p = path.as_ref();
    let ConfigFile { general, factory, host, target, tools, diag } =
        buildomat_common::read_toml(p)?;

    let mut hosts: IdHashMap<ConfigHost> = Default::default();
    for (id, hc) in host {
        hosts
            .insert_unique(ConfigHost { id: id.parse()?, config: hc })
            .unwrap();
    }

    let mut targets: IdHashMap<ConfigTarget> = Default::default();
    for (id, tc) in target {
        targets
            .insert_unique(ConfigTarget {
                id: id.to_string(),
                os_dir: tc.os_dir,
                diag: tc.diag,
            })
            .unwrap();
    }

    Ok(Config { general, factory, tools, hosts, targets, diag })
}
