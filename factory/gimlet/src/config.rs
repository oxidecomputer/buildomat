/*
 * Copyright 2025 Oxide Computer Company
 */

use std::{collections::HashMap, path::Path};

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
    pub nodename: Option<String>,
    #[serde(default)]
    pub nodenames: Vec<String>,
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

    // XXX debug_*?
    // XXX extra_ips
}

#[derive(Deserialize, Debug, Clone)]
pub(crate) struct ConfigFileTools {
    pub humility: String,
    pub bootserver: String,
    pub gimlets: String,
}

pub struct Config {
}

pub fn load<P: AsRef<Path>>(path: P) -> Result<Config> {
    todo!()
}
