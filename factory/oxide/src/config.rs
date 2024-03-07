/*
 * Copyright 2024 Oxide Computer Company
 */

use std::collections::HashMap;

use serde::Deserialize;

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConfigFile {
    pub oxide: ConfigFileOxide,
    pub general: ConfigFileGeneral,
    pub factory: ConfigFileFactory,
    pub target: HashMap<String, ConfigFileOxideTarget>,
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConfigFileGeneral {
    pub baseurl: String,
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConfigFileFactory {
    pub token: String,
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConfigFileOxideTarget {
    /// Image name to use as a base
    pub image: String,
    /// Size of the disk to create
    pub disk_size: String,
    /// Number of cpus to use
    pub cpu_cnt: u16,
    /// Memory for instance
    pub memory: String,
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConfigFileOxide {
    /// Rack running Oxide API
    pub rack_host: String,
    /// access token
    pub rack_token: String,
    /// Project to create instances in
    pub project: String,
    /// total of number of workers to create
    pub limit_total: usize
}
