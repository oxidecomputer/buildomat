/*
 * Copyright 2024 Oxide Computer Company
 */

use std::collections::HashMap;

use buildomat_types::config::ConfigFileDiag;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConfigFile {
    pub aws: ConfigFileAws,
    pub general: ConfigFileGeneral,
    pub factory: ConfigFileFactory,
    pub target: HashMap<String, ConfigFileAwsTarget>,
    #[serde(default)]
    pub diag: ConfigFileDiag,
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
pub(crate) struct ConfigFileAwsTarget {
    pub instance_type: String,
    pub root_size_gb: i32,
    pub ami: String,
    #[serde(default)]
    pub diag: ConfigFileDiag,
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConfigFileAws {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub region: String,
    pub vpc: String,
    pub subnet: String,
    pub tag: String,
    pub key: String,
    pub security_group: String,
    pub limit_total: usize,
}

impl ConfigFileAws {
    pub fn tagkey_worker(&self) -> String {
        format!("{}-worker_id", self.tag)
    }

    pub fn tagkey_lease(&self) -> String {
        format!("{}-lease_id", self.tag)
    }
}
