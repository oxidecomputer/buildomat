/*
 * Copyright 2021 Oxide Computer Company
 */

use std::collections::HashMap;

use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub(crate) struct ConfigFile {
    pub aws: ConfigFileAws,
    pub general: ConfigFileGeneral,
    pub factory: ConfigFileFactory,
    pub target: HashMap<String, ConfigFileAwsTarget>,
}

#[derive(Deserialize, Debug)]
pub(crate) struct ConfigFileGeneral {
    pub baseurl: String,
}

#[derive(Deserialize, Debug)]
pub(crate) struct ConfigFileFactory {
    pub token: String,
}

#[derive(Deserialize, Debug)]
pub(crate) struct ConfigFileAwsTarget {
    pub instance_type: String,
    pub root_size_gb: i64,
    pub ami: String,
}

#[derive(Deserialize, Debug)]
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
