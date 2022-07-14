/*
 * Copyright 2022 Oxide Computer Company
 */

use std::collections::HashMap;
use std::io::Read;

use anyhow::Result;
use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub(crate) struct ConfigFile {
    pub general: ConfigFileGeneral,
    pub factory: ConfigFileFactory,
    pub target: HashMap<String, ConfigFileTarget>,
    pub host: HashMap<String, ConfigFileHost>,
}

#[derive(Deserialize, Debug, Clone)]
pub(crate) struct ConfigFileGeneral {
    pub baseurl: String,
}

#[derive(Deserialize, Debug, Clone)]
pub(crate) struct ConfigFileFactory {
    pub token: String,
}

#[derive(Deserialize, Debug, Clone)]
pub(crate) struct ConfigFileTarget {
    pub nodename: String,
    pub os_dir: String,
}

#[derive(Deserialize, Debug, Clone)]
pub(crate) struct ConfigFileHost {
    #[allow(dead_code)]
    pub ip: String,
    pub console: String,
    pub lab_baseurl: String,
    pub nodename: String,
    pub lom_ip: String,
    pub lom_username: String,
    pub lom_password: String,
}
