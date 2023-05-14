/*
 * Copyright 2023 Oxide Computer Company
 */

use std::collections::HashMap;
use std::io::Read;

use anyhow::Result;
use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub(crate) struct ConfigFile {
    pub general: ConfigFileGeneral,
    pub factory: ConfigFileFactory,
    // pub target: HashMap<String, ConfigFileTarget>,
    // pub host: HashMap<String, ConfigFileHost>,
}

#[derive(Deserialize, Debug, Clone)]
pub(crate) struct ConfigFileGeneral {
    pub baseurl: String,
}

#[derive(Deserialize, Debug, Clone)]
pub(crate) struct ConfigFileFactory {
    pub token: String,
}
