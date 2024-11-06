/*
 * Copyright 2024 Oxide Computer Company
 */

use std::{collections::HashMap, net::Ipv4Addr, path::PathBuf};

use anyhow::{bail, Result};
use serde::Deserialize;

use crate::db::types::InstanceId;

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConfigFile {
    pub general: ConfigFileGeneral,
    pub factory: ConfigFileFactory,
    //pub template: ConfigFileSlotTemplate,
    //#[serde(default)]
    pub target: HashMap<String, ConfigFileTarget>,
    pub slot: HashMap<String, ConfigFileSlot>,
    //pub slots: u32,
    //pub software_dir: String,
    pub nodename: String,
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
#[serde(deny_unknown_fields)]
pub(crate) struct ConfigFileTarget {
    pub slot: String,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConfigFileSlot {
    pub vid: u16,
    pub pid: u16,
    pub serial: String,
    pub kind: Kind,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename = "snake_case")]
pub(crate) enum Kind {
    Basic,
    Rot,
}

impl ConfigFile {
    pub fn for_instance(&self, id: &InstanceId) -> Result<InstanceInSlot> {
        Ok(InstanceInSlot { config: self, id: id.clone() })
    }
}

pub(crate) struct InstanceInSlot<'a> {
    config: &'a ConfigFile,
    id: InstanceId,
}
