use std::collections::HashMap;

use anyhow::Result;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct Config {
    pub main: Main,
    pub block_dev: HashMap<String, BlockDev>,
    pub dev: HashMap<String, Dev>,
    pub cloudinit: CloudInit,
}

#[derive(Debug, Serialize)]
pub struct Main {
    pub name: String,
    pub bootrom: String,
    pub cpus: u32,
    pub memory: u32,

    pub exit_on_halt: u8,
    pub exit_on_reboot: u8,

    pub use_reservoir: bool,
}

#[derive(Debug, Serialize)]
pub struct BlockDev {
    #[serde(rename = "type")]
    pub type_: String,
    pub path: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct Dev {
    pub driver: String,
    #[serde(rename = "pci-path")]
    pub pci_path: String,
    pub block_dev: Option<String>,
    pub vnic: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct CloudInit {
    pub user_data: Option<String>,
    pub meta_data: Option<String>,
    pub network_config: Option<String>,
}

impl Config {
    pub fn to_toml(&self) -> Result<String> {
        Ok(toml::to_string_pretty(self)?)
    }
}
