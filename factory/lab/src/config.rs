/*
 * Copyright 2025 Oxide Computer Company
 */

use std::collections::HashMap;

use buildomat_types::config::ConfigFileExtraIps;
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
    pub nodename: Option<String>,
    #[serde(default)]
    pub nodenames: Vec<String>,
    pub os_dir: String,
}

impl ConfigFileTarget {
    pub fn runs_on_node(&self, nodename: &str) -> bool {
        if let Some(n) = self.nodename.as_deref() {
            if n == nodename {
                return true;
            }
        }
        for n in self.nodenames.iter() {
            if n.as_str() == nodename {
                return true;
            }
        }
        false
    }

    pub fn nodenames(&self) -> Vec<String> {
        let mut out = Vec::new();
        if let Some(n) = self.nodename.as_deref() {
            out.push(n.to_string());
        }
        for n in self.nodenames.iter() {
            out.push(n.to_string());
        }
        out
    }
}

#[derive(Deserialize, Debug, Clone)]
pub(crate) struct ConfigFileHost {
    pub ip: String,
    pub gateway: String,
    pub console: String,
    pub lab_baseurl: String,
    pub nodename: String,
    pub lom_ip: String,
    pub lom_username: String,
    pub lom_password: String,
    pub debug_os_dir: Option<String>,
    pub debug_os_postboot_sh: Option<String>,
    pub debug_boot_args: Option<String>,
    pub extra_ips: Option<ConfigFileExtraIps>,
}
