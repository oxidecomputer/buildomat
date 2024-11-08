/*
 * Copyright 2023 Oxide Computer Company
 */

use std::{collections::HashMap, net::Ipv4Addr, path::PathBuf};

use anyhow::{bail, Result};
use buildomat_types::config::ConfigFileDiag;
use serde::Deserialize;

use crate::db::types::InstanceId;

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConfigFile {
    pub general: ConfigFileGeneral,
    pub factory: ConfigFileFactory,
    pub template: ConfigFileSlotTemplate,
    #[serde(default)]
    pub target: HashMap<String, ConfigFileTarget>,
    pub slots: u32,
    pub software_dir: String,
    pub nodename: String,

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
#[serde(deny_unknown_fields)]
pub(crate) struct ConfigFileSlotTemplate {
    zoneroot_parent_dir: String,
    disk_zvol_parent: String,

    vnic_prefix: String,
    vlan_id: Option<u16>,
    physical: String,

    base_ip: Ipv4Addr,
    base_ip_prefix: u32,
    gateway: Ipv4Addr,

    ncpus: u32,
    ram_mb: u32,
    disk_gb: u64,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConfigFileTarget {
    pub image_path: Option<String>,
    pub image_zvol: Option<String>,

    #[serde(default)]
    pub diag: ConfigFileDiag,
}

impl ConfigFileTarget {
    pub fn source(&self) -> Result<ImageSource> {
        if self.image_zvol.is_some() && self.image_path.is_some() {
            bail!("a target may not specify both a zvol and an image path");
        }

        Ok(if let Some(zvol) = self.image_zvol.as_deref() {
            ImageSource::Zvol(zvol.to_string())
        } else if let Some(path) = self.image_path.as_deref() {
            ImageSource::File(PathBuf::from(path))
        } else {
            bail!("a target must specify either a zvol or an image path");
        })
    }
}

pub enum ImageSource {
    File(PathBuf),
    Zvol(String),
}

impl ConfigFile {
    pub fn for_instance_in_slot(
        &self,
        slot: u32,
        id: &InstanceId,
    ) -> Result<InstanceInSlot> {
        Ok(InstanceInSlot { config: self, slot, id: id.clone() })
    }

    pub fn socketdir(&self) -> Result<PathBuf> {
        let dir =
            PathBuf::from(&self.template.zoneroot_parent_dir).join("zonesock");

        if dir.is_symlink() || (dir.exists() && !dir.is_dir()) {
            bail!("{dir:?} exists but is not a directory");
        }

        Ok(dir)
    }

    pub fn softwaredir(&self) -> Result<PathBuf> {
        let dir = PathBuf::from(&self.software_dir);

        if dir.is_symlink()
            || !dir.is_dir()
            || !dir.join("propolis-standalone").is_file()
            || !dir.join("OVMF_CODE.fd").is_file()
        {
            bail!(
                "software directory {dir:?} should contain propolis and a ROM"
            );
        }

        Ok(dir)
    }
}

pub(crate) struct InstanceInSlot<'a> {
    config: &'a ConfigFile,
    slot: u32,
    id: InstanceId,
}

impl InstanceInSlot<'_> {
    pub fn zonepath(&self) -> String {
        format!(
            "{}/{}",
            self.config.template.zoneroot_parent_dir,
            self.id.flat_id()
        )
    }

    pub fn zvol_name(&self) -> String {
        format!(
            "{}/{}",
            self.config.template.disk_zvol_parent,
            self.id.flat_id()
        )
    }

    pub fn vnic(&self) -> String {
        format!("{}{}", self.config.template.vnic_prefix, self.slot)
    }

    pub fn physical(&self) -> &str {
        &self.config.template.physical
    }

    pub fn vlan(&self) -> Option<u16> {
        self.config.template.vlan_id
    }

    pub fn ip(&self) -> Ipv4Addr {
        let base = u32::from_be_bytes(self.config.template.base_ip.octets());
        let ip = base.checked_add(self.slot).unwrap();
        let bytes = ip.to_be_bytes();
        Ipv4Addr::new(bytes[0], bytes[1], bytes[2], bytes[3])
    }

    pub fn cidr(&self) -> u32 {
        self.config.template.base_ip_prefix
    }

    pub fn addr(&self) -> String {
        format!("{}/{}", self.ip(), self.cidr())
    }

    pub fn gateway(&self) -> Ipv4Addr {
        self.config.template.gateway
    }

    pub fn ncpus(&self) -> u32 {
        self.config.template.ncpus
    }

    pub fn ram_mb(&self) -> u32 {
        self.config.template.ram_mb
    }

    pub fn disk_bytes(&self) -> libc::c_long {
        self.config
            .template
            .disk_gb
            .checked_mul(1024 * 1024 * 1024)
            .unwrap()
            .try_into()
            .unwrap()
    }
}
