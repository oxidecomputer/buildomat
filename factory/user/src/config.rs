/*
 * Copyright 2026 Oxide Computer Company
 */

use crate::db::types::TargetId;
use anyhow::{bail, Result};
use serde::Deserialize;
use std::collections::{BTreeMap, HashSet};

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConfigFile {
    pub(crate) general: ConfigFileGeneral,
    pub(crate) factory: ConfigFileFactory,
    #[serde(default)]
    pub(crate) illumos: ConfigFileIllumos,
    pub(crate) slots: BTreeMap<String, ConfigFileSlot>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConfigFileGeneral {
    pub(crate) baseurl: String,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConfigFileFactory {
    pub(crate) token: String,
}

#[derive(Deserialize, Debug, Clone, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConfigFileIllumos {
    pub(crate) parent_zfs_dataset: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConfigFileSlot {
    pub(crate) target: TargetId,
    #[serde(default)]
    pub(crate) conflicts_with_slots: Vec<String>,
    #[serde(default)]
    pub(crate) add_to_groups: Vec<String>,
    #[serde(default)]
    pub(crate) env: BTreeMap<String, String>,
}

pub(crate) fn validate_config(config: &ConfigFile) -> Result<()> {
    /*
     * Validate that conflicts_with_slots is not malformed.
     */
    let slot_names = config.slots.keys().collect::<HashSet<_>>();
    for (slot_name, slot) in &config.slots {
        for conflict in &slot.conflicts_with_slots {
            if conflict == slot_name {
                bail!("slot {slot_name:?} cannot conflict with itself");
            }
            if !slot_names.contains(conflict) {
                bail!(
                    "slot {slot_name:?} cannot conflict with unknown \
                     slot {conflict:?}"
                );
            }
        }
    }

    Ok(())
}

#[cfg(test)]
pub(crate) struct ConfigBuilder {
    inner: ConfigFile,
}

#[cfg(test)]
impl ConfigBuilder {
    pub(crate) fn new() -> Self {
        Self {
            inner: ConfigFile {
                general: ConfigFileGeneral {
                    baseurl: "http://buildomat.invalid:9979".into(),
                },
                factory: ConfigFileFactory { token: "".into() },
                illumos: ConfigFileIllumos { parent_zfs_dataset: None },
                slots: BTreeMap::new(),
            },
        }
    }

    pub(crate) fn slot(
        mut self,
        name: &str,
        target: TargetId,
        mutate: impl FnOnce(&mut ConfigFileSlot),
    ) -> Self {
        let mut slot = ConfigFileSlot {
            target,
            conflicts_with_slots: Vec::new(),
            add_to_groups: Vec::new(),
            env: BTreeMap::new(),
        };
        mutate(&mut slot);
        self.inner.slots.insert(name.into(), slot);
        self
    }

    pub(crate) fn build(self) -> ConfigFile {
        self.inner
    }
}
