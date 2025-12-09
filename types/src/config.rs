/*
 * Copyright 2024 Oxide Computer Company
 */

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

use crate::metadata::{FactoryAddresses, FactoryMetadata, FactoryMetadataV1};

/**
 * This "extra_ips" configuration property appears against a host to denote
 * extra IP addresses that area available for jobs to use, beyond the single
 * DHCP address configured on the primary interface.
 */
#[derive(Deserialize, Debug, Clone)]
pub struct ConfigFileExtraIps {
    pub cidr: String,
    pub first: String,
    pub count: u32,
}

impl ConfigFileExtraIps {
    pub fn with_gateway(&self, name: &str, gateway: &str) -> FactoryAddresses {
        FactoryAddresses {
            name: name.to_string(),
            cidr: self.cidr.to_string(),
            first: self.first.to_string(),
            count: self.count,
            gateway: Some(gateway.to_string()),
            routed: false,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
#[serde(untagged)]
pub enum Inheritable {
    Inherit(ConfigFileInherit),
    Bool(bool),
    Num(u32),
    String(String),
}

/**
 * This "diag" configuration file section can appear at the global factory level
 * and at the level of each target.  Values specified at the target level
 * override those specified globally.  This section is shared by at least the
 * "aws" and "propolis" factories, and is likely appropriate for any other
 * factories that create ephemeral virtual machines to which we may wish
 * diagnostic access.
 */
#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct ConfigFileDiag {
    pub root_password_hash: Option<Inheritable>,
    pub root_authorized_keys: Option<Inheritable>,
    pub dump_to_rpool: Option<Inheritable>,
    pub pre_job_diagnostic_script: Option<Inheritable>,
    pub post_job_diagnostic_script: Option<Inheritable>,
    pub rpool_disable_sync: Option<Inheritable>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct ConfigFileInherit {
    pub inherit: bool,
}

trait InheritableConversion {
    fn as_string(&self) -> Result<Option<String>>;
    fn as_u32(&self) -> Result<Option<u32>>;
    fn as_bool(&self) -> Result<Option<bool>>;
}

impl InheritableConversion for Option<Inheritable> {
    fn as_string(&self) -> Result<Option<String>> {
        match self {
            Some(Inheritable::Inherit(_)) | None => Ok(None),
            Some(Inheritable::String(s)) => Ok(Some(s.to_string())),
            Some(Inheritable::Bool(_) | Inheritable::Num(_)) => {
                bail!("expected string, not bool or num");
            }
        }
    }

    fn as_u32(&self) -> Result<Option<u32>> {
        match self {
            Some(Inheritable::Inherit(_)) | None => Ok(None),
            Some(Inheritable::Num(n)) => Ok(Some(*n)),
            Some(Inheritable::Bool(_) | Inheritable::String(_)) => {
                bail!("expected u32, not bool or string");
            }
        }
    }

    fn as_bool(&self) -> Result<Option<bool>> {
        match self {
            Some(Inheritable::Inherit(_)) | None => Ok(None),
            Some(Inheritable::Bool(b)) => Ok(Some(*b)),
            Some(Inheritable::Num(_) | Inheritable::String(_)) => {
                bail!("expected bool, not u32 or string");
            }
        }
    }
}

fn inherit(
    base: &Option<Inheritable>,
    over: &Option<Inheritable>,
) -> Option<Inheritable> {
    match over {
        Some(Inheritable::Inherit(ConfigFileInherit { inherit })) => {
            if *inherit {
                base.clone()
            } else {
                /*
                 * Specifying "inherit = false" stops the inheritance chain at
                 * this level, but provides no value of its own.
                 */
                None
            }
        }
        None => {
            /*
             * If there is no value specified, inherit whatever the parent has:
             */
            base.clone()
        }
        other => {
            /*
             * Use the value provided at this level:
             */
            other.clone()
        }
    }
}

impl ConfigFileDiag {
    pub fn apply_overrides(&self, overrides: &Self) -> Self {
        let Self {
            root_password_hash,
            root_authorized_keys,
            dump_to_rpool,
            pre_job_diagnostic_script,
            post_job_diagnostic_script,
            rpool_disable_sync,
        } = overrides;

        Self {
            root_password_hash: inherit(
                &self.root_password_hash,
                root_password_hash,
            ),
            root_authorized_keys: inherit(
                &self.root_authorized_keys,
                root_authorized_keys,
            ),
            dump_to_rpool: inherit(&self.dump_to_rpool, dump_to_rpool),
            pre_job_diagnostic_script: inherit(
                &self.pre_job_diagnostic_script,
                pre_job_diagnostic_script,
            ),
            post_job_diagnostic_script: inherit(
                &self.post_job_diagnostic_script,
                post_job_diagnostic_script,
            ),
            rpool_disable_sync: inherit(
                &self.rpool_disable_sync,
                rpool_disable_sync,
            ),
        }
    }

    pub fn build_with_addresses(
        &self,
        addresses: Vec<FactoryAddresses>,
    ) -> Result<FactoryMetadata> {
        Ok(FactoryMetadata::V1(FactoryMetadataV1 {
            addresses,
            root_password_hash: self.root_password_hash.as_string()?,
            root_authorized_keys: self.root_authorized_keys.as_string()?,
            dump_to_rpool: self.dump_to_rpool.as_u32()?,
            pre_job_diagnostic_script: self
                .pre_job_diagnostic_script
                .as_string()?,
            post_job_diagnostic_script: self
                .post_job_diagnostic_script
                .as_string()?,
            rpool_disable_sync: self.rpool_disable_sync.as_bool()?,
        }))
    }

    pub fn build(&self) -> Result<FactoryMetadata> {
        self.build_with_addresses(Default::default())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn empty() -> Result<()> {
        let base = ConfigFileDiag {
            root_password_hash: None,
            root_authorized_keys: None,
            dump_to_rpool: None,
            pre_job_diagnostic_script: None,
            post_job_diagnostic_script: None,
            rpool_disable_sync: None,
        };

        let loaded = base.build()?;

        let expect = FactoryMetadata::V1(FactoryMetadataV1 {
            addresses: Default::default(),
            root_password_hash: None,
            root_authorized_keys: None,
            dump_to_rpool: None,
            pre_job_diagnostic_script: None,
            post_job_diagnostic_script: None,
            rpool_disable_sync: None,
        });

        assert_eq!(loaded, expect);
        Ok(())
    }

    #[test]
    fn two_empties() -> Result<()> {
        let base = ConfigFileDiag {
            root_password_hash: None,
            root_authorized_keys: None,
            dump_to_rpool: None,
            pre_job_diagnostic_script: None,
            post_job_diagnostic_script: None,
            rpool_disable_sync: None,
        };

        let over = ConfigFileDiag {
            root_password_hash: None,
            root_authorized_keys: None,
            dump_to_rpool: None,
            pre_job_diagnostic_script: None,
            post_job_diagnostic_script: None,
            rpool_disable_sync: None,
        };

        let loaded = base.apply_overrides(&over).build()?;

        let expect = FactoryMetadata::V1(FactoryMetadataV1 {
            addresses: Default::default(),
            root_password_hash: None,
            root_authorized_keys: None,
            dump_to_rpool: None,
            pre_job_diagnostic_script: None,
            post_job_diagnostic_script: None,
            rpool_disable_sync: None,
        });

        assert_eq!(loaded, expect);
        Ok(())
    }

    #[test]
    fn base_but_no_overrides() -> Result<()> {
        let base = ConfigFileDiag {
            root_password_hash: Some(Inheritable::String("password".into())),
            root_authorized_keys: Some(Inheritable::String("keys".into())),
            dump_to_rpool: Some(Inheritable::Num(100)),
            pre_job_diagnostic_script: Some(Inheritable::String("pre".into())),
            post_job_diagnostic_script: Some(Inheritable::String(
                "post".into(),
            )),
            rpool_disable_sync: Some(Inheritable::Bool(false)),
        };

        let over = ConfigFileDiag {
            root_password_hash: None,
            root_authorized_keys: None,
            dump_to_rpool: None,
            pre_job_diagnostic_script: None,
            post_job_diagnostic_script: None,
            rpool_disable_sync: None,
        };

        let loaded = base.apply_overrides(&over).build()?;

        let expect = FactoryMetadata::V1(FactoryMetadataV1 {
            addresses: Default::default(),
            root_password_hash: Some("password".to_string()),
            root_authorized_keys: Some("keys".to_string()),
            dump_to_rpool: Some(100),
            pre_job_diagnostic_script: Some("pre".to_string()),
            post_job_diagnostic_script: Some("post".to_string()),
            rpool_disable_sync: Some(false),
        });

        assert_eq!(loaded, expect);
        Ok(())
    }

    #[test]
    fn overrides_but_no_base() -> Result<()> {
        let base = ConfigFileDiag {
            root_password_hash: None,
            root_authorized_keys: None,
            dump_to_rpool: None,
            pre_job_diagnostic_script: None,
            post_job_diagnostic_script: None,
            rpool_disable_sync: None,
        };

        let over = ConfigFileDiag {
            root_password_hash: Some(Inheritable::String("password".into())),
            root_authorized_keys: Some(Inheritable::String("keys".into())),
            dump_to_rpool: Some(Inheritable::Num(100)),
            pre_job_diagnostic_script: Some(Inheritable::String("pre".into())),
            post_job_diagnostic_script: Some(Inheritable::String(
                "post".into(),
            )),
            rpool_disable_sync: Some(Inheritable::Bool(true)),
        };

        let loaded = base.apply_overrides(&over).build()?;

        let expect = FactoryMetadata::V1(FactoryMetadataV1 {
            addresses: Default::default(),
            root_password_hash: Some("password".to_string()),
            root_authorized_keys: Some("keys".to_string()),
            dump_to_rpool: Some(100),
            pre_job_diagnostic_script: Some("pre".to_string()),
            post_job_diagnostic_script: Some("post".to_string()),
            rpool_disable_sync: Some(true),
        });

        assert_eq!(loaded, expect);
        Ok(())
    }

    fn no_inherit() -> Option<Inheritable> {
        Some(Inheritable::Inherit(ConfigFileInherit { inherit: false }))
    }

    fn yes_inherit() -> Option<Inheritable> {
        Some(Inheritable::Inherit(ConfigFileInherit { inherit: true }))
    }

    #[test]
    fn base_with_removals() -> Result<()> {
        let base = ConfigFileDiag {
            root_password_hash: Some(Inheritable::String("password".into())),
            root_authorized_keys: Some(Inheritable::String("keys".into())),
            dump_to_rpool: Some(Inheritable::Num(100)),
            pre_job_diagnostic_script: Some(Inheritable::String("pre".into())),
            post_job_diagnostic_script: Some(Inheritable::String(
                "post".into(),
            )),
            rpool_disable_sync: Some(Inheritable::Bool(true)),
        };

        let over = ConfigFileDiag {
            root_password_hash: no_inherit(),
            root_authorized_keys: no_inherit(),
            dump_to_rpool: no_inherit(),
            pre_job_diagnostic_script: no_inherit(),
            post_job_diagnostic_script: no_inherit(),
            rpool_disable_sync: no_inherit(),
        };

        let loaded = base.apply_overrides(&over).build()?;

        let expect = FactoryMetadata::V1(FactoryMetadataV1 {
            addresses: Default::default(),
            root_password_hash: None,
            root_authorized_keys: None,
            dump_to_rpool: None,
            pre_job_diagnostic_script: None,
            post_job_diagnostic_script: None,
            rpool_disable_sync: None,
        });

        assert_eq!(loaded, expect);
        Ok(())
    }

    #[test]
    fn explicit_inherit_true() -> Result<()> {
        // When "inherit: true" is specified, values should be inherited from base
        let base = ConfigFileDiag {
            root_password_hash: Some(Inheritable::String("password".into())),
            root_authorized_keys: Some(Inheritable::String("keys".into())),
            dump_to_rpool: Some(Inheritable::Num(100)),
            pre_job_diagnostic_script: Some(Inheritable::String("pre".into())),
            post_job_diagnostic_script: Some(Inheritable::String(
                "post".into(),
            )),
            rpool_disable_sync: Some(Inheritable::Bool(true)),
        };

        let over = ConfigFileDiag {
            root_password_hash: yes_inherit(),
            root_authorized_keys: yes_inherit(),
            dump_to_rpool: yes_inherit(),
            pre_job_diagnostic_script: yes_inherit(),
            post_job_diagnostic_script: yes_inherit(),
            rpool_disable_sync: yes_inherit(),
        };

        let loaded = base.apply_overrides(&over).build()?;

        // inherit: true should pass through base values
        let expect = FactoryMetadata::V1(FactoryMetadataV1 {
            addresses: Default::default(),
            root_password_hash: Some("password".to_string()),
            root_authorized_keys: Some("keys".to_string()),
            dump_to_rpool: Some(100),
            pre_job_diagnostic_script: Some("pre".to_string()),
            post_job_diagnostic_script: Some("post".to_string()),
            rpool_disable_sync: Some(true),
        });

        assert_eq!(loaded, expect);
        Ok(())
    }

    #[test]
    fn mixed_inherit_and_override() -> Result<()> {
        // Some fields inherit, some override, some block inheritance
        let base = ConfigFileDiag {
            root_password_hash: Some(Inheritable::String("base-password".into())),
            root_authorized_keys: Some(Inheritable::String("base-keys".into())),
            dump_to_rpool: Some(Inheritable::Num(100)),
            pre_job_diagnostic_script: Some(Inheritable::String("base-pre".into())),
            post_job_diagnostic_script: Some(Inheritable::String(
                "base-post".into(),
            )),
            rpool_disable_sync: Some(Inheritable::Bool(false)),
        };

        let over = ConfigFileDiag {
            // Explicitly inherit from base
            root_password_hash: yes_inherit(),
            // Override with new value
            root_authorized_keys: Some(Inheritable::String("new-keys".into())),
            // Block inheritance (no value)
            dump_to_rpool: no_inherit(),
            // Implicit inherit (None = inherit from base)
            pre_job_diagnostic_script: None,
            // Override with new value
            post_job_diagnostic_script: Some(Inheritable::String(
                "new-post".into(),
            )),
            // Override boolean
            rpool_disable_sync: Some(Inheritable::Bool(true)),
        };

        let loaded = base.apply_overrides(&over).build()?;

        let expect = FactoryMetadata::V1(FactoryMetadataV1 {
            addresses: Default::default(),
            root_password_hash: Some("base-password".to_string()), // inherited
            root_authorized_keys: Some("new-keys".to_string()),    // overridden
            dump_to_rpool: None,                                   // blocked
            pre_job_diagnostic_script: Some("base-pre".to_string()), // implicit inherit
            post_job_diagnostic_script: Some("new-post".to_string()), // overridden
            rpool_disable_sync: Some(true),                        // overridden
        });

        assert_eq!(loaded, expect);
        Ok(())
    }
}
