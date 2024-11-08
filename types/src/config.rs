/*
 * Copyright 2024 Oxide Computer Company
 */

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

use crate::metadata::{FactoryMetadata, FactoryMetadataV1};

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
#[serde(untagged)]
pub enum StringOrBool {
    String(String),
    Bool(bool),
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
#[serde(untagged)]
pub enum NumOrBool {
    Num(u32),
    Bool(bool),
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
    pub root_password_hash: Option<StringOrBool>,
    pub root_authorized_keys: Option<StringOrBool>,
    pub dump_to_rpool: Option<NumOrBool>,
    pub pre_job_diagnostic_script: Option<StringOrBool>,
    pub post_job_diagnostic_script: Option<StringOrBool>,
}

fn build_string(val: &Option<StringOrBool>) -> Result<Option<String>> {
    match val {
        Some(StringOrBool::Bool(true)) => {
            bail!("cannot use \"true\" value for diag property");
        }
        Some(StringOrBool::Bool(false)) | None => Ok(None),
        Some(StringOrBool::String(s)) => Ok(Some(s.to_string())),
    }
}

fn build_num(val: &Option<NumOrBool>) -> Result<Option<u32>> {
    match val {
        Some(NumOrBool::Bool(true)) => {
            bail!("cannot use \"true\" value for diag property");
        }
        Some(NumOrBool::Bool(false)) | None => Ok(None),
        Some(NumOrBool::Num(n)) => Ok(Some(*n)),
    }
}

fn override_string(
    base: &Option<StringOrBool>,
    over: &Option<StringOrBool>,
) -> Result<Option<StringOrBool>> {
    match over {
        Some(StringOrBool::Bool(false)) => {
            /*
             * Use of the boolean value "false" tells us that the operator does
             * not want us to inherit the top-level value in this more specific
             * context; e.g., there might be a global diagnostic applied, but we
             * want to disable the diagnostic for just one target on the system.
             */
            Ok(None)
        }
        None => Ok(base.clone()),
        other => Ok(other.clone()),
    }
}

fn override_num(
    base: &Option<NumOrBool>,
    over: &Option<NumOrBool>,
) -> Result<Option<NumOrBool>> {
    match over {
        Some(NumOrBool::Bool(false)) => {
            /*
             * Use of the boolean value "false" tells us that the operator does
             * not want us to inherit the top-level value in this more specific
             * context; e.g., there might be a global diagnostic applied, but we
             * want to disable the diagnostic for just one target on the system.
             */
            Ok(None)
        }
        None => Ok(base.clone()),
        other => Ok(other.clone()),
    }
}

impl ConfigFileDiag {
    pub fn apply_overrides(&self, overrides: &Self) -> Result<Self> {
        let Self {
            root_password_hash,
            root_authorized_keys,
            dump_to_rpool,
            pre_job_diagnostic_script,
            post_job_diagnostic_script,
        } = overrides;

        Ok(Self {
            root_password_hash: override_string(
                &self.root_password_hash,
                &root_password_hash,
            )?,
            root_authorized_keys: override_string(
                &self.root_authorized_keys,
                &root_authorized_keys,
            )?,
            dump_to_rpool: override_num(&self.dump_to_rpool, &dump_to_rpool)?,
            pre_job_diagnostic_script: override_string(
                &self.pre_job_diagnostic_script,
                &pre_job_diagnostic_script,
            )?,
            post_job_diagnostic_script: override_string(
                &self.post_job_diagnostic_script,
                &post_job_diagnostic_script,
            )?,
        })
    }

    pub fn build(&self) -> Result<FactoryMetadata> {
        Ok(FactoryMetadata::V1(FactoryMetadataV1 {
            addresses: Default::default(),
            root_password_hash: build_string(&self.root_password_hash)?,
            root_authorized_keys: build_string(&self.root_authorized_keys)?,
            dump_to_rpool: build_num(&self.dump_to_rpool)?,
            pre_job_diagnostic_script: build_string(
                &self.pre_job_diagnostic_script,
            )?,
            post_job_diagnostic_script: build_string(
                &self.post_job_diagnostic_script,
            )?,
        }))
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
        };

        let loaded = base.build()?;

        let expect = FactoryMetadata::V1(FactoryMetadataV1 {
            addresses: Default::default(),
            root_password_hash: None,
            root_authorized_keys: None,
            dump_to_rpool: None,
            pre_job_diagnostic_script: None,
            post_job_diagnostic_script: None,
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
        };

        let over = ConfigFileDiag {
            root_password_hash: None,
            root_authorized_keys: None,
            dump_to_rpool: None,
            pre_job_diagnostic_script: None,
            post_job_diagnostic_script: None,
        };

        let loaded = base.apply_overrides(&over)?.build()?;

        let expect = FactoryMetadata::V1(FactoryMetadataV1 {
            addresses: Default::default(),
            root_password_hash: None,
            root_authorized_keys: None,
            dump_to_rpool: None,
            pre_job_diagnostic_script: None,
            post_job_diagnostic_script: None,
        });

        assert_eq!(loaded, expect);
        Ok(())
    }

    #[test]
    fn base_but_no_overrides() -> Result<()> {
        let base = ConfigFileDiag {
            root_password_hash: Some(StringOrBool::String("password".into())),
            root_authorized_keys: Some(StringOrBool::String("keys".into())),
            dump_to_rpool: Some(NumOrBool::Num(100)),
            pre_job_diagnostic_script: Some(StringOrBool::String("pre".into())),
            post_job_diagnostic_script: Some(StringOrBool::String(
                "post".into(),
            )),
        };

        let over = ConfigFileDiag {
            root_password_hash: None,
            root_authorized_keys: None,
            dump_to_rpool: None,
            pre_job_diagnostic_script: None,
            post_job_diagnostic_script: None,
        };

        let loaded = base.apply_overrides(&over)?.build()?;

        let expect = FactoryMetadata::V1(FactoryMetadataV1 {
            addresses: Default::default(),
            root_password_hash: Some("password".to_string()),
            root_authorized_keys: Some("keys".to_string()),
            dump_to_rpool: Some(100),
            pre_job_diagnostic_script: Some("pre".to_string()),
            post_job_diagnostic_script: Some("post".to_string()),
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
        };

        let over = ConfigFileDiag {
            root_password_hash: Some(StringOrBool::String("password".into())),
            root_authorized_keys: Some(StringOrBool::String("keys".into())),
            dump_to_rpool: Some(NumOrBool::Num(100)),
            pre_job_diagnostic_script: Some(StringOrBool::String("pre".into())),
            post_job_diagnostic_script: Some(StringOrBool::String(
                "post".into(),
            )),
        };

        let loaded = base.apply_overrides(&over)?.build()?;

        let expect = FactoryMetadata::V1(FactoryMetadataV1 {
            addresses: Default::default(),
            root_password_hash: Some("password".to_string()),
            root_authorized_keys: Some("keys".to_string()),
            dump_to_rpool: Some(100),
            pre_job_diagnostic_script: Some("pre".to_string()),
            post_job_diagnostic_script: Some("post".to_string()),
        });

        assert_eq!(loaded, expect);
        Ok(())
    }

    #[test]
    fn base_with_removals() -> Result<()> {
        let base = ConfigFileDiag {
            root_password_hash: Some(StringOrBool::String("password".into())),
            root_authorized_keys: Some(StringOrBool::String("keys".into())),
            dump_to_rpool: Some(NumOrBool::Num(100)),
            pre_job_diagnostic_script: Some(StringOrBool::String("pre".into())),
            post_job_diagnostic_script: Some(StringOrBool::String(
                "post".into(),
            )),
        };

        let over = ConfigFileDiag {
            root_password_hash: Some(StringOrBool::Bool(false)),
            root_authorized_keys: Some(StringOrBool::Bool(false)),
            dump_to_rpool: Some(NumOrBool::Bool(false)),
            pre_job_diagnostic_script: Some(StringOrBool::Bool(false)),
            post_job_diagnostic_script: Some(StringOrBool::Bool(false)),
        };

        let loaded = base.apply_overrides(&over)?.build()?;

        let expect = FactoryMetadata::V1(FactoryMetadataV1 {
            addresses: Default::default(),
            root_password_hash: None,
            root_authorized_keys: None,
            dump_to_rpool: None,
            pre_job_diagnostic_script: None,
            post_job_diagnostic_script: None,
        });

        assert_eq!(loaded, expect);
        Ok(())
    }
}
