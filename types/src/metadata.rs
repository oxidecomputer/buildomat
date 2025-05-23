/*
 * Copyright 2024 Oxide Computer Company
 */

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, JsonSchema, Clone, Debug, Eq, PartialEq)]
pub struct FactoryMetadataV1 {
    #[serde(default)]
    pub addresses: Vec<FactoryAddresses>,
    pub root_password_hash: Option<String>,
    pub root_authorized_keys: Option<String>,
    pub dump_to_rpool: Option<u32>,
    pub pre_job_diagnostic_script: Option<String>,
    pub post_job_diagnostic_script: Option<String>,
    pub rpool_disable_sync: Option<bool>,
}

#[derive(Serialize, Deserialize, JsonSchema, Clone, Debug, Eq, PartialEq)]
pub struct FactoryAddresses {
    pub name: String,
    pub cidr: String,
    pub first: String,
    pub count: u32,
    pub routed: bool,
    pub gateway: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Clone, Debug, Eq, PartialEq)]
#[serde(tag = "v")]
pub enum FactoryMetadata {
    #[serde(rename = "1")]
    V1(FactoryMetadataV1),
}

impl FactoryMetadata {
    pub fn root_password_hash(&self) -> Option<&str> {
        match self {
            FactoryMetadata::V1(md) => md.root_password_hash.as_deref(),
        }
    }

    pub fn root_authorized_keys(&self) -> Option<&str> {
        match self {
            FactoryMetadata::V1(md) => md.root_authorized_keys.as_deref(),
        }
    }

    pub fn addresses(&self) -> &[FactoryAddresses] {
        match self {
            FactoryMetadata::V1(md) => md.addresses.as_ref(),
        }
    }

    /**
     * Return the size in megabytes of the dump device to create, if one should
     * be created.
     */
    pub fn dump_to_rpool(&self) -> Option<u32> {
        match self {
            FactoryMetadata::V1(md) => md.dump_to_rpool,
        }
    }

    pub fn pre_job_diagnostic_script(&self) -> Option<&str> {
        match self {
            FactoryMetadata::V1(md) => md.pre_job_diagnostic_script.as_deref(),
        }
    }

    pub fn post_job_diagnostic_script(&self) -> Option<&str> {
        match self {
            FactoryMetadata::V1(md) => md.post_job_diagnostic_script.as_deref(),
        }
    }

    /**
     * Should the agent set "sync=disabled" on the root ZFS pool?  We expect
     * this to be useful in most cases, so we default to yes if the factory
     * configuration does not override.
     */
    pub fn rpool_disable_sync(&self) -> bool {
        match self {
            FactoryMetadata::V1(md) => md.rpool_disable_sync.unwrap_or(true),
        }
    }
}
