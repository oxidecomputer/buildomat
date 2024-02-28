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

    pub fn addresses(&self) -> &[FactoryAddresses] {
        match self {
            FactoryMetadata::V1(md) => md.addresses.as_ref(),
        }
    }
}
