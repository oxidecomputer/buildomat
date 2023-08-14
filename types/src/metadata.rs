/*
 * Copyright 2023 Oxide Computer Company
 */

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, JsonSchema, Clone, Debug, Eq, PartialEq)]
pub struct FactoryMetadataV1 {
    #[serde(default)]
    pub addresses: Vec<FactoryAddresses>,
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
