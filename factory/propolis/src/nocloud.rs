use anyhow::Result;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct Network {
    pub version: u32,
    pub config: Vec<Config>,
}

#[derive(Debug, Serialize)]
pub struct Config {
    #[serde(rename = "type")]
    pub type_: String,
    pub name: String,
    pub mac_address: String,
    pub subnets: Vec<Subnet>,
}

#[derive(Debug, Serialize)]
pub struct Subnet {
    #[serde(rename = "type")]
    pub type_: String,
    pub address: String,
    pub gateway: String,
    pub dns_nameservers: Vec<String>,
}

impl Network {
    pub fn to_yaml(&self) -> Result<String> {
        Ok(serde_yaml::to_string(self)?)
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct MetaData {
    pub instance_id: String,
    pub local_hostname: String,
}

impl MetaData {
    pub fn to_json(&self) -> Result<String> {
        let mut out = serde_json::to_string_pretty(self)?;
        out.push('\n');
        Ok(out)
    }
}
