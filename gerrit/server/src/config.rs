use serde::Deserialize;
use std::collections::HashMap;

#[derive(Deserialize, Debug)]
pub struct ConfigFile {
    pub gerrit: HashMap<String, ConfigGerrit>,
}

#[derive(Deserialize, Debug)]
pub struct ConfigGerrit {
    pub url: String,
}
