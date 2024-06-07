/*
 * Copyright 2024 Oxide Computer Company
 */

use std::collections::HashMap;
use std::io::Read;
use std::path::Path;

use anyhow::{anyhow, bail, Context, Result};
use serde::Deserialize;

#[derive(Deserialize, Clone)]
pub struct Config {
    pub default_profile: Option<String>,
    pub profile: HashMap<String, Profile>,
}

#[derive(Deserialize, Clone)]
pub struct Profile {
    pub name: Option<String>,
    pub url: String,
    pub secret: String,
    pub admin_token: Option<String>,
}

fn env(n: &str) -> Option<String> {
    std::env::var(n).map(Some).unwrap_or(None)
}

impl Profile {
    fn from_env() -> Option<Profile> {
        let url = env("INPUT_URL");
        let secret = env("INPUT_SECRET");
        let admin_token = env("INPUT_ADMIN_TOKEN");

        match (url, secret) {
            (Some(url), Some(secret)) => {
                Some(Profile { name: None, url, secret, admin_token })
            }
            _ => None,
        }
    }

    fn apply_env(&mut self) {
        if let Some(url) = env("INPUT_URL") {
            self.url = url;
        }
        if let Some(secret) = env("INPUT_SECRET") {
            self.secret = secret;
        }
        if let Some(admin_token) = env("INPUT_ADMIN_TOKEN") {
            self.admin_token = Some(admin_token);
        }
    }
}

fn read_file(p: &Path) -> Result<Config> {
    let mut f = std::fs::File::open(p)?;
    let mut s = String::new();
    f.read_to_string(&mut s)?;
    Ok(toml::from_str(&s)?)
}

pub fn load(profile_name: Option<&str>) -> Result<Profile> {
    /*
     * First, try to use the environment.  If we have a complete profile in the
     * environment we don't need to look at the file system at all.
     */
    if let Some(p) = Profile::from_env() {
        return Ok(p);
    }

    /*
     * Next, locate our configuration file.
     */
    let mut path = dirs_next::config_dir()
        .ok_or_else(|| anyhow!("could not find config directory"))?;
    path.push("buildomat");
    path.push("config.toml");

    let c: Config =
        read_file(&path).with_context(|| anyhow!("reading file {:?}", path))?;

    let env_profile = env("BUILDOMAT_PROFILE");

    let (profile_name, src) = if let Some(profile) = profile_name {
        (profile, "-p argument")
    } else if let Some(profile) = env_profile.as_deref() {
        (profile, "BUILDOMAT_PROFILE environment variable")
    } else if let Some(profile) = c.default_profile.as_deref() {
        (profile, "\"default_profile\" in config.toml")
    } else {
        ("default", "fallback default")
    };

    if let Some(profile) = c.profile.get(profile_name) {
        let mut profile = profile.clone();
        profile.name = Some(profile_name.to_string());
        profile.apply_env();
        Ok(profile)
    } else {
        bail!(
            "profile \"{}\" (from {}) not found in configuration file {:?}",
            profile_name,
            src,
            path
        );
    }
}
