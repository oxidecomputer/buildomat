/*
 * Copyright 2025 Oxide Computer Company
 */

//! Testbed Management Abstraction
//!
//! This module provides an abstraction over testbed state and availability.
//! Initially backed by configuration file + local state, but designed to
//! allow future migration to Inventron or other backends.
//!
//! # Design
//!
//! The `TestbedManager` trait could be implemented by:
//! - `ConfigTestbedManager` - Static configuration from TOML (current)
//! - `InventronTestbedManager` - Dynamic queries to Inventron (future)
//!
//! For now we use a simple struct since we only have one implementation.

use std::collections::HashMap;

use anyhow::{Result, bail};

use crate::config::ConfigFileTestbed;

/// Information about a testbed's capabilities and status.
#[derive(Debug, Clone)]
pub struct TestbedInfo {
    /// Testbed name (e.g., "grapefruit-7f495641")
    pub name: String,

    /// SP type (gimlet, grapefruit, sidecar, psc)
    pub sp_type: String,

    /// Buildomat targets this testbed can serve
    pub targets: Vec<String>,

    /// SSH host for remote execution (None = local)
    pub host: Option<String>,

    /// Path to sp-runner binary
    pub sp_runner_path: String,

    /// Path to sp-runner config file
    pub sp_runner_config: String,

    /// Baseline firmware version
    pub baseline: String,

    /// Whether testbed is enabled for CI
    pub enabled: bool,
}

impl TestbedInfo {
    /// Check if this testbed can serve the given target.
    pub fn supports_target(&self, target: &str) -> bool {
        self.enabled && self.targets.iter().any(|t| t == target)
    }

    /// Check if this testbed runs locally (no SSH).
    pub fn is_local(&self) -> bool {
        self.host.is_none()
    }
}

/// Manages testbed discovery and availability.
///
/// This is the abstraction layer that could be backed by different
/// implementations (config file, Inventron, etc.).
pub struct TestbedManager {
    testbeds: HashMap<String, TestbedInfo>,
}

impl TestbedManager {
    /// Create a TestbedManager from configuration.
    pub fn from_config(
        config: &HashMap<String, ConfigFileTestbed>,
    ) -> Result<Self> {
        let mut testbeds = HashMap::new();

        for (name, cfg) in config {
            if cfg.targets.is_empty() {
                bail!("testbed {} has no targets configured", name);
            }

            let info = TestbedInfo {
                name: name.clone(),
                sp_type: cfg.sp_type.clone(),
                targets: cfg.targets.clone(),
                host: cfg.host.clone(),
                sp_runner_path: cfg.sp_runner_path.clone(),
                sp_runner_config: cfg.sp_runner_config.clone(),
                baseline: cfg.baseline.clone(),
                enabled: cfg.enabled,
            };

            testbeds.insert(name.clone(), info);
        }

        Ok(TestbedManager { testbeds })
    }

    /// Check if a testbed exists in configuration.
    pub fn exists(&self, name: &str) -> bool {
        self.testbeds.contains_key(name)
    }

    /// Get information about a specific testbed.
    pub fn get(&self, name: &str) -> Option<&TestbedInfo> {
        self.testbeds.get(name)
    }

    /// List all testbed names.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.testbeds.keys().map(|s| s.as_str())
    }

    /// Find testbeds that can serve the given target.
    pub fn find_for_target(&self, target: &str) -> Vec<&TestbedInfo> {
        self.testbeds.values().filter(|t| t.supports_target(target)).collect()
    }

    /// Get all targets that can be served by available testbeds.
    pub fn available_targets(&self) -> Vec<String> {
        self.testbeds
            .values()
            .filter(|t| t.enabled)
            .flat_map(|t| t.targets.iter().cloned())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect()
    }

    /// Find first available testbed for a target.
    ///
    /// "Available" means enabled and not currently in use.
    /// The `in_use` parameter provides testbed names that are busy.
    pub fn find_available_for_target(
        &self,
        target: &str,
        in_use: &[String],
    ) -> Option<&TestbedInfo> {
        self.testbeds
            .values()
            .find(|t| t.supports_target(target) && !in_use.contains(&t.name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> HashMap<String, ConfigFileTestbed> {
        let mut config = HashMap::new();
        config.insert(
            "grapefruit-1234".to_string(),
            ConfigFileTestbed {
                sp_type: "grapefruit".to_string(),
                targets: vec!["sp-grapefruit".to_string()],
                host: None,
                sp_runner_path: "sp-runner".to_string(),
                sp_runner_config: "config.toml".to_string(),
                baseline: "v16".to_string(),
                enabled: true,
            },
        );
        config.insert(
            "gimlet-5678".to_string(),
            ConfigFileTestbed {
                sp_type: "gimlet".to_string(),
                targets: vec![
                    "sp-gimlet".to_string(),
                    "sp-gimlet-attestation".to_string(),
                ],
                host: Some("testbed.local".to_string()),
                sp_runner_path: "sp-runner".to_string(),
                sp_runner_config: "config.toml".to_string(),
                baseline: "v16".to_string(),
                enabled: true,
            },
        );
        config
    }

    #[test]
    fn test_from_config() {
        let mgr = TestbedManager::from_config(&test_config()).unwrap();
        assert!(mgr.exists("grapefruit-1234"));
        assert!(mgr.exists("gimlet-5678"));
        assert!(!mgr.exists("nonexistent"));
    }

    #[test]
    fn test_find_for_target() {
        let mgr = TestbedManager::from_config(&test_config()).unwrap();

        let gf = mgr.find_for_target("sp-grapefruit");
        assert_eq!(gf.len(), 1);
        assert_eq!(gf[0].name, "grapefruit-1234");

        let gm = mgr.find_for_target("sp-gimlet");
        assert_eq!(gm.len(), 1);
        assert_eq!(gm[0].name, "gimlet-5678");

        let att = mgr.find_for_target("sp-gimlet-attestation");
        assert_eq!(att.len(), 1);
    }

    #[test]
    fn test_find_available() {
        let mgr = TestbedManager::from_config(&test_config()).unwrap();

        // Nothing in use
        let t = mgr.find_available_for_target("sp-grapefruit", &[]);
        assert!(t.is_some());

        // Testbed in use
        let t = mgr.find_available_for_target(
            "sp-grapefruit",
            &["grapefruit-1234".to_string()],
        );
        assert!(t.is_none());
    }
}
