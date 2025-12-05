/*
 * Copyright 2025 Oxide Computer Company
 */

//! Configuration for SP-Test Factory
//!
//! Configuration is loaded from a TOML file with the following structure:
//!
//! ```toml
//! [general]
//! baseurl = "http://buildomat-server:8080"
//!
//! [factory]
//! token = "factory-api-token"
//!
//! # Testbed definitions - each testbed is a hardware test station
//! [testbed.grapefruit-7f495641]
//! sp_type = "grapefruit"
//! targets = ["sp-grapefruit"]
//! # HTTP dispatch mode - factory dispatches to sp-runner service
//! sp_runner_url = "http://localhost:9090"
//!
//! [testbed.gimlet-agent-mode]
//! sp_type = "gimlet"
//! targets = ["sp-gimlet"]
//! # Agent mode (no sp_runner_url) - runs buildomat-agent directly
//! # Optional: SSH to remote host (omit for local execution)
//! # host = "testbed-host.local"
//!
//! [testbed.gimlet-a1b2c3d4]
//! sp_type = "gimlet"
//! targets = ["sp-gimlet"]
//!
//! # Target definitions - buildomat targets this factory can provide
//! [target.sp-grapefruit]
//! # Targets are matched to testbeds via the testbed's "targets" list
//!
//! [target.sp-gimlet]
//! ```

use std::collections::HashMap;

use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub struct ConfigFile {
    pub general: ConfigFileGeneral,
    pub factory: ConfigFileFactory,
    #[serde(default)]
    pub testbed: HashMap<String, ConfigFileTestbed>,
    #[serde(default)]
    pub target: HashMap<String, ConfigFileTarget>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ConfigFileGeneral {
    pub baseurl: String,

    /// Base directory for agent work directories.
    /// Defaults to /tmp/buildomat-sp-test or BUILDOMAT_WORK_DIR env var.
    #[serde(default)]
    pub work_dir: Option<String>,

    /// Use privilege elevation (pfexec) for agent operations.
    /// Default: true (for backwards compatibility)
    ///
    /// When true:
    /// - Agent runs with pfexec for root access
    /// - SMF service is created/cleaned up
    /// - ZFS datasets are managed
    ///
    /// When false:
    /// - Agent runs as current user
    /// - All directories must be writable by current user
    /// - No SMF or ZFS operations
    #[serde(default = "default_true")]
    pub use_privilege_elevation: bool,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ConfigFileFactory {
    pub token: String,
}

/// Configuration for a single hardware testbed.
#[derive(Deserialize, Debug, Clone)]
pub struct ConfigFileTestbed {
    /// SP type (gimlet, grapefruit, sidecar, psc)
    pub sp_type: String,

    /// Buildomat targets this testbed can serve
    pub targets: Vec<String>,

    /// SSH host for remote execution (None = local execution)
    #[serde(default)]
    pub host: Option<String>,

    /// SSH user for remote execution (default: current user)
    #[serde(default)]
    pub ssh_user: Option<String>,

    /// Path to sp-runner on testbed host
    #[serde(default = "default_sp_runner_path")]
    pub sp_runner_path: String,

    /// Path to sp-runner config on testbed host
    #[serde(default = "default_sp_runner_config")]
    pub sp_runner_config: String,

    /// Baseline firmware version to use
    #[serde(default = "default_baseline")]
    pub baseline: String,

    /// Whether this testbed is enabled for CI
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// HTTP URL for sp-runner service (enables HTTP dispatch mode).
    ///
    /// When set, the factory dispatches jobs to sp-runner's buildomat-service
    /// API via HTTP instead of running buildomat-agent directly.
    ///
    /// Example: "http://localhost:9090"
    ///
    /// If not set (None), the factory uses the traditional agent-based
    /// execution mode (either local or SSH depending on `host`).
    #[serde(default)]
    pub sp_runner_url: Option<String>,
}

fn default_true() -> bool {
    true
}

fn default_sp_runner_path() -> String {
    "sp-runner".to_string()
}

fn default_sp_runner_config() -> String {
    "~/Oxide/ci/config.toml".to_string()
}

fn default_baseline() -> String {
    "v16".to_string()
}

fn default_enabled() -> bool {
    true
}

/// Configuration for a buildomat target.
///
/// Targets are what buildomat jobs request. The factory matches
/// targets to testbeds based on each testbed's `targets` list.
#[derive(Deserialize, Debug, Clone, Default)]
pub struct ConfigFileTarget {
    // Currently no target-specific configuration.
    // Future: test type, timeout, etc.
}
