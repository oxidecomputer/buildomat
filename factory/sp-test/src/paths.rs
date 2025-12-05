/*
 * Copyright 2025 Oxide Computer Company
 */

// Allow dead code - these helpers are for future refactoring of executor.rs
#![allow(dead_code)]

//! Path Management for SP-Test Factory
//!
//! This module provides centralized path management for the factory.
//! Paths can be configured via config file, environment variables,
//! or use sensible defaults.
//!
//! # Path Hierarchy
//!
//! ```text
//! work_base/                          # e.g., /tmp/buildomat-sp-test
//! ├── {instance_id}/                  # Per-instance work directory
//! │   ├── opt/buildomat/              # Agent OPT directory (set via BUILDOMAT_OPT)
//! │   │   ├── etc/
//! │   │   │   ├── agent.json          # Agent config
//! │   │   │   ├── job.json            # Job in progress marker
//! │   │   │   └── job-done.json       # Job completed marker
//! │   │   └── lib/
//! │   │       └── agent               # Agent binary copy
//! │   ├── input/                      # Job input files
//! │   └── work/                       # Job working directory
//! └── http-{instance_id}/             # HTTP dispatch mode work dir
//!     ├── input/
//!     └── output/
//! ```

use std::path::PathBuf;

/// Default base directory for factory work directories.
pub const DEFAULT_WORK_BASE: &str = "/tmp/buildomat-sp-test";

/// Configuration for factory paths.
///
/// This struct can be constructed from config file values, environment
/// variables, or defaults.
#[derive(Debug, Clone)]
pub struct PathConfig {
    /// Base directory for all work directories.
    /// Each instance gets a subdirectory under this path.
    pub work_base: PathBuf,
}

impl Default for PathConfig {
    fn default() -> Self {
        PathConfig {
            work_base: std::env::var("BUILDOMAT_WORK_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from(DEFAULT_WORK_BASE)),
        }
    }
}

impl PathConfig {
    /// Create a new path config with the given work base directory.
    pub fn new(work_base: impl Into<PathBuf>) -> Self {
        PathConfig { work_base: work_base.into() }
    }

    /// Get the work directory for a specific instance.
    ///
    /// For local agent mode, this is where the agent's BUILDOMAT_OPT,
    /// input, and work directories are created.
    pub fn instance_work_dir(&self, instance_id: &str) -> PathBuf {
        self.work_base.join(sanitize_instance_id(instance_id))
    }

    /// Get the OPT directory for an instance.
    ///
    /// This is set as BUILDOMAT_OPT for the agent, and is where the agent
    /// stores its configuration and the job marker files.
    pub fn instance_opt_dir(&self, instance_id: &str) -> PathBuf {
        self.instance_work_dir(instance_id).join("opt/buildomat")
    }

    /// Get the input directory for an instance.
    pub fn instance_input_dir(&self, instance_id: &str) -> PathBuf {
        self.instance_work_dir(instance_id).join("input")
    }

    /// Get the work/output directory for an instance.
    pub fn instance_output_dir(&self, instance_id: &str) -> PathBuf {
        self.instance_work_dir(instance_id).join("work")
    }

    /// Get the work directory for HTTP dispatch mode.
    ///
    /// HTTP dispatch uses a different prefix to distinguish from agent mode.
    pub fn http_work_dir(&self, instance_id: &str) -> PathBuf {
        self.work_base
            .join(format!("http-{}", sanitize_instance_id(instance_id)))
    }

    /// Get the SSH mode work directory.
    ///
    /// SSH mode uses a different prefix for tracking purposes.
    pub fn ssh_work_dir(&self, instance_id: &str) -> PathBuf {
        self.work_base
            .join(format!("ssh-{}", sanitize_instance_id(instance_id)))
    }
}

/// Paths for a specific instance.
///
/// This struct holds all the paths needed for executing a job on an instance.
/// It's constructed from a `PathConfig` and instance ID.
#[derive(Debug, Clone)]
pub struct InstancePaths {
    /// The instance ID.
    pub instance_id: String,

    /// Base work directory for this instance.
    pub work_dir: PathBuf,

    /// OPT directory (for BUILDOMAT_OPT env var).
    pub opt_dir: PathBuf,

    /// Input directory for job inputs.
    pub input_dir: PathBuf,

    /// Output/work directory for job outputs.
    pub output_dir: PathBuf,
}

impl InstancePaths {
    /// Create instance paths for local agent mode.
    pub fn for_local(config: &PathConfig, instance_id: &str) -> Self {
        let work_dir = config.instance_work_dir(instance_id);
        InstancePaths {
            instance_id: instance_id.to_string(),
            opt_dir: work_dir.join("opt/buildomat"),
            input_dir: work_dir.join("input"),
            output_dir: work_dir.join("work"),
            work_dir,
        }
    }

    /// Create instance paths for HTTP dispatch mode.
    pub fn for_http(config: &PathConfig, instance_id: &str) -> Self {
        let work_dir = config.http_work_dir(instance_id);
        InstancePaths {
            instance_id: instance_id.to_string(),
            opt_dir: PathBuf::new(), // Not used in HTTP mode
            input_dir: work_dir.join("input"),
            output_dir: work_dir.join("output"),
            work_dir,
        }
    }

    /// Create instance paths for SSH mode.
    pub fn for_ssh(config: &PathConfig, instance_id: &str) -> Self {
        let work_dir = config.ssh_work_dir(instance_id);
        InstancePaths {
            instance_id: instance_id.to_string(),
            opt_dir: work_dir.join("opt/buildomat"),
            input_dir: work_dir.join("input"),
            output_dir: work_dir.join("work"),
            work_dir,
        }
    }

    /// Get the OPT directory as a string, for use in commands.
    pub fn opt_dir_str(&self) -> &str {
        self.opt_dir.to_str().unwrap_or("/opt/buildomat")
    }

    /// Get the input directory as a string.
    pub fn input_dir_str(&self) -> &str {
        self.input_dir.to_str().unwrap_or("/input")
    }

    /// Get the output directory as a string.
    pub fn output_dir_str(&self) -> &str {
        self.output_dir.to_str().unwrap_or("/work")
    }
}

/// Remote paths for SSH mode.
///
/// When running jobs over SSH, we need to know the remote paths
/// (on the testbed host) separately from local tracking paths.
#[derive(Debug, Clone)]
pub struct RemotePaths {
    /// Remote work directory base.
    pub work_dir: String,

    /// Remote OPT directory.
    pub opt_dir: String,

    /// Remote input directory.
    pub input_dir: String,

    /// Remote output directory.
    pub output_dir: String,
}

impl RemotePaths {
    /// Create remote paths for a given instance ID.
    ///
    /// Uses the default remote work base (/tmp/buildomat-sp-test).
    pub fn new(instance_id: &str) -> Self {
        let work_dir = format!(
            "{}/{}",
            DEFAULT_WORK_BASE,
            sanitize_instance_id(instance_id)
        );
        RemotePaths {
            opt_dir: format!("{}/opt/buildomat", work_dir),
            input_dir: format!("{}/input", work_dir),
            output_dir: format!("{}/work", work_dir),
            work_dir,
        }
    }

    /// Create a mkdir command for setting up remote directories.
    pub fn mkdir_command(&self) -> String {
        format!(
            "mkdir -p {d}/opt/buildomat/etc {d}/opt/buildomat/lib {d}/input {d}/work",
            d = self.work_dir
        )
    }

    /// Create the agent install command for this remote path setup.
    pub fn agent_install_command(
        &self,
        nodename: &str,
        baseurl: &str,
        bootstrap: &str,
    ) -> String {
        format!(
            "cd {d} && \
             pfexec env HOME={d} \
             BUILDOMAT_OPT={d}/opt/buildomat \
             buildomat-agent install -N {nodename} {baseurl} {bootstrap}",
            d = self.work_dir,
            nodename = nodename,
            baseurl = baseurl,
            bootstrap = bootstrap
        )
    }

    /// Create the agent run command for this remote path setup.
    pub fn agent_run_command(&self) -> String {
        format!(
            "cd {d} && \
             pfexec env HOME={d} \
             BUILDOMAT_OPT={d}/opt/buildomat \
             BUILDOMAT_INPUT={d}/input \
             BUILDOMAT_WORK={d}/work \
             buildomat-agent run",
            d = self.work_dir
        )
    }

    /// Create a cleanup command for removing the remote work directory.
    pub fn cleanup_command(&self) -> String {
        format!("rm -rf {}", self.work_dir)
    }
}

/// Sanitize an instance ID for use in file paths.
///
/// Replaces `/` with `_` to create valid directory names.
fn sanitize_instance_id(instance_id: &str) -> String {
    instance_id.replace('/', "_")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_config_default() {
        // Clear env var if set
        std::env::remove_var("BUILDOMAT_WORK_DIR");
        let config = PathConfig::default();
        assert_eq!(config.work_base, PathBuf::from(DEFAULT_WORK_BASE));
    }

    #[test]
    fn test_path_config_env_var() {
        std::env::set_var("BUILDOMAT_WORK_DIR", "/custom/work");
        let config = PathConfig::default();
        assert_eq!(config.work_base, PathBuf::from("/custom/work"));
        std::env::remove_var("BUILDOMAT_WORK_DIR");
    }

    #[test]
    fn test_instance_paths_local() {
        let config = PathConfig::new("/tmp/test");
        let paths = InstancePaths::for_local(&config, "factory/worker_123");

        assert_eq!(
            paths.work_dir,
            PathBuf::from("/tmp/test/factory_worker_123")
        );
        assert_eq!(
            paths.opt_dir,
            PathBuf::from("/tmp/test/factory_worker_123/opt/buildomat")
        );
        assert_eq!(
            paths.input_dir,
            PathBuf::from("/tmp/test/factory_worker_123/input")
        );
        assert_eq!(
            paths.output_dir,
            PathBuf::from("/tmp/test/factory_worker_123/work")
        );
    }

    #[test]
    fn test_instance_paths_http() {
        let config = PathConfig::new("/tmp/test");
        let paths = InstancePaths::for_http(&config, "worker_456");

        assert_eq!(paths.work_dir, PathBuf::from("/tmp/test/http-worker_456"));
        assert_eq!(
            paths.input_dir,
            PathBuf::from("/tmp/test/http-worker_456/input")
        );
        assert_eq!(
            paths.output_dir,
            PathBuf::from("/tmp/test/http-worker_456/output")
        );
    }

    #[test]
    fn test_remote_paths() {
        let paths = RemotePaths::new("factory/worker_789");

        assert_eq!(paths.work_dir, "/tmp/buildomat-sp-test/factory_worker_789");
        assert_eq!(
            paths.opt_dir,
            "/tmp/buildomat-sp-test/factory_worker_789/opt/buildomat"
        );
    }

    #[test]
    fn test_sanitize_instance_id() {
        assert_eq!(sanitize_instance_id("simple"), "simple");
        assert_eq!(sanitize_instance_id("with/slash"), "with_slash");
        assert_eq!(
            sanitize_instance_id("multiple/nested/slashes"),
            "multiple_nested_slashes"
        );
    }
}
