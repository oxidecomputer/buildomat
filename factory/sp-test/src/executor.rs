/*
 * Copyright 2025 Oxide Computer Company
 */

//! Agent Execution
//!
//! This module handles starting and monitoring the buildomat-agent for
//! test execution on testbeds.
//!
//! # Execution Modes
//!
//! - **Local**: Agent runs directly on the factory host (for development)
//! - **SSH**: Agent runs on a remote testbed host via SSH (for production)
//!
//! # Local Mode
//!
//! In local mode, the factory:
//! 1. Creates a temporary working directory for the agent
//! 2. Runs `buildomat-agent install <baseurl> <bootstrap>`
//! 3. Runs `buildomat-agent run` in the background
//! 4. Monitors the process for completion
//! 5. Cleans up the working directory

use std::path::PathBuf;
use std::process::Stdio;

use anyhow::{Context, Result, bail};
use slog::{Logger, debug, info};
use tokio::process::{Child, Command};
use tokio::sync::Mutex;

use crate::Central;
use crate::db::Instance;
use crate::testbed::TestbedInfo;

/// Configuration for agent execution.
#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    /// Path to buildomat-agent binary
    pub agent_path: PathBuf,

    /// Base directory for agent working directories
    pub work_base: PathBuf,

    /// Buildomat server URL
    pub baseurl: String,
}

impl ExecutorConfig {
    /// Create config from Central state.
    pub fn from_central(c: &Central) -> Self {
        // Default agent path - can be overridden via config
        let agent_path = std::env::var("BUILDOMAT_AGENT_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                // Try to find agent relative to factory binary
                let exe = std::env::current_exe().ok();
                exe.and_then(|p| p.parent().map(|d| d.join("buildomat-agent")))
                    .unwrap_or_else(|| PathBuf::from("buildomat-agent"))
            });

        // Work directory base
        let work_base = std::env::var("BUILDOMAT_WORK_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/tmp/buildomat-sp-test"));

        ExecutorConfig {
            agent_path,
            work_base,
            baseurl: c.config.general.baseurl.clone(),
        }
    }
}

/// Handle to a running agent process.
pub struct AgentHandle {
    /// Instance this agent is running for
    pub instance_id: String,

    /// Testbed name
    #[allow(dead_code)]
    pub testbed_name: String,

    /// Working directory for this agent
    pub work_dir: PathBuf,

    /// Child process handle
    child: Mutex<Option<Child>>,

    /// Logger for this agent
    #[allow(dead_code)]
    log: Logger,
}

impl AgentHandle {
    /// Check if the agent process is still running.
    pub async fn is_running(&self) -> bool {
        let mut guard = self.child.lock().await;
        if let Some(ref mut child) = *guard {
            match child.try_wait() {
                Ok(None) => true,     // Still running
                Ok(Some(_)) => false, // Exited
                Err(_) => false,      // Error checking
            }
        } else {
            false
        }
    }

    /// Wait for the agent to complete and return exit status.
    #[allow(dead_code)]
    pub async fn wait(&self) -> Result<std::process::ExitStatus> {
        let mut guard = self.child.lock().await;
        if let Some(ref mut child) = *guard {
            child.wait().await.context("waiting for agent")
        } else {
            bail!("agent process not available")
        }
    }

    /// Kill the agent process.
    pub async fn kill(&self) -> Result<()> {
        let mut guard = self.child.lock().await;
        if let Some(ref mut child) = *guard {
            child.kill().await.context("killing agent")?;
        }
        Ok(())
    }
}

/// Executor for running agents.
pub struct Executor {
    config: ExecutorConfig,
    log: Logger,
}

impl Executor {
    pub fn new(config: ExecutorConfig, log: Logger) -> Self {
        Executor { config, log }
    }

    /// Start an agent for the given instance on the given testbed.
    ///
    /// This creates a working directory, installs the agent, and starts it
    /// running in the background.
    pub async fn start_local(
        &self,
        instance: &Instance,
        testbed: &TestbedInfo,
    ) -> Result<AgentHandle> {
        let instance_id = instance.id();
        info!(
            self.log,
            "starting local agent for instance {} on testbed {}",
            instance_id,
            testbed.name
        );

        // Create working directory for this instance
        let work_dir =
            self.config.work_base.join(instance_id.replace('/', "_"));
        tokio::fs::create_dir_all(&work_dir)
            .await
            .with_context(|| format!("creating work dir {:?}", work_dir))?;

        // Create subdirectories the agent expects
        let opt_dir = work_dir.join("opt/buildomat");
        tokio::fs::create_dir_all(opt_dir.join("etc")).await?;
        tokio::fs::create_dir_all(opt_dir.join("lib")).await?;

        // Create /work and /input directories
        let input_dir = work_dir.join("input");
        let output_dir = work_dir.join("work");
        tokio::fs::create_dir_all(&input_dir).await?;
        tokio::fs::create_dir_all(&output_dir).await?;

        info!(
            self.log,
            "created work directory";
            "path" => %work_dir.display(),
            "instance" => &instance_id
        );

        // Step 1: Install the agent
        // The agent install command registers with the server and creates config
        info!(self.log, "installing agent"; "instance" => &instance_id);

        let install_status = Command::new(&self.config.agent_path)
            .arg("install")
            .arg("-N")
            .arg(&testbed.name) // Use testbed name as nodename
            .arg(&self.config.baseurl)
            .arg(&instance.bootstrap)
            .env("HOME", &work_dir)
            .env("BUILDOMAT_OPT", opt_dir.to_str().unwrap_or("/opt/buildomat"))
            .current_dir(&work_dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .status()
            .await
            .context("running agent install")?;

        if !install_status.success() {
            bail!(
                "agent install failed with status {} for instance {}",
                install_status,
                instance_id
            );
        }

        info!(self.log, "agent installed successfully"; "instance" => &instance_id);

        // Step 2: Start the agent running
        info!(self.log, "starting agent run"; "instance" => &instance_id);

        let child = Command::new(&self.config.agent_path)
            .arg("run")
            .env("HOME", &work_dir)
            .env("BUILDOMAT_OPT", opt_dir.to_str().unwrap_or("/opt/buildomat"))
            .env("BUILDOMAT_INPUT", input_dir.to_str().unwrap_or("/input"))
            .env("BUILDOMAT_WORK", output_dir.to_str().unwrap_or("/work"))
            .current_dir(&work_dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("spawning agent run")?;

        info!(
            self.log,
            "agent started";
            "instance" => &instance_id,
            "pid" => child.id()
        );

        Ok(AgentHandle {
            instance_id,
            testbed_name: testbed.name.clone(),
            work_dir,
            child: Mutex::new(Some(child)),
            log: self.log.clone(),
        })
    }

    /// Clean up after an agent has finished.
    pub async fn cleanup(&self, handle: &AgentHandle) -> Result<()> {
        info!(
            self.log,
            "cleaning up agent";
            "instance" => &handle.instance_id
        );

        // Kill process if still running
        handle.kill().await.ok();

        // Optionally remove work directory
        // For now, keep it for debugging
        debug!(
            self.log,
            "work directory preserved for debugging";
            "path" => %handle.work_dir.display()
        );

        Ok(())
    }
}
