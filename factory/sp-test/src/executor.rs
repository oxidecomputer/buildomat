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
//! - **Local**: Agent runs directly on the factory host (for development on illumos)
//! - **SSH**: Agent runs on a remote testbed host via SSH (for cross-platform dev)
//!
//! # Local Mode
//!
//! In local mode, the factory:
//! 1. Creates a temporary working directory for the agent
//! 2. Runs `buildomat-agent install <baseurl> <bootstrap>`
//! 3. Runs `buildomat-agent run` in the background
//! 4. Monitors the process for completion
//! 5. Cleans up the working directory
//!
//! # SSH Mode
//!
//! In SSH mode, the factory:
//! 1. SSHes to the remote host and creates work directory
//! 2. Runs `buildomat-agent install` over SSH
//! 3. Runs `buildomat-agent run` over SSH (keeping SSH session as monitored process)
//! 4. When SSH session exits, agent has completed
//! 5. Cleans up remote work directory

use std::path::PathBuf;
use std::process::Stdio;

use anyhow::{Context, Result, bail};
use slog::{Logger, debug, info, warn};
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

    /// SSH options for secure but non-interactive connections.
    fn ssh_options() -> Vec<String> {
        vec![
            "StrictHostKeyChecking=no".to_string(),
            "UserKnownHostsFile=/dev/null".to_string(),
            "ConnectTimeout=10".to_string(),
            "LogLevel=error".to_string(),
            "BatchMode=yes".to_string(),
        ]
    }

    /// Execute a command over SSH and wait for completion.
    async fn ssh_exec(
        &self,
        host: &str,
        user: &str,
        script: &str,
    ) -> Result<String> {
        let mut cmd = Command::new("ssh");

        for opt in Self::ssh_options() {
            cmd.arg("-o").arg(opt);
        }

        cmd.arg(format!("{}@{}", user, host));
        cmd.arg(script);

        let output = cmd
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await
            .with_context(|| format!("ssh to {}@{}", user, host))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            bail!(
                "ssh {}@{} {:?} failed: {}",
                user,
                host,
                script,
                stderr.trim()
            );
        }

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    /// Spawn a long-running SSH command (returns Child handle).
    async fn ssh_spawn(
        &self,
        host: &str,
        user: &str,
        script: &str,
    ) -> Result<Child> {
        let mut cmd = Command::new("ssh");

        for opt in Self::ssh_options() {
            cmd.arg("-o").arg(opt);
        }

        // Use -t -t for force pseudo-terminal allocation to ensure remote
        // process is killed when SSH connection drops
        cmd.arg("-t").arg("-t");
        cmd.arg(format!("{}@{}", user, host));
        cmd.arg(script);

        let child = cmd
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| {
            format!("spawning ssh to {}@{}", user, host)
        })?;

        Ok(child)
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

    /// Start an agent on a remote testbed via SSH.
    ///
    /// This SSHes to the testbed host, creates directories, installs and runs
    /// the agent. The SSH session remains open as the monitored process.
    pub async fn start_ssh(
        &self,
        instance: &Instance,
        testbed: &TestbedInfo,
    ) -> Result<AgentHandle> {
        let instance_id = instance.id();
        let host = testbed.host.as_ref().ok_or_else(|| {
            anyhow::anyhow!("testbed {} has no host configured", testbed.name)
        })?;

        // Default to root user (can be made configurable)
        let user = "root";

        info!(
            self.log,
            "starting SSH agent for instance {} on testbed {} ({})",
            instance_id,
            testbed.name,
            host
        );

        // Remote work directory path
        let remote_work_dir =
            format!("/tmp/buildomat-sp-test/{}", instance_id.replace('/', "_"));

        // Step 1: Create remote directories
        info!(self.log, "creating remote work directory";
            "instance" => &instance_id,
            "path" => &remote_work_dir
        );

        let mkdir_script = format!(
            "mkdir -p {d}/opt/buildomat/etc {d}/opt/buildomat/lib {d}/input {d}/work",
            d = remote_work_dir
        );
        self.ssh_exec(host, user, &mkdir_script).await?;

        // Step 2: Install agent on remote host
        info!(self.log, "installing agent via SSH"; "instance" => &instance_id);

        // The install script sets up agent config and registers with server
        let install_script = format!(
            "cd {d} && \
             HOME={d} \
             BUILDOMAT_OPT={d}/opt/buildomat \
             buildomat-agent install -N {nodename} {baseurl} {bootstrap}",
            d = remote_work_dir,
            nodename = testbed.name,
            baseurl = self.config.baseurl,
            bootstrap = instance.bootstrap
        );

        match self.ssh_exec(host, user, &install_script).await {
            Ok(_) => {
                info!(self.log, "agent installed successfully via SSH";
                    "instance" => &instance_id
                );
            }
            Err(e) => {
                // Try to clean up remote directory on failure
                warn!(self.log, "agent install failed, cleaning up";
                    "instance" => &instance_id,
                    "error" => %e
                );
                let cleanup = format!("rm -rf {}", remote_work_dir);
                let _ = self.ssh_exec(host, user, &cleanup).await;
                return Err(e);
            }
        }

        // Step 3: Start agent run via SSH (keep session open)
        info!(self.log, "starting agent run via SSH"; "instance" => &instance_id);

        // The run script executes the agent and stays connected
        let run_script = format!(
            "cd {d} && \
             HOME={d} \
             BUILDOMAT_OPT={d}/opt/buildomat \
             BUILDOMAT_INPUT={d}/input \
             BUILDOMAT_WORK={d}/work \
             buildomat-agent run",
            d = remote_work_dir
        );

        let child = self.ssh_spawn(host, user, &run_script).await?;

        info!(
            self.log,
            "SSH agent started";
            "instance" => &instance_id,
            "pid" => child.id(),
            "host" => host
        );

        // Use a local work_dir path for tracking (even though actual work is remote)
        let work_dir = self
            .config
            .work_base
            .join(format!("ssh-{}", instance_id.replace('/', "_")));

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
