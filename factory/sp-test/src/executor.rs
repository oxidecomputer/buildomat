/*
 * Copyright 2025 Oxide Computer Company
 */

//! Job Execution
//!
//! This module handles starting and monitoring test jobs on testbeds.
//!
//! # Execution Modes
//!
//! - **HTTP**: Dispatches to sp-runner service via HTTP API (preferred for production)
//! - **Local**: Runs buildomat-agent directly on factory host (development on illumos)
//! - **SSH**: Runs buildomat-agent on remote host via SSH (cross-platform dev)
//!
//! # HTTP Mode
//!
//! In HTTP mode, the factory:
//! 1. Downloads artifacts from buildomat storage to local work directory
//! 2. Calls sp-runner's HTTP API to submit the job
//! 3. Polls for job completion via HTTP
//! 4. Uploads results back to buildomat storage
//!
//! # Local Mode (Agent)
//!
//! In local mode, the factory:
//! 1. Creates a temporary working directory for the agent
//! 2. Runs `buildomat-agent install <baseurl> <bootstrap>`
//! 3. Runs `buildomat-agent run` in the background
//! 4. Monitors the process for completion
//! 5. Cleans up the working directory
//!
//! # SSH Mode (Agent)
//!
//! In SSH mode, the factory:
//! 1. SSHes to the remote host and creates work directory
//! 2. Runs `buildomat-agent install` over SSH
//! 3. Runs `buildomat-agent run` over SSH (keeping SSH session as monitored process)
//! 4. When SSH session exits, agent has completed
//! 5. Cleans up remote work directory

use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::{Context, Result, bail};
use buildomat_common::genkey;
use futures::StreamExt;
use slog::{Logger, debug, error, info, warn};
use tokio::io::AsyncWriteExt;
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

    /// Use privilege elevation (pfexec) for agent operations.
    /// When false, agent runs as current user without root.
    pub use_privilege_elevation: bool,
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

        // Work directory base - prefer config, then env var, then default
        let work_base = c
            .config
            .general
            .work_dir
            .as_ref()
            .map(PathBuf::from)
            .or_else(|| {
                std::env::var("BUILDOMAT_WORK_DIR").ok().map(PathBuf::from)
            })
            .unwrap_or_else(|| PathBuf::from("/tmp/buildomat-sp-test"));

        ExecutorConfig {
            agent_path,
            work_base,
            baseurl: c.config.general.baseurl.clone(),
            use_privilege_elevation: c.config.general.use_privilege_elevation,
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

/// Handle for an HTTP-dispatched job.
///
/// Unlike [`AgentHandle`] which monitors a child process, this handle
/// tracks a job submitted to sp-runner's HTTP API. The job runs on
/// sp-runner, and we poll for completion via HTTP.
pub struct HttpJobHandle {
    /// Instance this job is running for
    pub instance_id: String,

    /// Testbed name (kept for future logging/debugging)
    #[allow(dead_code)]
    pub testbed_name: String,

    /// sp-runner job ID
    pub job_id: String,

    /// Working directory for this job (local to factory)
    pub work_dir: PathBuf,

    /// Buildomat job ID (for result uploads)
    pub buildomat_job_id: Option<String>,

    /// Worker token for buildomat API (for result uploads)
    worker_token: String,

    /// Buildomat server URL
    baseurl: String,

    /// sp-runner client for polling
    client: sp_runner_client::Client,

    /// Logger for this job
    log: Logger,
}

impl HttpJobHandle {
    /// Check if the job is still running by polling sp-runner.
    pub async fn is_running(&self) -> bool {
        match self.client.get_status(&self.job_id).await {
            Ok(status) => !status.is_terminal(),
            Err(e) => {
                // Log error but treat as "not running" to trigger cleanup
                debug!(self.log, "failed to poll job status";
                    "job_id" => &self.job_id,
                    "error" => %e);
                false
            }
        }
    }

    /// Get the final result from sp-runner.
    pub async fn get_result(
        &self,
    ) -> Result<sp_runner_client::JobResultResponse> {
        self.client.get_result(&self.job_id).await
    }

    /// No-op kill for HTTP jobs - we don't have a way to cancel remotely yet.
    pub async fn kill(&self) -> Result<()> {
        // TODO: Add cancel endpoint to sp-runner API
        Ok(())
    }
}

/// Executor for running jobs.
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

        // Step 0: Clean up any leftover artifacts from previous runs
        if self.config.use_privilege_elevation {
            // With privilege elevation, clean up ZFS datasets
            debug!(self.log, "cleaning up ZFS datasets"; "instance" => &instance_id);
            let _ = Command::new("pfexec")
                .args(["zfs", "destroy", "-r", "rpool/input"])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .await;
        }

        // Clean up any leftover job.json from previous runs.
        // The agent checks for this file and will report failure if it exists,
        // assuming a previous agent crashed. On persistent machines (not VMs),
        // this file persists between jobs and must be removed.
        // Use the configured opt_dir, not hardcoded /opt/buildomat
        let job_json = opt_dir.join("etc/job.json");
        if job_json.exists() {
            info!(self.log, "cleaning up stale job.json"; "instance" => &instance_id, "path" => %job_json.display());
            let _ = tokio::fs::remove_file(&job_json).await;
        }
        // Also clean up job-done.json (completed job marker)
        let job_done_json = opt_dir.join("etc/job-done.json");
        if job_done_json.exists() {
            debug!(self.log, "cleaning up job-done.json"; "instance" => &instance_id);
            let _ = tokio::fs::remove_file(&job_done_json).await;
        }

        // Step 1: Install the agent
        // The agent install command registers with the server and creates config
        info!(self.log, "installing agent"; "instance" => &instance_id);

        // Conditionally use pfexec for privilege elevation
        let mut install_cmd = if self.config.use_privilege_elevation {
            let mut cmd = Command::new("pfexec");
            cmd.arg(&self.config.agent_path);
            cmd
        } else {
            Command::new(&self.config.agent_path)
        };

        // Set up environment for agent install
        install_cmd.env("HOME", &work_dir);
        install_cmd.env("BUILDOMAT_OPT", opt_dir.to_str().unwrap());

        // In non-root mode, tell agent to skip root-requiring operations
        if !self.config.use_privilege_elevation {
            install_cmd.env("BUILDOMAT_NONROOT", "1");
        }

        let install_status = install_cmd
            .arg("install")
            .arg(&self.config.baseurl)
            .arg(&instance.bootstrap)
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

        // Conditionally use pfexec for privilege elevation
        let mut run_cmd = if self.config.use_privilege_elevation {
            let mut cmd = Command::new("pfexec");
            cmd.arg(&self.config.agent_path);
            cmd
        } else {
            Command::new(&self.config.agent_path)
        };

        // Set up environment for agent run
        run_cmd.env("HOME", &work_dir);
        run_cmd.env("BUILDOMAT_OPT", opt_dir.to_str().unwrap());
        run_cmd.env("BUILDOMAT_INPUT", input_dir.to_str().unwrap());
        run_cmd.env("BUILDOMAT_WORK", output_dir.to_str().unwrap());

        // In non-root mode, tell agent to skip root-requiring operations
        if !self.config.use_privilege_elevation {
            run_cmd.env("BUILDOMAT_NONROOT", "1");
        }

        let child = run_cmd
            .arg("run")
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

        // SSH user - from testbed config, environment, or error if not set
        let user: String = if let Some(u) = testbed.ssh_user.as_deref() {
            u.to_string()
        } else if let Ok(u) = std::env::var("USER") {
            u
        } else {
            bail!(
                "testbed {} has no ssh_user configured and USER env var is not set",
                testbed.name
            );
        };
        let user = user.as_str();

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
        // Use pfexec for privilege elevation (agent needs to create /var/run/buildomat.sock)
        let install_script = format!(
            "cd {d} && \
             pfexec env HOME={d} \
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
        // Use pfexec for privilege elevation (agent needs /var/run/buildomat.sock)
        let run_script = format!(
            "cd {d} && \
             pfexec env HOME={d} \
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

        // Clean up SMF service and ZFS datasets only when using privilege elevation
        // These operations require root and are only used in VM-based execution
        if self.config.use_privilege_elevation {
            debug!(self.log, "cleaning up SMF service"; "instance" => &handle.instance_id);
            let _ = Command::new("pfexec")
                .args([
                    "svcadm",
                    "disable",
                    "-s",
                    "svc:/site/buildomat/agent:default",
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .await;
            let _ = Command::new("pfexec")
                .args(["svccfg", "delete", "svc:/site/buildomat/agent:default"])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .await;
            let _ = Command::new("pfexec")
                .args([
                    "rm",
                    "-f",
                    "/var/svc/manifest/site/buildomat-agent.xml",
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .await;

            // Clean up ZFS datasets for next run
            let _ = Command::new("pfexec")
                .args(["zfs", "destroy", "-r", "rpool/input"])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .await;
        }

        // Optionally remove work directory
        // For now, keep it for debugging
        debug!(
            self.log,
            "work directory preserved for debugging";
            "path" => %handle.work_dir.display()
        );

        Ok(())
    }

    /// Start a job via HTTP dispatch to sp-runner service.
    ///
    /// This downloads artifacts from buildomat, submits a job to sp-runner's
    /// HTTP API, and returns a handle for monitoring completion.
    ///
    /// # Arguments
    ///
    /// * `instance` - The buildomat instance (contains worker/bootstrap info)
    /// * `testbed` - Testbed configuration (must have `sp_runner_url` set)
    /// * `_buildomat_client` - Unused (artifacts downloaded via worker protocol)
    ///
    /// # Returns
    ///
    /// An `HttpJobHandle` that can be used to poll for completion.
    pub async fn start_http(
        &self,
        instance: &Instance,
        testbed: &TestbedInfo,
        _buildomat_client: &buildomat_client::Client,
    ) -> Result<HttpJobHandle> {
        let instance_id = instance.id();
        let sp_runner_url =
            testbed.sp_runner_url.as_ref().ok_or_else(|| {
                anyhow::anyhow!(
                    "testbed {} does not have sp_runner_url configured",
                    testbed.name
                )
            })?;

        info!(
            self.log,
            "starting HTTP dispatch for instance {} on testbed {}",
            instance_id,
            testbed.name;
            "sp_runner_url" => sp_runner_url
        );

        // Generate a worker token for buildomat API
        let worker_token = genkey(64);

        // Create work directory for this instance
        let work_dir = self
            .config
            .work_base
            .join(format!("http-{}", instance_id.replace('/', "_")));
        let input_dir = work_dir.join("input");
        let output_dir = work_dir.join("output");

        tokio::fs::create_dir_all(&input_dir)
            .await
            .with_context(|| format!("creating input dir {:?}", input_dir))?;
        tokio::fs::create_dir_all(&output_dir)
            .await
            .with_context(|| format!("creating output dir {:?}", output_dir))?;

        info!(
            self.log,
            "created work directories";
            "instance" => &instance_id,
            "input" => %input_dir.display(),
            "output" => %output_dir.display()
        );

        // Download artifacts from buildomat (acting as the worker)
        let buildomat_job_id = self
            .download_artifacts(
                &self.config.baseurl,
                &instance.bootstrap,
                &worker_token,
                &input_dir,
            )
            .await
            .context("downloading artifacts")?;

        // Create sp-runner client
        let client = sp_runner_client::ClientBuilder::new(sp_runner_url)
            .build()
            .context("creating sp-runner client")?;

        // Submit job to sp-runner
        let request = sp_runner_client::JobSubmitRequest {
            testbed: testbed.name.clone(),
            input_dir: input_dir.to_string_lossy().to_string(),
            output_dir: output_dir.to_string_lossy().to_string(),
            baseline: testbed.baseline.clone(),
            test_type: "update-rollback".to_string(),
            disable_watchdog: false,
        };

        let job_id = client
            .submit_job(&request)
            .await
            .context("submitting job to sp-runner")?;

        info!(
            self.log,
            "submitted job to sp-runner";
            "instance" => &instance_id,
            "job_id" => &job_id,
            "testbed" => &testbed.name,
            "buildomat_job_id" => &buildomat_job_id
        );

        Ok(HttpJobHandle {
            instance_id,
            testbed_name: testbed.name.clone(),
            job_id,
            work_dir,
            buildomat_job_id,
            worker_token,
            baseurl: self.config.baseurl.clone(),
            client,
            log: self.log.clone(),
        })
    }

    /// Download job artifacts from buildomat storage.
    ///
    /// For HTTP dispatch mode, the factory acts as the worker to download
    /// artifacts. This involves:
    /// 1. Bootstrapping as the worker using the instance's bootstrap token
    /// 2. Calling worker_ping() to get the job info and input list
    /// 3. Downloading each input file via worker_job_input_download()
    ///
    /// # Arguments
    ///
    /// * `baseurl` - The buildomat server URL
    /// * `bootstrap` - The bootstrap token from factory_worker_create
    /// * `worker_token` - Token for authenticating subsequent worker API calls
    /// * `dest` - Directory to download artifacts into
    ///
    /// # Returns
    ///
    /// The job ID assigned to this worker (needed for result uploads).
    async fn download_artifacts(
        &self,
        baseurl: &str,
        bootstrap: &str,
        worker_token: &str,
        dest: &Path,
    ) -> Result<Option<String>> {
        info!(
            self.log,
            "downloading artifacts";
            "baseurl" => baseurl,
            "dest" => %dest.display()
        );

        // Create unauthenticated client for bootstrap
        let client = buildomat_client::ClientBuilder::new(baseurl)
            .build()
            .context("creating buildomat client for bootstrap")?;

        // Bootstrap as the worker
        let bootstrap_result = client
            .worker_bootstrap()
            .body_map(|body| body.bootstrap(bootstrap).token(worker_token))
            .send()
            .await
            .context("worker bootstrap")?;

        let worker_id = bootstrap_result.into_inner().id;
        info!(self.log, "bootstrapped as worker"; "worker_id" => &worker_id);

        // Create authenticated client for subsequent calls
        let auth_client = buildomat_client::ClientBuilder::new(baseurl)
            .bearer_token(worker_token)
            .build()
            .context("creating authenticated buildomat client")?;

        // Ping to get job assignment
        let ping_result =
            auth_client.worker_ping().send().await.context("worker ping")?;
        let ping = ping_result.into_inner();

        // Check if we have a job assigned
        let job = match ping.job {
            Some(job) => job,
            None => {
                warn!(self.log, "no job assigned to worker"; "worker_id" => &worker_id);
                return Ok(None);
            }
        };

        let job_id = job.id.clone();
        info!(
            self.log,
            "job assigned";
            "job_id" => &job_id,
            "job_name" => &job.name,
            "input_count" => job.inputs.len()
        );

        // Download each input
        for input in &job.inputs {
            let input_path = dest.join(&input.name);
            info!(
                self.log,
                "downloading input";
                "name" => &input.name,
                "id" => &input.id,
                "path" => %input_path.display()
            );

            // Ensure parent directory exists
            if let Some(parent) = input_path.parent() {
                if parent != dest {
                    tokio::fs::create_dir_all(parent).await.with_context(
                        || format!("creating parent dir {:?}", parent),
                    )?;
                }
            }

            // Download the file
            let resp = auth_client
                .worker_job_input_download()
                .job(&job_id)
                .input(&input.id)
                .send()
                .await
                .with_context(|| format!("downloading input {}", input.name))?;

            let mut body = resp.into_inner();
            let mut file = tokio::fs::File::create(&input_path)
                .await
                .with_context(|| format!("creating file {:?}", input_path))?;

            let mut total_bytes = 0u64;
            while let Some(chunk) = body.next().await {
                let mut chunk = chunk.context("reading chunk")?;
                total_bytes += chunk.len() as u64;
                file.write_all_buf(&mut chunk)
                    .await
                    .context("writing chunk")?;
            }

            info!(
                self.log,
                "downloaded input";
                "name" => &input.name,
                "bytes" => total_bytes
            );
        }

        info!(
            self.log,
            "artifact download complete";
            "job_id" => &job_id,
            "count" => job.inputs.len()
        );

        Ok(Some(job_id))
    }

    /// Clean up after an HTTP job has finished.
    ///
    /// Uploads results to buildomat storage and cleans up work directory.
    pub async fn cleanup_http(
        &self,
        handle: &HttpJobHandle,
        _buildomat_client: &buildomat_client::Client,
    ) -> Result<()> {
        info!(
            self.log,
            "cleaning up HTTP job";
            "instance" => &handle.instance_id,
            "job_id" => &handle.job_id
        );

        // Get job result from sp-runner
        match handle.get_result().await {
            Ok(result) => {
                info!(
                    self.log,
                    "job completed";
                    "instance" => &handle.instance_id,
                    "outcome" => &result.outcome,
                    "duration_secs" => result.duration_secs,
                    "tests_run" => result.tests_run
                );

                // Upload result files to buildomat (if we have a buildomat job)
                if let Some(ref buildomat_job_id) = handle.buildomat_job_id {
                    let output_dir = handle.work_dir.join("output");
                    if let Err(e) = self
                        .upload_results(
                            &handle.baseurl,
                            &handle.worker_token,
                            buildomat_job_id,
                            &output_dir,
                        )
                        .await
                    {
                        error!(
                            self.log,
                            "failed to upload results";
                            "instance" => &handle.instance_id,
                            "error" => %e
                        );
                    }
                } else {
                    debug!(
                        self.log,
                        "no buildomat job ID, skipping result upload";
                        "instance" => &handle.instance_id
                    );
                }
            }
            Err(e) => {
                error!(
                    self.log,
                    "failed to get job result";
                    "instance" => &handle.instance_id,
                    "error" => %e
                );
            }
        }

        // Keep work directory for debugging
        debug!(
            self.log,
            "work directory preserved for debugging";
            "path" => %handle.work_dir.display()
        );

        Ok(())
    }

    /// Upload result files to buildomat storage.
    ///
    /// Uses the worker protocol to upload output files:
    /// 1. Read file in chunks (5MB each)
    /// 2. Upload each chunk via worker_job_upload_chunk()
    /// 3. Register the file via worker_job_add_output()
    ///
    /// # Arguments
    ///
    /// * `baseurl` - Buildomat server URL
    /// * `worker_token` - Token for worker API authentication
    /// * `job_id` - Buildomat job ID
    /// * `output_dir` - Directory containing output files
    async fn upload_results(
        &self,
        baseurl: &str,
        worker_token: &str,
        job_id: &str,
        output_dir: &Path,
    ) -> Result<()> {
        // Create authenticated client
        let client = buildomat_client::ClientBuilder::new(baseurl)
            .bearer_token(worker_token)
            .build()
            .context("creating buildomat client for uploads")?;

        // Upload known result files
        for name in ["test-report.json", "test-summary.json"] {
            let path = output_dir.join(name);
            if !path.exists() {
                debug!(self.log, "result file not found, skipping"; "name" => name);
                continue;
            }

            info!(self.log, "uploading result file"; "name" => name);

            // Read file and get size
            let metadata = tokio::fs::metadata(&path)
                .await
                .with_context(|| format!("stat {:?}", path))?;
            let size = metadata.len();

            // Read file in chunks and upload each
            let mut file = tokio::fs::File::open(&path)
                .await
                .with_context(|| format!("open {:?}", path))?;

            let chunk_size = 5 * 1024 * 1024; // 5MB chunks
            let mut chunks = Vec::new();
            let mut total_read = 0u64;

            loop {
                use tokio::io::AsyncReadExt;

                let mut buf = vec![0u8; chunk_size];
                let n = file
                    .read(&mut buf)
                    .await
                    .with_context(|| format!("read {:?}", path))?;

                if n == 0 {
                    break;
                }

                buf.truncate(n);
                total_read += n as u64;

                // Upload chunk
                let chunk_result = client
                    .worker_job_upload_chunk()
                    .job(job_id)
                    .body(bytes::Bytes::from(buf))
                    .send()
                    .await
                    .with_context(|| format!("uploading chunk for {}", name))?;

                chunks.push(chunk_result.into_inner().id);
            }

            debug!(
                self.log,
                "uploaded chunks";
                "name" => name,
                "chunks" => chunks.len(),
                "bytes" => total_read
            );

            // Register the output file (use random string for commit_id)
            let commit_id = genkey(26);
            let add_output = buildomat_client::types::WorkerAddOutput {
                chunks,
                path: name.to_string(),
                size,
                commit_id,
            };

            // Poll until complete
            loop {
                let result = client
                    .worker_job_add_output()
                    .job(job_id)
                    .body(&add_output)
                    .send()
                    .await
                    .with_context(|| format!("add_output for {}", name))?;

                let result = result.into_inner();
                if result.complete {
                    if let Some(e) = result.error {
                        warn!(
                            self.log,
                            "output file error";
                            "name" => name,
                            "error" => &e
                        );
                    } else {
                        info!(self.log, "uploaded result file"; "name" => name);
                    }
                    break;
                }

                // Not complete yet, wait and retry
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
        }

        Ok(())
    }
}
