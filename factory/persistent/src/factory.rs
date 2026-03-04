/*
 * Copyright 2026 Oxide Computer Company
 */

use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::{bail, Result};
use buildomat_client::types::WorkerPingOutputRule;
use buildomat_common::genkey;
use slog::{debug, error, info, warn, Logger};
use tokio::process::{Child, Command};

use crate::config::ConfigFile;
use crate::process::{
    format_exit_status, graceful_kill, GRACEFUL_KILL_TIMEOUT,
};
use crate::worker_ops;

/// Length of generated worker tokens in characters.
/// Matches token generation in agent and server (genkey(64) calls).
/// TODO: Consider moving to buildomat_common as a shared constant.
const WORKER_TOKEN_LENGTH: usize = 64;

/// Per-job directory paths, created under the configured job_dir.
#[derive(Debug)]
pub(crate) struct JobPaths {
    /// Root directory for this job: {job_dir}/{job_id}/
    pub(crate) root: PathBuf,
    /// Working directory: {job_dir}/{job_id}/work/
    pub(crate) work: PathBuf,
    /// Input artifacts: {job_dir}/{job_id}/artifacts/
    pub(crate) artifacts: PathBuf,
    /// Output files: {job_dir}/{job_id}/output/
    pub(crate) output: PathBuf,
}

impl JobPaths {
    pub(crate) fn new(base: &Path, job_id: &str) -> Self {
        let root = base.join(job_id);
        JobPaths {
            work: root.join("work"),
            artifacts: root.join("artifacts"),
            output: root.join("output"),
            root,
        }
    }

    /// Create all directories for this job.
    pub(crate) fn create_all(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.work)?;
        std::fs::create_dir_all(&self.artifacts)?;
        std::fs::create_dir_all(&self.output)?;
        Ok(())
    }

    /// Remove all directories for this job.
    pub(crate) fn remove_all(&self) -> std::io::Result<()> {
        match std::fs::remove_dir_all(&self.root) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }
}

/// In-memory instance tracking (no database needed for single-job execution)
pub(crate) struct Instance {
    pub(crate) worker_id: String,
    pub(crate) job_id: String,
    pub(crate) target: String,
    pub(crate) child: Child,
    pub(crate) paths: JobPaths,
    /// Client authenticated as the worker, for upload APIs
    pub(crate) worker_client: buildomat_client::Client,
    /// Output rules from the job, for filtering which files to upload
    pub(crate) output_rules: Vec<WorkerPingOutputRule>,
    /// Background task streaming stdout/stderr to the server
    pub(crate) append_task: tokio::task::JoinHandle<()>,
    /// Post-job diagnostic script to run after the command completes.
    /// If present, `worker_diagnostics_enable` has already been called.
    pub(crate) post_diag_script: Option<String>,
}

pub(crate) struct Central {
    pub(crate) log: Logger,
    pub(crate) client: buildomat_client::Client,
    pub(crate) config: ConfigFile,
    pub(crate) instances: Vec<Instance>,
}

pub(crate) async fn factory_loop(c: &mut Central) -> Result<()> {
    check_instances(c).await?;

    if !c.instances.is_empty() {
        c.client.factory_ping().send().await?;
        return Ok(());
    }

    accept_work(c).await
}

/// Poll existing instances for completion.
///
/// For each instance, query the server for worker status.  If the worker
/// has been recycled, its process has exited, or the worker no longer
/// exists, remove it from the list after running `complete_instance`.
async fn check_instances(c: &mut Central) -> Result<()> {
    let mut to_remove = Vec::new();
    for (idx, instance) in c.instances.iter_mut().enumerate() {
        let w = match c
            .client
            .factory_worker_get()
            .worker(&instance.worker_id)
            .send()
            .await
        {
            Ok(resp) => resp.into_inner(),
            Err(e) => {
                warn!(
                    c.log,
                    "failed to query worker {}, skipping",
                    instance.worker_id;
                    "error" => %e,
                );
                continue;
            }
        };

        let (complete, exit_status) = match &w.worker {
            Some(worker) => {
                if worker.recycle {
                    info!(
                        c.log,
                        "worker {} recycled by server, killing process",
                        worker.id
                    );
                    let status = graceful_kill(
                        &c.log,
                        &mut instance.child,
                        GRACEFUL_KILL_TIMEOUT,
                    )
                    .await;
                    (true, status)
                } else {
                    /*
                     * Check if the command has completed (non-blocking).
                     */
                    match instance.child.try_wait() {
                        Ok(Some(status)) => {
                            info!(c.log, "worker {} command completed", worker.id;
                                "exit_status" => %format_exit_status(&status));
                            (true, Some(status))
                        }
                        Ok(None) => {
                            debug!(
                                c.log,
                                "worker {} command still running", worker.id
                            );
                            (false, None)
                        }
                        Err(e) => {
                            error!(
                                c.log,
                                "worker {} error checking process: {:?}",
                                worker.id,
                                e
                            );
                            (true, None)
                        }
                    }
                }
            }
            None => {
                warn!(
                    c.log,
                    "worker {} no longer exists, killing process",
                    instance.worker_id
                );
                let status = graceful_kill(
                    &c.log,
                    &mut instance.child,
                    GRACEFUL_KILL_TIMEOUT,
                )
                .await;
                (true, status)
            }
        };

        if complete {
            to_remove.push((idx, w.worker.is_some(), exit_status));
        }
    }

    /*
     * Remove completed instances (in reverse order to preserve indices)
     * and finalize each one.
     *
     * Note: the instance is removed from the vec before complete_instance
     * runs.  If this future is cancelled (e.g., the main select! fires a
     * signal), the instance is dropped without network cleanup.  This is
     * acceptable: graceful_kill has already sent SIGTERM above, and the
     * server will recycle orphaned workers on its own timeout.
     */
    for (idx, had_worker, exit_status) in to_remove.into_iter().rev() {
        let instance = c.instances.remove(idx);
        complete_instance(&c.log, &c.client, instance, had_worker, exit_status)
            .await;
    }

    Ok(())
}

/// Finalize a completed instance.
///
/// Flushes events, uploads outputs, reports completion, runs
/// post-job diagnostics, destroys the worker, and cleans up
/// the job directory.
///
/// All errors are logged as warnings rather than propagated, matching
/// the shutdown path.  The instance has already been removed from the
/// vec by the caller, so there is no opportunity to retry; best-effort
/// cleanup is the right strategy here.
async fn complete_instance(
    log: &Logger,
    factory_client: &buildomat_client::Client,
    mut instance: Instance,
    had_worker: bool,
    exit_status: Option<std::process::ExitStatus>,
) {
    if had_worker {
        /*
         * Report job result based on exit status.
         * Exit code 0 = success, anything else = failure.
         */
        let success = exit_status.map(|s| s.success()).unwrap_or(false);

        if !success {
            info!(log, "worker {} failed", instance.worker_id;
                "job" => &instance.job_id,
                "target" => &instance.target);
        }

        /*
         * Wait for the event streamer to flush all remaining
         * output before we upload files and complete the job.
         */
        if let Err(e) = (&mut instance.append_task).await {
            warn!(log, "event streamer task panicked";
                "job" => &instance.job_id,
                "error" => %e);
        }

        /*
         * Upload output files before destroying the worker.
         * We attempt upload regardless of success/failure so that
         * logs and diagnostics can be collected even from failed jobs.
         */
        if let Err(e) = worker_ops::upload_outputs(
            log,
            &instance.worker_client,
            &instance.job_id,
            &instance.paths.output,
            &instance.output_rules,
        )
        .await
        {
            warn!(log, "failed to upload outputs";
                "job" => &instance.job_id,
                "error" => %e);
        }

        /*
         * Tell the server whether the job passed or failed.
         * This must happen before factory_worker_destroy so
         * the server records the outcome.
         */
        if let Err(e) = instance
            .worker_client
            .worker_job_complete()
            .job(&instance.job_id)
            .body_map(|b| b.failed(!success))
            .send()
            .await
        {
            warn!(log, "failed to report job completion";
                "job" => &instance.job_id,
                "error" => %e);
        }

        /*
         * Run post-job diagnostic script if configured.
         * The server has been told to wait (via diagnostics_enable)
         * so we can gather extra info even after reporting completion.
         */
        if let Some(script) = &instance.post_diag_script {
            let diag_status = worker_ops::run_diagnostic_script(
                log,
                &instance.worker_client,
                &instance.job_id,
                script,
                "diag.post",
                &instance.paths.work,
            )
            .await;

            let hold = match diag_status {
                Ok(status) => !status.success(),
                Err(e) => {
                    warn!(log, "post-job diagnostic failed";
                        "job" => &instance.job_id,
                        "error" => %e);
                    false
                }
            };

            if let Err(e) = instance
                .worker_client
                .worker_diagnostics_complete()
                .body_map(|b| b.hold(hold))
                .send()
                .await
            {
                warn!(log, "failed to complete diagnostics";
                    "job" => &instance.job_id,
                    "error" => %e);
            }
        }

        if let Err(e) = factory_client
            .factory_worker_destroy()
            .worker(&instance.worker_id)
            .send()
            .await
        {
            warn!(log, "failed to destroy worker";
                "worker" => &instance.worker_id,
                "error" => %e);
        } else {
            info!(log, "destroyed worker {}", instance.worker_id;
                "success" => success);
        }
    }

    /*
     * Clean up the per-job directory.
     */
    if let Err(e) = instance.paths.remove_all() {
        warn!(log, "failed to clean up job directory";
            "job" => &instance.job_id,
            "path" => %instance.paths.root.display(),
            "error" => %e);
    } else {
        debug!(log, "cleaned up job directory";
            "job" => &instance.job_id,
            "path" => %instance.paths.root.display());
    }
}

/// Check for new work and start processing it.
///
/// Requests a lease from the server, creates a worker, bootstraps it,
/// downloads inputs, sets up diagnostics, spawns the external command,
/// and pushes the new instance onto `c.instances`.
///
/// If any setup step fails after the worker is created on the server,
/// the worker is destroyed best-effort to avoid leaking it.
async fn accept_work(c: &mut Central) -> Result<()> {
    let res = c
        .client
        .factory_lease()
        .body_map(|b| b.supported_targets(c.config.targets()))
        .send()
        .await?
        .into_inner();

    let Some(lease) = res.lease else {
        debug!(c.log, "no work available");
        return Ok(());
    };

    info!(c.log, "received lease"; "job" => &lease.job, "target" => &lease.target);

    if !c.config.target.contains_key(&lease.target) {
        bail!(
            "server assigned unsupported target {:?} \
             (we advertised {:?})",
            lease.target,
            c.config.targets(),
        );
    }

    /*
     * Create a worker for this lease.
     */
    let w = c
        .client
        .factory_worker_create()
        .body_map(|b| b.target(&lease.target).wait_for_flush(false))
        .send()
        .await?;

    let worker_id = w.id.to_string();
    let bootstrap = w.bootstrap.to_string();

    info!(c.log, "created worker"; "id" => &worker_id, "target" => &lease.target);

    /*
     * Set up the instance: associate, bootstrap, download inputs,
     * configure diagnostics, and spawn the command.  If any step
     * fails, destroy the worker so we don't leak it on the server.
     */
    let result =
        setup_instance(c, &worker_id, &bootstrap, &lease.job, &lease.target)
            .await;

    if let Err(ref e) = result {
        warn!(c.log, "worker setup failed, destroying orphan";
            "worker" => &worker_id, "error" => %e);
        let _ =
            c.client.factory_worker_destroy().worker(&worker_id).send().await;
        let _ =
            JobPaths::new(&c.config.execution.job_dir, &lease.job).remove_all();
    }

    result
}

/// Set up and push a new instance after the worker has been created.
///
/// This is separated from `accept_work` so that the caller can
/// destroy the worker on error, preventing server-side leaks.
async fn setup_instance(
    c: &mut Central,
    worker_id: &str,
    bootstrap: &str,
    job_id: &str,
    target: &str,
) -> Result<()> {
    /*
     * Associate the worker with an instance ID.
     */
    let instance_id = format!("persistent-{}", worker_id);
    c.client
        .factory_worker_associate()
        .worker(worker_id)
        .body_map(|b| b.private(instance_id.clone()))
        .send()
        .await?;

    info!(c.log, "associated worker with instance";
        "worker" => worker_id,
        "instance" => &instance_id);

    /*
     * Create per-job directories.
     */
    let exec = &c.config.execution;
    let paths = JobPaths::new(&exec.job_dir, job_id);
    paths.create_all()?;

    info!(c.log, "created job directories";
        "job" => job_id,
        "root" => %paths.root.display());

    /*
     * Bootstrap as a worker so we can use worker APIs (like downloading
     * inputs).  Generate a local token for authentication.
     */
    let worker_token = genkey(WORKER_TOKEN_LENGTH);
    let worker_client =
        buildomat_client::ClientBuilder::new(&c.config.general.baseurl)
            .bearer_token(&worker_token)
            .build()?;

    worker_client
        .worker_bootstrap()
        .body_map(|b| b.bootstrap(bootstrap).token(&worker_token))
        .send()
        .await?;

    info!(c.log, "bootstrapped as worker"; "worker" => worker_id);

    /*
     * Get job information including input files and output rules.
     */
    let ping = worker_client.worker_ping().send().await?.into_inner();

    let output_rules = if let Some(job) = &ping.job {
        info!(c.log, "job has {} input(s), {} output rule(s)",
            job.inputs.len(), job.output_rules.len();
            "job" => &job.id,
            "name" => &job.name);

        /*
         * Download each input file to the artifacts directory.
         */
        for input in &job.inputs {
            worker_ops::download_input(
                &c.log,
                &worker_client,
                &job.id,
                &input.id,
                &input.name,
                &paths.artifacts,
            )
            .await?;
        }

        if !job.inputs.is_empty() {
            info!(c.log, "all inputs downloaded";
                "count" => job.inputs.len(),
                "dir" => %paths.artifacts.display());
        }

        job.output_rules.clone()
    } else {
        debug!(c.log, "no job info available yet"; "worker" => worker_id);
        Vec::new()
    };

    /*
     * Write job metadata to the artifact directory so the external
     * command can access task scripts and other server-provided
     * information.  The reserved "buildomat/" prefix must not collide
     * with dependency names used in job file `copy_outputs` directives.
     */
    if let Some(job) = &ping.job {
        let meta_dir = paths.artifacts.join("buildomat");
        std::fs::create_dir_all(&meta_dir)?;
        let meta_path = meta_dir.join("job.json");
        std::fs::write(&meta_path, serde_json::to_string_pretty(job)?)?;
        info!(c.log, "wrote job metadata"; "path" => %meta_path.display());
    }

    /*
     * Extract diagnostic scripts from factory metadata.
     * If a post-job diagnostic script is configured, tell the server to
     * wait for diagnostics before recycling this worker.
     */
    let mut post_diag_script: Option<String> = None;

    if let Some(ref md) = ping.factory_metadata {
        if let Some(script) = md.post_job_diagnostic_script() {
            worker_client.worker_diagnostics_enable().send().await?;
            post_diag_script = Some(script.to_string());
            info!(c.log, "post-job diagnostic script registered";
                "worker" => worker_id);
        }

        if let Some(script) = md.pre_job_diagnostic_script() {
            info!(c.log, "running pre-job diagnostic script";
                "worker" => worker_id);
            let status = worker_ops::run_diagnostic_script(
                &c.log,
                &worker_client,
                job_id,
                script,
                "diag.pre",
                &paths.work,
            )
            .await?;

            if !status.success() {
                warn!(c.log, "pre-job diagnostic script failed";
                    "worker" => worker_id,
                    "status" => %format_exit_status(&status));
            }
        }
    }

    /*
     * Spawn the external command with job information in environment
     * variables.  Pipe stdout/stderr so we can stream them as events
     * to the server.
     */
    let exec = &c.config.execution;
    let mut child = Command::new(&exec.command)
        .args(&exec.args)
        .current_dir(&paths.work)
        .env("BUILDOMAT_JOB_ID", job_id)
        .env("BUILDOMAT_TARGET", target)
        .env("BUILDOMAT_WORKER_ID", worker_id)
        .env("BUILDOMAT_WORK_DIR", &paths.work)
        .env("BUILDOMAT_ARTIFACT_DIR", &paths.artifacts)
        .env("BUILDOMAT_OUTPUT_DIR", &paths.output)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    let child_stdout = child.stdout.take().unwrap();
    let child_stderr = child.stderr.take().unwrap();

    let append_task = worker_ops::spawn_event_streamer(
        c.log.clone(),
        worker_client.clone(),
        job_id.to_string(),
        child_stdout,
        child_stderr,
    );

    info!(c.log, "spawned command";
        "command" => &exec.command,
        "job" => job_id,
        "target" => target);

    c.instances.push(Instance {
        worker_id: worker_id.to_string(),
        job_id: job_id.to_string(),
        target: target.to_string(),
        child,
        paths,
        worker_client,
        output_rules,
        append_task,
        post_diag_script,
    });

    Ok(())
}

/// Clean up active instances during graceful shutdown.
///
/// For each active instance: kill the child process gracefully, flush
/// events, upload outputs (best-effort), report job failure, destroy
/// the worker, and clean up the job directory.
///
/// All errors are warnings — we are exiting and want to clean up as
/// much as possible. Diagnostic scripts are NOT run during shutdown
/// (the user sent a signal, they want a prompt exit).
pub(crate) async fn shutdown(c: &mut Central) {
    if c.instances.is_empty() {
        return;
    }

    info!(c.log, "shutting down {} active instance(s)", c.instances.len());

    for mut instance in c.instances.drain(..) {
        info!(c.log, "shutting down instance";
            "worker" => &instance.worker_id,
            "job" => &instance.job_id);

        // 1. Gracefully kill the child process.
        let _status =
            graceful_kill(&c.log, &mut instance.child, GRACEFUL_KILL_TIMEOUT)
                .await;

        // 2. Wait for the event streamer to flush.
        if let Err(e) = (&mut instance.append_task).await {
            warn!(c.log, "event streamer task panicked during shutdown";
                "job" => &instance.job_id,
                "error" => %e);
        }

        // 3. Upload outputs (best-effort).
        if let Err(e) = worker_ops::upload_outputs(
            &c.log,
            &instance.worker_client,
            &instance.job_id,
            &instance.paths.output,
            &instance.output_rules,
        )
        .await
        {
            warn!(c.log, "failed to upload outputs during shutdown";
                "job" => &instance.job_id,
                "error" => %e);
        }

        // 4. Report job failure.
        if let Err(e) = instance
            .worker_client
            .worker_job_complete()
            .job(&instance.job_id)
            .body_map(|b| b.failed(true))
            .send()
            .await
        {
            warn!(c.log, "failed to report job failure during shutdown";
                "job" => &instance.job_id,
                "error" => %e);
        }

        // 5. Destroy the worker.
        if let Err(e) = c
            .client
            .factory_worker_destroy()
            .worker(&instance.worker_id)
            .send()
            .await
        {
            warn!(c.log, "failed to destroy worker during shutdown";
                "worker" => &instance.worker_id,
                "error" => %e);
        }

        // 6. Clean up job directory.
        if let Err(e) = instance.paths.remove_all() {
            warn!(c.log, "failed to clean up job directory during shutdown";
                "job" => &instance.job_id,
                "path" => %instance.paths.root.display(),
                "error" => %e);
        }

        info!(c.log, "instance shut down";
            "worker" => &instance.worker_id,
            "job" => &instance.job_id);
    }

    info!(c.log, "shutdown complete");
}
