/*
 * Copyright 2026 Oxide Computer Company
 */

//! Worker operations for artifact download and upload.
//!
//! This module contains functions that interact with the buildomat worker APIs
//! for downloading input artifacts and uploading output files. These are
//! analogous to methods on `ClientWrap` in the agent crate, but designed for
//! use by the factory rather than an in-VM agent.
//!
//! # Differences from `ClientWrap`
//!
//! **Retry behavior:** `ClientWrap` methods retry forever because the agent
//! runs in an ephemeral VM and must eventually complete. Our functions return
//! errors immediately, letting the factory's main loop decide retry policy.
//!
//! **Return types:** `ClientWrap::output()` returns `Option<String>` where
//! `None` = success and `Some(msg)` = error.
//! Here the `Result<T, Error>` works cleanly with the `?` operator and
//! anyhow's error context.
//!
//! **Scope:** `ClientWrap` also handles event streaming, task completion, and
//! diagnostics. This module only covers artifact download/upload operations.
//!
//! # See Also
//!
//! - `agent/src/main.rs` - `ClientWrap` implementation
//! - `agent/src/upload.rs` - Full output upload logic with all rule types

use std::collections::HashSet;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use anyhow::{bail, ensure, Result};
use buildomat_client::types::{WorkerAppendJobOrTask, WorkerPingOutputRule};
use chrono::Utc;
use futures::StreamExt;
use rusty_ulid::Ulid;
use slog::{debug, info, warn, Logger};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};

use buildomat_common::UPLOAD_CHUNK_SIZE;

/// Polling interval when waiting for add_output to complete.
/// The server processes uploaded chunks asynchronously; we poll until done.
const ADD_OUTPUT_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Maximum number of add_output polls before giving up (~60 seconds).
const MAX_ADD_OUTPUT_POLLS: u32 = 120;

/// Maximum number of events to batch in a single append call.
const EVENT_BATCH_SIZE: usize = 100;

/// Send a batch of events to the server.
///
/// Calls `worker_job_append` with the given events. Returns `Result<()>`.
pub async fn append_events(
    client: &buildomat_client::Client,
    job_id: &str,
    events: Vec<WorkerAppendJobOrTask>,
) -> Result<()> {
    client.worker_job_append().job(job_id).body(events).send().await?;
    Ok(())
}

/// Spawn a background task that reads stdout/stderr from a child process
/// and streams lines to the server as events via `worker_job_append`.
///
/// Returns a `JoinHandle` that resolves when both streams are drained and
/// all remaining events have been flushed.
pub fn spawn_event_streamer(
    log: Logger,
    client: buildomat_client::Client,
    job_id: String,
    stdout: tokio::process::ChildStdout,
    stderr: tokio::process::ChildStderr,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut stdout_reader = tokio::io::BufReader::new(stdout).lines();
        let mut stderr_reader = tokio::io::BufReader::new(stderr).lines();

        let mut batch: Vec<WorkerAppendJobOrTask> = Vec::new();
        let mut stdout_done = false;
        let mut stderr_done = false;

        loop {
            // Wait for a line from either stream
            let event = tokio::select! {
                line = stdout_reader.next_line(), if !stdout_done => {
                    match line {
                        Ok(Some(text)) => Some(("stdout", text)),
                        Ok(None) => {
                            stdout_done = true;
                            None
                        }
                        Err(e) => {
                            warn!(log, "error reading stdout"; "error" => %e);
                            stdout_done = true;
                            None
                        }
                    }
                }
                line = stderr_reader.next_line(), if !stderr_done => {
                    match line {
                        Ok(Some(text)) => Some(("stderr", text)),
                        Ok(None) => {
                            stderr_done = true;
                            None
                        }
                        Err(e) => {
                            warn!(log, "error reading stderr"; "error" => %e);
                            stderr_done = true;
                            None
                        }
                    }
                }
            };

            if let Some((stream, payload)) = event {
                batch.push(WorkerAppendJobOrTask {
                    payload,
                    stream: stream.to_string(),
                    task: None,
                    time: Utc::now(),
                });
            }

            // Flush if batch is full or both streams are done
            let should_flush = batch.len() >= EVENT_BATCH_SIZE
                || (stdout_done && stderr_done && !batch.is_empty());

            if should_flush {
                /*
                 * Clone the batch before sending so that events are
                 * preserved if the append call fails.  The agent
                 * retries forever; we retry on the next flush cycle
                 * (the events stay in `batch`).
                 *
                 * When both streams are done this is our last chance,
                 * so retry a few times before giving up.
                 */
                let attempts = if stdout_done && stderr_done { 3 } else { 1 };
                for attempt in 0..attempts {
                    match append_events(&client, &job_id, batch.clone()).await {
                        Ok(()) => {
                            batch.clear();
                            break;
                        }
                        Err(e) => {
                            warn!(log, "failed to append events";
                                "error" => %e,
                                "attempt" => attempt + 1,
                                "of" => attempts);
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
            }

            if stdout_done && stderr_done {
                break;
            }
        }
    })
}

/// Run a diagnostic script and stream its output as events.
///
/// Writes `script` to a temp file, runs it with `bash`, streams stdout/stderr
/// as events with the given `stream_prefix` (e.g., "diag.pre" or "diag.post"),
/// and returns the exit status.
pub async fn run_diagnostic_script(
    log: &Logger,
    client: &buildomat_client::Client,
    job_id: &str,
    script: &str,
    stream_prefix: &str,
    work_dir: &Path,
) -> Result<std::process::ExitStatus> {
    use tokio::process::Command;

    // Write script to a temp file in the work dir
    let script_path = work_dir.join(format!(".{}-diag.sh", stream_prefix));
    tokio::fs::write(&script_path, script).await?;

    info!(log, "running {} diagnostic script", stream_prefix;
        "path" => %script_path.display());

    let mut child = Command::new("bash")
        .arg(&script_path)
        .current_dir(work_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    let child_stdout = child.stdout.take().unwrap();
    let child_stderr = child.stderr.take().unwrap();

    // Stream output as events with the diagnostic stream name
    let mut stdout_reader = tokio::io::BufReader::new(child_stdout).lines();
    let mut stderr_reader = tokio::io::BufReader::new(child_stderr).lines();

    let mut batch: Vec<WorkerAppendJobOrTask> = Vec::new();
    let mut stdout_done = false;
    let mut stderr_done = false;
    let stdout_stream = format!("{}.stdout", stream_prefix);
    let stderr_stream = format!("{}.stderr", stream_prefix);

    loop {
        let event = tokio::select! {
            line = stdout_reader.next_line(), if !stdout_done => {
                match line {
                    Ok(Some(text)) => Some((stdout_stream.as_str(), text)),
                    Ok(None) => { stdout_done = true; None }
                    Err(e) => {
                        warn!(log, "error reading diag stdout"; "error" => %e);
                        stdout_done = true;
                        None
                    }
                }
            }
            line = stderr_reader.next_line(), if !stderr_done => {
                match line {
                    Ok(Some(text)) => Some((stderr_stream.as_str(), text)),
                    Ok(None) => { stderr_done = true; None }
                    Err(e) => {
                        warn!(log, "error reading diag stderr"; "error" => %e);
                        stderr_done = true;
                        None
                    }
                }
            }
        };

        if let Some((stream, payload)) = event {
            batch.push(WorkerAppendJobOrTask {
                payload,
                stream: stream.to_string(),
                task: None,
                time: Utc::now(),
            });
        }

        let should_flush = batch.len() >= EVENT_BATCH_SIZE
            || (stdout_done && stderr_done && !batch.is_empty());

        if should_flush {
            let attempts = if stdout_done && stderr_done { 3 } else { 1 };
            for attempt in 0..attempts {
                match append_events(client, job_id, batch.clone()).await {
                    Ok(()) => {
                        batch.clear();
                        break;
                    }
                    Err(e) => {
                        warn!(log, "failed to append diag events";
                            "error" => %e,
                            "attempt" => attempt + 1,
                            "of" => attempts);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        }

        if stdout_done && stderr_done {
            break;
        }
    }

    let status = child.wait().await?;

    // Clean up script file
    let _ = tokio::fs::remove_file(&script_path).await;

    info!(log, "{} diagnostic script completed", stream_prefix;
        "status" => %crate::process::format_exit_status(&status));

    Ok(status)
}

/// Download a single input file from the server.
///
/// Downloads to a temporary `.partial` file first, then renames to the final
/// path on success. This ensures that the final file either exists completely
/// or not at all, making cancellation and error handling easier to reason about.
///
/// Analogous to `ClientWrap::input()` in the agent, but returns `Result`
/// instead of retrying forever.
pub async fn download_input(
    log: &Logger,
    client: &buildomat_client::Client,
    job_id: &str,
    input_id: &str,
    input_name: &str,
    dest_dir: &Path,
) -> Result<()> {
    let dest_path = dest_dir.join(input_name);

    /*
     * Reject paths that escape the destination directory (defense in
     * depth).  Check for ".." components explicitly because
     * Path::starts_with operates on un-normalized components and
     * does not catch traversal like "foo/../../other".
     */
    ensure!(
        !Path::new(input_name)
            .components()
            .any(|c| { matches!(c, std::path::Component::ParentDir) }),
        "input name {:?} contains path traversal components",
        input_name,
    );
    ensure!(
        dest_path.starts_with(dest_dir),
        "input name {:?} escapes artifact directory",
        input_name,
    );

    /*
     * Download to a temp file first, rename on success.  Create parent
     * directories if the input name contains path separators.
     */
    let temp_path = dest_dir.join(format!("{}.partial", input_name));
    if let Some(parent) = dest_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    debug!(log, "downloading input"; "id" => input_id, "name" => input_name);

    let result: Result<()> = async {
        let response = client
            .worker_job_input_download()
            .job(job_id)
            .input(input_id)
            .send()
            .await?;

        let mut body = response.into_inner();
        let mut file = tokio::fs::File::create(&temp_path).await?;

        while let Some(chunk) = body.next().await {
            let chunk = chunk?;
            file.write_all(&chunk).await?;
        }

        file.flush().await?;
        drop(file);

        tokio::fs::rename(&temp_path, &dest_path).await?;
        Ok(())
    }
    .await;

    if result.is_err() {
        let _ = tokio::fs::remove_file(&temp_path).await;
        return result;
    }

    info!(log, "downloaded input";
        "name" => input_name,
        "path" => %dest_path.display());

    Ok(())
}

/// Upload a chunk of file data, returning the chunk ID.
///
/// Analogous to `ClientWrap::chunk()` in the agent, but returns `Result`
/// instead of retrying forever.
pub async fn upload_chunk(
    client: &buildomat_client::Client,
    job_id: &str,
    data: Vec<u8>,
) -> Result<String> {
    let response =
        client.worker_job_upload_chunk().job(job_id).body(data).send().await?;

    Ok(response.into_inner().id)
}

/// Upload a single output file to the server.
///
/// Reads the file in 5MB chunks, uploads each chunk, then registers the
/// complete file with the server.
///
/// If `size_change_ok` is true, size changes and file disappearance are
/// warnings rather than errors (matching `agent/src/upload.rs` behavior).
///
/// Analogous to `ClientWrap::output()` in the agent, but returns `Result`
/// instead of `Option<String>`.
pub async fn upload_output(
    log: &Logger,
    client: &buildomat_client::Client,
    job_id: &str,
    file_path: &Path,
    output_path: &str,
    size_change_ok: bool,
) -> Result<()> {
    let metadata = match tokio::fs::metadata(file_path).await {
        Ok(md) => md,
        Err(e)
            if e.kind() == std::io::ErrorKind::NotFound && size_change_ok =>
        {
            warn!(
                log,
                "file {:?} disappeared between scan and upload", file_path
            );
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };
    let file_size = metadata.len();

    debug!(log, "uploading output";
        "path" => output_path,
        "size" => file_size);

    /* Read and upload file in 5MB chunks. */
    let mut file = tokio::fs::File::open(file_path).await?;
    let mut chunk_ids = Vec::new();
    let mut total_read = 0u64;

    loop {
        let mut buf = vec![0u8; UPLOAD_CHUNK_SIZE];
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        buf.truncate(n);
        total_read += n as u64;

        let chunk_id = upload_chunk(client, job_id, buf).await?;
        chunk_ids.push(chunk_id);
    }

    /* Verify file size hasn't changed. */
    if total_read != file_size {
        let msg = format!(
            "file {:?} changed size mid upload: {} -> {}",
            file_path, file_size, total_read,
        );
        if size_change_ok {
            warn!(log, "{}", msg);
        } else {
            bail!("{}", msg);
        }
    }

    /* Register the output with the server. */
    let commit_id = Ulid::generate().to_string();
    let body = buildomat_client::types::WorkerAddOutput {
        chunks: chunk_ids,
        path: output_path.to_string(),
        size: total_read,
        commit_id,
    };

    let mut polls = 0u32;
    loop {
        let response = client
            .worker_job_add_output()
            .job(job_id)
            .body(&body)
            .send()
            .await?;

        let result = response.into_inner();

        if !result.complete {
            polls += 1;
            if polls >= MAX_ADD_OUTPUT_POLLS {
                bail!(
                    "timed out waiting for output {} to complete \
                     after {} polls",
                    output_path,
                    polls,
                );
            }
            tokio::time::sleep(ADD_OUTPUT_POLL_INTERVAL).await;
            continue;
        }

        if let Some(err) = result.error {
            bail!("failed to add output {}: {}", output_path, err);
        }

        break;
    }

    info!(log, "uploaded output";
        "path" => output_path,
        "size" => total_read);

    Ok(())
}

/// Upload output files matching the job's output rules.
///
/// This function processes output rules similarly to `agent/src/upload.rs`:
/// 1. Separate rules into include patterns and ignore patterns
/// 2. Use glob to find files matching include patterns
/// 3. Filter out files matching any ignore pattern
/// 4. Upload remaining files
/// 5. Check that all `require_match` patterns matched at least one file
///
/// The `size_change_ok` flag on each rule is passed through to `upload_output`
/// to control whether file size changes are warnings or errors.
///
/// If no output rules are provided, all files in the output directory are
/// uploaded (for backwards compatibility with simple use cases).
pub async fn upload_outputs(
    log: &Logger,
    client: &buildomat_client::Client,
    job_id: &str,
    output_dir: &Path,
    output_rules: &[WorkerPingOutputRule],
) -> Result<()> {
    /* If no rules, upload everything in output directory. */
    if output_rules.is_empty() {
        return upload_all_outputs(log, client, job_id, output_dir).await;
    }

    /*
     * Separate rules by type, pre-compile glob patterns.
     * Track which include rules have require_match and size_change_ok.
     */
    let mut include_patterns = Vec::new();
    let mut ignore_patterns = Vec::new();
    let mut required: Vec<(glob::Pattern, bool)> = Vec::new();
    let mut relaxed: Vec<glob::Pattern> = Vec::new();

    for rule in output_rules {
        let pattern = match glob::Pattern::new(&rule.rule) {
            Ok(p) => p,
            Err(e) => {
                warn!(log, "invalid glob pattern";
                    "rule" => &rule.rule,
                    "error" => %e);
                continue;
            }
        };

        if rule.ignore {
            ignore_patterns.push(pattern);
        } else {
            if rule.require_match {
                required.push((pattern.clone(), false));
            }
            if rule.size_change_ok {
                relaxed.push(pattern.clone());
            }
            include_patterns.push(rule.rule.clone());
        }
    }

    /*
     * Use glob to find files matching include patterns.
     * Patterns are relative to the output directory.
     */
    let mut files_to_upload = Vec::new();
    let mut seen = HashSet::new();

    let escaped_prefix = glob::Pattern::escape(&output_dir.to_string_lossy());

    for rule_str in &include_patterns {
        // Construct glob pattern relative to output directory.
        // Escape the output_dir prefix so that any glob metacharacters
        // in the configured job_dir (e.g., brackets) are treated as
        // literal characters.
        let glob_str = format!("{}/{}", escaped_prefix, rule_str);

        match glob::glob(&glob_str) {
            Ok(paths) => {
                for entry in paths {
                    match entry {
                        Ok(path) => {
                            // Skip if already seen
                            if seen.contains(&path) {
                                continue;
                            }
                            seen.insert(path.clone());

                            // Skip directories
                            match tokio::fs::metadata(&path).await {
                                Ok(md) if !md.is_file() => continue,
                                Err(e) => {
                                    warn!(log, "failed to stat file";
                                        "path" => %path.display(),
                                        "error" => %e);
                                    continue;
                                }
                                Ok(_) => {}
                            }

                            // Get path relative to output_dir for ignore check
                            let rel_path = path
                                .strip_prefix(output_dir)
                                .unwrap_or(&path)
                                .to_string_lossy()
                                .to_string();

                            // Check if file matches any ignore pattern
                            let rel = Path::new(&rel_path);
                            let ignored = ignore_patterns
                                .iter()
                                .any(|p| p.matches_path(rel));

                            if ignored {
                                debug!(log, "ignoring file";
                                    "path" => &rel_path);
                                continue;
                            }

                            files_to_upload.push((path, rel_path));
                        }
                        Err(e) => {
                            warn!(log, "glob error"; "error" => %e);
                        }
                    }
                }
            }
            Err(e) => {
                warn!(log, "invalid glob pattern";
                    "pattern" => %glob_str,
                    "error" => %e);
            }
        }
    }

    if files_to_upload.is_empty() {
        debug!(log, "no output files to upload");
    } else {
        info!(log, "uploading {} output file(s)", files_to_upload.len());

        for (file_path, output_path) in &files_to_upload {
            let out = Path::new(output_path);
            let size_change_ok = relaxed.iter().any(|g| g.matches_path(out));
            upload_output(
                log,
                client,
                job_id,
                file_path,
                output_path,
                size_change_ok,
            )
            .await?;

            // Mark any required patterns that this file matched.
            for (g, used) in required.iter_mut() {
                if g.matches_path(out) {
                    *used = true;
                }
            }
        }
    }

    /* Check that all require_match patterns were satisfied. */
    for (g, used) in &required {
        if !*used {
            bail!("rule \"{}\" required a match, but was not used", g);
        }
    }

    Ok(())
}

/// Upload all files in the output directory (recursive).
///
/// Used when no output rules are specified.
async fn upload_all_outputs(
    log: &Logger,
    client: &buildomat_client::Client,
    job_id: &str,
    output_dir: &Path,
) -> Result<()> {
    let mut files_to_upload = Vec::new();

    // Recursively collect all files
    collect_files_recursive(output_dir, output_dir, &mut files_to_upload)
        .await?;

    if files_to_upload.is_empty() {
        debug!(log, "no output files to upload");
        return Ok(());
    }

    info!(log, "uploading {} output file(s)", files_to_upload.len());

    for (file_path, rel_path) in files_to_upload {
        upload_output(log, client, job_id, &file_path, &rel_path, false)
            .await?;
    }

    Ok(())
}

/// Recursively collect all files in a directory.
///
/// Note: `entry.metadata()` follows symlinks, so a symlink pointing
/// outside the output directory will have its target's contents uploaded.
/// This matches the agent's behavior (glob follows symlinks by default).
pub(crate) async fn collect_files_recursive(
    base_dir: &Path,
    current_dir: &Path,
    files: &mut Vec<(std::path::PathBuf, String)>,
) -> Result<()> {
    let mut entries = tokio::fs::read_dir(current_dir).await?;

    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        let metadata = entry.metadata().await?;

        if metadata.is_file() {
            let rel_path = path
                .strip_prefix(base_dir)
                .unwrap_or(&path)
                .to_string_lossy()
                .to_string();
            files.push((path, rel_path));
        } else if metadata.is_dir() {
            Box::pin(collect_files_recursive(base_dir, &path, files)).await?;
        }
    }

    Ok(())
}
