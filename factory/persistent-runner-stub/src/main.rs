/*
 * Copyright 2026 Oxide Computer Company
 */

//! Stub runner binary for integration testing the persistent factory.
//!
//! This binary exercises the full external command interface: environment
//! variables, directory layout, input artifacts, output files, and
//! stdout/stderr streaming. It communicates purely via env vars, the
//! filesystem, and its exit code — no buildomat client crates are needed.
//!
//! # Modes
//!
//! Select a mode with `--mode <mode>`:
//!
//! - `success` — full lifecycle: verify env/dirs, read inputs, write outputs,
//!   exit 0.
//! - `fail` — same as success but exit 1.
//! - `slow` — heartbeat to stdout for `--duration N` seconds, then exit 0.
//! - `crash` — exit 137 immediately, no outputs written.
//! - `no-output` — exit 0 without writing any output files.
//! - `large-output` — write a 6MB file (exceeds 5MB chunk size), exit 0.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use buildomat_common::*;
use slog::{error, info, Logger};

const ENV_VARS: &[&str] = &[
    "BUILDOMAT_JOB_ID",
    "BUILDOMAT_TARGET",
    "BUILDOMAT_WORKER_ID",
    "BUILDOMAT_WORK_DIR",
    "BUILDOMAT_ARTIFACT_DIR",
    "BUILDOMAT_OUTPUT_DIR",
];

fn main() -> Result<()> {
    let mut opts = getopts::Options::new();
    opts.reqopt("m", "mode", "execution mode", "MODE");
    opts.optopt("d", "duration", "duration in seconds (for slow mode)", "SECS");

    let args: Vec<String> = std::env::args().skip(1).collect();
    let matches = match opts.parse(&args) {
        Ok(m) => m,
        Err(e) => {
            eprintln!("ERROR: {}", e);
            eprintln!("{}", opts.usage("usage: persistent-runner-stub"));
            std::process::exit(2);
        }
    };

    let mode = matches.opt_str("mode").unwrap();

    // Crash mode exits immediately — no env checks, no output.
    if mode == "crash" {
        std::process::exit(137);
    }

    let log = make_log("persistent-runner-stub");

    info!(log, "starting"; "mode" => &mode);

    // Verify environment variables.
    let env = read_env(&log)?;

    // Verify directories exist and cwd matches BUILDOMAT_WORK_DIR.
    verify_dirs(&log, &env)?;

    match mode.as_str() {
        "success" => run_full_lifecycle(&log, &env, 0)?,
        "fail" => run_full_lifecycle(&log, &env, 1)?,
        "slow" => {
            let duration: u64 = matches
                .opt_str("duration")
                .unwrap_or_else(|| "5".to_string())
                .parse()
                .context("--duration must be an integer")?;
            run_slow(&log, &env, duration)?;
        }
        "no-output" => {
            info!(log, "no-output mode, skipping output files");
        }
        "large-output" => run_large_output(&log, &env)?,
        other => bail!("unknown mode: {}", other),
    }

    // Exit with appropriate code for fail mode.
    if mode == "fail" {
        std::process::exit(1);
    }

    info!(log, "exiting 0");
    Ok(())
}

/// Environment values extracted from BUILDOMAT_* variables.
struct Env {
    job_id: String,
    target: String,
    worker_id: String,
    work_dir: PathBuf,
    artifact_dir: PathBuf,
    output_dir: PathBuf,
}

/// Read and verify all required environment variables.
fn read_env(log: &Logger) -> Result<Env> {
    let mut missing = Vec::new();

    for var in ENV_VARS {
        match std::env::var(var) {
            Ok(val) => info!(log, "env"; "var" => *var, "val" => val),
            Err(_) => {
                error!(log, "env var not set"; "var" => *var);
                missing.push(*var);
            }
        }
    }

    if !missing.is_empty() {
        bail!("missing required environment variables: {}", missing.join(", "));
    }

    Ok(Env {
        job_id: std::env::var("BUILDOMAT_JOB_ID").unwrap(),
        target: std::env::var("BUILDOMAT_TARGET").unwrap(),
        worker_id: std::env::var("BUILDOMAT_WORKER_ID").unwrap(),
        work_dir: PathBuf::from(std::env::var("BUILDOMAT_WORK_DIR").unwrap()),
        artifact_dir: PathBuf::from(
            std::env::var("BUILDOMAT_ARTIFACT_DIR").unwrap(),
        ),
        output_dir: PathBuf::from(
            std::env::var("BUILDOMAT_OUTPUT_DIR").unwrap(),
        ),
    })
}

/// Verify that expected directories exist and cwd is correct.
fn verify_dirs(log: &Logger, env: &Env) -> Result<()> {
    let cwd = std::env::current_dir().context("failed to get cwd")?;

    if cwd != env.work_dir {
        bail!(
            "cwd mismatch: expected {}, got {}",
            env.work_dir.display(),
            cwd.display()
        );
    }
    info!(log, "cwd matches BUILDOMAT_WORK_DIR");

    for (name, path) in [
        ("work_dir", &env.work_dir),
        ("artifact_dir", &env.artifact_dir),
        ("output_dir", &env.output_dir),
    ] {
        if !path.is_dir() {
            bail!(
                "{} does not exist or is not a directory: {}",
                name,
                path.display()
            );
        }
        info!(log, "directory exists"; "name" => name,
            "path" => %path.display());
    }

    Ok(())
}

/// List input artifacts, returning paths and sizes.
fn list_inputs(artifact_dir: &Path) -> Result<Vec<(PathBuf, u64)>> {
    let mut inputs = Vec::new();
    if !artifact_dir.is_dir() {
        return Ok(inputs);
    }
    walk_dir(artifact_dir, &mut inputs)?;
    inputs.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(inputs)
}

fn walk_dir(dir: &Path, files: &mut Vec<(PathBuf, u64)>) -> Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let md = entry.metadata()?;
        if md.is_file() {
            files.push((path, md.len()));
        } else if md.is_dir() {
            walk_dir(&path, files)?;
        }
    }
    Ok(())
}

/// Full lifecycle: list inputs, write outputs, stream to both fds.
fn run_full_lifecycle(log: &Logger, env: &Env, exit_code: i32) -> Result<()> {
    // List input artifacts.
    let inputs = list_inputs(&env.artifact_dir)?;
    if inputs.is_empty() {
        info!(log, "no input artifacts found");
    } else {
        info!(log, "found input artifacts"; "count" => inputs.len());
        for (path, size) in &inputs {
            let rel = path.strip_prefix(&env.artifact_dir).unwrap_or(path);
            info!(log, "input artifact";
                "path" => %rel.display(), "size" => size);
        }
    }

    // Write result.log (matches *.log require_match rules).
    let result_log = env.output_dir.join("result.log");
    {
        let mut f = fs::File::create(&result_log)?;
        writeln!(f, "job_id: {}", env.job_id)?;
        writeln!(f, "target: {}", env.target)?;
        writeln!(f, "worker_id: {}", env.worker_id)?;
        writeln!(f, "exit_code: {}", exit_code)?;
    }
    info!(log, "wrote output"; "path" => %result_log.display());

    // Write scratch.tmp (matches *.tmp ignore rules).
    let scratch = env.output_dir.join("scratch.tmp");
    fs::write(&scratch, "temporary scratch data\n")?;
    info!(log, "wrote output"; "path" => %scratch.display());

    // Write logs/detail.log (tests nested directory upload).
    let logs_dir = env.output_dir.join("logs");
    fs::create_dir_all(&logs_dir)?;
    let detail_log = logs_dir.join("detail.log");
    fs::write(&detail_log, "detailed log output\n")?;
    info!(log, "wrote output"; "path" => %detail_log.display());

    // Copy input artifacts to artifacts-copy/ (proves the pipeline).
    if !inputs.is_empty() {
        let copy_dir = env.output_dir.join("artifacts-copy");
        fs::create_dir_all(&copy_dir)?;
        for (path, _) in &inputs {
            let rel = path.strip_prefix(&env.artifact_dir).unwrap_or(path);
            let dest = copy_dir.join(rel);
            if let Some(parent) = dest.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(path, &dest)?;
            info!(log, "copied artifact";
                "src" => %rel.display(),
                "dest" => %dest.display());
        }
    }

    /*
     * Write to both stdout and stderr with distinct markers.  These use
     * raw println!/eprintln! intentionally — the factory pipes stdout and
     * stderr separately, and the e2e tests verify that events arrive on
     * the correct stream.  slog only writes to one fd, so we need the
     * raw writes here to cover both.
     */
    println!("stub: stdout marker for event streaming verification");
    eprintln!("stub: stderr marker for event streaming verification");

    if exit_code == 0 {
        info!(log, "exiting"; "code" => exit_code);
    } else {
        error!(log, "exiting"; "code" => exit_code);
    }

    Ok(())
}

/// Slow mode: heartbeat to stdout for the given duration.
fn run_slow(log: &Logger, env: &Env, duration: u64) -> Result<()> {
    info!(log, "slow mode, heartbeating"; "duration" => duration);

    // Write a minimal output so the job has something to upload.
    let heartbeat_log = env.output_dir.join("result.log");
    fs::write(&heartbeat_log, "slow mode heartbeat log\n")?;

    for i in 0..duration {
        info!(log, "heartbeat"; "tick" => i + 1, "of" => duration);
        std::thread::sleep(std::time::Duration::from_secs(1));
    }

    info!(log, "slow mode complete");
    Ok(())
}

/// Large output mode: write a file that exceeds the upload chunk size.
fn run_large_output(log: &Logger, env: &Env) -> Result<()> {
    let size = buildomat_common::UPLOAD_CHUNK_SIZE + 1024 * 1024;
    let large_file = env.output_dir.join("large-output.bin");

    info!(log, "writing large output";
        "size" => size,
        "chunk_size" => buildomat_common::UPLOAD_CHUNK_SIZE);
    let data = vec![0xABu8; size];
    fs::write(&large_file, &data)?;
    info!(log, "wrote output"; "path" => %large_file.display());

    // Also write result.log for require_match rules.
    let result_log = env.output_dir.join("result.log");
    fs::write(&result_log, "large-output mode\n")?;

    info!(log, "large-output mode complete");
    Ok(())
}
