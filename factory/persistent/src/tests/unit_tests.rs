use std::path::PathBuf;
use std::time::Duration;

use tokio::process::Command;

use crate::factory::JobPaths;
use crate::process::{format_exit_status, graceful_kill};
use crate::test_support::test_logger;

#[test]
fn format_exit_status_code() {
    use std::process::Command;

    // Test with a successful command
    let output = Command::new("true").status().unwrap();
    assert_eq!(format_exit_status(&output), "exit code 0");

    // Test with a failing command
    let output = Command::new("false").status().unwrap();
    assert_eq!(format_exit_status(&output), "exit code 1");
}

#[test]
fn job_paths_structure() {
    let base = PathBuf::from("/var/lib/buildomat/jobs");
    let paths = JobPaths::new(&base, "job-123");

    assert_eq!(paths.root, PathBuf::from("/var/lib/buildomat/jobs/job-123"));
    assert_eq!(
        paths.work,
        PathBuf::from("/var/lib/buildomat/jobs/job-123/work")
    );
    assert_eq!(
        paths.artifacts,
        PathBuf::from("/var/lib/buildomat/jobs/job-123/artifacts")
    );
    assert_eq!(
        paths.output,
        PathBuf::from("/var/lib/buildomat/jobs/job-123/output")
    );
}

#[test]
fn job_paths_create_and_remove() {
    let temp = tempfile::TempDir::new().unwrap();

    let paths = JobPaths::new(temp.path(), "test-job");

    // Create directories
    paths.create_all().unwrap();
    assert!(paths.root.exists());
    assert!(paths.work.exists());
    assert!(paths.artifacts.exists());
    assert!(paths.output.exists());

    // Remove directories
    paths.remove_all().unwrap();
    assert!(!paths.root.exists());
}

// ---------------------------------------------------------------
// graceful_kill unit tests
// ---------------------------------------------------------------

/// Test that graceful_kill sends SIGTERM and the process exits promptly.
/// `sleep` handles SIGTERM by default, so it exits immediately.
#[tokio::test]
async fn test_graceful_kill_sigterm_respected() {
    let log = test_logger();
    let mut child = Command::new("sleep").arg("60").spawn().unwrap();

    let start = std::time::Instant::now();
    let status = graceful_kill(&log, &mut child, Duration::from_secs(5)).await;

    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(2),
        "should exit quickly after SIGTERM, took {:?}",
        elapsed
    );
    // Process was killed by signal — no exit code.
    let status = status.expect("should have exit status");
    assert!(
        !status.success(),
        "process killed by signal should not be success"
    );
}

/// Test that graceful_kill escalates to SIGKILL when the process
/// ignores SIGTERM.
#[tokio::test]
async fn test_graceful_kill_escalates_to_sigkill() {
    let log = test_logger();
    // This bash process traps (ignores) SIGTERM and sleeps forever.
    let mut child = Command::new("bash")
        .args(["-c", "trap '' TERM; sleep 60"])
        .spawn()
        .unwrap();

    let start = std::time::Instant::now();
    let status =
        graceful_kill(&log, &mut child, Duration::from_millis(200)).await;

    let elapsed = start.elapsed();
    // Should take roughly 200ms (the timeout) plus a little overhead.
    assert!(
        elapsed < Duration::from_secs(2),
        "should exit after SIGKILL timeout, took {:?}",
        elapsed
    );
    let status = status.expect("should have exit status");
    assert!(
        !status.success(),
        "process killed by SIGKILL should not be success"
    );
}

/// Test that graceful_kill handles an already-exited process.
#[tokio::test]
async fn test_graceful_kill_already_exited() {
    let log = test_logger();
    let mut child = Command::new("true").spawn().unwrap();

    // Wait for it to finish naturally.
    let _ = child.wait().await.unwrap();

    // Now call graceful_kill — should not hang.
    let status = graceful_kill(&log, &mut child, Duration::from_secs(5)).await;

    let status = status.expect("should have exit status");
    assert!(status.success(), "true should exit with code 0");
}
