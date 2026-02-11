/*
 * Copyright 2026 Oxide Computer Company
 */

use std::process::ExitStatus;
use std::time::Duration;

use slog::{info, warn, Logger};
use tokio::process::Child;

/// How long to wait for a child process to exit after SIGTERM before
/// escalating to SIGKILL. Matches the agent's behavior in
/// `agent/src/exec.rs`.
pub(crate) const GRACEFUL_KILL_TIMEOUT: Duration = Duration::from_secs(10);

/// Send SIGTERM to a child process, wait for it to exit, escalate to
/// SIGKILL if it does not exit within the timeout.
///
/// Returns the exit status if the process was still running, or the
/// already-reaped status if the process had already exited.
pub(crate) async fn graceful_kill(
    log: &Logger,
    child: &mut Child,
    timeout: Duration,
) -> Option<ExitStatus> {
    let pid = match child.id() {
        Some(pid) => pid,
        None => {
            // Process already reaped — try_wait to get the status.
            return child.try_wait().ok().flatten();
        }
    };

    // Send SIGTERM.
    // SAFETY: pid is a valid child process ID obtained from child.id().
    let ret = unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) };
    if ret != 0 {
        warn!(log, "failed to send SIGTERM to pid {}", pid;
            "errno" => std::io::Error::last_os_error().to_string());
    } else {
        info!(log, "sent SIGTERM to pid {}", pid);
    }

    // Wait for exit, with timeout.
    match tokio::time::timeout(timeout, child.wait()).await {
        Ok(Ok(status)) => {
            info!(log, "child exited after SIGTERM";
                "status" => %format_exit_status(&status));
            Some(status)
        }
        Ok(Err(e)) => {
            warn!(log, "error waiting for child after SIGTERM";
                "error" => %e);
            None
        }
        Err(_) => {
            warn!(log, "child did not exit within {:?}, sending SIGKILL",
                timeout; "pid" => pid);
            if let Err(e) = child.kill().await {
                warn!(log, "failed to SIGKILL child"; "error" => %e);
            }
            match child.wait().await {
                Ok(status) => Some(status),
                Err(e) => {
                    warn!(log, "error waiting for child after SIGKILL";
                        "error" => %e);
                    None
                }
            }
        }
    }
}

/// Format an exit status for logging.
pub(crate) fn format_exit_status(status: &ExitStatus) -> String {
    if let Some(code) = status.code() {
        format!("exit code {}", code)
    } else {
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            if let Some(signal) = status.signal() {
                return format!("killed by signal {}", signal);
            }
        }
        "unknown".to_string()
    }
}
