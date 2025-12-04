/*
 * Copyright 2025 Oxide Computer Company
 */

//! SP-Runner HTTP Client Library
//!
//! Provides a client for interacting with the sp-runner buildomat-service API.
//! This library enables programmatic access to sp-runner's hardware testing
//! capabilities, including job submission, status polling, and result retrieval.
//!
//! # Architecture
//!
//! The sp-runner service runs on testbed hosts and provides an HTTP API for
//! dispatching firmware tests to connected hardware. This client library
//! abstracts the HTTP protocol into a simple async Rust API.
//!
//! ```text
//! ┌─────────────────┐     HTTP      ┌──────────────────┐
//! │  Factory/CLI    │──────────────▶│  sp-runner       │
//! │  (this client)  │◀──────────────│  buildomat-svc   │
//! └─────────────────┘               └────────┬─────────┘
//!                                            │
//!                                            ▼
//!                                   ┌──────────────────┐
//!                                   │   Hardware       │
//!                                   │   (SP/ROT)       │
//!                                   └──────────────────┘
//! ```
//!
//! # Example
//!
//! ```no_run
//! use sp_runner_client::{Client, ClientBuilder, JobSubmitRequest};
//! use std::time::Duration;
//!
//! # async fn example() -> anyhow::Result<()> {
//! // Create client
//! let client = ClientBuilder::new("http://localhost:9090").build()?;
//!
//! // Check service health
//! let health = client.health().await?;
//! println!("Service status: {}, testbeds available: {}",
//!     health.status, health.testbeds_available);
//!
//! // Submit a test job
//! let job_id = client.submit_job(&JobSubmitRequest {
//!     testbed: "grapefruit-7f495641".to_string(),
//!     input_dir: "/work/input".to_string(),
//!     output_dir: "/work/output".to_string(),
//!     baseline: "v16".to_string(),
//!     ..Default::default()
//! }).await?;
//! println!("Job submitted: {}", job_id);
//!
//! // Poll until complete
//! loop {
//!     let status = client.get_status(&job_id).await?;
//!     println!("Status: {} - {}", status.status, status.progress);
//!     if status.is_terminal() {
//!         break;
//!     }
//!     tokio::time::sleep(Duration::from_secs(5)).await;
//! }
//!
//! // Get results
//! let result = client.get_result(&job_id).await?;
//! println!("Outcome: {}, duration: {}s", result.outcome, result.duration_secs);
//! # Ok(())
//! # }
//! ```
//!
//! # Error Handling
//!
//! All client methods return `anyhow::Result`. Errors can occur due to:
//! - Network failures (connection refused, timeout)
//! - HTTP errors (4xx, 5xx responses)
//! - JSON parsing failures
//!
//! The client includes the HTTP status code and response body in error messages
//! for easier debugging.

use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

/// Builder for constructing [`Client`] instances.
///
/// Follows the builder pattern used by `buildomat_client::ClientBuilder`.
///
/// # Example
///
/// ```
/// use sp_runner_client::ClientBuilder;
///
/// let client = ClientBuilder::new("http://localhost:9090")
///     .timeout(std::time::Duration::from_secs(300))
///     .build()
///     .expect("Failed to build client");
/// ```
pub struct ClientBuilder {
    url: String,
    timeout: Duration,
}

impl ClientBuilder {
    /// Create a new client builder with the given base URL.
    ///
    /// # Arguments
    ///
    /// * `url` - Base URL of the sp-runner service (e.g., "http://localhost:9090")
    ///
    /// # Example
    ///
    /// ```
    /// use sp_runner_client::ClientBuilder;
    ///
    /// let builder = ClientBuilder::new("http://localhost:9090");
    /// ```
    pub fn new(url: &str) -> Self {
        ClientBuilder {
            url: url.trim_end_matches('/').to_string(),
            timeout: Duration::from_secs(3600), // 1 hour default for long tests
        }
    }

    /// Set the request timeout.
    ///
    /// Default is 1 hour to accommodate long-running hardware tests.
    ///
    /// # Example
    ///
    /// ```
    /// use sp_runner_client::ClientBuilder;
    /// use std::time::Duration;
    ///
    /// let builder = ClientBuilder::new("http://localhost:9090")
    ///     .timeout(Duration::from_secs(7200)); // 2 hours
    /// ```
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Build the client.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client cannot be constructed.
    ///
    /// # Example
    ///
    /// ```
    /// use sp_runner_client::ClientBuilder;
    ///
    /// let client = ClientBuilder::new("http://localhost:9090")
    ///     .build()
    ///     .expect("Failed to build client");
    /// ```
    pub fn build(self) -> Result<Client> {
        let http = reqwest::Client::builder()
            .timeout(self.timeout)
            .connect_timeout(Duration::from_secs(15))
            .build()
            .context("failed to build HTTP client")?;

        Ok(Client { url: self.url, http })
    }
}

/// HTTP client for sp-runner buildomat-service API.
///
/// Provides methods to submit jobs, poll status, and retrieve results.
/// All methods are async and return `anyhow::Result`.
///
/// # Thread Safety
///
/// The client is `Clone` and can be shared across threads. Each clone
/// shares the underlying HTTP connection pool.
///
/// # Example
///
/// ```no_run
/// use sp_runner_client::{Client, ClientBuilder, JobSubmitRequest};
///
/// # async fn example() -> anyhow::Result<()> {
/// let client = ClientBuilder::new("http://localhost:9090").build()?;
///
/// // List available testbeds
/// let testbeds = client.list_testbeds().await?;
/// for tb in &testbeds.testbeds {
///     println!("{}: {} ({})", tb.name, tb.board, tb.status);
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct Client {
    url: String,
    http: reqwest::Client,
}

impl Client {
    /// Check service health.
    ///
    /// Returns the service status, version, and availability information.
    /// Use this to verify the service is running before submitting jobs.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use sp_runner_client::ClientBuilder;
    ///
    /// # async fn example() -> anyhow::Result<()> {
    /// let client = ClientBuilder::new("http://localhost:9090").build()?;
    /// let health = client.health().await?;
    ///
    /// if health.status == "healthy" && health.testbeds_available > 0 {
    ///     println!("Service ready to accept jobs");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn health(&self) -> Result<HealthResponse> {
        let url = format!("{}/health", self.url);
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .context("health check request failed")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            bail!("health check failed: {} - {}", status, body);
        }

        resp.json().await.context("failed to parse health response")
    }

    /// List all testbeds and their status.
    ///
    /// Returns information about all configured testbeds including their
    /// availability, hardware type, and any running jobs.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use sp_runner_client::ClientBuilder;
    ///
    /// # async fn example() -> anyhow::Result<()> {
    /// let client = ClientBuilder::new("http://localhost:9090").build()?;
    /// let testbeds = client.list_testbeds().await?;
    ///
    /// // Find available testbeds
    /// let available: Vec<_> = testbeds.testbeds.iter()
    ///     .filter(|t| t.status == "available")
    ///     .collect();
    /// println!("{} testbeds available", available.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_testbeds(&self) -> Result<TestbedsResponse> {
        let url = format!("{}/testbeds", self.url);
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .context("list testbeds request failed")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            bail!("list testbeds failed: {} - {}", status, body);
        }

        resp.json().await.context("failed to parse testbeds response")
    }

    /// Submit a test job.
    ///
    /// Submits a hardware test job for execution on the specified testbed.
    /// The job is validated and queued, then executed asynchronously.
    /// Returns immediately with a job ID that can be used to track progress.
    ///
    /// # Arguments
    ///
    /// * `request` - Job submission parameters including testbed, directories, and options
    ///
    /// # Returns
    ///
    /// The job ID (UUID) that can be used with [`get_status`](Client::get_status)
    /// and [`get_result`](Client::get_result).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The testbed doesn't exist or isn't available
    /// - The input directory doesn't exist
    /// - Network or server errors occur
    ///
    /// # Example
    ///
    /// ```no_run
    /// use sp_runner_client::{ClientBuilder, JobSubmitRequest};
    ///
    /// # async fn example() -> anyhow::Result<()> {
    /// let client = ClientBuilder::new("http://localhost:9090").build()?;
    ///
    /// let job_id = client.submit_job(&JobSubmitRequest {
    ///     testbed: "grapefruit-7f495641".to_string(),
    ///     input_dir: "/work/input".to_string(),
    ///     output_dir: "/work/output".to_string(),
    ///     baseline: "v16".to_string(),
    ///     test_type: "update-rollback".to_string(),
    ///     disable_watchdog: false,
    /// }).await?;
    ///
    /// println!("Submitted job: {}", job_id);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn submit_job(
        &self,
        request: &JobSubmitRequest,
    ) -> Result<String> {
        let url = format!("{}/job", self.url);
        let resp = self
            .http
            .post(&url)
            .json(request)
            .send()
            .await
            .context("submit job request failed")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            bail!("submit job failed: {} - {}", status, body);
        }

        let result: JobSubmitResponse =
            resp.json().await.context("failed to parse submit response")?;
        Ok(result.job_id)
    }

    /// Get current job status.
    ///
    /// Returns the current state of a job including progress information.
    /// Poll this endpoint periodically to track job execution.
    ///
    /// # Arguments
    ///
    /// * `job_id` - The job ID returned from [`submit_job`](Client::submit_job)
    ///
    /// # Example
    ///
    /// ```no_run
    /// use sp_runner_client::ClientBuilder;
    /// use std::time::Duration;
    ///
    /// # async fn example() -> anyhow::Result<()> {
    /// let client = ClientBuilder::new("http://localhost:9090").build()?;
    /// let job_id = "550e8400-e29b-41d4-a716-446655440000";
    ///
    /// // Poll until job completes
    /// loop {
    ///     let status = client.get_status(job_id).await?;
    ///     println!("{}: {}", status.status, status.progress);
    ///
    ///     if status.is_terminal() {
    ///         break;
    ///     }
    ///     tokio::time::sleep(Duration::from_secs(5)).await;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_status(&self, job_id: &str) -> Result<JobStatusResponse> {
        let url = format!("{}/job/{}/status", self.url, job_id);
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .context("get status request failed")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            bail!("get status failed: {} - {}", status, body);
        }

        resp.json().await.context("failed to parse status response")
    }

    /// Get job result.
    ///
    /// Returns the result of a completed job including test outcome and
    /// paths to detailed output files. Only call this after the job has
    /// reached a terminal state (completed or failed).
    ///
    /// # Arguments
    ///
    /// * `job_id` - The job ID returned from [`submit_job`](Client::submit_job)
    ///
    /// # Errors
    ///
    /// Returns an error if the job is still running or doesn't exist.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use sp_runner_client::ClientBuilder;
    ///
    /// # async fn example() -> anyhow::Result<()> {
    /// let client = ClientBuilder::new("http://localhost:9090").build()?;
    /// let job_id = "550e8400-e29b-41d4-a716-446655440000";
    ///
    /// let result = client.get_result(job_id).await?;
    /// println!("Outcome: {}", result.outcome);
    /// println!("Duration: {}s", result.duration_secs);
    /// println!("Tests run: {}", result.tests_run);
    ///
    /// if let Some(report) = result.report_path {
    ///     println!("Full report at: {}", report);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_result(&self, job_id: &str) -> Result<JobResultResponse> {
        let url = format!("{}/job/{}/result", self.url, job_id);
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .context("get result request failed")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            bail!("get result failed: {} - {}", status, body);
        }

        resp.json().await.context("failed to parse result response")
    }

    /// Trigger testbed recovery.
    ///
    /// Initiates recovery procedures for a testbed that is in a failed state.
    /// Recovery resets the testbed to a known-good state.
    ///
    /// # Arguments
    ///
    /// * `testbed` - Name of the testbed to recover
    /// * `force` - If true, attempt recovery even if testbed appears healthy
    ///
    /// # Example
    ///
    /// ```no_run
    /// use sp_runner_client::ClientBuilder;
    ///
    /// # async fn example() -> anyhow::Result<()> {
    /// let client = ClientBuilder::new("http://localhost:9090").build()?;
    ///
    /// let result = client.recover_testbed("grapefruit-7f495641", false).await?;
    /// println!("Recovery {}: {}", result.status, result.message);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn recover_testbed(
        &self,
        testbed: &str,
        force: bool,
    ) -> Result<RecoverResponse> {
        let url = format!("{}/testbed/{}/recover", self.url, testbed);
        let request = RecoverRequest { force };

        let resp = self
            .http
            .post(&url)
            .json(&request)
            .send()
            .await
            .context("recover testbed request failed")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            bail!("recover testbed failed: {} - {}", status, body);
        }

        resp.json().await.context("failed to parse recover response")
    }
}

// ============================================================================
// Request/Response Types
// ============================================================================

/// Request to submit a new test job.
///
/// All fields except optional ones are required for job submission.
///
/// # Example
///
/// ```
/// use sp_runner_client::JobSubmitRequest;
///
/// let request = JobSubmitRequest {
///     testbed: "grapefruit-7f495641".to_string(),
///     input_dir: "/work/input".to_string(),
///     output_dir: "/work/output".to_string(),
///     baseline: "v16".to_string(),
///     ..Default::default()
/// };
///
/// assert_eq!(request.test_type, "update-rollback");
/// assert!(!request.disable_watchdog);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobSubmitRequest {
    /// Name of the testbed to use (e.g., "grapefruit-7f495641").
    pub testbed: String,

    /// Directory containing firmware artifacts to test.
    pub input_dir: String,

    /// Directory for test outputs (reports, logs).
    pub output_dir: String,

    /// Baseline firmware version (e.g., "v16").
    pub baseline: String,

    /// Test type to execute (default: "update-rollback").
    #[serde(default = "default_test_type")]
    pub test_type: String,

    /// Disable watchdog timer during tests.
    #[serde(default)]
    pub disable_watchdog: bool,
}

fn default_test_type() -> String {
    "update-rollback".to_string()
}

impl Default for JobSubmitRequest {
    fn default() -> Self {
        JobSubmitRequest {
            testbed: String::new(),
            input_dir: String::new(),
            output_dir: String::new(),
            baseline: String::new(),
            test_type: default_test_type(),
            disable_watchdog: false,
        }
    }
}

/// Response from job submission.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobSubmitResponse {
    /// Unique job identifier (UUID).
    pub job_id: String,
}

/// Job status values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    /// Job is queued but not yet started.
    Pending,
    /// Job is currently running.
    Running,
    /// Job completed successfully (tests may have passed or failed).
    Completed,
    /// Job failed due to infrastructure error.
    Failed,
}

impl JobStatus {
    /// Returns true if the job is still active (pending or running).
    ///
    /// # Example
    ///
    /// ```
    /// use sp_runner_client::JobStatus;
    ///
    /// assert!(JobStatus::Pending.is_active());
    /// assert!(JobStatus::Running.is_active());
    /// assert!(!JobStatus::Completed.is_active());
    /// ```
    pub fn is_active(&self) -> bool {
        matches!(self, JobStatus::Pending | JobStatus::Running)
    }

    /// Returns true if the job has reached a terminal state.
    ///
    /// # Example
    ///
    /// ```
    /// use sp_runner_client::JobStatus;
    ///
    /// assert!(!JobStatus::Pending.is_terminal());
    /// assert!(JobStatus::Completed.is_terminal());
    /// assert!(JobStatus::Failed.is_terminal());
    /// ```
    pub fn is_terminal(&self) -> bool {
        matches!(self, JobStatus::Completed | JobStatus::Failed)
    }
}

impl std::fmt::Display for JobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobStatus::Pending => write!(f, "pending"),
            JobStatus::Running => write!(f, "running"),
            JobStatus::Completed => write!(f, "completed"),
            JobStatus::Failed => write!(f, "failed"),
        }
    }
}

/// Response containing job status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobStatusResponse {
    /// Current job status.
    pub status: JobStatus,

    /// Human-readable progress message.
    pub progress: String,

    /// Testbed being used for this job.
    pub testbed: String,

    /// Error message if job failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl JobStatusResponse {
    /// Returns true if the job has reached a terminal state.
    ///
    /// Convenience method equivalent to `self.status.is_terminal()`.
    ///
    /// # Example
    ///
    /// ```
    /// use sp_runner_client::{JobStatus, JobStatusResponse};
    ///
    /// let status = JobStatusResponse {
    ///     status: JobStatus::Running,
    ///     progress: "Testing...".to_string(),
    ///     testbed: "test".to_string(),
    ///     error: None,
    /// };
    /// assert!(!status.is_terminal());
    /// ```
    pub fn is_terminal(&self) -> bool {
        self.status.is_terminal()
    }
}

/// Response containing job results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobResultResponse {
    /// Test outcome: "passed", "failed", or "error".
    pub outcome: String,

    /// Path to the detailed test report JSON file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub report_path: Option<String>,

    /// Path to the test summary JSON file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary_path: Option<String>,

    /// Path to the test log file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_path: Option<String>,

    /// Test duration in seconds.
    pub duration_secs: u64,

    /// Number of test steps executed.
    pub tests_run: u32,

    /// Error message if outcome is "error".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl JobResultResponse {
    /// Check if the job result indicates success.
    ///
    /// # Example
    ///
    /// ```
    /// use sp_runner_client::JobResultResponse;
    ///
    /// let result = JobResultResponse {
    ///     outcome: "passed".to_string(),
    ///     report_path: None,
    ///     summary_path: None,
    ///     log_path: None,
    ///     duration_secs: 60,
    ///     tests_run: 10,
    ///     error: None,
    /// };
    /// assert!(result.is_success());
    /// ```
    pub fn is_success(&self) -> bool {
        self.outcome == "passed"
    }
}

/// Information about a testbed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestbedInfo {
    /// Testbed name (unique identifier).
    pub name: String,

    /// Board type (e.g., "grapefruit", "gimlet-e").
    pub board: String,

    /// Current status: "available", "busy", "offline", "failed-needs-recovery".
    pub status: String,

    /// SP target name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sp_target: Option<String>,

    /// ROT target name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rot_target: Option<String>,

    /// Currently running job ID, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_job: Option<String>,
}

impl TestbedInfo {
    /// Check if the testbed is available for new jobs.
    pub fn is_available(&self) -> bool {
        self.status == "available"
    }
}

/// Response containing list of testbeds.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestbedsResponse {
    /// List of testbeds.
    pub testbeds: Vec<TestbedInfo>,
}

impl TestbedsResponse {
    /// Iterate over available testbeds.
    pub fn available(&self) -> impl Iterator<Item = &TestbedInfo> {
        self.testbeds.iter().filter(|t| t.is_available())
    }

    /// Find a testbed by name.
    pub fn find(&self, name: &str) -> Option<&TestbedInfo> {
        self.testbeds.iter().find(|t| t.name == name)
    }
}

/// Service health response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    /// Service status: "healthy" or "degraded".
    pub status: String,

    /// Service version.
    pub version: String,

    /// Number of testbeds currently available.
    pub testbeds_available: usize,

    /// Number of jobs currently running.
    pub jobs_running: usize,
}

impl HealthResponse {
    /// Check if the service is healthy.
    pub fn is_healthy(&self) -> bool {
        self.status == "healthy"
    }
}

/// Request to recover a testbed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoverRequest {
    /// Force recovery even if testbed appears healthy.
    #[serde(default)]
    pub force: bool,
}

/// Response from testbed recovery operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoverResponse {
    /// Recovery status: "started", "completed", or "not_needed".
    pub status: String,

    /// Human-readable message.
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_job_status_predicates() {
        assert!(JobStatus::Pending.is_active());
        assert!(JobStatus::Running.is_active());
        assert!(!JobStatus::Completed.is_active());
        assert!(!JobStatus::Failed.is_active());

        assert!(!JobStatus::Pending.is_terminal());
        assert!(!JobStatus::Running.is_terminal());
        assert!(JobStatus::Completed.is_terminal());
        assert!(JobStatus::Failed.is_terminal());
    }

    #[test]
    fn test_job_status_display() {
        assert_eq!(JobStatus::Pending.to_string(), "pending");
        assert_eq!(JobStatus::Running.to_string(), "running");
        assert_eq!(JobStatus::Completed.to_string(), "completed");
        assert_eq!(JobStatus::Failed.to_string(), "failed");
    }

    #[test]
    fn test_job_submit_request_default() {
        let req = JobSubmitRequest::default();
        assert_eq!(req.test_type, "update-rollback");
        assert!(!req.disable_watchdog);
    }

    #[test]
    fn test_job_submit_request_serialization() {
        let req = JobSubmitRequest {
            testbed: "test-tb".to_string(),
            input_dir: "/in".to_string(),
            output_dir: "/out".to_string(),
            baseline: "v16".to_string(),
            test_type: "update-rollback".to_string(),
            disable_watchdog: false,
        };

        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("test-tb"));
        assert!(json.contains("update-rollback"));
    }

    #[test]
    fn test_job_result_is_success() {
        let passed = JobResultResponse {
            outcome: "passed".to_string(),
            report_path: None,
            summary_path: None,
            log_path: None,
            duration_secs: 60,
            tests_run: 10,
            error: None,
        };
        assert!(passed.is_success());

        let failed = JobResultResponse {
            outcome: "failed".to_string(),
            ..passed.clone()
        };
        assert!(!failed.is_success());
    }

    #[test]
    fn test_testbed_info_is_available() {
        let available = TestbedInfo {
            name: "tb".to_string(),
            board: "grapefruit".to_string(),
            status: "available".to_string(),
            sp_target: None,
            rot_target: None,
            current_job: None,
        };
        assert!(available.is_available());

        let busy =
            TestbedInfo { status: "busy".to_string(), ..available.clone() };
        assert!(!busy.is_available());
    }

    #[test]
    fn test_testbeds_response_helpers() {
        let response = TestbedsResponse {
            testbeds: vec![
                TestbedInfo {
                    name: "tb1".to_string(),
                    board: "grapefruit".to_string(),
                    status: "available".to_string(),
                    sp_target: None,
                    rot_target: None,
                    current_job: None,
                },
                TestbedInfo {
                    name: "tb2".to_string(),
                    board: "gimlet".to_string(),
                    status: "busy".to_string(),
                    sp_target: None,
                    rot_target: None,
                    current_job: Some("job-1".to_string()),
                },
            ],
        };

        assert_eq!(response.available().count(), 1);
        assert!(response.find("tb1").is_some());
        assert!(response.find("tb2").is_some());
        assert!(response.find("nonexistent").is_none());
    }

    #[test]
    fn test_health_response_is_healthy() {
        let healthy = HealthResponse {
            status: "healthy".to_string(),
            version: "0.1.0".to_string(),
            testbeds_available: 1,
            jobs_running: 0,
        };
        assert!(healthy.is_healthy());

        let degraded =
            HealthResponse { status: "degraded".to_string(), ..healthy };
        assert!(!degraded.is_healthy());
    }
}
