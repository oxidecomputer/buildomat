use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::{Method, Request, Response, StatusCode};

use crate::config::*;
use crate::factory::*;
use crate::test_support::{json_response, start_mock_server, test_logger};

/// Tracks all requests the mock server receives during an e2e test.
#[derive(Default)]
struct E2eMockState {
    /// Ordered log of (method, path) for every request.
    requests: Vec<(String, String)>,
    /// Input artifact data keyed by input id.
    input_data: HashMap<String, Vec<u8>>,
    /// Uploaded chunk bodies in order.
    chunk_uploads: Vec<Vec<u8>>,
    /// Counter for chunk IDs.
    next_chunk_id: u32,
    /// Recorded add_output request bodies.
    add_output_requests: Vec<serde_json::Value>,
    /// Recorded worker_job_complete request bodies.
    complete_requests: Vec<serde_json::Value>,
    /// Recorded factory_worker_associate request bodies.
    associate_requests: Vec<serde_json::Value>,
    /// Recorded worker_job_append event batches.
    append_requests: Vec<Vec<serde_json::Value>>,
    /// Factory metadata to return in worker_ping (for diagnostic scripts).
    factory_metadata: Option<serde_json::Value>,
    /// Recorded diagnostics_enable calls.
    diagnostics_enable_count: u32,
    /// Recorded diagnostics_complete request bodies.
    diagnostics_complete_requests: Vec<serde_json::Value>,
    /// Whether the command has been started (controls factory_worker_get
    /// returning recycle=false so the factory keeps polling).
    lease_returned: bool,
    /// When true, factory_worker_get returns recycle=true.
    force_recycle: bool,
    /// When true, worker_bootstrap returns 500.
    fail_bootstrap: bool,
}

type E2eState = Arc<Mutex<E2eMockState>>;

/// Handle HTTP requests for the full buildomat server mock.
async fn e2e_handler(
    req: Request<Incoming>,
    state: E2eState,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    use http_body_util::BodyExt;

    let method = req.method().clone();
    let path = req.uri().path().to_string();

    // Record every request
    {
        let mut st = state.lock().unwrap();
        st.requests.push((method.to_string(), path.clone()));
    }

    // --- Factory APIs ---

    // GET /0/factory/ping
    if method == Method::GET && path == "/0/factory/ping" {
        return json_response(StatusCode::OK, serde_json::json!({"ok": true}));
    }

    // POST /0/factory/lease
    if method == Method::POST && path == "/0/factory/lease" {
        let mut st = state.lock().unwrap();
        if !st.lease_returned {
            st.lease_returned = true;
            return json_response(
                StatusCode::OK,
                serde_json::json!({
                    "lease": {
                        "job": "test-job-001",
                        "target": "test-target"
                    }
                }),
            );
        }
        // No more leases after the first
        return json_response(
            StatusCode::OK,
            serde_json::json!({"lease": null}),
        );
    }

    // POST /0/factory/worker (create)
    if method == Method::POST && path == "/0/factory/worker" {
        return json_response(
            StatusCode::CREATED,
            serde_json::json!({
                "bootstrap": "bootstrap-token-abc",
                "hold": false,
                "id": "worker-001",
                "online": false,
                "private": null,
                "recycle": false,
                "target": "test-target"
            }),
        );
    }

    // PATCH /0/factory/worker/{worker} (associate)
    if method == Method::PATCH && path.starts_with("/0/factory/worker/") {
        let body_bytes =
            req.into_body().collect().await.unwrap().to_bytes().to_vec();
        let body_json: serde_json::Value =
            serde_json::from_slice(&body_bytes).unwrap();
        state.lock().unwrap().associate_requests.push(body_json);
        return Ok(Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(Full::new(Bytes::new()))
            .unwrap());
    }

    // GET /0/factory/worker/{worker}
    if method == Method::GET && path.starts_with("/0/factory/worker/") {
        let recycle = state.lock().unwrap().force_recycle;
        return json_response(
            StatusCode::OK,
            serde_json::json!({
                "worker": {
                    "bootstrap": "bootstrap-token-abc",
                    "hold": false,
                    "id": "worker-001",
                    "online": true,
                    "private": "persistent-worker-001",
                    "recycle": recycle,
                    "target": "test-target"
                }
            }),
        );
    }

    // DELETE /0/factory/worker/{worker} (destroy)
    if method == Method::DELETE && path.starts_with("/0/factory/worker/") {
        return json_response(StatusCode::OK, serde_json::json!(true));
    }

    // --- Worker APIs ---

    // POST /0/worker/bootstrap
    if method == Method::POST && path == "/0/worker/bootstrap" {
        if state.lock().unwrap().fail_bootstrap {
            return json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                serde_json::json!({"message": "injected bootstrap failure"}),
            );
        }
        return json_response(
            StatusCode::CREATED,
            serde_json::json!({"id": "worker-001"}),
        );
    }

    // GET /0/worker/ping
    if method == Method::GET && path == "/0/worker/ping" {
        let st = state.lock().unwrap();
        let has_input = !st.input_data.is_empty();
        let mut inputs = Vec::new();
        if has_input {
            for key in st.input_data.keys() {
                inputs.push(serde_json::json!({
                    "id": key,
                    "name": format!("{}.txt", key),
                }));
            }
        }
        let factory_metadata = st.factory_metadata.clone();
        return json_response(
            StatusCode::OK,
            serde_json::json!({
                "poweroff": false,
                "factory_info": null,
                "factory_metadata": factory_metadata,
                "job": {
                    "id": "test-job-001",
                    "name": "e2e-test-job",
                    "inputs": inputs,
                    "output_rules": [{
                        "rule": "*",
                        "require_match": false,
                        "ignore": false,
                        "size_change_ok": false,
                    }],
                    "tasks": []
                }
            }),
        );
    }

    // GET /0/worker/job/{job}/inputs/{input} -> download
    if method == Method::GET && path.contains("/inputs/") {
        let parts: Vec<&str> = path.split('/').collect();
        let input_id = parts.last().unwrap_or(&"");
        let st = state.lock().unwrap();
        if let Some(data) = st.input_data.get(*input_id) {
            return Ok(Response::builder()
                .status(StatusCode::OK)
                .body(Full::new(Bytes::from(data.clone())))
                .unwrap());
        }
        return Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Full::new(Bytes::from("not found")))
            .unwrap());
    }

    // POST /0/worker/job/{job}/chunk -> upload chunk
    if method == Method::POST && path.ends_with("/chunk") {
        let body_bytes =
            req.into_body().collect().await.unwrap().to_bytes().to_vec();
        let mut st = state.lock().unwrap();
        st.chunk_uploads.push(body_bytes);
        let id = format!("chunk-{:04}", st.next_chunk_id);
        st.next_chunk_id += 1;
        return json_response(
            StatusCode::CREATED,
            serde_json::json!({"id": id}),
        );
    }

    // POST /1/worker/job/{job}/output -> add output
    if method == Method::POST && path.ends_with("/output") {
        let body_bytes =
            req.into_body().collect().await.unwrap().to_bytes().to_vec();
        let body_json: serde_json::Value =
            serde_json::from_slice(&body_bytes).unwrap();
        state.lock().unwrap().add_output_requests.push(body_json);
        return json_response(
            StatusCode::OK,
            serde_json::json!({"complete": true, "error": null}),
        );
    }

    // POST /0/worker/diagnostics/enable
    if method == Method::POST && path == "/0/worker/diagnostics/enable" {
        state.lock().unwrap().diagnostics_enable_count += 1;
        return Ok(Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(Full::new(Bytes::new()))
            .unwrap());
    }

    // POST /0/worker/diagnostics/complete
    if method == Method::POST && path == "/0/worker/diagnostics/complete" {
        let body_bytes =
            req.into_body().collect().await.unwrap().to_bytes().to_vec();
        let body_json: serde_json::Value =
            serde_json::from_slice(&body_bytes).unwrap();
        state.lock().unwrap().diagnostics_complete_requests.push(body_json);
        return Ok(Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(Full::new(Bytes::new()))
            .unwrap());
    }

    // POST /1/worker/job/{job}/append
    if method == Method::POST && path.ends_with("/append") {
        let body_bytes =
            req.into_body().collect().await.unwrap().to_bytes().to_vec();
        let body_json: Vec<serde_json::Value> =
            serde_json::from_slice(&body_bytes).unwrap();
        state.lock().unwrap().append_requests.push(body_json);
        return Ok(Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(Full::new(Bytes::new()))
            .unwrap());
    }

    // POST /0/worker/job/{job}/complete
    if method == Method::POST && path.ends_with("/complete") {
        let body_bytes =
            req.into_body().collect().await.unwrap().to_bytes().to_vec();
        let body_json: serde_json::Value =
            serde_json::from_slice(&body_bytes).unwrap();
        state.lock().unwrap().complete_requests.push(body_json);
        return Ok(Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(Full::new(Bytes::new()))
            .unwrap());
    }

    Ok(Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Full::new(Bytes::from(format!("unknown: {} {}", method, path))))
        .unwrap())
}

/// Start the e2e mock server.
async fn start_e2e_server() -> (String, E2eState) {
    let state: E2eState = Arc::new(Mutex::new(E2eMockState::default()));
    let url = start_mock_server(Arc::clone(&state), e2e_handler).await;
    (url, state)
}

// ---------------------------------------------------------------
// E2E factory_loop tests
// ---------------------------------------------------------------

/// End-to-end test: full factory_loop lifecycle with a successful command.
///
/// Exercises: lease → create worker → associate → bootstrap →
/// download inputs → spawn command → (command runs) →
/// upload outputs → worker_job_complete(failed=false) → destroy worker.
#[tokio::test]
async fn e2e_factory_loop_success() {
    let (url, state) = start_e2e_server().await;
    let temp = tempfile::TempDir::new().unwrap();
    let job_dir = temp.path().join("jobs");
    std::fs::create_dir_all(&job_dir).unwrap();

    // Provide an input artifact the command will copy to output
    state
        .lock()
        .unwrap()
        .input_data
        .insert("input-001".into(), b"hello from input".to_vec());

    let config = ConfigFile {
        general: ConfigFileGeneral { baseurl: url.clone() },
        factory: ConfigFileFactory { token: "factory-token".into() },
        execution: ConfigFileExecution {
            command: "bash".into(),
            args: vec![
                "-c".into(),
                // Copy input to output, proving both download and upload
                // work end-to-end.
                "cp \"$BUILDOMAT_ARTIFACT_DIR\"/input-001.txt \
                    \"$BUILDOMAT_OUTPUT_DIR\"/result.txt"
                    .into(),
            ],
            job_dir: job_dir.clone(),
        },
        target: {
            let mut m = HashMap::new();
            m.insert("test-target".into(), ConfigFileTarget {});
            m
        },
    };

    let client = buildomat_client::ClientBuilder::new(&url)
        .bearer_token("factory-token")
        .build()
        .unwrap();

    let mut central =
        Central { log: test_logger(), client, config, instances: Vec::new() };

    // Acquire lease, create worker, spawn command
    factory_loop(&mut central).await.unwrap();
    assert_eq!(central.instances.len(), 1, "should have one active instance");

    // Poll until the command completes.
    poll_until_done(&mut central, Duration::from_secs(10)).await;

    // Verify the job directory was cleaned up
    let job_path = job_dir.join("test-job-001");
    assert!(!job_path.exists(), "job directory should be removed");

    let st = state.lock().unwrap();

    // Verify worker_job_complete was called with failed=false
    assert_eq!(
        st.complete_requests.len(),
        1,
        "should have one complete request"
    );
    assert_eq!(
        st.complete_requests[0]["failed"], false,
        "job should be marked as succeeded"
    );

    // Verify output was uploaded (the copied input file)
    assert_eq!(
        st.add_output_requests.len(),
        1,
        "should have one output upload"
    );
    assert_eq!(st.add_output_requests[0]["path"], "result.txt");
    assert_eq!(
        st.add_output_requests[0]["size"],
        "hello from input".len() as u64
    );

    // Verify chunk data matches the input content
    assert_eq!(st.chunk_uploads.len(), 1);
    assert_eq!(st.chunk_uploads[0], b"hello from input");

    // Verify associate was called
    assert_eq!(st.associate_requests.len(), 1);
    assert_eq!(st.associate_requests[0]["private"], "persistent-worker-001");

    // Verify key API calls were made.
    // Check (method, path) pairs since some endpoints share paths.
    let has = |m: &str, pred: &dyn Fn(&str) -> bool| -> bool {
        st.requests.iter().any(|(method, path)| method == m && pred(path))
    };

    assert!(
        has("POST", &|p| p == "/0/factory/lease"),
        "should have called factory_lease"
    );
    assert!(
        has("POST", &|p| p == "/0/factory/worker"),
        "should have called factory_worker_create"
    );
    assert!(
        has("PATCH", &|p| p.starts_with("/0/factory/worker/")),
        "should have called factory_worker_associate"
    );
    assert!(
        has("POST", &|p| p == "/0/worker/bootstrap"),
        "should have called worker_bootstrap"
    );
    assert!(
        has("GET", &|p| p == "/0/worker/ping"),
        "should have called worker_ping"
    );
    assert!(
        has("GET", &|p| p.contains("/inputs/")),
        "should have downloaded input"
    );
    assert!(
        has("POST", &|p| p.ends_with("/complete")),
        "should have called worker_job_complete"
    );
    assert!(
        has("DELETE", &|p| p.starts_with("/0/factory/worker/")),
        "should have called factory_worker_destroy"
    );
}

/// End-to-end test: factory_loop with a failing command.
#[tokio::test]
async fn e2e_factory_loop_failure() {
    let (url, state) = start_e2e_server().await;
    let temp = tempfile::TempDir::new().unwrap();
    let job_dir = temp.path().join("jobs");
    std::fs::create_dir_all(&job_dir).unwrap();

    let config = ConfigFile {
        general: ConfigFileGeneral { baseurl: url.clone() },
        factory: ConfigFileFactory { token: "factory-token".into() },
        execution: ConfigFileExecution {
            command: "bash".into(),
            args: vec!["-c".into(), "exit 1".into()],
            job_dir: job_dir.clone(),
        },
        target: {
            let mut m = HashMap::new();
            m.insert("test-target".into(), ConfigFileTarget {});
            m
        },
    };

    let client = buildomat_client::ClientBuilder::new(&url)
        .bearer_token("factory-token")
        .build()
        .unwrap();

    let mut central =
        Central { log: test_logger(), client, config, instances: Vec::new() };

    // Acquire lease and spawn (failing) command
    factory_loop(&mut central).await.unwrap();
    assert_eq!(central.instances.len(), 1);

    // Poll until the command completes.
    poll_until_done(&mut central, Duration::from_secs(10)).await;

    let st = state.lock().unwrap();

    // Verify worker_job_complete was called with failed=true
    assert_eq!(st.complete_requests.len(), 1);
    assert_eq!(
        st.complete_requests[0]["failed"], true,
        "job should be marked as failed"
    );

    // No output files created, so no uploads
    assert_eq!(st.add_output_requests.len(), 0);
    assert_eq!(st.chunk_uploads.len(), 0);
}

/// End-to-end test: factory_loop returns early when no lease is available.
#[tokio::test]
async fn e2e_factory_loop_no_work() {
    let (url, state) = start_e2e_server().await;
    let temp = tempfile::TempDir::new().unwrap();

    // Mark lease as already returned so mock returns null
    state.lock().unwrap().lease_returned = true;

    let config = ConfigFile {
        general: ConfigFileGeneral { baseurl: url.clone() },
        factory: ConfigFileFactory { token: "factory-token".into() },
        execution: ConfigFileExecution {
            command: "true".into(),
            args: vec![],
            job_dir: temp.path().to_path_buf(),
        },
        target: {
            let mut m = HashMap::new();
            m.insert("test-target".into(), ConfigFileTarget {});
            m
        },
    };

    let client = buildomat_client::ClientBuilder::new(&url)
        .bearer_token("factory-token")
        .build()
        .unwrap();

    let mut central =
        Central { log: test_logger(), client, config, instances: Vec::new() };

    // Should return without creating any instances
    factory_loop(&mut central).await.unwrap();
    assert_eq!(central.instances.len(), 0);

    let st = state.lock().unwrap();
    // Should have called lease but nothing else
    let paths: Vec<&str> =
        st.requests.iter().map(|(_, p)| p.as_str()).collect();
    assert!(paths.contains(&"/0/factory/lease"));
    assert!(
        !paths.contains(&"/0/factory/worker"),
        "should not create worker when no lease"
    );
}

/// End-to-end test: event streaming sends stdout/stderr lines to the
/// server via worker_job_append.
#[tokio::test]
async fn e2e_factory_loop_event_streaming() {
    let (url, state) = start_e2e_server().await;
    let temp = tempfile::TempDir::new().unwrap();
    let job_dir = temp.path().join("jobs");
    std::fs::create_dir_all(&job_dir).unwrap();

    let config = ConfigFile {
        general: ConfigFileGeneral { baseurl: url.clone() },
        factory: ConfigFileFactory { token: "factory-token".into() },
        execution: ConfigFileExecution {
            command: "bash".into(),
            args: vec![
                "-c".into(),
                "echo hello-stdout; echo hello-stderr >&2".into(),
            ],
            job_dir: job_dir.clone(),
        },
        target: {
            let mut m = HashMap::new();
            m.insert("test-target".into(), ConfigFileTarget {});
            m
        },
    };

    let client = buildomat_client::ClientBuilder::new(&url)
        .bearer_token("factory-token")
        .build()
        .unwrap();

    let mut central =
        Central { log: test_logger(), client, config, instances: Vec::new() };

    // Acquire lease, spawn command
    factory_loop(&mut central).await.unwrap();
    assert_eq!(central.instances.len(), 1);

    // Poll until the command completes.
    poll_until_done(&mut central, Duration::from_secs(10)).await;

    let st = state.lock().unwrap();

    // Verify append was called at least once
    assert!(
        !st.append_requests.is_empty(),
        "should have at least one append request"
    );

    // Collect all events across batches
    let all_events: Vec<&serde_json::Value> =
        st.append_requests.iter().flatten().collect();

    // Verify stdout line was sent
    let has_stdout = all_events
        .iter()
        .any(|e| e["stream"] == "stdout" && e["payload"] == "hello-stdout");
    assert!(has_stdout, "should have stdout event; got: {:?}", all_events);

    // Verify stderr line was sent
    let has_stderr = all_events
        .iter()
        .any(|e| e["stream"] == "stderr" && e["payload"] == "hello-stderr");
    assert!(has_stderr, "should have stderr event; got: {:?}", all_events);

    // Verify each event has a timestamp
    for event in &all_events {
        assert!(
            event["time"].is_string(),
            "event should have a time field: {:?}",
            event
        );
    }
}

/// End-to-end test: pre- and post-job diagnostic scripts are executed
/// and their output is streamed as events.
#[tokio::test]
async fn e2e_factory_loop_diagnostic_scripts() {
    let (url, state) = start_e2e_server().await;
    let temp = tempfile::TempDir::new().unwrap();
    let job_dir = temp.path().join("jobs");
    std::fs::create_dir_all(&job_dir).unwrap();

    // Configure factory_metadata with diagnostic scripts
    state.lock().unwrap().factory_metadata = Some(serde_json::json!({
        "v": "1",
        "pre_job_diagnostic_script": "echo pre-diag-output",
        "post_job_diagnostic_script": "echo post-diag-output",
    }));

    let config = ConfigFile {
        general: ConfigFileGeneral { baseurl: url.clone() },
        factory: ConfigFileFactory { token: "factory-token".into() },
        execution: ConfigFileExecution {
            command: "bash".into(),
            args: vec!["-c".into(), "echo job-running".into()],
            job_dir: job_dir.clone(),
        },
        target: {
            let mut m = HashMap::new();
            m.insert("test-target".into(), ConfigFileTarget {});
            m
        },
    };

    let client = buildomat_client::ClientBuilder::new(&url)
        .bearer_token("factory-token")
        .build()
        .unwrap();

    let mut central =
        Central { log: test_logger(), client, config, instances: Vec::new() };

    // Lease, bootstrap, run pre-diag, spawn command
    factory_loop(&mut central).await.unwrap();
    assert_eq!(central.instances.len(), 1);

    // Poll until the command completes.
    poll_until_done(&mut central, Duration::from_secs(10)).await;

    let st = state.lock().unwrap();

    // Verify diagnostics_enable was called
    assert_eq!(
        st.diagnostics_enable_count, 1,
        "should have called diagnostics_enable"
    );

    // Verify diagnostics_complete was called with hold=false (script succeeded)
    assert_eq!(
        st.diagnostics_complete_requests.len(),
        1,
        "should have called diagnostics_complete"
    );
    assert_eq!(
        st.diagnostics_complete_requests[0]["hold"], false,
        "hold should be false for successful diag script"
    );

    // Collect all events
    let all_events: Vec<&serde_json::Value> =
        st.append_requests.iter().flatten().collect();

    // Verify pre-diagnostic output was streamed
    let has_pre_diag = all_events.iter().any(|e| {
        e["stream"].as_str().map(|s| s.starts_with("diag.pre")).unwrap_or(false)
            && e["payload"] == "pre-diag-output"
    });
    assert!(has_pre_diag, "should have pre-diag event; got: {:?}", all_events);

    // Verify post-diagnostic output was streamed
    let has_post_diag = all_events.iter().any(|e| {
        e["stream"]
            .as_str()
            .map(|s| s.starts_with("diag.post"))
            .unwrap_or(false)
            && e["payload"] == "post-diag-output"
    });
    assert!(
        has_post_diag,
        "should have post-diag event; got: {:?}",
        all_events
    );

    // Verify the actual job output was also streamed
    let has_job_output = all_events
        .iter()
        .any(|e| e["stream"] == "stdout" && e["payload"] == "job-running");
    assert!(
        has_job_output,
        "should have job stdout event; got: {:?}",
        all_events
    );
}

// ---------------------------------------------------------------
// E2E tests with persistent-runner-stub
// ---------------------------------------------------------------

/// Path to the runner stub binary.
fn stub_binary() -> PathBuf {
    let test_exe =
        std::env::current_exe().expect("should be able to find test binary");
    let target_dir = test_exe.parent().unwrap().parent().unwrap();
    target_dir.join("buildomat-persistent-runner-stub")
}

/// Create a ConfigFile that runs the stub in the given mode.
fn stub_config(
    url: &str,
    job_dir: &Path,
    mode: &str,
    extra_args: &[&str],
) -> ConfigFile {
    let mut args = vec!["--mode".to_string(), mode.to_string()];
    for a in extra_args {
        args.push(a.to_string());
    }
    ConfigFile {
        general: ConfigFileGeneral { baseurl: url.to_string() },
        factory: ConfigFileFactory { token: "factory-token".into() },
        execution: ConfigFileExecution {
            command: stub_binary().to_string_lossy().into_owned(),
            args,
            job_dir: job_dir.to_path_buf(),
        },
        target: {
            let mut m = HashMap::new();
            m.insert("test-target".into(), ConfigFileTarget {});
            m
        },
    }
}

/// Helper: set up a Central and run one factory_loop iteration to
/// acquire a lease and spawn the command, then return Central.
async fn spawn_stub(url: &str, config: ConfigFile) -> Central {
    let client = buildomat_client::ClientBuilder::new(url)
        .bearer_token("factory-token")
        .build()
        .unwrap();

    let mut central =
        Central { log: test_logger(), client, config, instances: Vec::new() };

    factory_loop(&mut central).await.unwrap();
    assert_eq!(central.instances.len(), 1, "should have spawned one instance");

    central
}

/// Run factory_loop in a polling loop until the instance is cleaned
/// up, with a timeout.
async fn poll_until_done(central: &mut Central, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        tokio::time::sleep(Duration::from_millis(200)).await;
        factory_loop(central).await.unwrap();
        if central.instances.is_empty() {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for instance to complete");
        }
    }
}

/// E2E test: stub in success mode — full lifecycle with outputs.
#[tokio::test]
async fn e2e_stub_success() {
    let (url, state) = start_e2e_server().await;
    let temp = tempfile::TempDir::new().unwrap();
    let job_dir = temp.path().join("jobs");
    std::fs::create_dir_all(&job_dir).unwrap();

    let config = stub_config(&url, &job_dir, "success", &[]);
    let mut central = spawn_stub(&url, config).await;

    poll_until_done(&mut central, Duration::from_secs(10)).await;

    let st = state.lock().unwrap();

    // Job should succeed.
    assert_eq!(st.complete_requests.len(), 1);
    assert_eq!(
        st.complete_requests[0]["failed"], false,
        "success mode should pass"
    );

    // Outputs should be uploaded.
    assert!(!st.add_output_requests.is_empty(), "should have uploaded outputs");

    // Events should have been streamed.
    assert!(!st.append_requests.is_empty(), "should have events");

    // Worker should be destroyed.
    let has_destroy = st
        .requests
        .iter()
        .any(|(m, p)| m == "DELETE" && p.starts_with("/0/factory/worker/"));
    assert!(has_destroy, "should have destroyed worker");

    // Job directory should be cleaned up.
    assert!(
        !job_dir.join("test-job-001").exists(),
        "job directory should be removed"
    );
}

/// E2E test: stub in fail mode — job reports failure.
#[tokio::test]
async fn e2e_stub_fail() {
    let (url, state) = start_e2e_server().await;
    let temp = tempfile::TempDir::new().unwrap();
    let job_dir = temp.path().join("jobs");
    std::fs::create_dir_all(&job_dir).unwrap();

    let config = stub_config(&url, &job_dir, "fail", &[]);
    let mut central = spawn_stub(&url, config).await;

    poll_until_done(&mut central, Duration::from_secs(10)).await;

    let st = state.lock().unwrap();

    // Job should fail.
    assert_eq!(st.complete_requests.len(), 1);
    assert_eq!(
        st.complete_requests[0]["failed"], true,
        "fail mode should report failure"
    );

    // Outputs should still be uploaded (even on failure).
    assert!(
        !st.add_output_requests.is_empty(),
        "should upload outputs even on failure"
    );
}

/// E2E test: stub in slow mode — heartbeat events, completes normally.
#[tokio::test]
async fn e2e_stub_slow() {
    let (url, state) = start_e2e_server().await;
    let temp = tempfile::TempDir::new().unwrap();
    let job_dir = temp.path().join("jobs");
    std::fs::create_dir_all(&job_dir).unwrap();

    let config = stub_config(&url, &job_dir, "slow", &["--duration", "2"]);
    let mut central = spawn_stub(&url, config).await;

    poll_until_done(&mut central, Duration::from_secs(15)).await;

    let st = state.lock().unwrap();

    // Job should succeed.
    assert_eq!(st.complete_requests.len(), 1);
    assert_eq!(st.complete_requests[0]["failed"], false);

    // Should have heartbeat events (one per second for 2 seconds).
    let all_events: Vec<&serde_json::Value> =
        st.append_requests.iter().flatten().collect();
    assert!(
        all_events.len() >= 2,
        "should have heartbeat events, got {}",
        all_events.len()
    );
}

/// E2E test: stub in crash mode — exits 137, no outputs, job fails.
#[tokio::test]
async fn e2e_stub_crash() {
    let (url, state) = start_e2e_server().await;
    let temp = tempfile::TempDir::new().unwrap();
    let job_dir = temp.path().join("jobs");
    std::fs::create_dir_all(&job_dir).unwrap();

    let config = stub_config(&url, &job_dir, "crash", &[]);
    let mut central = spawn_stub(&url, config).await;

    poll_until_done(&mut central, Duration::from_secs(10)).await;

    let st = state.lock().unwrap();

    // Job should fail.
    assert_eq!(st.complete_requests.len(), 1);
    assert_eq!(
        st.complete_requests[0]["failed"], true,
        "crash mode should report failure"
    );

    // No output files written by the crash stub.
    assert_eq!(st.add_output_requests.len(), 0, "crash mode writes no outputs");
}

/// E2E test: stub in no-output mode — no files, job passes.
#[tokio::test]
async fn e2e_stub_no_output() {
    let (url, state) = start_e2e_server().await;
    let temp = tempfile::TempDir::new().unwrap();
    let job_dir = temp.path().join("jobs");
    std::fs::create_dir_all(&job_dir).unwrap();

    let config = stub_config(&url, &job_dir, "no-output", &[]);
    let mut central = spawn_stub(&url, config).await;

    poll_until_done(&mut central, Duration::from_secs(10)).await;

    let st = state.lock().unwrap();

    // Job should succeed.
    assert_eq!(st.complete_requests.len(), 1);
    assert_eq!(st.complete_requests[0]["failed"], false);

    // No output files.
    assert_eq!(st.add_output_requests.len(), 0);
}

/// E2E test: stub in large-output mode — multi-chunk upload.
#[tokio::test]
async fn e2e_stub_large_output() {
    let (url, state) = start_e2e_server().await;
    let temp = tempfile::TempDir::new().unwrap();
    let job_dir = temp.path().join("jobs");
    std::fs::create_dir_all(&job_dir).unwrap();

    let config = stub_config(&url, &job_dir, "large-output", &[]);
    let mut central = spawn_stub(&url, config).await;

    poll_until_done(&mut central, Duration::from_secs(30)).await;

    let st = state.lock().unwrap();

    // Job should succeed.
    assert_eq!(st.complete_requests.len(), 1);
    assert_eq!(st.complete_requests[0]["failed"], false);

    // Should have multiple chunks (file is > UPLOAD_CHUNK_SIZE).
    assert!(
        st.chunk_uploads.len() > 1,
        "large-output should produce multiple chunks, got {}",
        st.chunk_uploads.len()
    );

    // Find the add_output request for the large file specifically.
    let large_req = st
        .add_output_requests
        .iter()
        .find(|r| {
            r["path"]
                .as_str()
                .map(|p| p.contains("large-output"))
                .unwrap_or(false)
        })
        .expect("should have uploaded large-output.bin");
    let expected_size = buildomat_common::UPLOAD_CHUNK_SIZE + 1024 * 1024;
    assert_eq!(
        large_req["size"].as_u64().unwrap(),
        expected_size as u64,
        "large-output file size should match"
    );
    // The large file itself required multiple chunks.
    let large_chunks = large_req["chunks"].as_array().unwrap();
    assert!(
        large_chunks.len() > 1,
        "large-output.bin should use multiple chunks, got {}",
        large_chunks.len()
    );
}

/// E2E test: recycle path uses graceful_kill.
#[tokio::test]
async fn e2e_stub_recycle() {
    let (url, state) = start_e2e_server().await;
    let temp = tempfile::TempDir::new().unwrap();
    let job_dir = temp.path().join("jobs");
    std::fs::create_dir_all(&job_dir).unwrap();

    let config = stub_config(&url, &job_dir, "slow", &["--duration", "30"]);
    let mut central = spawn_stub(&url, config).await;

    // Let the stub run briefly to produce some output.
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Trigger recycle on the next poll.
    state.lock().unwrap().force_recycle = true;

    // Next factory_loop should detect recycle, kill the child, and clean up.
    poll_until_done(&mut central, Duration::from_secs(15)).await;

    let st = state.lock().unwrap();

    // Job should fail (recycled).
    assert_eq!(st.complete_requests.len(), 1);
    assert_eq!(
        st.complete_requests[0]["failed"], true,
        "recycled job should report failure"
    );

    // Worker should be destroyed.
    let has_destroy = st
        .requests
        .iter()
        .any(|(m, p)| m == "DELETE" && p.starts_with("/0/factory/worker/"));
    assert!(has_destroy, "should have destroyed worker");

    // Job directory should be cleaned up.
    assert!(
        !job_dir.join("test-job-001").exists(),
        "job directory should be removed after recycle"
    );
}

/// E2E test: shutdown() kills child, flushes events, uploads outputs,
/// reports failure, destroys worker, cleans directory.
#[tokio::test]
async fn e2e_stub_shutdown() {
    let (url, state) = start_e2e_server().await;
    let temp = tempfile::TempDir::new().unwrap();
    let job_dir = temp.path().join("jobs");
    std::fs::create_dir_all(&job_dir).unwrap();

    let config = stub_config(&url, &job_dir, "slow", &["--duration", "30"]);
    let mut central = spawn_stub(&url, config).await;

    // Let the stub run briefly to produce some events.
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Call shutdown() directly — this is what signal handling invokes.
    shutdown(&mut central).await;

    assert_eq!(
        central.instances.len(),
        0,
        "shutdown should drain all instances"
    );

    let st = state.lock().unwrap();

    // Job should be reported as failed.
    assert_eq!(st.complete_requests.len(), 1);
    assert_eq!(
        st.complete_requests[0]["failed"], true,
        "shutdown should report failure"
    );

    // Worker should be destroyed.
    let has_destroy = st
        .requests
        .iter()
        .any(|(m, p)| m == "DELETE" && p.starts_with("/0/factory/worker/"));
    assert!(has_destroy, "shutdown should destroy worker");

    // Job directory should be cleaned up.
    assert!(
        !job_dir.join("test-job-001").exists(),
        "shutdown should clean up job directory"
    );

    // Events should have been streamed (heartbeat from the 1s of running).
    assert!(
        !st.append_requests.is_empty(),
        "should have flushed events during shutdown"
    );
}

/// E2E test: when worker setup fails (bootstrap error), the factory
/// destroys the orphaned worker and cleans up the job directory.
#[tokio::test]
async fn e2e_setup_failure_destroys_worker() {
    let (url, state) = start_e2e_server().await;
    let temp = tempfile::TempDir::new().unwrap();
    let job_dir = temp.path().join("jobs");
    std::fs::create_dir_all(&job_dir).unwrap();

    // Make bootstrap fail so setup_instance errors after worker creation.
    state.lock().unwrap().fail_bootstrap = true;

    let config = ConfigFile {
        general: ConfigFileGeneral { baseurl: url.clone() },
        factory: ConfigFileFactory { token: "factory-token".into() },
        execution: ConfigFileExecution {
            command: "true".into(),
            args: vec![],
            job_dir: job_dir.clone(),
        },
        target: {
            let mut m = HashMap::new();
            m.insert("test-target".into(), ConfigFileTarget {});
            m
        },
    };

    let client = buildomat_client::ClientBuilder::new(&url)
        .bearer_token("factory-token")
        .build()
        .unwrap();

    let mut central =
        Central { log: test_logger(), client, config, instances: Vec::new() };

    // factory_loop should return an error (bootstrap failed).
    let result = factory_loop(&mut central).await;
    assert!(result.is_err(), "factory_loop should fail on bootstrap error");

    // No instance should have been added.
    assert_eq!(
        central.instances.len(),
        0,
        "no instance should exist after setup failure"
    );

    let st = state.lock().unwrap();

    // Worker should have been created.
    let has_create = st
        .requests
        .iter()
        .any(|(m, p)| m == "POST" && p == "/0/factory/worker");
    assert!(has_create, "should have called factory_worker_create");

    // Worker should have been destroyed (the leak guard).
    let has_destroy = st
        .requests
        .iter()
        .any(|(m, p)| m == "DELETE" && p.starts_with("/0/factory/worker/"));
    assert!(has_destroy, "should have destroyed worker after setup failure");

    // Job directory should have been cleaned up.
    assert!(
        !job_dir.join("test-job-001").exists(),
        "job directory should be removed after setup failure"
    );
}
