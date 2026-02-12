use std::collections::{HashMap, HashSet};
use std::fs;
use std::sync::{Arc, Mutex};

use buildomat_client::types::WorkerPingOutputRule;
use buildomat_common::UPLOAD_CHUNK_SIZE;
use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::{Method, Request, Response, StatusCode};
use tempfile::TempDir;

use crate::test_support::{start_mock_server, test_logger};
use crate::worker_ops;

#[test]
fn test_collect_files_recursive() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let temp = TempDir::new().unwrap();
        let base = temp.path();

        // Create nested structure
        fs::create_dir_all(base.join("subdir")).unwrap();
        fs::write(base.join("top.txt"), "top").unwrap();
        fs::write(base.join("subdir/nested.txt"), "nested").unwrap();

        let mut files = Vec::new();
        worker_ops::collect_files_recursive(base, base, &mut files)
            .await
            .unwrap();

        assert_eq!(files.len(), 2);

        let rel_paths: HashSet<_> =
            files.iter().map(|(_, r)| r.as_str()).collect();
        assert!(rel_paths.contains("top.txt"));
        assert!(rel_paths.contains("subdir/nested.txt"));
    });
}

// ---------------------------------------------------------------
// Mock server infrastructure
// ---------------------------------------------------------------

/// Tracks what the mock server has received and how it should respond.
#[derive(Default)]
struct MockState {
    /// Recorded request bodies keyed by path.
    chunk_uploads: Vec<Vec<u8>>,
    /// Canned download data for input downloads, keyed by input id.
    download_data: HashMap<String, Vec<u8>>,
    /// Counter for chunk IDs (incremented on each upload).
    next_chunk_id: u32,
    /// Sequence of add_output results to return (polled in order).
    add_output_results: Vec<(bool, Option<String>)>,
    /// Index into add_output_results.
    add_output_index: usize,
    /// Recorded add_output request bodies.
    add_output_requests: Vec<serde_json::Value>,
}

type SharedState = Arc<Mutex<MockState>>;

/// Handle a single HTTP request against our mock server.
async fn mock_handler(
    req: Request<Incoming>,
    state: SharedState,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    use http_body_util::BodyExt;

    let method = req.method().clone();
    let path = req.uri().path().to_string();

    // GET /0/worker/job/{job}/inputs/{input} -> download
    if method == Method::GET && path.contains("/inputs/") {
        let parts: Vec<&str> = path.split('/').collect();
        // path = /0/worker/job/{job}/inputs/{input}
        //        0  1      2    3     4       5
        let input_id = parts.last().unwrap_or(&"");
        let st = state.lock().unwrap();
        if let Some(data) = st.download_data.get(*input_id) {
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
        let json = serde_json::json!({ "id": id });
        return Ok(Response::builder()
            .status(StatusCode::CREATED)
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(json.to_string())))
            .unwrap());
    }

    // POST /1/worker/job/{job}/output -> add output
    if method == Method::POST && path.ends_with("/output") {
        let body_bytes =
            req.into_body().collect().await.unwrap().to_bytes().to_vec();
        let body_json: serde_json::Value =
            serde_json::from_slice(&body_bytes).unwrap();
        let mut st = state.lock().unwrap();
        st.add_output_requests.push(body_json);
        let idx = st.add_output_index;
        let (complete, error) = if idx < st.add_output_results.len() {
            st.add_output_index += 1;
            st.add_output_results[idx].clone()
        } else {
            // Default: complete with no error
            (true, None)
        };
        let json = serde_json::json!({
            "complete": complete,
            "error": error,
        });
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(json.to_string())))
            .unwrap());
    }

    Ok(Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Full::new(Bytes::from("unknown route")))
        .unwrap())
}

/// Start the worker_ops mock HTTP server, returning its base URL and shared state.
async fn start_wo_mock_server() -> (String, SharedState) {
    let state: SharedState = Arc::new(Mutex::new(MockState::default()));
    let url = start_mock_server(Arc::clone(&state), mock_handler).await;
    (url, state)
}

/// Build a buildomat client pointing at the mock server.
fn mock_client(base_url: &str) -> buildomat_client::Client {
    buildomat_client::Client::new_with_client(base_url, reqwest::Client::new())
}

// ---------------------------------------------------------------
// download_input tests
// ---------------------------------------------------------------

#[tokio::test]
async fn test_download_input_basic() {
    let (url, state) = start_wo_mock_server().await;
    let log = test_logger();
    let client = mock_client(&url);
    let temp = TempDir::new().unwrap();

    let content = b"hello, artifact!";
    state
        .lock()
        .unwrap()
        .download_data
        .insert("input-001".into(), content.to_vec());

    worker_ops::download_input(
        &log,
        &client,
        "job-123",
        "input-001",
        "artifact.bin",
        temp.path(),
    )
    .await
    .unwrap();

    let result = fs::read(temp.path().join("artifact.bin")).unwrap();
    assert_eq!(result, content);
}

#[tokio::test]
async fn test_download_input_nested_name() {
    let (url, state) = start_wo_mock_server().await;
    let log = test_logger();
    let client = mock_client(&url);
    let temp = TempDir::new().unwrap();

    state
        .lock()
        .unwrap()
        .download_data
        .insert("input-002".into(), b"nested data".to_vec());

    worker_ops::download_input(
        &log,
        &client,
        "job-123",
        "input-002",
        "sub/dir/file.txt",
        temp.path(),
    )
    .await
    .unwrap();

    let result = fs::read(temp.path().join("sub/dir/file.txt")).unwrap();
    assert_eq!(result, b"nested data");
}

#[tokio::test]
async fn test_download_input_no_partial_on_success() {
    let (url, state) = start_wo_mock_server().await;
    let log = test_logger();
    let client = mock_client(&url);
    let temp = TempDir::new().unwrap();

    state
        .lock()
        .unwrap()
        .download_data
        .insert("input-003".into(), b"data".to_vec());

    worker_ops::download_input(
        &log,
        &client,
        "job-123",
        "input-003",
        "out.bin",
        temp.path(),
    )
    .await
    .unwrap();

    // .partial file should have been renamed away
    assert!(!temp.path().join("out.bin.partial").exists());
    assert!(temp.path().join("out.bin").exists());
}

#[tokio::test]
async fn test_download_input_server_error() {
    let (url, _state) = start_wo_mock_server().await;
    let log = test_logger();
    let client = mock_client(&url);
    let temp = TempDir::new().unwrap();

    // No download_data configured → 404
    let result = worker_ops::download_input(
        &log,
        &client,
        "job-123",
        "nonexistent",
        "out.bin",
        temp.path(),
    )
    .await;

    assert!(result.is_err());
    // No partial file left behind
    assert!(!temp.path().join("out.bin.partial").exists());
}

/// Test that when the server drops the connection mid-body, the
/// `.partial` file is cleaned up.  The existing `server_error` test
/// uses a 404, so progenitor errors out before File::create and the
/// cleanup code is never exercised.  This test sends a 200 with a
/// Content-Length header, delivers a few bytes, then drops the
/// connection — forcing the error to occur inside the body-read loop
/// after the partial file has been created.
#[tokio::test]
async fn test_download_input_mid_body_drop_cleans_partial() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let log = test_logger();
    let temp = TempDir::new().unwrap();

    /*
     * Start a raw TCP server that accepts one connection, sends valid
     * HTTP response headers with Content-Length: 10000, writes a small
     * chunk of body data, then drops the connection.
     */
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("http://{}", addr);

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();

        // Drain the incoming request.
        let mut buf = vec![0u8; 4096];
        let _ = stream.read(&mut buf).await;

        // Send response headers + partial body.
        stream
            .write_all(
                b"HTTP/1.1 200 OK\r\n\
                  Content-Length: 10000\r\n\
                  \r\n\
                  partial",
            )
            .await
            .unwrap();
        stream.flush().await.unwrap();

        // Drop the connection — client sees an incomplete body.
        drop(stream);
    });

    let client = mock_client(&url);

    let result = worker_ops::download_input(
        &log,
        &client,
        "job-123",
        "input-001",
        "out.bin",
        temp.path(),
    )
    .await;

    // Download should fail (incomplete body).
    assert!(result.is_err());

    // The .partial file should have been cleaned up.
    assert!(
        !temp.path().join("out.bin.partial").exists(),
        "partial file should be removed on mid-body error"
    );

    // The final file should not exist either.
    assert!(!temp.path().join("out.bin").exists());
}

// ---------------------------------------------------------------
// download_input path traversal tests
// ---------------------------------------------------------------

#[tokio::test]
async fn test_download_input_rejects_parent_traversal() {
    let (url, _state) = start_wo_mock_server().await;
    let log = test_logger();
    let client = mock_client(&url);
    let temp = TempDir::new().unwrap();

    let result = worker_ops::download_input(
        &log,
        &client,
        "job-123",
        "input-001",
        "../escape.txt",
        temp.path(),
    )
    .await;

    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("path traversal"),
        "expected path traversal error, got: {}",
        msg
    );
}

#[tokio::test]
async fn test_download_input_rejects_nested_traversal() {
    let (url, _state) = start_wo_mock_server().await;
    let log = test_logger();
    let client = mock_client(&url);
    let temp = TempDir::new().unwrap();

    let result = worker_ops::download_input(
        &log,
        &client,
        "job-123",
        "input-001",
        "foo/../../escape.txt",
        temp.path(),
    )
    .await;

    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("path traversal"),
        "expected path traversal error, got: {}",
        msg
    );
}

// ---------------------------------------------------------------
// upload_chunk tests
// ---------------------------------------------------------------

#[tokio::test]
async fn test_upload_chunk_basic() {
    let (url, _state) = start_wo_mock_server().await;
    let client = mock_client(&url);

    let chunk_id =
        worker_ops::upload_chunk(&client, "job-123", b"some data".to_vec())
            .await
            .unwrap();

    assert_eq!(chunk_id, "chunk-0000");
}

#[tokio::test]
async fn test_upload_chunk_server_receives_data() {
    let (url, state) = start_wo_mock_server().await;
    let client = mock_client(&url);

    let data = vec![0xDE, 0xAD, 0xBE, 0xEF];
    worker_ops::upload_chunk(&client, "job-123", data.clone()).await.unwrap();

    let st = state.lock().unwrap();
    assert_eq!(st.chunk_uploads.len(), 1);
    assert_eq!(st.chunk_uploads[0], data);
}

// ---------------------------------------------------------------
// upload_output tests
// ---------------------------------------------------------------

#[tokio::test]
async fn test_upload_output_small_file() {
    let (url, state) = start_wo_mock_server().await;
    let log = test_logger();
    let client = mock_client(&url);
    let temp = TempDir::new().unwrap();

    // Create a small file (< UPLOAD_CHUNK_SIZE)
    let content = b"small file content";
    let file_path = temp.path().join("small.txt");
    fs::write(&file_path, content).unwrap();

    worker_ops::upload_output(
        &log,
        &client,
        "job-123",
        &file_path,
        "small.txt",
        false,
    )
    .await
    .unwrap();

    let st = state.lock().unwrap();
    // Should have uploaded exactly one chunk
    assert_eq!(st.chunk_uploads.len(), 1);
    assert_eq!(st.chunk_uploads[0], content);
    // Should have one add_output request
    assert_eq!(st.add_output_requests.len(), 1);
    let req = &st.add_output_requests[0];
    assert_eq!(req["path"], "small.txt");
    assert_eq!(req["size"], content.len() as u64);
    assert_eq!(req["chunks"].as_array().unwrap().len(), 1);
    assert_eq!(req["chunks"][0], "chunk-0000");
}

#[tokio::test]
async fn test_upload_output_multi_chunk() {
    let (url, state) = start_wo_mock_server().await;
    let log = test_logger();
    let client = mock_client(&url);
    let temp = TempDir::new().unwrap();

    // Create a file larger than one chunk (UPLOAD_CHUNK_SIZE = 5MB).
    let total_size = UPLOAD_CHUNK_SIZE * 2 + 1000;
    let content = vec![0xABu8; total_size];
    let file_path = temp.path().join("big.bin");
    fs::write(&file_path, &content).unwrap();

    worker_ops::upload_output(
        &log, &client, "job-123", &file_path, "big.bin", false,
    )
    .await
    .unwrap();

    let st = state.lock().unwrap();
    // Must have uploaded more than one chunk
    assert!(
        st.chunk_uploads.len() > 1,
        "expected multiple chunks, got {}",
        st.chunk_uploads.len()
    );
    // Total bytes across all chunks must equal file size
    let total_uploaded: usize = st.chunk_uploads.iter().map(|c| c.len()).sum();
    assert_eq!(total_uploaded, total_size);
    // Each individual chunk must be <= UPLOAD_CHUNK_SIZE
    for (i, chunk) in st.chunk_uploads.iter().enumerate() {
        assert!(
            chunk.len() <= UPLOAD_CHUNK_SIZE,
            "chunk {} was {} bytes, exceeds max {}",
            i,
            chunk.len(),
            UPLOAD_CHUNK_SIZE
        );
    }
    // add_output should reference the same number of chunk IDs
    let req = &st.add_output_requests[0];
    let chunks = req["chunks"].as_array().unwrap();
    assert_eq!(chunks.len(), st.chunk_uploads.len());
    assert_eq!(req["size"], total_size as u64);
}

#[tokio::test]
async fn test_upload_output_server_error() {
    let (url, state) = start_wo_mock_server().await;
    let log = test_logger();
    let client = mock_client(&url);
    let temp = TempDir::new().unwrap();

    // Configure server to return an error on add_output
    state.lock().unwrap().add_output_results =
        vec![(true, Some("disk full".into()))];

    let file_path = temp.path().join("fail.txt");
    fs::write(&file_path, b"data").unwrap();

    let result = worker_ops::upload_output(
        &log, &client, "job-123", &file_path, "fail.txt", false,
    )
    .await;

    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("disk full"),
        "expected 'disk full' in error, got: {}",
        err_msg
    );
}

#[tokio::test]
async fn test_upload_output_polling() {
    let (url, state) = start_wo_mock_server().await;
    let log = test_logger();
    let client = mock_client(&url);
    let temp = TempDir::new().unwrap();

    // Server returns "not complete" first, then "complete"
    state.lock().unwrap().add_output_results = vec![
        (false, None), // first poll: not complete
        (true, None),  // second poll: complete
    ];

    let file_path = temp.path().join("poll.txt");
    fs::write(&file_path, b"data").unwrap();

    worker_ops::upload_output(
        &log, &client, "job-123", &file_path, "poll.txt", false,
    )
    .await
    .unwrap();

    let st = state.lock().unwrap();
    // Should have called add_output twice (once incomplete, once complete)
    assert_eq!(st.add_output_requests.len(), 2);
}

// ---------------------------------------------------------------
// upload_outputs tests
// ---------------------------------------------------------------

#[tokio::test]
async fn test_upload_outputs_no_rules() {
    let (url, state) = start_wo_mock_server().await;
    let log = test_logger();
    let client = mock_client(&url);
    let temp = TempDir::new().unwrap();

    // Create files in the output directory
    let out_dir = temp.path().join("output");
    fs::create_dir_all(out_dir.join("sub")).unwrap();
    fs::write(out_dir.join("a.txt"), "aaa").unwrap();
    fs::write(out_dir.join("sub/b.txt"), "bbb").unwrap();

    worker_ops::upload_outputs(&log, &client, "job-123", &out_dir, &[])
        .await
        .unwrap();

    let st = state.lock().unwrap();
    // Two files → two add_output requests
    assert_eq!(st.add_output_requests.len(), 2);
    let paths: HashSet<String> = st
        .add_output_requests
        .iter()
        .map(|r| r["path"].as_str().unwrap().to_string())
        .collect();
    assert!(paths.contains("a.txt"));
    assert!(paths.contains("sub/b.txt"));
}

#[tokio::test]
async fn test_upload_outputs_include_pattern() {
    let (url, state) = start_wo_mock_server().await;
    let log = test_logger();
    let client = mock_client(&url);
    let temp = TempDir::new().unwrap();

    let out_dir = temp.path().join("output");
    fs::create_dir_all(&out_dir).unwrap();
    fs::write(out_dir.join("keep.log"), "log data").unwrap();
    fs::write(out_dir.join("skip.txt"), "text data").unwrap();

    let rules = vec![WorkerPingOutputRule {
        ignore: false,
        require_match: false,
        rule: "*.log".to_string(),
        size_change_ok: false,
    }];

    worker_ops::upload_outputs(&log, &client, "job-123", &out_dir, &rules)
        .await
        .unwrap();

    let st = state.lock().unwrap();
    assert_eq!(st.add_output_requests.len(), 1);
    assert_eq!(st.add_output_requests[0]["path"], "keep.log");
}

#[tokio::test]
async fn test_upload_outputs_ignore_pattern() {
    let (url, state) = start_wo_mock_server().await;
    let log = test_logger();
    let client = mock_client(&url);
    let temp = TempDir::new().unwrap();

    let out_dir = temp.path().join("output");
    fs::create_dir_all(&out_dir).unwrap();
    fs::write(out_dir.join("keep.txt"), "keep").unwrap();
    fs::write(out_dir.join("skip.tmp"), "skip").unwrap();

    let rules = vec![
        // Include everything
        WorkerPingOutputRule {
            ignore: false,
            require_match: false,
            rule: "*".to_string(),
            size_change_ok: false,
        },
        // But ignore .tmp files
        WorkerPingOutputRule {
            ignore: true,
            require_match: false,
            rule: "*.tmp".to_string(),
            size_change_ok: false,
        },
    ];

    worker_ops::upload_outputs(&log, &client, "job-123", &out_dir, &rules)
        .await
        .unwrap();

    let st = state.lock().unwrap();
    assert_eq!(st.add_output_requests.len(), 1);
    assert_eq!(st.add_output_requests[0]["path"], "keep.txt");
}

#[tokio::test]
async fn test_upload_outputs_empty_dir() {
    let (url, state) = start_wo_mock_server().await;
    let log = test_logger();
    let client = mock_client(&url);
    let temp = TempDir::new().unwrap();

    let out_dir = temp.path().join("empty_output");
    fs::create_dir_all(&out_dir).unwrap();

    worker_ops::upload_outputs(&log, &client, "job-123", &out_dir, &[])
        .await
        .unwrap();

    let st = state.lock().unwrap();
    assert_eq!(st.add_output_requests.len(), 0);
    assert_eq!(st.chunk_uploads.len(), 0);
}

// ---------------------------------------------------------------
// require_match tests
// ---------------------------------------------------------------

#[tokio::test]
async fn test_upload_outputs_require_match_satisfied() {
    let (url, _state) = start_wo_mock_server().await;
    let log = test_logger();
    let client = mock_client(&url);
    let temp = TempDir::new().unwrap();

    let out_dir = temp.path().join("output");
    fs::create_dir_all(&out_dir).unwrap();
    fs::write(out_dir.join("report.log"), "log data").unwrap();

    let rules = vec![WorkerPingOutputRule {
        ignore: false,
        require_match: true,
        rule: "*.log".to_string(),
        size_change_ok: false,
    }];

    // Should succeed because *.log matched report.log
    worker_ops::upload_outputs(&log, &client, "job-123", &out_dir, &rules)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_upload_outputs_require_match_fails() {
    let (url, _state) = start_wo_mock_server().await;
    let log = test_logger();
    let client = mock_client(&url);
    let temp = TempDir::new().unwrap();

    let out_dir = temp.path().join("output");
    fs::create_dir_all(&out_dir).unwrap();
    // No .log files exist
    fs::write(out_dir.join("data.txt"), "text").unwrap();

    let rules = vec![
        WorkerPingOutputRule {
            ignore: false,
            require_match: true,
            rule: "*.log".to_string(),
            size_change_ok: false,
        },
        WorkerPingOutputRule {
            ignore: false,
            require_match: false,
            rule: "*.txt".to_string(),
            size_change_ok: false,
        },
    ];

    let result =
        worker_ops::upload_outputs(&log, &client, "job-123", &out_dir, &rules)
            .await;
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("required a match, but was not used"),
        "unexpected error: {}",
        msg
    );
    assert!(msg.contains("*.log"), "error should mention the pattern: {}", msg);
}

// ---------------------------------------------------------------
// size_change_ok tests
// ---------------------------------------------------------------

#[tokio::test]
async fn test_upload_output_size_change_ok_skips_disappeared_file() {
    let (url, state) = start_wo_mock_server().await;
    let log = test_logger();
    let client = mock_client(&url);
    let temp = TempDir::new().unwrap();

    let file_path = temp.path().join("gone.txt");
    // File does not exist

    // With size_change_ok=true, should warn and succeed
    worker_ops::upload_output(
        &log, &client, "job-123", &file_path, "gone.txt", true,
    )
    .await
    .unwrap();

    // No chunks or outputs should have been uploaded
    let st = state.lock().unwrap();
    assert_eq!(st.chunk_uploads.len(), 0);
    assert_eq!(st.add_output_requests.len(), 0);
}

#[tokio::test]
async fn test_upload_output_missing_file_fails_without_size_change_ok() {
    let (url, _state) = start_wo_mock_server().await;
    let log = test_logger();
    let client = mock_client(&url);
    let temp = TempDir::new().unwrap();

    let file_path = temp.path().join("gone.txt");
    // File does not exist

    // With size_change_ok=false, should return error
    let result = worker_ops::upload_output(
        &log, &client, "job-123", &file_path, "gone.txt", false,
    )
    .await;
    assert!(result.is_err());
}
