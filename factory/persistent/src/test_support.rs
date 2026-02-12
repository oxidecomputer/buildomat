/*
 * Copyright 2026 Oxide Computer Company
 */

//! Shared test infrastructure for mock servers and helpers.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use slog::Logger;
use tokio::net::TcpListener;

/// Create a no-op logger for tests.
pub(crate) fn test_logger() -> Logger {
    use slog::Drain;
    let decorator = slog_term::PlainDecorator::new(std::io::sink());
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = std::sync::Mutex::new(drain).fuse();
    Logger::root(drain, slog::o!())
}

/// Build a JSON response for mock servers.
pub(crate) fn json_response(
    status: StatusCode,
    body: serde_json::Value,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    Ok(Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(body.to_string())))
        .unwrap())
}

/// Start a mock HTTP server with the given handler function.
///
/// Returns the base URL and shared state. The handler receives each
/// request along with the shared state.
pub(crate) async fn start_mock_server<S, F, Fut>(
    state: Arc<Mutex<S>>,
    handler: F,
) -> String
where
    S: Send + 'static,
    F: Fn(Request<Incoming>, Arc<Mutex<S>>) -> Fut + Clone + Send + 'static,
    Fut: std::future::Future<
            Output = Result<Response<Full<Bytes>>, hyper::Error>,
        > + Send,
{
    let listener =
        TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("http://{}", addr);

    let server_state = Arc::clone(&state);
    tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(conn) => conn,
                Err(_) => continue,
            };
            let st = Arc::clone(&server_state);
            let handler = handler.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let svc = service_fn(move |req| {
                    let st = Arc::clone(&st);
                    (handler.clone())(req, st)
                });
                let _ = http1::Builder::new().serve_connection(io, svc).await;
            });
        }
    });

    url
}
