/*
 * Copyright 2026 Oxide Computer Company
 */

use std::io::Write as _;
use std::process::Stdio;
use std::sync::{Arc, Mutex};

use anyhow::{bail, Context as _, Result};
use buildomat_client::Client;
use tokio::io::{AsyncBufReadExt as _, BufReader};
use tokio::process::{ChildStderr, ChildStdout};
use tokio::sync::oneshot;

use crate::server::ServerConfig;
use crate::{cargo, Context};

/**
 * Some parts of the setup need to issue API calls to buildomat-server.
 *
 * This function spawns a server in the background, waits for it to listen for
 * requests, and then invokes the closure passing a configured API client to it.
 * The server is cleaned up afterwards.
 */
pub(crate) async fn with_api<F, T>(ctx: &Context, f: F) -> Result<T>
where
    F: AsyncFnOnce(&Client) -> Result<T>,
{
    let mut cmd = cargo();
    cmd.args(["run", "-q", "-p", "buildomat-server", "--"]);
    cmd.args(["-f", "config.toml"]);
    cmd.current_dir(ctx.root.join("server"));
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());

    let mut server = cmd.spawn().context("failed to start buildomat server")?;

    let (listening_tx, listening_rx) = oneshot::channel();
    let output = Arc::new(Mutex::new(Vec::new()));
    tokio::spawn(log_until_listening(
        server.stdout.take().unwrap(),
        server.stderr.take().unwrap(),
        listening_tx,
        output.clone(),
    ));

    tokio::select! {
        /*
         * Good: the server started listening for connections.
         */
        _ = listening_rx => {}
        /*
         * Bad: the server died before it started listening for connections.
         */
        exit = server.wait() => {
            let exit = exit.context("failed to join the buildomat server")?;
            println!();
            println!("buildomat server logs:");
            std::io::stderr().write_all(&output.lock().unwrap())?;
            println!();
            bail!("buildomat server failed with {exit}");
        }
    }

    let config = ServerConfig::from_context(ctx)?;
    let client = buildomat_client::ClientBuilder::new("http://127.0.0.1:9979")
        .bearer_token(&config.admin.token)
        .build()
        .context("failed to create the buildomat client")?;

    let result = f(&client).await;
    server.kill().await?;
    if result.is_err() {
        println!();
        println!("buildomat server logs:");
        std::io::stderr().write_all(&output.lock().unwrap())?;
        println!();
    }
    result
}

async fn log_until_listening(
    stdout: ChildStdout,
    stderr: ChildStderr,
    complete: oneshot::Sender<()>,
    output: Arc<Mutex<Vec<u8>>>,
) {
    #[derive(serde::Deserialize)]
    struct Log<'a> {
        msg: &'a str,
    }

    let mut out = BufReader::new(stdout);
    let mut err = BufReader::new(stderr);
    let mut out_buf = Vec::new();
    let mut err_buf = Vec::new();
    let mut complete = Some(complete);
    loop {
        tokio::select! {
            _ = out.read_until(b'\n', &mut out_buf) => {
                if let Ok(Log { msg }) = serde_json::from_slice(&out_buf) {
                    /*
                     * The "listening" message is emitted by dropshot.  It's not
                     * super pretty, but it works great in practice.
                     */
                    if msg == "listening" {
                        if let Some(complete) = complete.take() {
                            let _ = complete.send(());
                        }
                    }
                }
                output.lock().unwrap().extend_from_slice(&out_buf);
                out_buf.clear();
            }
            _ = err.read_until(b'\n', &mut err_buf) => {
                output.lock().unwrap().extend_from_slice(&err_buf);
                err_buf.clear();
            }
        }
    }
}
