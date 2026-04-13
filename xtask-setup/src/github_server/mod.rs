/*
 * Copyright 2026 Oxide Computer Company
 */

use std::sync::{Arc, Mutex};

use anyhow::{bail, Context as _, Result};
use buildomat_client::types::UserCreate;
use buildomat_client::Client as Buildomat;
use buildomat_common::genkey;
use dialoguer::Input;
use dropshot::{
    endpoint, ApiDescription, Body, ConfigDropshot, HttpError, Query,
    RequestContext, ServerBuilder,
};
use http::header::{CONTENT_TYPE, USER_AGENT};
use http::{HeaderValue, Response};
use slog::{o, Discard, Logger};
use tokio::sync::oneshot;

use crate::server::ServerConfig;
use crate::with_api::with_api;
use crate::Context;

pub(crate) async fn setup(ctx: &Context) -> Result<()> {
    let root = ctx.root.join("github-server");
    if root.exists() {
        std::fs::remove_dir_all(&root)
            .context("failed to remove old github-server directory")?;
    }
    std::fs::create_dir_all(&root)
        .context("failed to create github-server directory")?;
    std::fs::create_dir_all(root.join("etc"))
        .context("failed to create github-server/etc directory")?;
    std::fs::create_dir_all(root.join("var"))
        .context("failed to create github-server/var directory")?;

    let server_config = ServerConfig::from_context(ctx)?;

    println!();
    println!("buildomat-github-server setup");
    println!("=============================");

    let base_url = get_base_url(ctx)?;
    let app = create_github_app(&base_url, &ctx.setup_name).await?;
    let slug = &app.slug;

    println!("Make sure to install the newly created app on your account:");
    println!();
    println!("    https://github.com/settings/apps/{slug}/installations");
    println!();

    let user_token = with_api(ctx, async |bmat| {
        let user_token = create_user(bmat).await?;
        Ok(user_token)
    })
    .await?;

    /*
     * Write the configuration using the app.
     */
    std::fs::write(
        root.join("etc").join("app.toml"),
        toml::to_string_pretty(&Config {
            id: app.id,
            webhook_secret: app.webhook_secret,
            base_url,
            confroot: ".github/buildomat".into(),
            allow_owners: vec![app.owner.login],
            buildomat: ConfigBuildomat {
                url: server_config.general.baseurl,
                token: user_token,
            },
            sqlite: ConfigSqlite {},
        })?,
    )?;
    std::fs::write(root.join("etc").join("privkey.pem"), app.pem)?;

    /*
     * The GitHub server requires the SQLite database to be already present.
     */
    std::fs::write(root.join("var/data.sqlite3"), b"")
        .context("failed ot create empty database file")?;

    std::fs::write(root.join("complete"), b"true\n")
        .context("failed to mark the GitHub server setup as complete")?;

    Ok(())
}

fn get_base_url(ctx: &Context) -> Result<String> {
    println!();
    println!("Buildomat's GitHub integration needs a PUBLICLY ACCESSIBLE ");
    println!("domain name that proxies requests to localhost on port 4021.");
    println!();
    println!("The domain name needs to be working RIGHT NOW for the setup");
    println!("to finish, as automated GitHub App creation needs a server.");
    println!();

    Ok(Input::with_theme(&ctx.input_theme)
        .with_prompt("Domain name")
        .validate_with(|url: &String| -> _ {
            if !url.starts_with("http://") && !url.starts_with("https://") {
                Err("missing http:// or https:// at the start")
            } else {
                Ok(())
            }
        })
        .interact()?
        .trim_end_matches('/')
        .into())
}

async fn create_user(bmat: &Buildomat) -> Result<String> {
    /*
     * Buildomat doesn't have a way to retrieve or regenerate the token of an
     * existing user.  Since the setup process might fail between here and
     * completion, we can't use a fixed name, since executing the setup again
     * would try to create an user with a duplicate name.
     */
    let name = format!("github-{}", genkey(8));

    let user = bmat
        .user_create()
        .body(UserCreate { name })
        .send()
        .await
        .context("failed to create buildomat user for the GitHub server")?
        .into_inner();

    for privilege in [
        "admin.job.read",
        "admin.target.read",
        "admin.user.read",
        "admin.worker.read",
        "delegate",
    ] {
        bmat.user_privilege_grant()
            .user(&user.id)
            .privilege(privilege)
            .send()
            .await
            .context("failed to grant privilege to the user")?;
    }

    Ok(user.token)
}

/**
 * GitHub doesn't really have a good API to programmatically GitHub Apps.
 *
 * One of the options is to build a URL containing the app configuration as
 * query parameters, and instruct users to click on it.  This will pre-fill the
 * app creation form, but then it's the user's responsibility to gather the app
 * ID, to generate a private key, and to download it.
 *
 * The other option is what GitHub calls the "manifest flow": users click a
 * button in their browser that sends a POST request to GitHub, which logs the
 * user in and shows them a page letting them choose the app name.  Once they
 * choose the app name, GitHub creates an exchange token and redirects the user
 * browser to a callback URL.  That exchange token can be programmatically
 * exchanged for the app ID and private key.
 *
 * The manifest flow is a better UX, but requires a web server to be stood up
 * (to be able to serve the POST form and accept the calback).  Normally this
 * would be a non-starter for CLI applications, but we already need the user to
 * setup a public DNS record pointing to their local machine.  We can thus spin
 * up this temporary server on the same port used by buildomat-github-server.
 */
async fn create_github_app(
    base_url: &str,
    app_name: &str,
) -> Result<CreateAppResponse> {
    let mut api = ApiDescription::new();
    api.register(setup_page)?;
    api.register(callback_page)?;

    let (callback_code_tx, callback_code_rx) = oneshot::channel();
    let ctx = Arc::new(CreationServerState {
        base_url: base_url.into(),
        app_name: app_name.into(),
        callback_code_tx: Mutex::new(Some(callback_code_tx)),
        /*
         * This secret will be included in the "state" query parameter passed to
         * GitHub: it will be sent back in the callback, to ensure the callback
         * belongs to request we just sent.
         */
        secret: genkey(64),
    });

    let mut server = ServerBuilder::new(api, ctx, Logger::root(Discard, o!()))
        .config(ConfigDropshot {
            /*
             * This must be the same port as buildomat-github-server.
             */
            bind_address: ([0, 0, 0, 0], 4021).into(),
            ..ConfigDropshot::default()
        })
        .start()
        .context("failed to start the server")?;

    println!();
    println!("To configure the GitHub App used by buildomat, open this URL:");
    println!();
    println!("    {base_url}/__setup");
    println!();

    /*
     * Spin the web server down as soon as we receive a callback.
     */
    let callback_code = tokio::select! {
        _ = &mut server => bail!("server exited without receiving a callback"),
        code = callback_code_rx => {
            let _ = server.close().await;
            code?
        }
    };

    /*
     * Exchange the code we received in the callback with the app private key.
     */
    Ok(reqwest::Client::new()
        .post(format!(
            "https://api.github.com/app-manifests/{callback_code}/conversions"
        ))
        .header(USER_AGENT, "oxidecomputer/buildomat local setup")
        .header("X-GitHub-Api-Version", "2026-03-10")
        .send()
        .await
        .and_then(|resp| resp.error_for_status())
        .context("failed to ask GitHub to finish creating the GitHub App")?
        .json()
        .await
        .context("failed to deserialize response from GitHub")?)
}

struct CreationServerState {
    secret: String,
    base_url: String,
    app_name: String,
    callback_code_tx: Mutex<Option<oneshot::Sender<String>>>,
}

#[endpoint {
    method = GET,
    path = "/__setup",
}]
async fn setup_page(
    rqctx: RequestContext<Arc<CreationServerState>>,
) -> Result<Response<Body>, HttpError> {
    let ctx = rqctx.context();
    let manifest = serde_json::json!({
        "name": ctx.app_name,
        "url": ctx.base_url,
        "hook_attributes": {
            "url": format!("{}/webhook", ctx.base_url),
        },
        "redirect_url": format!("{}/__setup/callback", ctx.base_url),
        "public": false,
        "default_permissions": {
            "checks": "write",
            "contents": "read",
            "metadata": "read",
            "pull_requests": "read",
            "members": "read",
        },
        "default_events": [
            "check_run",
            "check_suite",
            "create",
            "delete",
            "public",
            "pull_request",
            "push",
            "repository",
        ],
    })
    .to_string()
    /*
     * Basic escaping to ensure we don't mess up the HTML.  All of the inputs we
     * deal with are trusted, so we don't care about proper escaping.
     */
    .replace('"', "&quot;");

    let body = include_str!("./index.html")
        .replace("{{secret}}", &rqctx.context().secret)
        .replace("{{manifest}}", &manifest);

    let mut response = Response::new(Body::from(body));
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("text/html"));
    Ok(response)
}

#[derive(serde::Deserialize, schemars::JsonSchema)]
struct CallbackQuery {
    state: String,
    code: String,
}

#[endpoint {
    method = GET,
    path = "/__setup/callback"
}]
async fn callback_page(
    rqctx: RequestContext<Arc<CreationServerState>>,
    query: Query<CallbackQuery>,
) -> Result<Response<Body>, HttpError> {
    let ctx = rqctx.context();
    let query = query.into_inner();

    let body = if query.state == ctx.secret {
        /*
         * We might receive another callback before we can properly shut down
         * the HTTP server.  In that case, discard the new callback.
         */
        if let Some(tx) = ctx.callback_code_tx.lock().unwrap().take() {
            let _ = tx.send(query.code);
        }
        include_str!("./success.html")
    } else {
        include_str!("./failure.html")
    };

    let mut response = Response::new(Body::from(body));
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("text/html"));
    Ok(response)
}

#[derive(serde::Deserialize)]
struct CreateAppResponse {
    id: u64,
    slug: String,
    owner: CreateAppOwner,
    webhook_secret: String,
    pem: String,
}

#[derive(serde::Deserialize)]
struct CreateAppOwner {
    login: String,
}

#[derive(serde::Serialize)]
struct Config {
    id: u64,
    webhook_secret: String,
    base_url: String,
    confroot: String,
    allow_owners: Vec<String>,

    buildomat: ConfigBuildomat,
    sqlite: ConfigSqlite,
}

#[derive(serde::Serialize)]
struct ConfigBuildomat {
    url: String,
    token: String,
}

#[derive(serde::Serialize)]
struct ConfigSqlite {}
