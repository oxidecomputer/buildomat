/*
 * Copyright 2026 Oxide Computer Company
 */

mod factory_aws;
mod github_server;
mod server;
mod with_api;

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::process::Stdio;

use anyhow::{bail, Context as _, Result};
use buildomat_common::genkey;
use dialoguer::theme::ColorfulTheme;
use dialoguer::MultiSelect;
use tokio::process::Command;

#[tokio::main]
async fn main() -> Result<()> {
    let root = find_cargo_workspace_root().await?.join(".local");
    std::fs::create_dir_all(&root)?;

    /*
     * Some parts of the setup process spawn the server in the background to
     * make API calls to it.  Build it ahead of time.
     */
    println!("Building the buildomat server...");
    run(cargo().args(["build", "-p", "buildomat-server"])).await?;
    println!();

    /*
     * A random "setup name", and it will be used to distinguish this local
     * setup from others (for example as the GitHub App name, or in AWS tags).
     */
    let setup_name_file = root.join("setup-name");
    let setup_name = if setup_name_file.exists() {
        std::fs::read_to_string(&setup_name_file)
            .context("failed to read the setup name file")?
            .trim()
            .to_string()
    } else {
        let setup_name = format!("bmat-local-{}", genkey(8));
        std::fs::write(&setup_name_file, format!("{setup_name}\n"))
            .context("failed to write the setup name file")?;
        setup_name
    };

    let input_theme = ColorfulTheme {
        success_prefix: dialoguer::console::style(String::new()),
        ..ColorfulTheme::default()
    };

    let ctx = Context { root, setup_name, input_theme };

    let mut functions: Vec<Option<Pin<Box<dyn Future<Output = Result<()>>>>>> =
        Vec::new();
    let mut selector = MultiSelect::with_theme(&ctx.input_theme).with_prompt(
        "Select which components to initialize (<space> to toggle)",
    );
    if !ctx.root.join("server").join("complete").exists() {
        selector = selector.item_checked("buildomat-server", true);
        functions.push(Some(Box::pin(server::setup(&ctx))));
    }
    if !ctx.root.join("factory-aws").join("complete").exists() {
        selector = selector.item_checked("buildomat-factory-aws", true);
        functions.push(Some(Box::pin(factory_aws::setup(&ctx))));
    }
    if !ctx.root.join("github-server").join("complete").exists() {
        selector = selector.item_checked("buildomat-github-server", true);
        functions.push(Some(Box::pin(github_server::setup(&ctx))));
    }
    if functions.is_empty() {
        println!("Everything is already setup!");
    } else {
        for idx in selector.interact()? {
            functions.get_mut(idx).and_then(|f| f.take()).unwrap().await?;
        }
    }

    Ok(())
}

struct Context {
    root: PathBuf,
    setup_name: String,
    input_theme: ColorfulTheme,
}

async fn find_cargo_workspace_root() -> Result<PathBuf> {
    #[derive(serde::Deserialize)]
    struct CargoMetadata {
        workspace_root: PathBuf,
    }

    let metadata = run_stdout(cargo().args(["metadata", "--format-version=1"]))
        .await
        .context("failed to retrieve the workspace root from Cargo")?;
    Ok(serde_json::from_str::<CargoMetadata>(&metadata)
        .context("invalid output of \"cargo metadata\"")?
        .workspace_root)
}

async fn run(command: &mut Command) -> Result<()> {
    let name = command.as_std().get_program().to_os_string();

    let status = command
        .spawn()
        .with_context(|| format!("failed to invoke \"{}\"", name.display()))?
        .wait()
        .await
        .with_context(|| format!("failed to invoke \"{}\"", name.display()))?;
    if !status.success() {
        bail!("\"{}\" failed with {status}", name.display());
    }

    Ok(())
}

async fn run_stdout(command: &mut Command) -> Result<String> {
    let name = command.as_std().get_program().to_os_string();
    command.stdout(Stdio::piped());

    let output = command
        .spawn()
        .with_context(|| format!("failed to invoke \"{}\"", name.display()))?
        .wait_with_output()
        .await
        .with_context(|| format!("failed to invoke \"{}\"", name.display()))?;
    if !output.status.success() {
        bail!("\"{}\" failed with {}", name.display(), output.status);
    }

    String::from_utf8(output.stdout).with_context(|| {
        format!("\"{}\" emitted non-UTF-8 data to stdout", name.display())
    })
}

fn cargo() -> Command {
    Command::new(std::env::var_os("CARGO").expect("not running under Cargo"))
}
