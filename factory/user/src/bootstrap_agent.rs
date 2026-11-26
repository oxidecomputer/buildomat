/*
 * Copyright 2026 Oxide Computer Company
 */

use anyhow::{anyhow, bail, Context as _, Result};
use async_compression::tokio::write::GzipDecoder;
use buildomat_client::prelude::StreamExt as _;
use smf::{scf_type_t, Property, PropertyGroup, Scf, Snapshot};
use std::collections::{BTreeMap, HashMap};
use std::os::unix::fs::{chroot, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;
use tempfile::{NamedTempFile, TempPath};
use tokio::fs::File;
use tokio::io::BufWriter;

pub(crate) async fn run() -> Result<()> {
    let ctx = BootstrapContext::from_smf()?;

    /*
     * We want to run the agent inside of a chroot where most of the filesystem
     * is read-only and we have isolated writeable directories.  The factory
     * prepares the chroot for us, we just need to get into it.
     */
    chroot(&ctx.chroot)
        .with_context(|| format!("failed to chroot in {:?}", ctx.chroot))?;

    let agent = download_agent(&ctx).await?;
    mark_executable(&agent).await?;

    let mut cmd = Command::new(&agent);
    cmd.arg("install");
    for (k, v) in &ctx.env {
        cmd.arg("-e").arg(format!("{k}={v}"));
    }
    cmd.arg("--no-service");
    cmd.arg("--no-setuid");
    cmd.arg("--use-var-run");
    cmd.arg(&ctx.baseurl).arg(&ctx.bootstrap_token);

    cmd.spawn().context("failed to start the agent")?;

    Ok(())
}

async fn download_agent(ctx: &BootstrapContext) -> Result<TempPath> {
    let baseurl = &ctx.baseurl;
    let query = agent_query_string()?;
    loop {
        println!("attempting to download the gz agent...");
        match download_gz(format!("{baseurl}/file/agent.gz?{query}")).await {
            Ok(dest) => return Ok(dest),
            Err(e) => {
                eprintln!("failed to download gz agent: {e}");
            }
        }

        println!("attempting to download the plaintext agent...");
        match download_plain(format!("{baseurl}/file/agent?{query}")).await {
            Ok(dest) => return Ok(dest),
            Err(e) => {
                eprintln!("failed to download plaintext agent: {e}");
            }
        }

        println!("downloads failed, retrying in a second");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn download_gz(url: String) -> Result<TempPath> {
    let (dest, dest_path) = NamedTempFile::new()?.into_parts();

    let mut resp = reqwest::get(&url).await?.error_for_status()?.bytes_stream();
    let mut dest = GzipDecoder::new(BufWriter::new(File::from_std(dest)));

    while let Some(chunk) = resp.next().await {
        tokio::io::copy(&mut chunk?.as_ref(), &mut dest).await.with_context(
            || format!("failed to download {url:?} to {dest_path:?}"),
        )?;
    }

    Ok(dest_path)
}

async fn download_plain(url: String) -> Result<TempPath> {
    let (dest, dest_path) = NamedTempFile::new()?.into_parts();

    let mut resp = reqwest::get(&url).await?.error_for_status()?.bytes_stream();
    let mut dest = BufWriter::new(File::from_std(dest));

    while let Some(chunk) = resp.next().await {
        tokio::io::copy(&mut chunk?.as_ref(), &mut dest).await.with_context(
            || format!("failed to download {url:?} to {dest_path:?}"),
        )?;
    }

    Ok(dest_path)
}

async fn mark_executable(path: &Path) -> Result<()> {
    let mut perms = tokio::fs::metadata(path).await?.permissions();
    perms.set_mode(0o755);
    tokio::fs::set_permissions(path, perms).await?;
    Ok(())
}

/*
 * Give the server some hints as to what OS we're running so that it can give us
 * the most appropriate agent binary.
 */
fn agent_query_string() -> Result<String> {
    let os_release = std::fs::read_to_string("/etc/os-release")
        .context("failed to read /etc/os-release")?;
    let mut os_release = os_release
        .split('\n')
        .map(|line| line.trim())
        .filter(|line| !line.is_empty())
        .flat_map(|line| line.split_once('='))
        .collect::<HashMap<_, _>>();

    let mut query = Vec::new();
    query.push(format!("kernel={}", command_output("uname", &["-r"])?));
    query.push(format!("proc={}", command_output("uname", &["-p"])?));
    query.push(format!("mach={}", command_output("uname", &["-m"])?));
    query.push(format!("plat={}", command_output("uname", &["-i"])?));
    query.push(format!("id={}", os_release.remove("ID").unwrap_or_default()));
    query.push(format!(
        "id_like={}",
        os_release.remove("ID_LIKE").unwrap_or_default()
    ));
    query.push(format!(
        "version_id={}",
        os_release.remove("VERSION_ID").unwrap_or_default()
    ));
    Ok(query.join("&"))
}

#[derive(Debug)]
pub(crate) struct BootstrapContext {
    pub(crate) baseurl: String,
    pub(crate) bootstrap_token: String,
    pub(crate) chroot: PathBuf,
    pub(crate) env: BTreeMap<String, String>,
}

impl BootstrapContext {
    fn from_smf() -> Result<Self> {
        let scf = Scf::new().context("failed to connect to SMF")?;
        let inst = scf
            .get_self_instance()
            .context("failed to get the current SMF instance")?;
        let snapshot = inst
            .get_running_snapshot()
            .context("failed to get the running SMF snapshot")?;

        let pg = get_pg(&snapshot, "buildomat")?;
        let pg_env = get_pg(&snapshot, "buildomat-env")?;

        let mut env = BTreeMap::new();
        for prop in pg_env.properties().context("failed to list properties")? {
            let prop = prop.context("failed to list properties")?;
            env.insert(prop.name()?, prop_value(&pg_env, &prop)?);
        }

        Ok(BootstrapContext {
            baseurl: get_prop(&pg, "baseurl")?,
            bootstrap_token: get_prop(&pg, "bootstrap_token")?,
            chroot: get_prop(&pg, "chroot")?.into(),
            env,
        })
    }
}

fn get_pg<'a>(snap: &'a Snapshot<'a>, name: &str) -> Result<PropertyGroup<'a>> {
    snap.get_pg(name)
        .with_context(|| format!("failed to get property group {name:?}"))?
        .ok_or_else(|| anyhow!("missing property group {name:?}"))
}

fn get_prop(pg: &PropertyGroup<'_>, name: &str) -> Result<String> {
    let id = format!("{}/{name}", pg.name()?);
    let prop = pg
        .get_property(name)
        .with_context(|| format!("failed to get property {id:?}"))?
        .ok_or_else(|| anyhow!("missing property {id:?}"))?;

    prop_value(pg, &prop)
}

fn prop_value(pg: &PropertyGroup<'_>, prop: &Property<'_>) -> Result<String> {
    let id = format!("{}/{}", pg.name()?, prop.name()?);

    if !matches!(prop.type_()?, scf_type_t::SCF_TYPE_ASTRING) {
        bail!("property {id:?} is not of type astring");
    }

    let value = prop
        .value()
        .with_context(|| format!("failed to get value of property {id:?}"))?
        .ok_or_else(|| anyhow!("missing value of property {id:?}"))?;

    value
        .as_string()
        .with_context(|| format!("failed to coerce property {id:?} as string"))
}

fn command_output(cmd: &str, args: &[&str]) -> Result<String> {
    let output = Command::new(cmd)
        .args(args)
        .stdout(Stdio::piped())
        .spawn()?
        .wait_with_output()?;
    if !output.status.success() {
        bail!("{cmd} failed with {}", output.status);
    }
    Ok(String::from_utf8(output.stdout)
        .context("non-UTF-8 output")?
        .trim_end_matches(['\n', '\r'])
        .into())
}
