use std::path::PathBuf;
use std::process::{Command, Stdio};

use anyhow::{bail, Result};

fn openapi() -> Result<()> {
    let xtask_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    let buildomat_client_dir = {
        let mut t = xtask_dir.clone();
        assert!(t.pop());
        t.push("openapi");
        t
    };

    let buildomat_client_file = {
        let mut t = buildomat_client_dir.clone();
        t.push("openapi.json");
        t
    };
    let buildomat_client_tmp = {
        let mut t = buildomat_client_dir.clone();
        t.push("openapi.tmp.json");
        t
    };

    std::fs::remove_file(&buildomat_client_tmp).ok();
    let status = Command::new(env!("CARGO"))
        .arg("run")
        .arg("-p")
        .arg("buildomat-server")
        .arg("--")
        .arg("-S")
        .arg(&buildomat_client_tmp)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()?;

    if !status.success() {
        bail!("could not generate openapi.json");
    }

    std::fs::rename(&buildomat_client_tmp, &buildomat_client_file)?;
    println!("generated {:?}", &buildomat_client_file);

    Ok(())
}

fn main() -> Result<()> {
    match std::env::args().skip(1).next().as_deref() {
        Some("openapi") => openapi(),
        Some(_) | None => {
            bail!("do not know how to do that");
        }
    }
}
