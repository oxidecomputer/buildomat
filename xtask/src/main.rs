/*
 * Copyright 2025 Oxide Computer Company
 */

use std::cmp::Ordering;
use std::io::{Seek, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use tempfile::NamedTempFile;

use anyhow::{bail, Result};

pub trait OutputExt {
    fn info(&self) -> String;
}

impl OutputExt for std::process::Output {
    fn info(&self) -> String {
        let mut out = String::new();

        if let Some(code) = self.status.code() {
            out.push_str(&format!("exit code {}", code));
        }

        /*
         * Attempt to render stderr from the command:
         */
        let stderr = String::from_utf8_lossy(&self.stderr).trim().to_string();
        let extra = if stderr.is_empty() {
            /*
             * If there is no stderr output, this command might emit its
             * failure message on stdout:
             */
            String::from_utf8_lossy(&self.stdout).trim().to_string()
        } else {
            stderr
        };

        if !extra.is_empty() {
            if !out.is_empty() {
                out.push_str(": ");
            }
            out.push_str(&extra);
        }

        out
    }
}

fn openapi() -> Result<()> {
    let xtask_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    let buildomat_client_dir = {
        let mut t = xtask_dir.clone();
        assert!(t.pop());
        t.push("client");
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

fn build_linux_agent() -> Result<()> {
    let files = Command::new("git").arg("ls-files").output()?;

    if !files.status.success() {
        bail!("could not list files via git: {}", files.info());
    }

    let files = String::from_utf8(files.stdout)?;

    let mut cpio_list = NamedTempFile::new()?;
    for l in files.lines() {
        writeln!(cpio_list, "{l}")?;
    }
    cpio_list.flush()?;
    let (mut cpio_list, _) = cpio_list.into_parts();

    let cpio = NamedTempFile::new()?;
    let (cpio, cpio_path) = cpio.into_parts();

    cpio_list.rewind()?;
    let out = Command::new("cpio")
        .env_clear()
        .arg("-o")
        .arg("-q")
        .stdin(cpio_list)
        .stdout(cpio)
        .stderr(Stdio::piped())
        .output()?;

    if !out.status.success() {
        bail!("cpio error: {}", out.info());
    }

    let cpio_path = cpio_path.keep()?;
    println!("cpio @ {cpio_path:?}");

    let out = Command::new("buildomat")
        .arg("job")
        .arg("run")
        .arg("-W")
        .arg("-n")
        .arg("build-linux-agent")
        .arg("-c")
        .arg(include_str!("../scripts/build_linux_agent.sh"))
        .arg("-t")
        .arg("ubuntu-18.04")
        .arg("-O")
        .arg("=/out/buildomat-agent-linux.gz")
        .arg("-O")
        .arg("=/out/*.sha256.txt")
        .arg("-i")
        .arg(format!("src.cpio={}", cpio_path.to_str().unwrap()))
        .output()?;

    if !out.status.success() {
        bail!("could not create job: {}", out.info());
    }

    let jid = String::from_utf8(out.stdout)?.trim().to_string();
    println!("job ID -> {jid:?}");

    let out = Command::new("buildomat")
        .arg("job")
        .arg("tail")
        .arg(&jid)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()?;

    if !out.success() {
        bail!("job {jid} did not complete successfully");
    }

    println!("job {jid} completed!  outputs:");

    Command::new("buildomat")
        .arg("job")
        .arg("outputs")
        .arg(&jid)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()?;

    println!("downloading built agent...");

    Command::new("buildomat")
        .arg("job")
        .arg("copy")
        .arg(&jid)
        .arg("/out/buildomat-agent-linux.gz")
        .arg("./buildomat-agent-linux.gz")
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()?;

    println!("unpacking agent...");

    Command::new("gunzip")
        .arg("./buildomat-agent-linux.gz")
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()?;

    println!("ok");

    Ok(())
}

fn crates() -> Result<()> {
    let xtask_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let root = {
        let mut t = xtask_dir.clone();
        assert!(t.pop());
        t
    };

    let res = Command::new(env!("CARGO"))
        .arg("tree")
        .arg("--depth")
        .arg("0")
        .arg("--prefix")
        .arg("none")
        .arg("--workspace")
        .current_dir(&root)
        .output()?;

    if !res.status.success() {
        bail!("could not list crates in workspace?");
    }

    let mut crates = Vec::new();

    let out = String::from_utf8(res.stdout)?;
    for l in out.lines() {
        if l.trim().is_empty() {
            continue;
        }

        if let Some((p, tail)) = l.split_once(' ') {
            if let Some((_ver, path)) = tail.split_once(' ') {
                if let Some(tail) = path.strip_prefix('(') {
                    if let Some(path) = tail.strip_suffix(')') {
                        let n = p
                            .split('-')
                            .map(str::to_string)
                            .collect::<Vec<_>>();

                        crates.push((n, path.to_string()));
                        continue;
                    }
                }
            }
        }

        eprintln!("WARNING: weird line from cargo tree: {l:?}");
    }

    crates.sort_by(|a, b| {
        let aa = a.0.first().map(|v| v.starts_with("buildomat"));
        let bb = b.0.first().map(|v| v.starts_with("buildomat"));
        if aa.unwrap_or(false) && !bb.unwrap_or(false) {
            Ordering::Less
        } else if !aa.unwrap_or(false) && bb.unwrap_or(false) {
            Ordering::Greater
        } else {
            let al = a.0.len();
            let bl = b.0.len();

            if al != bl {
                al.cmp(&bl)
            } else {
                for i in 0..al {
                    match a.0.get(i).unwrap().cmp(b.0.get(i).unwrap()) {
                        o @ (Ordering::Less | Ordering::Greater) => return o,
                        Ordering::Equal => continue,
                    }
                }

                Ordering::Equal
            }
        }
    });

    for (n, p) in crates.iter() {
        if n.len() < 3 && n[0].starts_with("buildomat") {
            println!("{:<28} {}", n.join("-"), p);
        }
    }

    let mut prior = "".to_string();
    for (n, p) in crates.iter() {
        if n.len() >= 3 {
            let pfx = n[0..n.len() - 1].join("-");
            if pfx != prior {
                println!();
                prior = pfx;
            }
            println!("{:<28} {}", n.join("-"), p);
        }
    }

    println!();
    for (n, p) in crates.iter() {
        if n.len() < 3 && !n[0].starts_with("buildomat") {
            println!("{:<28} {}", n.join("-"), p);
        }
    }

    Ok(())
}

fn main() -> Result<()> {
    match std::env::args().nth(1).as_deref() {
        Some("openapi") => openapi(),
        Some("build-linux-agent") => build_linux_agent(),
        Some("crates") => crates(),
        Some(_) | None => {
            bail!("do not know how to do that");
        }
    }
}
