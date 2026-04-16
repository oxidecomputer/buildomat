/*
 * Copyright 2025 Oxide Computer Company
 */

use std::cmp::Ordering;
use std::io::{Seek, Write};
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{bail, Context as _, Result};
use tempfile::NamedTempFile;

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
    let status = cargo()
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

#[derive(Copy, Clone, Debug)]
enum AgentBuild {
    Linux,
    Helios,
}

impl AgentBuild {
    fn job_name(&self) -> &str {
        match self {
            AgentBuild::Linux => "build-linux-agent",
            AgentBuild::Helios => "build-helios-agent",
        }
    }

    fn script(&self) -> &str {
        match self {
            AgentBuild::Linux => {
                include_str!("../scripts/build_linux_agent.sh")
            }
            AgentBuild::Helios => {
                include_str!("../scripts/build_helios_agent.sh")
            }
        }
    }

    fn output_filename(&self) -> &str {
        match self {
            AgentBuild::Linux => "buildomat-agent-linux.gz",
            AgentBuild::Helios => "buildomat-agent.gz",
        }
    }

    fn use_target(&self) -> &str {
        /*
         * We should use the earliest version of a target when building the
         * agent.  Using a newer target may result in binaries that don't work
         * in older images.
         */
        match self {
            AgentBuild::Linux => "ubuntu-18.04",
            AgentBuild::Helios => "helios-2.0-20240204",
        }
    }
}

fn build_agent(ab: AgentBuild) -> Result<()> {
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
        .arg(ab.job_name())
        .arg("-c")
        .arg(ab.script())
        .arg("-t")
        .arg(ab.use_target())
        .arg("-O")
        .arg(format!("=/out/{}", ab.output_filename()))
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
        .arg(format!("/out/{}", ab.output_filename()))
        .arg(format!("./{}", ab.output_filename()))
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()?;

    println!("unpacking agent...");

    Command::new("gunzip")
        .arg(format!("./{}", ab.output_filename()))
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

    let res = cargo()
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

fn local_setup() -> Result<()> {
    /*
     * This command is implemented as a separate crate because it depends on the
     * AWS SDK, reqwest and dropshot.  Adding all of those dependencies to xtask
     * would unreasonably slow down compilation.
     */
    Err(cargo().args(["run", "--bin", "xtask-setup"]).exec().into())
}

fn local_buildomat() -> Result<()> {
    let local = local_setup_root_for("server")?;
    Err(cargo()
        .args(["run", "--bin", "buildomat", "--"])
        .args(std::env::args_os().skip(3).collect::<Vec<_>>())
        .env("BUILDOMAT_CONFIG", local.join("cli-config.toml"))
        .exec()
        .into())
}

fn local_build_linux_agent(dest: &Path) -> Result<()> {
    eprintln!("building the agent for Linux...");
    let status = cargo()
        .args(["build", "--bin", "buildomat-agent"])
        .arg("--release")
        .arg("--target=x86_64-unknown-linux-musl")
        .status()?;
    if !status.success() {
        std::process::exit(status.code().unwrap_or(1));
    }
    std::fs::copy(
        PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").unwrap()).join(
            "../target/x86_64-unknown-linux-musl/release/buildomat-agent",
        ),
        dest,
    )?;

    Ok(())
}

fn local_build_illumos_agent(dest: &Path) -> Result<()> {
    eprintln!("building the agent for illumos...");
    let status = cargo()
        .args(["build", "--bin", "buildomat-agent"])
        .arg("--release")
        .arg("--target=x86_64-unknown-illumos")
        .status()?;
    if !status.success() {
        std::process::exit(status.code().unwrap_or(1));
    }
    std::fs::copy(
        PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").unwrap())
            .join("../target/x86_64-unknown-illumos/release/buildomat-agent"),
        dest,
    )?;

    Ok(())
}

fn local_buildomat_server() -> Result<()> {
    let local = local_setup_root_for("server")?;

    /*
     * The server requires the agent binary to be present in the current working
     * directory, so that it can serve it to workers.  To avoid confusion when
     * making changes to the agent but forgetting to recompile it, we compile it
     * on each build (changes not touching the agent should be instant anyway).
     *
     * We are unconditionally building the x86_64-unknown-linux-musl variant of
     * the agent.  Linux is what xtask-setup configures the AWS factory to use,
     * and using musl prevents issues with older glibcs or building on NixOS.
     *
     * When the illumos target is installed, we also build the illumos agent.
     */
    local_build_linux_agent(&local.join("buildomat-agent-linux"))?;
    if is_target_installed("x86_64-unknown-illumos") {
        local_build_illumos_agent(&local.join("buildomat-agent"))?;
    }

    eprintln!();
    eprintln!("running the server...");
    Err(cargo()
        .args(["run", "--bin", "buildomat-server", "--"])
        /*
         * The server requires both an explicit configuration file and to be
         * executed from the correct working directory.
         */
        .arg("-f")
        .arg(local.join("config.toml"))
        .current_dir(&local)
        .exec()
        .into())
}

fn local_buildomat_factory_aws() -> Result<()> {
    let local = local_setup_root_for("factory-aws")?;
    Err(cargo()
        .args(["run", "--bin", "buildomat-factory-aws", "--"])
        .arg("-f")
        .arg(local.join("config.toml"))
        .exec()
        .into())
}

fn local_buildomat_github_server() -> Result<()> {
    let local = local_setup_root_for("github-server")?;
    Err(cargo()
        .args(["run", "--bin", "buildomat-github-server", "--"])
        .current_dir(&local)
        .exec()
        .into())
}

fn local_setup_root_for(component: &str) -> Result<PathBuf> {
    let env = std::env::var_os("CARGO_MANIFEST_DIR")
        .context("xtask is not running under Cargo")?;
    let root = PathBuf::from(env)
        .join("..")
        .join(".local")
        .join(component)
        .canonicalize()?;

    if !root.exists() {
        bail!("{component} is not ready, run \"cargo xtask local setup\"");
    }
    Ok(root)
}

fn local() -> Result<()> {
    subcommands(
        2,
        &[
            ("setup", "initialize the local environment", local_setup),
            ("buildomat", "run the CLI", local_buildomat),
            ("buildomat-server", "run the server", local_buildomat_server),
            (
                "buildomat-factory-aws",
                "run jobs on AWS",
                local_buildomat_factory_aws,
            ),
            (
                "buildomat-github-server",
                "integrate with GitHub",
                local_buildomat_github_server,
            ),
        ],
    )
}

fn main() -> Result<()> {
    subcommands(
        1,
        &[
            ("openapi", "regenerate the server openapi.json", openapi),
            (
                "build-linux-agent",
                "start a buildomat job to build the agent on Linux",
                || build_agent(AgentBuild::Linux),
            ),
            (
                "build-agent",
                "start a buildomat job to build the agent on Helios",
                || build_agent(AgentBuild::Helios),
            ),
            ("crates", "list the crates in the workspace", crates),
            ("local", "run buildomat locally", local),
        ],
    )
}

/*
 * Exceedingly simple command line parser with support for xtask, only
 * supporting -h/--help and subcommands.  We only need those features right now
 * so pulling a library doesn't make sense.  If we reach a point of needing more
 * complex argument parsing we should replace this with a proper library.
 */
fn subcommands(
    level: usize,
    commands: &[(&str, &str, fn() -> Result<()>)],
) -> Result<()> {
    let mut cmd = std::env::args().nth(level);

    /*
     * Treat --help/-h as no subcommand, which shows the help message.
     */
    if cmd.as_deref() == Some("--help") || cmd.as_deref() == Some("-h") {
        cmd = None;
    }

    for (candidate, _, function) in commands {
        if cmd.as_deref() == Some(*candidate) {
            return function();
        }
    }

    let padding = commands.iter().map(|c| c.0.len()).max().unwrap() + 3;
    eprintln!("available subcommands:");
    for (command, description, _) in commands {
        eprintln!("- {command:<padding$}{description}");
    }

    if let Some(cmd) = cmd {
        eprintln!();
        eprintln!("error: unknown subcommand {cmd}");
        std::process::exit(1);
    } else {
        Ok(())
    }
}

fn cargo() -> Command {
    Command::new(std::env::var_os("CARGO").expect("not running under Cargo"))
}

fn is_target_installed(target: &str) -> bool {
    let output = Command::new("rustc")
        .arg("--print=sysroot")
        .output()
        .expect("failed to invoke \"rustc --print=sysroot\"");
    if !output.status.success() {
        panic!("\"rustc --print=sysroot\" exited with {}", output.status);
    }

    let sysroot = PathBuf::from(
        String::from_utf8(output.stdout)
            .expect("non-UTF-8 sysroot")
            .trim_end_matches('\n'),
    );

    sysroot.join("lib").join("rustlib").join(target).is_dir()
}
