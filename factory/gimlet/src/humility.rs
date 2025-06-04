#![allow(unused)]

/*
 * Copyright 2025 Oxide Computer Company
 */

use std::{
    io::{Read, Write},
    os::{fd::FromRawFd, unix::process::CommandExt},
    process::{Command, Stdio},
};

use anyhow::{anyhow, bail, Result};
use debug_parser::{Value, ValueKind};
use slog::{info, warn, Logger};

use crate::pipe::*;
use buildomat_common::OutputExt;

pub trait ValueExt {
    fn is_unit(&self) -> bool;
    fn from_hex(&self) -> Result<u64>;
    fn from_bytes(&self) -> Result<Vec<u8>>;
    fn from_bytes_utf8(&self) -> Result<String>;
}

impl ValueExt for Value {
    fn is_unit(&self) -> bool {
        if self.name.is_some() {
            false
        } else {
            match &self.kind {
                ValueKind::Tuple(u) => u.values.is_empty(),
                _ => false,
            }
        }
    }

    fn from_hex(&self) -> Result<u64> {
        match &self.kind {
            ValueKind::Term(t) => {
                Ok(u64::from_str_radix(t.trim().trim_start_matches("0x"), 16)?)
            }
            _ => bail!("wrong kind?"),
        }
    }

    fn from_bytes(&self) -> Result<Vec<u8>> {
        match &self.kind {
            ValueKind::List(l) => {
                let mut out = Vec::new();

                for v in &l.values {
                    match &v.kind {
                        ValueKind::Term(t) => {
                            let Some(hex) = t.strip_prefix("0x") else {
                                bail!("expected a hex number, got {t:?}");
                            };

                            out.push(u8::from_str_radix(&hex, 16)?);
                        }
                        _ => bail!("list should only contain terms"),
                    }
                }

                Ok(out)
            }
            _ => bail!("value is not a list"),
        }
    }

    fn from_bytes_utf8(&self) -> Result<String> {
        Ok(String::from_utf8(self.from_bytes()?)?)
    }
}

pub struct HiffyCaller {
    pub log: Logger,
    pub humility: String,
    pub method: String,
    pub archive: String,
    pub probe: Option<String>,
    pub args: Vec<(String, String)>,
}

pub struct HiffyOutcome {
    pub stdout: String,
    #[allow(unused)]
    pub key: String,
    pub value: Value,
}

impl HiffyOutcome {
    pub fn unit(&self) -> Result<()> {
        if self.stdout.contains("Err(") {
            bail!("looks like an error: {:?}", self.stdout);
        }

        if !self.value.is_unit() {
            bail!("result is not (): {:?}", self.stdout);
        }

        Ok(())
    }
}

impl HiffyCaller {
    pub fn arg(&mut self, k: &str, v: &str) -> &mut HiffyCaller {
        self.args.push((k.into(), v.into()));
        self
    }

    pub fn describe(&self) -> String {
        let mut msg = format!("{}(", self.method);
        for (i, (k, v)) in self.args.iter().enumerate() {
            if i > 0 {
                msg.push_str(", ");
            }
            msg.push_str(&format!("{k} = {v:?}"));
        }
        msg.push(')');
        msg
    }

    pub fn humility(&self) -> Command {
        let mut cmd = std::process::Command::new("/usr/bin/pfexec");

        cmd.env_clear();
        cmd.env("HUMILITY_ARCHIVE", &self.archive);
        if let Some(probe) = &self.probe {
            cmd.env("HUMILITY_PROBE", probe);
        }

        cmd.arg(&self.humility);

        cmd
    }

    pub fn call_input(&mut self, buf: &[u8]) -> Result<HiffyOutcome> {
        info!(
            self.log,
            "hiffy call {} (input {} bytes)",
            self.describe(),
            buf.len(),
        );

        let mut cmd = self.humility();
        cmd.arg("hiffy");

        cmd.arg("-c");
        cmd.arg(&self.method);

        for (k, v) in &self.args {
            cmd.arg("-a");
            cmd.arg(format!("{k}={v}"));
        }

        cmd.arg("-i");
        cmd.arg("/dev/fd/4");

        let (rpipe, wpipe) = mkpipe()?;
        let rpipec = rpipe.clone();
        let wpipec = wpipe.clone();
        unsafe {
            cmd.pre_exec(move || {
                wpipec.close();
                let fd = rpipec.take();
                if libc::dup2(fd, 4) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                closefrom(5);
                Ok(())
            });
        }

        cmd.stdin(Stdio::null());
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());
        let child = cmd.spawn()?;

        let mut writef = unsafe { std::fs::File::from_raw_fd(wpipe.take()) };
        rpipe.close();

        let buf = buf.to_vec();
        let jh = std::thread::spawn(move || -> Result<()> {
            writef.write_all(&buf)?;
            Ok(())
        });

        let out = child
            .wait_with_output()
            .map_err(|e| anyhow!("command output: {e}"))?;

        jh.join().unwrap().map_err(|e| anyhow!("writing to pipe: {e}"))?;

        if !out.status.success() {
            bail!("could not invoke {:?}: {}", self.method, out.info());
        }

        let out = out.stdout_string()?.trim_end_matches('\n').to_string();
        if let Some((k, v)) = out.split_once(" => ") {
            let parsed = debug_parser::parse(v);
            if parsed.name.as_deref() == Some("Err") {
                warn!(self.log, "{v}");
            }

            Ok(HiffyOutcome {
                stdout: out.to_string(),
                key: k.to_string(),
                value: parsed,
            })
        } else {
            bail!("humility output was weird: {out:?}");
        }
    }

    pub fn call_output(&mut self, size: u64) -> Result<(String, Vec<u8>)> {
        info!(
            self.log,
            "hiffy call {} (output {size} bytes)...",
            self.describe(),
        );

        let mut cmd = self.humility();
        cmd.arg("hiffy");

        cmd.arg("-c");
        cmd.arg(&self.method);

        for (k, v) in &self.args {
            cmd.arg("-a");
            cmd.arg(format!("{k}={v}"));
        }

        cmd.arg("-o");
        cmd.arg("/dev/fd/4");

        cmd.arg("-n");
        cmd.arg(size.to_string());

        let (rpipe, wpipe) = mkpipe()?;
        let rpipec = rpipe.clone();
        let wpipec = wpipe.clone();
        unsafe {
            cmd.pre_exec(move || {
                rpipec.close();
                let fd = wpipec.take();
                if libc::dup2(fd, 4) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                closefrom(5);
                Ok(())
            });
        }

        cmd.stdin(Stdio::null());
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());
        let child = cmd.spawn()?;

        let mut readf = unsafe { std::fs::File::from_raw_fd(rpipe.take()) };
        wpipe.close();

        let jh = std::thread::spawn(move || -> Result<Vec<u8>> {
            let mut buf = Vec::new();
            readf.read_to_end(&mut buf)?;
            Ok(buf)
        });

        let out = child
            .wait_with_output()
            .map_err(|e| anyhow!("command output: {e}"))?;

        let buf = jh
            .join()
            .unwrap()
            .map_err(|e| anyhow!("reading from pipe: {e}"))?;

        if !out.status.success() {
            bail!("could not invoke {:?}: {}", self.method, out.info());
        }

        let stdout = out.stdout_string()?;
        if stdout.lines().any(|l| l.contains(" => Err(")) {
            bail!("could not invoke {:?}: {}", self.method, stdout);
        }

        Ok((stdout, buf))
    }

    pub fn call(&mut self) -> Result<HiffyOutcome> {
        info!(self.log, "hiffy call {}...", self.describe());

        let mut cmd = self.humility();
        cmd.arg("hiffy");

        cmd.arg("-c");
        cmd.arg(&self.method);

        for (k, v) in &self.args {
            cmd.arg("-a");
            cmd.arg(format!("{k}={v}"));
        }

        let out = cmd.output()?;

        if !out.status.success() {
            bail!("could not invoke {:?}: {}", self.method, out.info());
        }

        let out = out.stdout_string()?.trim_end_matches('\n').to_string();
        if let Some((k, v)) = out.split_once(" => ") {
            let parsed = debug_parser::parse(v);
            if parsed.name.as_deref() == Some("Err") {
                warn!(self.log, "{v}");
            }

            Ok(HiffyOutcome {
                stdout: out.to_string(),
                key: k.to_string(),
                value: parsed,
            })
        } else {
            bail!("humility output was weird: {out:?}");
        }
    }
}

pub enum PathStep {
    Name(String),
    Map(String),
    Tuple(usize),
}

pub fn locate_term<'a>(v: &'a Value, path: &[PathStep]) -> Result<&'a Value> {
    let mut w = v;

    for ps in path {
        match ps {
            PathStep::Name(n) => {
                if let Some(wn) = &w.name {
                    if wn != n {
                        bail!("wanted name {n:?}, got {wn:?}");
                    }
                } else {
                    bail!("wanted name {n:?}, but value has no name");
                }
            }
            PathStep::Map(mk) => match &w.kind {
                ValueKind::Map(m) => {
                    if let Some(kv) = m.values.iter().find(|a| &a.key == mk) {
                        w = &kv.value;
                    } else {
                        bail!("could not find key {mk:?} in map");
                    }
                }
                _ => bail!("wanted a map, but had some other kind"),
            },
            PathStep::Tuple(idx) => match &w.kind {
                ValueKind::Tuple(t) => {
                    if *idx >= t.values.len() {
                        bail!(
                            "wanted tuple value {idx}, but only {} values",
                            t.values.len()
                        );
                    }

                    w = &t.values[*idx];
                }
                _ => bail!("wanted a map, but had some other kind"),
            },
        }
    }

    Ok(w)
}
