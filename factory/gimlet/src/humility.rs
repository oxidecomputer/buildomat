/*
 * Copyright 2025 Oxide Computer Company
 */

use std::{os::unix::process::CommandExt as _, process::{Command, Stdio}};

use crate::pipe::*;

struct HiffyCaller {
    humility: String,
    method: String,
    args: Vec<(String, String)>,
}

struct HiffyOutcome {
    stdout: String,
    #[allow(unused)]
    key: String,
    value: Value,
}

impl HiffyOutcome {
    fn unit(&self) -> Result<()> {
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
    fn arg(&mut self, k: &str, v: &str) -> &mut HiffyCaller {
        self.args.push((k.into(), v.into()));
        self
    }

    fn describe(&self) -> String {
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

    fn call_input(&mut self, buf: &[u8]) -> Result<HiffyOutcome> {
        println!(
            " * hiffy call {} (input {} bytes)...",
            self.describe(),
            buf.len()
        );

        let mut cmd = Command::new(&self.humility);
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
                eprintln!("WARNING: {v}");
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

    fn call_output(&mut self, size: u64) -> Result<(String, Vec<u8>)> {
        println!(" * hiffy call {} (output {size} bytes)...", self.describe());

        let mut cmd = Command::new(&self.humility);
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

    fn call(&mut self) -> Result<HiffyOutcome> {
        println!(" * hiffy call {}...", self.describe());

        let mut cmd = Command::new(&self.humility);
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
                eprintln!("WARNING: {v}");
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



