/*
 * Copyright 2025 Oxide Computer Company
 */

use std::{
    collections::HashMap,
    os::unix::{fs::OpenOptionsExt, io::AsRawFd},
    path::Path,
    process::{Command, Stdio},
    time::{Duration, Instant},
};

use anyhow::{anyhow, bail, Result};
use debug_parser::ValueKind;

use crate::humility::{self, HiffyCaller, PathStep, ValueExt};
use crate::{
    config::{ConfigFileHost, ConfigFileTools},
    disks::Slot,
};
use buildomat_common::*;

#[derive(Debug)]
struct Host {
    probe: String,
    archive: String,

    tools: ConfigFileTools,
    host: ConfigFileHost,
}

impl Host {
    fn hiffy(&self, cmd: &str) -> HiffyCaller {
        HiffyCaller {
            humility: self.tools.humility.to_string(),
            archive: self.archive.to_string(),
            probe: Some(self.probe.to_string()),
            method: cmd.to_string(),
            args: Default::default(),
        }
    }

    fn power_state(&self) -> Result<String> {
        let res = self.hiffy("Sequencer.get_state").call()?;
        Ok(res.value.to_string().trim().to_string())
    }

    fn power_off(&self) -> Result<()> {
        if self.power_state()? == "A2" {
            println!("already powered off");
            return Ok(());
        }

        println!("powering off");
        self.hiffy("Sequencer.set_state").arg("state", "A2").call()?.unit()?;
        Ok(())
    }

    fn power_on(&self) -> Result<()> {
        if self.power_state()? == "A0" {
            println!("already powered on");
            return Ok(());
        }

        println!("powering on");
        self.hiffy("Sequencer.set_state").arg("state", "A0").call()?.unit()?;
        Ok(())
    }

    fn boot_net(&self) -> Result<()> {
        println!("setting startup options to boot from network");
        self.hiffy("ControlPlaneAgent.set_startup_options")
            .arg("startup_options", "0x80")
            .call()?
            .unit()?;
        Ok(())
    }

    fn pick_bsu(&self, bsu: u32) -> Result<()> {
        println!("picking BSU {bsu}");
        self.hiffy("HostFlash.set_dev")
            .arg("dev", &format!("Flash{bsu}"))
            .call()?
            .unit()?;
        Ok(())
    }

    fn write_rom(&self, os_dir: &str) -> Result<()> {
        let file = format!("{os_dir}/rom");
        println!("writing rom {file:?}");

        let res = Command::new("/usr/bin/pfexec")
            .env_clear()
            .env("HUMILITY_ARCHIVE", &self.archive)
            .env("HUMILITY_PROBE", &self.probe)
            .arg(&self.tools.humility)
            .arg("qspi")
            .arg("-D")
            .arg(file)
            .stdout(Stdio::inherit())
            .stdin(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()?;

        if !res.success() {
            bail!("rom write failed");
        }

        Ok(())
    }

    fn boot_server(&self, os_dir: &str) -> Result<()> {
        let file = format!("{os_dir}/zfs.img");
        println!("running boot server for {file:?}");

        let res = Command::new("/usr/bin/pfexec")
            .env_clear()
            .arg(&self.tools.bootserver)
            .arg("-s")
            .arg("-t")
            .arg("300")
            .arg("e1000g0")
            .arg(file)
            .arg(&self.host.mac)
            .stdout(Stdio::inherit())
            .stdin(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()?;

        if !res.success() {
            bail!("boot server failed");
        }

        Ok(())
    }

    fn ssh_options() -> Vec<String> {
        vec![
            "StrictHostKeyChecking=no".to_string(),
            "UserKnownHostsFile=/dev/null".to_string(),
            "ConnectTimeout=5".to_string(),
            "LogLevel=error".to_string(),
            "User=root".to_string(),
        ]
    }

    pub fn ssh(&self) -> Command {
        let mut cmd = Command::new("/usr/bin/ssh");
        cmd.env_clear();

        for opt in Self::ssh_options() {
            cmd.arg("-o").arg(opt);
        }

        cmd.arg(&self.host.ip);

        cmd
    }

    pub fn scp(&self) -> Command {
        let mut cmd = Command::new("/usr/bin/scp");
        cmd.env_clear();

        for opt in Self::ssh_options() {
            cmd.arg("-o").arg(opt);
        }

        cmd
    }

    fn open(tools: &ConfigFileTools, host: &ConfigFileHost) -> Result<Self> {
        let tools = tools.clone();
        let host = host.clone();
        let slot = &host.slot;

        let output =
            Command::new(tools.gimlets).env_clear().arg(slot).output()?;
        if !output.status.success() {
            bail!("could not get status for slot {slot}: {}", output.info());
        }

        let out = String::from_utf8(output.stdout)?;
        let mut info = out
            .lines()
            .filter_map(|l| {
                if let Some(t) = l.strip_prefix("export ") {
                    let kv = t.split('=').collect::<Vec<_>>();
                    if kv.len() == 2 {
                        if let Some(v) = kv[1]
                            .strip_prefix('\'')
                            .and_then(|v| v.strip_suffix('\''))
                        {
                            return Some((kv[0].to_string(), v.to_string()));
                        }
                    }
                }

                None
            })
            .collect::<HashMap<String, String>>();

        let probe = info
            .remove("HUMILITY_PROBE")
            .ok_or_else(|| anyhow!("no HUMILITY_PROBE from gimlets?"))?;
        let archive = info
            .remove("HUMILITY_ARCHIVE")
            .ok_or_else(|| anyhow!("no HUMILITY_ARCHIVE from gimlets?"))?;

        /*
         * Attempt to raise the Gimlet and get its serial number.
         */
        let mut hc = HiffyCaller {
            humility: tools.humility.to_string(),
            archive: archive.to_string(),
            probe: Some(probe.to_string()),
            method: "ControlPlaneAgent.identity".to_string(),
            args: Default::default(),
        };

        let res = hc.call()?;

        let serial = humility::locate_term(
            &res.value,
            &[
                PathStep::Name("VpdIdentity".into()),
                PathStep::Map("serial".into()),
            ],
        )?
        .from_bytes_utf8()?;

        let partno = humility::locate_term(
            &res.value,
            &[
                PathStep::Name("VpdIdentity".into()),
                PathStep::Map("part_number".into()),
            ],
        )?
        .from_bytes_utf8()?;

        let revision = humility::locate_term(
            &res.value,
            &[
                PathStep::Name("VpdIdentity".into()),
                PathStep::Map("revision".into()),
            ],
        )?
        .from_hex()?;

        if serial != host.serial {
            bail!(
                "system in slot {slot} has serial {serial:?} != {:?}",
                host.serial,
            );
        }

        if partno != host.partno {
            bail!(
                "system in slot {slot} has partno {partno:?} != {:?}",
                host.partno,
            );
        }

        if revision != host.rev {
            bail!(
                "system in slot {slot} has revision {revision:?} != {:?}",
                host.rev,
            );
        }

        Ok(Self { probe, archive, tools, host })
    }
}
