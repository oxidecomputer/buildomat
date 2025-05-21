/*
 * Copyright 2025 Oxide Computer Company
 */

use std::{
    collections::HashMap,
    path::Path,
    process::{Command, Stdio},
};

use anyhow::{anyhow, bail, Result};
use debug_parser::ValueKind;

use buildomat_common::*;
use humility::{HiffyCaller, PathStep, ValueExt};

mod humility;
mod pipe;

#[derive(Debug)]
struct System {
    partno: String,
    revision: u64,
    serial: String,
    probe: String,
    archive: String,
}

impl System {
    fn hiffy(&self, cmd: &str) -> HiffyCaller {
        HiffyCaller {
            humility: "/usr/bin/humility".to_string(),
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

    /*
       *   +--> HostFlash.get_mux
    |       <ok>                        HfMuxState
    |       <error>                     HfError
    |
    +--> HostFlash.set_mux
    |       state                       HfMuxState
    |       <ok>                        ()
    |       <error>                     HfError
    |
    +--> HostFlash.get_dev
    |       <ok>                        HfDevSelect
    |       <error>                     HfError
    |
    +--> HostFlash.set_dev
    |       dev                         HfDevSelect
    |       <ok>                        ()
    |       <error>                     HfError
    |
    +--> HostFlash.hash
    |       address                     u32
    |       len                         u32
    |       <ok>                        [u8; drv_hash_api::SHA256_SZ]
    |       <error>                     HfError
    |
    +--> HostFlash.get_persistent_data
    |       <ok>                        HfPersistentData
    |       <error>                     HfError
    |
    +--> HostFlash.write_persistent_data
            dev_select                  HfDevSelect
            <ok>                        ()
            <error>                     HfError
      */

    fn write_rom<P: AsRef<Path>>(&self, file: P) -> Result<()> {
        let file = file.as_ref();
        println!("writing rom {file:?}");

        let res = Command::new("/usr/bin/pfexec")
            .env_clear()
            .env("HUMILITY_ARCHIVE", &self.archive)
            .env("HUMILITY_PROBE", &self.probe)
            .arg("/usr/bin/humility")
            .arg("qspi")
            .arg("-D")
            .arg("/staff/dock/buildomat/gimlet/os/latest/rom")
            .stdout(Stdio::inherit())
            .stdin(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()?;

        if !res.success() {
            bail!("rom write failed");
        }

        Ok(())
    }

    fn open(slot: &str) -> Result<Self> {
        let output =
            Command::new("/usr/bin/gimlets").env_clear().arg(&slot).output()?;
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
            humility: "/usr/bin/humility".to_string(),
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

        Ok(System { serial, partno, revision, probe, archive })
    }
}

fn main() -> Result<()> {
    let a = getopts::Options::new()
        .parsing_style(getopts::ParsingStyle::FloatingFrees)
        .parse(std::env::args_os().skip(1))?;

    if a.free.len() != 1 {
        bail!("which slot?");
    }

    let slot = a.free[0].to_string();

    let sys = System::open(&slot)?;

    println!("sys = {sys:#?}");

    sys.power_off()?;
    sys.boot_net()?;
    sys.pick_bsu(0)?;
    sys.write_rom("/staff/dock/buildomat/gimlet/os/latest/rom")?;
    sys.power_on()?;

    Ok(())
}
