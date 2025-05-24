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

use buildomat_common::*;
use disks::Slot;
use humility::{HiffyCaller, PathStep, ValueExt};

mod disks;
mod efi;
mod humility;
mod pipe;

#[derive(Debug, Clone)]
struct GlobalConfig {
    humility: String,
    bootserver: String,
}

#[derive(Debug, Clone)]
struct SlotConfig {
    slot: String,
    mac: String,
    ip: String,
    os: String,
}

#[derive(Debug)]
struct System {
    partno: String,
    revision: u64,
    serial: String,
    probe: String,
    archive: String,
    scfg: SlotConfig,
    gcfg: GlobalConfig,
}

impl System {
    fn hiffy(&self, cmd: &str) -> HiffyCaller {
        HiffyCaller {
            humility: self.gcfg.humility.to_string(),
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

    fn write_rom(&self) -> Result<()> {
        let file = format!("{}/rom", self.scfg.os);
        println!("writing rom {file:?}");

        let res = Command::new("/usr/bin/pfexec")
            .env_clear()
            .env("HUMILITY_ARCHIVE", &self.archive)
            .env("HUMILITY_PROBE", &self.probe)
            .arg(&self.gcfg.humility)
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

    fn boot_server(&self) -> Result<()> {
        let file = format!("{}/zfs.img", self.scfg.os);
        println!("running boot server for {file:?}");

        let res = Command::new("/usr/bin/pfexec")
            .env_clear()
            .arg(&self.gcfg.bootserver)
            .arg("-s")
            .arg("-t")
            .arg("300")
            .arg("e1000g0")
            .arg(file)
            .arg(&self.scfg.mac)
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

        cmd.arg(&self.scfg.ip);

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

    fn open(gcfg: &GlobalConfig, scfg: &SlotConfig) -> Result<Self> {
        let scfg = scfg.clone();
        let gcfg = gcfg.clone();
        let slot = &scfg.slot;

        let output =
            Command::new("/usr/bin/gimlets").env_clear().arg(slot).output()?;
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
            humility: gcfg.humility.to_string(),
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

        Ok(System { serial, partno, revision, probe, archive, scfg, gcfg })
    }
}

fn nvmeadm_format(ns: &str, fmt: Option<&str>) -> Result<()> {
    let mut cmd = Command::new("/usr/sbin/nvmeadm");
    cmd.env_clear();
    cmd.arg("format");
    cmd.arg(ns);
    if let Some(fmt) = fmt {
        cmd.arg(fmt);
    }
    let out = cmd.output()?;

    if !out.status.success() {
        let e = String::from_utf8_lossy(&out.stderr).trim().to_string();

        bail!("nvmeadm format {ns} {fmt:?} failed: {e}");
    }

    Ok(())
}

fn cleanup() -> Result<()> {
    const MAX_TIME: u64 = 60;

    println!(" * formatting disks...");
    let start = Instant::now();
    loop {
        let dur = Instant::now().saturating_duration_since(start);
        if dur.as_secs() > MAX_TIME {
            bail!("giving up after {MAX_TIME} seconds");
        }

        match format_disks() {
            Ok(()) => {
                println!(" * disks formatted!");
                break;
            }
            Err(e) => {
                println!(" * disk format failure: {e}");
                std::thread::sleep(Duration::from_secs(2));
                continue;
            }
        }
    }

    println!(" * preparing M.2 disks...");
    let start = Instant::now();
    loop {
        let dur = Instant::now().saturating_duration_since(start);
        if dur.as_secs() > MAX_TIME {
            bail!("giving up after {MAX_TIME} seconds");
        }

        match prepare_m2() {
            Ok(()) => {
                println!(" * M.2 disks prepared!");
                break;
            }
            Err(e) => {
                println!(" * M.2 disk preparation failure: {e}");
                std::thread::sleep(Duration::from_secs(2));
                continue;
            }
        }
    }

    Ok(())
}

fn prepare_m2() -> Result<()> {
    let disks = disks::list_disks()?;

    let Some(d) = disks.get(&Slot::M2(0)) else {
        bail!("could not find the M.2 disk for BSU0");
    };

    let chr = d.primary().chr.clone();
    let mut f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(&chr)
        .map_err(|e| anyhow!("open {chr:?} failure: {e}"))?;

    let mut vtoc = efi::Gpt::init_from_fd(f.as_raw_fd())?;

    /*
     * Knock together a partition table that meets our needs.
     */
    let nparts = vtoc.parts().len();
    let nrs = vtoc.reserved_sectors();
    let first = vtoc.lba_first();
    let last = vtoc.lba_last();

    /*
     * Create the reserved partition in the last slot:
     */
    let p = &mut vtoc.parts_mut()[nparts - 1];
    p.p_tag = efi::sys::V_RESERVED;
    p.p_start = last - u64::from(nrs) + 1;
    p.p_size = u64::from(nrs);

    /*
     * Create the four boot image slots:
     */
    let mut next_start = first;
    let size = (4 * 1024 * 1024 * 1024) / vtoc.block_size();
    for i in 0usize..4 {
        let p = &mut vtoc.parts_mut()[i];
        p.p_tag = efi::sys::V_USR;
        p.p_start = next_start;
        p.p_size = size.into();
        next_start = next_start.checked_add(size).unwrap();
    }

    /*
     * Create the dump slice:
     */
    let size = (1 * 1024 * 1024 * 1024 * 1024) / vtoc.block_size();
    let p = &mut vtoc.parts_mut()[4];
    p.p_tag = efi::sys::V_USR;
    p.p_start = next_start;
    p.p_size = size;
    next_start = next_start.checked_add(size).unwrap();

    /*
     * Use the remaining space for a ZFS pool slice.
     */
    let end = vtoc.parts()[nparts - 1].p_start - 1;
    let size = end - next_start + 1;
    let p = &mut vtoc.parts_mut()[5];
    p.p_tag = efi::sys::V_USR;
    p.p_start = next_start;
    p.p_size = size;

    vtoc.write_to_fd(f.as_raw_fd())?;

    /*
     * Create a plausible facsimile of the BSU pool we generally expect:
     */
    let id = uuid::Uuid::new_v4();
    let pool = format!("oxi_{id}");
    let out = Command::new("/sbin/zpool")
        .env_clear()
        .arg("create")
        .arg("-O")
        .arg(&format!("mountpoint=/pool/int/{id}"))
        .arg("-O")
        .arg("compress=on")
        .arg(&pool)
        .arg(&format!("{}s5", d.name))
        .output()?;

    if !out.status.success() {
        let e = String::from_utf8_lossy(&out.stderr).trim().to_string();

        bail!("zpool create failure: {e}");
    }

    println!(" * zpool {pool} created");

    /*
     * Update the BSU symlink:
     */
    std::fs::create_dir_all("/pool/bsu")?;
    std::fs::remove_file("/pool/bsu/0").ok();
    std::os::unix::fs::symlink(&format!("../int/{id}"), "/pool/bsu/0")?;

    /*
     * Create the swap device:
     */
    let out = Command::new("/usr/lib/buildomat/mkswap").env_clear().output()?;

    if !out.status.success() {
        let e = String::from_utf8_lossy(&out.stderr).trim().to_string();

        bail!("mkswap failure: {e}");
    }

    println!(" * swap device attached");

    Ok(())
}

fn format_disks() -> Result<()> {
    /*
     * First, list the disks in the system.  We expect there to be two M.2 SSDs
     * and nine U.2 SSDs.  The tenth U.2 SSD slot contains the K.2 NIC we use to
     * get the system on the network.
     */
    let disks = disks::list_disks()?;

    const WANT: usize = 2 + 9;
    if disks.len() != WANT {
        let list = disks
            .iter()
            .map(|d| d.slot.label().unwrap_or("?"))
            .collect::<Vec<_>>()
            .join(", ");

        bail!("want {WANT} disks, but found {}: {list}", disks.len());
    }

    /*
     * Format every disk that we found, so that no detritus from previous jobs
     * is left behind.
     */
    for d in disks {
        /*
         * Find the NVMe namespace for this disk.
         */
        let out = Command::new("/usr/sbin/nvmeadm")
            .env_clear()
            .arg("list")
            .arg("-p")
            .arg("-o")
            .arg("disk,instance,namespace")
            .output()?;

        if !out.status.success() {
            let e = String::from_utf8_lossy(&out.stderr).trim().to_string();

            bail!("nvmeadm list failure: {e}");
        }

        let Some(nvme) = String::from_utf8(out.stdout)?
            .lines()
            .filter_map(|l| {
                let t = l.split(':').collect::<Vec<_>>();
                if t.len() != 3 {
                    return None;
                }

                if t[0] == d.name {
                    return Some(format!("{}/{}", t[1], t[2]));
                }

                None
            })
            .next()
        else {
            bail!("could not locate NVMe namespace for {}", d.name);
        };

        let label = d.slot.label().unwrap_or("?");
        match d.slot {
            Slot::M2(_) => {
                /*
                 * We want to create a partition table on the M.2 devices.
                 * First, format them using the 4K sector format.
                 *
                 * XXX The format we're using, "1", depends on a lot of things
                 * including the drive model and the firmware.  This value is
                 * only correct for the narrow set of devices we're expecting to
                 * see in these Gimlet systems.
                 */
                println!("M.2 device {label} ({}): {nvme}", d.name);
                nvmeadm_format(&nvme, Some("1"))?;
            }
            Slot::U2(_) => {
                /*
                 * Just format the U.2 disks with the default settings and leave
                 * them blank.
                 */
                println!("U.2 device {label} ({}): {nvme}", d.name);
                //XXX nvmeadm_format(&nvme, None)?;
            }
        }
    }

    Ok(())
}

fn main() -> Result<()> {
    let exe = std::env::current_exe()?;

    let a = getopts::Options::new()
        .parsing_style(getopts::ParsingStyle::FloatingFrees)
        .parse(std::env::args_os().skip(1))?;

    if a.free.len() != 1 {
        bail!("which slot?");
    }

    if a.free[0] == "cleanup" {
        return cleanup();
    }

    let gcfg = GlobalConfig {
        humility: "/usr/bin/humility".to_string(),
        bootserver: "/staff/bin/bootserver".to_string(),
    };

    let scfg = SlotConfig {
        slot: a.free[0].to_string(),
        mac: "e8:ea:6a:09:7f:6a".to_string(),
        ip: "172.20.2.180".to_string(),
        os: "/staff/dock/buildomat/gimlet/os/latest".to_string(),
    };

    let sys = System::open(&gcfg, &scfg)?;

    println!("sys = {sys:#?}");

    let start = Instant::now();
    sys.power_off()?;
    sys.boot_net()?;
    sys.pick_bsu(0)?;
    sys.write_rom()?;
    sys.power_on()?;
    sys.boot_server()?;

    println!("waiting for system to come up at {}", sys.scfg.ip);
    let phase = Instant::now();
    let out = loop {
        let now = Instant::now();

        let dur = now.saturating_duration_since(phase);
        if dur.as_secs() > 300 {
            bail!("gave up after 300 seconds");
        }

        let out = sys.ssh().arg("pilot local info -j").output()?;
        if !out.status.success() {
            std::thread::sleep(Duration::from_secs(1));
            continue;
        }

        let Ok(out) = String::from_utf8(out.stdout) else {
            std::thread::sleep(Duration::from_secs(1));
            continue;
        };

        break out;
    };

    println!("got output: | {out} |");

    let dur = Instant::now().saturating_duration_since(start);
    println!("duration = {} seconds", dur.as_secs());

    /*
     * Copy the cleanup program to the remote system.
     */
    let out = sys
        .scp()
        .arg(&exe)
        .arg(&format!("{}:/tmp/cleanup", sys.scfg.ip))
        .output()?;
    if !out.status.success() {
        bail!("could not scp {exe:?} to {}", sys.scfg.ip);
    }

    /*
     * Run the cleanup program on the remote system.
     */
    let out = sys.ssh().arg("/tmp/cleanup cleanup").output()?;
    if !out.status.success() {
        let e = String::from_utf8_lossy(&out.stderr).trim().to_string();

        bail!("could not scp {exe:?} to {}: {e}", sys.scfg.ip);
    }

    let out = String::from_utf8(out.stdout)?;
    println!("output from cleanup = |{out}|");

    Ok(())
}
