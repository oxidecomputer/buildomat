/*
 * Copyright 2025 Oxide Computer Company
 */

use std::{
    os::{fd::AsRawFd, unix::fs::OpenOptionsExt},
    process::{Command, Stdio},
    time::{Duration, Instant},
};

use anyhow::{anyhow, bail, Result};
use buildomat_common::*;

use crate::{disks, efi};
use disks::Slot;

#[derive(Debug)]
struct Status {
    current: String,
    next: Option<String>,
}

fn svcs(fmri: &str) -> Result<Status> {
    let out = Command::new("/bin/svcs")
        .env_clear()
        .arg("-Ho")
        .arg("sta,nsta")
        .arg(fmri)
        .output()?;

    if !out.status.success() {
        bail!("could not get status of {fmri}: {}", out.info());
    }

    let out = String::from_utf8(out.stdout)?;
    let out = out.lines().collect::<Vec<_>>();
    if out.len() != 1 {
        bail!("could not get status of {fmri}: unexpected output: {out:?}");
    }

    let t = out[0].split_ascii_whitespace().collect::<Vec<_>>();
    if t.len() != 2 {
        bail!("could not get status of {fmri}: unexpected output: {out:?}");
    }

    Ok(Status {
        current: t[0].trim().to_string(),
        next: if t[1].trim() == "-" {
            None
        } else {
            Some(t[1].trim().to_string())
        },
    })
}

pub fn zpool_import(pool: &str) -> Result<()> {
    let out = Command::new("/sbin/zpool")
        .env_clear()
        .arg("import")
        .arg(pool)
        .output()?;

    if !out.status.success() {
        let e = String::from_utf8_lossy(&out.stderr).trim().to_string();

        bail!("zpool import {pool:?} failure: {e}");
    }

    Ok(())
}

pub fn zpool_unimported_list() -> Result<Vec<String>> {
    let out = Command::new("/sbin/zpool").env_clear().arg("import").output()?;

    if !out.status.success() {
        let e = String::from_utf8_lossy(&out.stderr).trim().to_string();

        bail!("zpool import (to list) failure: {e}");
    }

    let out = String::from_utf8(out.stdout)?;
    let mut pools = out
        .lines()
        .map(|l| l.trim())
        .filter(|l| l.starts_with("pool:"))
        .map(|l| l.split(':').map(|t| t.trim()).collect::<Vec<_>>())
        .filter(|t| t.len() == 2 && t[0] == "pool")
        .map(|t| t[1].to_string())
        .collect::<Vec<_>>();

    pools.sort();

    Ok(pools)
}

pub fn setup() -> Result<()> {
    const MAX_TIME: u64 = 60;

    /*
     * First, make sure we wait for the postboot service to come online.  In the
     * images we are using, this service is generally responsible for bringing
     * up the network and then adjusting the clock and the boot wall time.
     */
    let fmri = "svc:/site/postboot:default";
    println!(" * waiting for {fmri} to come online...");
    let start = Instant::now();
    loop {
        let dur = Instant::now().saturating_duration_since(start);
        if dur.as_secs() > MAX_TIME {
            bail!("giving up after {MAX_TIME} seconds");
        }

        if let Ok(st) = svcs(&fmri) {
            if st.next.is_none() && st.current == "ON" {
                println!(" * {fmri} now online!");
                break;
            }
        }

        std::thread::sleep(Duration::from_millis(250));
    }

    /*
     * Import the zpool we created during the cleaning process; there should
     * only be one!
     */
    let pools = zpool_unimported_list()?;
    if pools.len() == 0 {
        bail!("no unimported pool found!");
    } else if pools.len() > 1 {
        bail!("more than one unimported pool found!");
    }

    let pool = &pools[0];
    let Some(pool_id) = pool.strip_prefix("oxi_") else {
        bail!("unexpected pool name {pool:?}");
    };

    println!(" * importing pool {pool:?}...");
    zpool_import(&pool)?;

    /*
     * Update the BSU symlink:
     */
    std::fs::create_dir_all("/pool/bsu")?;
    std::fs::remove_file("/pool/bsu/0").ok();
    std::os::unix::fs::symlink(&format!("../int/{pool_id}"), "/pool/bsu/0")?;

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

pub fn cleanup() -> Result<()> {
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
    let f = std::fs::OpenOptions::new()
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
                nvmeadm_format(&nvme, None)?;
            }
        }
    }

    Ok(())
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
