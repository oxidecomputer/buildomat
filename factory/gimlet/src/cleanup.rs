/*
 * Copyright 2025 Oxide Computer Company
 */

use std::{
    os::{fd::AsRawFd, unix::fs::OpenOptionsExt},
    process::{Command, Stdio},
    time::{Duration, Instant},
};

use anyhow::{anyhow, bail, Result};

use crate::{disks, efi};
use disks::Slot;

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
