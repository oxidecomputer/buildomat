/*
 * Copyright 2026 Oxide Computer Company
 */

use crate::config::ConfigFileSlot;
use crate::db::types::WorkerId;
use anyhow::{bail, Context as _, Result};
use buildomat_common::unix::{Gid, Uid};
use rand::RngExt as _;
use std::ops::Range;
use std::path::Path;
use std::process::Command;

const ID_RANGE: Range<u32> = 2u32.pow(20)..2u32.pow(30);

pub(super) fn create(
    worker: WorkerId,
    slot: &ConfigFileSlot,
) -> Result<(Uid, Gid)> {
    remove(worker).context("failed to cleanup users before creating them")?;

    let gid = loop {
        let gid = rand::rng().random_range(ID_RANGE);

        let output = Command::new("/usr/sbin/groupadd")
            .arg("-g")
            .arg(gid.to_string())
            .arg(name(worker))
            .output()
            .context("failed to spawn groupadd")?;

        match output.status.code() {
            Some(0) => break gid,
            /*
             * Exit status of 4 indicates the random gid we generated already
             * exists: reroll another one in the next iteration.
             */
            Some(4) => continue,
            _ => {
                bail!(
                    "adding group {:?} exited with {}: {}",
                    name(worker),
                    output.status,
                    String::from_utf8_lossy(&output.stderr)
                );
            }
        }
    };

    let home = Path::new("/home").join(name(worker));
    let uid = loop {
        let uid = rand::rng().random_range(ID_RANGE);

        let mut cmd = Command::new("/usr/sbin/useradd");
        cmd.arg("-u").arg(uid.to_string());
        cmd.arg("-d").arg(&home);
        cmd.arg("-g").arg(gid.to_string());
        for group in &slot.add_to_groups {
            cmd.arg("-G").arg(group);
        }
        cmd.arg(name(worker));

        let output = cmd.output().context("failed to spawn useradd")?;
        match output.status.code() {
            Some(0) => break uid,
            /*
             * Exit status of 4 indicates the random gid we generated already
             * exists: reroll another one in the next iteration.
             */
            Some(4) => continue,
            _ => {
                bail!(
                    "adding user {:?} exited with {}: {}",
                    name(worker),
                    output.status,
                    String::from_utf8_lossy(&output.stderr)
                );
            }
        }
    };

    Ok((Uid(uid), Gid(gid)))
}

pub(super) fn remove(worker: WorkerId) -> Result<()> {
    let userdel = Command::new("/usr/sbin/userdel")
        .arg(name(worker))
        .output()
        .context("failed to invoke userdel")?;
    match userdel.status.code() {
        Some(0) => {}
        /*
         * Exit status of 6 indicates that the user doesn't exist.
         */
        Some(6) => {}
        _ => {
            bail!(
                "deleting group {:?} exited with {}: {}",
                name(worker),
                userdel.status,
                String::from_utf8_lossy(&userdel.stderr),
            );
        }
    }

    let groupdel = Command::new("/usr/sbin/groupdel")
        .arg(name(worker))
        .output()
        .context("failed to invoke groupdel")?;
    match groupdel.status.code() {
        Some(0) => {}
        /*
         * Exit status of 6 indicates that the group doesn't exist.
         */
        Some(6) => {}
        _ => {
            bail!(
                "deleting group {:?} exited with {}: {}",
                name(worker),
                groupdel.status,
                String::from_utf8_lossy(&groupdel.stderr),
            );
        }
    }

    Ok(())
}

pub(super) fn name(worker: WorkerId) -> String {
    format!("bmat-{worker}")
}
