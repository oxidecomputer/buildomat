#![allow(unused)]

/*
 * Copyright 2025 Oxide Computer Company
 */

use std::path::PathBuf;

use anyhow::{bail, Result};
use iddqd::{id_upcast, IdOrdItem, IdOrdMap};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Slot {
    M2(u32),
    U2(u32),
}

impl Slot {
    pub fn type_name(&self) -> &'static str {
        match self {
            Slot::M2(_) => "M.2",
            Slot::U2(_) => "U.2",
        }
    }

    pub fn num(&self) -> u32 {
        match self {
            Slot::M2(n) | Slot::U2(n) => *n,
        }
    }

    pub fn label(&self) -> Option<&'static str> {
        Some(match self {
            Slot::M2(0) => "BSU0",
            Slot::M2(1) => "BSU1",
            Slot::U2(0) => "N0",
            Slot::U2(1) => "N1",
            Slot::U2(2) => "N2",
            Slot::U2(3) => "N3",
            Slot::U2(4) => "N4",
            Slot::U2(5) => "N5",
            Slot::U2(6) => "N6",
            Slot::U2(7) => "N7",
            Slot::U2(8) => "N8",
            Slot::U2(9) => "N9",
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DiskSlice {
    pub slice: u32,
    pub blk: PathBuf,
    pub chr: PathBuf,
}

impl IdOrdItem for DiskSlice {
    type Key<'a> = u32;

    fn key(&self) -> Self::Key<'_> {
        self.slice
    }

    id_upcast!();
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Disk {
    pub slot: Slot,
    pub dev: String,
    pub slices: IdOrdMap<DiskSlice>,
    pub name: String,
}

impl IdOrdItem for Disk {
    type Key<'a> = &'a Slot;

    fn key(&self) -> Self::Key<'_> {
        &self.slot
    }

    id_upcast!();
}

impl Disk {
    pub fn primary(&self) -> &DiskSlice {
        self.slices.get(&0).unwrap()
    }

    pub fn backup(&self) -> Option<&DiskSlice> {
        self.slices.get(&1)
    }
}

pub type Disks = IdOrdMap<Disk>;
pub type DiskSlices = IdOrdMap<DiskSlice>;

#[cfg(not(target_os = "illumos"))]
pub fn list_disks() -> Result<Disks> {
    bail!("only works on illumos systems");
}

#[cfg(target_os = "illumos")]
pub fn list_disks() -> Result<Disks> {
    use std::collections::BTreeMap;

    let mut di = {
        match devinfo::DevInfo::new_force_load() {
            Ok(di) => di,
            Err(_) => devinfo::DevInfo::new()?,
        }
    };
    let links = {
        match devinfo::DevLinks::new(true) {
            Ok(links) => links,
            Err(_) => devinfo::DevLinks::new(false)?,
        }
    };
    let slice_from_path = |device: &str,
                           slice: u32|
     -> Result<Option<devinfo::DevLink>> {
        let mut paths = links
            .links_for_path(device)?
            .into_iter()
            .filter(|l| {
                l.linktype() == devinfo::DevLinkType::Primary
                    && l.path()
                        .file_name()
                        .map(|f| {
                            f.to_string_lossy().ends_with(&format!("s{slice}"))
                        })
                        .unwrap_or(false)
            })
            .collect::<Vec<_>>();

        if paths.len() > 1 {
            bail!("got weird paths {:?} for {}", paths, device);
        } else {
            Ok(paths.pop())
        }
    };

    /*
     * Find the device nodes for NVMe disks in the system.
     */
    let mut blks = di.walk_driver("blkdev");
    let mut nodes: BTreeMap<Slot, devinfo::Node> = Default::default();
    while let Some(d) = blks.next().transpose()? {
        let Some(pd) = d.parent()? else {
            continue;
        };

        if pd.driver_name().as_deref() != Some("nvme") {
            continue;
        };

        let Some(bridge) = pd.parent()? else { continue };

        let mut pw = bridge.props();
        while let Some(pr) = pw.next().transpose()? {
            if pr.name() != "physical-slot#" {
                continue;
            }

            let num: u32 = pr.as_i64().unwrap().try_into().unwrap();
            nodes.insert(
                match num {
                    17..=18 => Slot::M2(num - 17),
                    0..=9 => Slot::U2(num),
                    _ => continue,
                },
                d.clone(),
            );
        }
    }

    let mut out: Disks = Default::default();
    for (slot, d) in nodes {
        let dev = d.devfs_path()?;

        /*
         * Collect a table of slice number, node type, and link path:
         */
        let links = d
            .minors()
            .map(|m| {
                let m = m?;

                /*
                 * Get the public name of the disk, rather than the /devices
                 * path:
                 */
                let device = m.devfs_path()?;

                (0..=8)
                    .map(|num| {
                        Ok(slice_from_path(&device, num)?.map(|link| {
                            (num, m.spec_type(), link.path().to_owned())
                        }))
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .filter_map(|v| v)
            .collect::<Vec<_>>();

        /*
         * Use the table to collect a dictionary that maps slice number to link
         * path, for all slices where we have found both a block and a character
         * device:
         */
        let slices: DiskSlices = (0..=8)
            .filter_map(|num| {
                let blk = links
                    .iter()
                    .find(|(lnum, spec, _)| {
                        *lnum == num && matches!(spec, devinfo::SpecType::Block)
                    })
                    .map(|(_, _, path)| path.to_owned());
                let chr = links
                    .iter()
                    .find(|(lnum, spec, _)| {
                        *lnum == num && matches!(spec, devinfo::SpecType::Char)
                    })
                    .map(|(_, _, path)| path.to_owned());

                blk.zip(chr).map(|(blk, chr)| DiskSlice {
                    slice: num,
                    blk,
                    chr,
                })
            })
            .collect();

        if let Some(primary) = slices.get(&0) {
            /*
             * Make sure that every disk we see has a slice 0, and use it to
             * determine the short name of the disk:
             */
            let name = if let Some(suf) =
                primary.blk.to_str().unwrap().strip_prefix("/dev/dsk/")
            {
                if let Some(disk) = suf.strip_suffix("s0") {
                    disk.to_string()
                } else {
                    bail!("disk {:?} not slice 0?", primary.blk);
                }
            } else {
                bail!("disk {:?} not in /dev/dsk?", primary.blk);
            };

            out.insert_unique(Disk { slot, name, dev, slices }).unwrap();
        }
    }

    Ok(out)
}
