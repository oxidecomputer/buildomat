/*
 * Copyright 2024 Oxide Computer Company
 */

use std::{
    collections::{HashMap, HashSet},
    os::fd::AsRawFd,
    path::PathBuf,
    process::Command,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{
    config::ImageSource,
    db::types::*,
    net::{dladm_create_vnic, dladm_delete_vnic, dladm_vnic_get, Vnic},
    zones::*,
    Central,
};
use anyhow::{anyhow, bail, Result};
use buildomat_common::OutputExt;
use slog::{debug, error, info, o, warn, Logger};
use zone::Zone;

pub const ZFS: &str = "/sbin/zfs";

async fn vm_worker_one(
    log: &Logger,
    c: &Arc<Central>,
    instances_with_workers: &mut HashSet<InstanceId>,
) -> Result<()> {
    /*
     * List the zones that exist on the system.  We create zones with a name
     * that has the prefix "bmat-"; ignore any other zones.
     */
    debug!(log, "listing zones...");
    let zones = zone::Adm::list()
        .await?
        .into_iter()
        .filter(|z| z.name().starts_with("bmat-"))
        .map(|z| (z.name().to_string(), z))
        .collect::<HashMap<String, Zone>>();

    debug!(log, "found zones: {:?}", zones);

    /*
     * Make sure we have an instance record for each zone found on the system.
     */
    for zone in zones.values() {
        let id =
            match InstanceId::from_zonename(&c.config.nodename, zone.name()) {
                Ok(id) => id,
                Err(e) => {
                    /*
                     * XXX We should remove this zone.
                     */
                    error!(log, "{e}");
                    continue;
                }
            };

        if c.db.instance_get(&id)?.is_some() {
            continue;
        }

        /*
         * XXX We should remove this zone.
         */
        error!(log, "zone for {id} has no instance");
    }

    /*
     * Now that the book-keeping is out of the way, ensure that we have an
     * instance worker spawned for each active instance.
     */
    let instances = c.db.instances_active()?;

    for i in instances {
        let id = i.id();

        if instances_with_workers.contains(&id) {
            continue;
        }
        instances_with_workers.insert(id.clone());

        let c = Arc::clone(c);
        tokio::spawn(async move {
            instance_worker(c, id).await;
        });
    }

    Ok(())
}

pub(crate) async fn vm_worker(c: Arc<Central>) -> Result<()> {
    let log = c.log.new(o!("component" => "worker"));

    /*
     * Keep track of the instances for which we have kicked off the per-instance
     * worker task:
     */
    let mut instances_with_workers: HashSet<InstanceId> = Default::default();

    loop {
        if let Err(e) =
            vm_worker_one(&log, &c, &mut instances_with_workers).await
        {
            error!(log, "worker error: {:?}", e);
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn instance_worker(c: Arc<Central>, id: InstanceId) {
    let log = c.log.new(o!(
        "component" => "instance_worker",
        "instance" => id.to_string(),
    ));

    info!(log, "instance worker starting");

    let mut mon: Option<crate::svc::Monitor> = None;
    let mut ser: Option<crate::serial::SerialForZone> = None;

    loop {
        match instance_worker_one(&log, &c, &id, &mut mon, &mut ser).await {
            Ok(DoNext::Immediate) => continue,
            Ok(DoNext::Sleep) => (),
            Ok(DoNext::Shutdown) => {
                info!(log, "instance worker shutting down");
                return;
            }
            Err(e) => {
                error!(log, "instance worker error: {:?}", e);
            }
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

fn ensure_vnic(
    log: &Logger,
    name: &str,
    physical: &str,
    vlan: Option<u16>,
) -> Result<Vnic> {
    if let Some(vnic) = dladm_vnic_get(name)? {
        /*
         * The VNIC exists already.  Check to see if it has the right
         * properties.
         */
        let mut ok = true;
        if vnic.physical != physical {
            warn!(
                log,
                "VNIC {name} is over NIC {:?}, not {physical:?}", vnic.physical
            );
            ok = false;
        }
        if vnic.vlan != vlan {
            warn!(log, "VNIC {name} has VLAN {:?}, not {vlan:?}", vnic.vlan);
            ok = false;
        }

        if ok {
            return Ok(vnic);
        }

        /*
         * Attempt to destroy the VNIC so that we can recreate it.
         */
        warn!(log, "removing VNIC {name} to recreate it...");
        dladm_delete_vnic(name)?;
    }

    info!(log, "creating VNIC {name} (over {physical}, vlan {vlan:?})");
    dladm_create_vnic(name, physical, vlan)?;

    /*
     * Refetch the VNIC so that we can get its auto-generated MAC address.
     */
    dladm_vnic_get(name)?
        .ok_or_else(|| anyhow!("VNIC {name} disappeared after creation"))
}

enum DoNext {
    Sleep,
    Immediate,
    Shutdown,
}

async fn instance_worker_one(
    log: &Logger,
    c: &Central,
    id: &InstanceId,
    mon: &mut Option<crate::svc::Monitor>,
    ser: &mut Option<crate::serial::SerialForZone>,
) -> Result<DoNext> {
    let i = c.db.instance_get(id)?;

    let Some(i) = i else {
        bail!("no instance {id} in the database?");
    };
    let id = i.id();

    let Some(targ) = c.config.target.get(&i.target) else {
        bail!(
            "instance {id} has target {} that is not in config file",
            i.target,
        );
    };

    let zn = id.zonename();

    match i.state {
        InstanceState::Unconfigured => {
            let t = c.config.for_instance_in_slot(i.slot, &id)?;

            /*
             * The zone needs to be configured.
             */
            info!(log, "zone {zn}: configuring...");
            let mut zc =
                zone::Config::create(&zn, true, zone::CreationOptions::Blank);
            zc.get_global()
                .set_path(t.zonepath())
                .set_brand("omicron1")
                .set_ip_type(zone::IpType::Exclusive)
                .set_autoboot(false);

            /*
             * Add the guest VNIC that propolis will use for the virtio NIC:
             */
            zc.add_net(&zone::Net { physical: t.vnic(), ..Default::default() });

            zc.add_device(&zone::Device { name: "vmm*".to_string() });
            zc.add_device(&zone::Device { name: "viona".to_string() });

            /*
             * If we are using a zvol instead of a file-based image, include the
             * zvol device so that it is visible inside the zone:
             */
            if let ImageSource::Zvol(_) = targ.source()? {
                zc.add_device(&zone::Device {
                    name: format!("zvol/rdsk/{}", t.zvol_name()),
                });
            }

            zc.add_fs(&zone::Fs {
                ty: "lofs".into(),
                dir: "/serial".into(),
                special: c.config.socketdir()?.to_str().unwrap().into(),
                raw: None,
                options: vec!["ro".into()],
            });

            zc.add_fs(&zone::Fs {
                ty: "lofs".into(),
                dir: "/software".into(),
                special: c.config.softwaredir()?.to_str().unwrap().into(),
                raw: None,
                options: vec!["ro".into()],
            });

            match zc.run().await {
                Ok(_) => {
                    info!(log, "zone {zn} configured!");
                    c.db.instance_new_state(&id, InstanceState::Configured)?;
                    Ok(DoNext::Immediate)
                }
                Err(e) => bail!("zone {zn} config error: {e}"),
            }
        }
        InstanceState::Configured => {
            /*
             * To be crash safe, check for an incomplete zone here that might
             * need to be uninstalled before we try again.
             */
            let state = zone::Adm::list()
                .await?
                .into_iter()
                .find(|z| z.name() == zn)
                .map(|z| z.state());
            if matches!(state, Some(zone::State::Incomplete)) {
                info!(log, "zone {zn} incomplete; uninstalling...");
                match zone::Adm::new(&zn).uninstall(true).await {
                    Ok(_) => {
                        info!(log, "zone {zn} installed!");
                        c.db.instance_new_state(&id, InstanceState::Installed)?;
                    }
                    Err(e) => bail!("zone {zn} install error: {e}"),
                }
            }

            /*
             * The zone needs to be installed.
             */
            match zone::Adm::new(&zn).install(Default::default()).await {
                Ok(_) => {
                    info!(log, "zone {zn} installed!");
                    c.db.instance_new_state(&id, InstanceState::Installed)?;
                    Ok(DoNext::Immediate)
                }
                Err(e) => bail!("zone {zn} install error: {e}"),
            }
        }
        InstanceState::Installed => {
            let t = c.config.for_instance_in_slot(i.slot, &id)?;

            /*
             * Confirm that the VNIC is correctly configured, and get the MAC
             * address so that we can use it to provide network configuration
             * metadata to the guest.
             */
            let vnic = tokio::task::block_in_place(|| {
                ensure_vnic(log, &t.vnic(), t.physical(), t.vlan())
            })?;

            /*
             * The root disk image needs to be copied into the zone.  The
             * propolis-standalone configuration needs to be generated and
             * stored in the zone as well.
             */
            let psc = {
                use crate::propolis::*;

                Config {
                    main: Main {
                        name: id.flat_id(),
                        bootrom: "/software/OVMF_CODE.fd".into(),

                        cpus: t.ncpus(),
                        memory: t.ram_mb(),

                        exit_on_halt: 50,
                        exit_on_reboot: 51,

                        use_reservoir: true,
                    },
                    block_dev: [
                        (
                            "root".into(),
                            BlockDev {
                                type_: "file".into(),
                                path: Some(match targ.source()? {
                                    ImageSource::File(_) => {
                                        "/vm/disk.raw".into()
                                    }
                                    ImageSource::Zvol(_) => format!(
                                        "/dev/zvol/rdsk/{}",
                                        t.zvol_name(),
                                    ),
                                }),
                            },
                        ),
                        (
                            "cidata_be".into(),
                            BlockDev { type_: "cloudinit".into(), path: None },
                        ),
                    ]
                    .into_iter()
                    .collect(),
                    dev: [
                        (
                            "disk".into(),
                            Dev {
                                driver: "pci-virtio-block".into(),
                                pci_path: "0.5.0".into(),
                                block_dev: Some("root".into()),
                                vnic: None,
                            },
                        ),
                        (
                            "net".into(),
                            Dev {
                                driver: "pci-virtio-viona".into(),
                                pci_path: "0.10.0".into(),
                                block_dev: None,
                                vnic: Some(t.vnic()),
                            },
                        ),
                        (
                            "cidata".into(),
                            Dev {
                                driver: "pci-virtio-block".into(),
                                pci_path: "0.16.0".into(),
                                block_dev: Some("cidata_be".into()),
                                vnic: None,
                            },
                        ),
                    ]
                    .into_iter()
                    .collect(),
                    cloudinit: CloudInit {
                        user_data: Some(
                            include_str!("../scripts/user_data.sh")
                                .replace("%URL%", &c.config.general.baseurl)
                                .replace("%STRAP%", &i.bootstrap)
                                .to_string(),
                        ),
                        meta_data: Some(
                            crate::nocloud::MetaData {
                                instance_id: id.flat_id(),
                                local_hostname: id.local_hostname(),
                            }
                            .to_json()?,
                        ),
                        network_config: Some(
                            crate::nocloud::Network {
                                version: 1,
                                config: vec![crate::nocloud::Config {
                                    type_: "physical".into(),
                                    name: "net0".into(),
                                    mac_address: vnic.mac.to_string(),
                                    subnets: vec![crate::nocloud::Subnet {
                                        type_: "static".into(),
                                        address: t.addr(),
                                        gateway: t.gateway().to_string(),
                                        dns_nameservers: vec![
                                            "8.8.8.8".into(),
                                            "8.8.4.4".into(),
                                        ],
                                    }],
                                }],
                            }
                            .to_yaml()?,
                        ),
                    },
                }
            }
            .to_toml()?;

            let zp = PathBuf::from(t.zonepath());
            let root = zp.join("root");
            let vmdir = root.join("vm");
            let smfdir =
                root.join("var").join("svc").join("manifest").join("site");
            let siteprofile =
                root.join("var").join("svc").join("profile").join("site.xml");

            info!(log, "add /vm files to zone {zn}...");
            if vmdir.exists() {
                std::fs::remove_dir_all(&vmdir)?;
            }
            std::fs::create_dir(&vmdir)?;

            std::fs::write(vmdir.join("config.toml"), psc.as_bytes())?;

            for (name, script, bundle) in [
                (
                    "propolis",
                    include_str!("../scripts/propolis.sh"),
                    include_str!("../smf/propolis.xml"),
                ),
                (
                    "serial",
                    include_str!("../scripts/serial.sh"),
                    include_str!("../smf/serial.xml"),
                ),
            ] {
                std::fs::write(vmdir.join(format!("{name}.sh")), script)?;
                std::fs::write(smfdir.join(format!("{name}.xml")), bundle)?;
            }

            std::fs::write(&siteprofile, include_str!("../smf/site.xml"))?;

            info!(log, "make root disk...");
            match targ.source()? {
                ImageSource::File(image_path) => {
                    let mut image = std::fs::File::open(image_path)?;
                    let mut dst = std::fs::OpenOptions::new()
                        .create(true)
                        .truncate(true)
                        .write(true)
                        .open(vmdir.join("disk.raw"))?;
                    std::io::copy(&mut image, &mut dst)?;

                    info!(log, "image is copied; expanding volume....");
                    if unsafe {
                        libc::ftruncate(dst.as_raw_fd(), t.disk_bytes())
                    } != 0
                    {
                        let e = std::io::Error::last_os_error();
                        bail!("ftruncate failure: {e}");
                    }
                }
                /*
                 * XXX idempotent/ensure
                 */
                ImageSource::Zvol(zvol) => {
                    /*
                     * Create the disk for this instance by cloning the source
                     * dataset and then adjusting the zvol size.
                     */
                    let out = Command::new(ZFS)
                        .env_clear()
                        .arg("clone")
                        .arg(zvol)
                        .arg(t.zvol_name())
                        .output()?;

                    if !out.status.success() {
                        bail!("zfs clone failed: {}", out.info());
                    }

                    info!(log, "image is cloned; expanding volume....");
                    let out = Command::new(ZFS)
                        .env_clear()
                        .arg("set")
                        .arg(format!("volsize={}", t.disk_bytes()))
                        .arg(t.zvol_name())
                        .output()?;

                    if !out.status.success() {
                        bail!("zfs set volsize failed: {}", out.info());
                    }
                }
            }

            info!(log, "zone {zn}: booting");
            zone::Adm::new(&zn).boot().await?;

            info!(log, "zone {zn}: online!");
            c.db.instance_new_state(&id, InstanceState::ZoneOnline)?;
            Ok(DoNext::Immediate)
        }
        InstanceState::ZoneOnline => {
            /*
             * Confirm that the zone exists.  It's possible that the host, which
             * boots from a ramdisk, has rebooted and taken some partial zones
             * along with it.
             */
            if !zone_exists(&zn)? {
                warn!(log, "zone {zn}: no longer exists; giving up!");
                c.db.instance_new_state(&id, InstanceState::Destroying)?;
                return Ok(DoNext::Immediate);
            }

            let ser = if let Some(ser) = ser.as_mut() {
                ser
            } else {
                *ser = Some(c.serial.zone_add(&zn)?);
                ser.as_mut().unwrap()
            };

            let mon = if let Some(mon) = mon.as_ref() {
                mon
            } else {
                *mon = Some(crate::svc::Monitor::new(
                    log.clone(),
                    zn.clone(),
                    vec!["svc:/site/buildomat/propolis:default".into()],
                )?);
                mon.as_ref().unwrap()
            };

            /*
             * XXX Read some serial data...
             */
            let deadline =
                Instant::now().checked_add(Duration::from_millis(250)).unwrap();
            while Instant::now() < deadline {
                if let Ok(Ok(sd)) =
                    tokio::time::timeout(Duration::from_millis(250), ser.recv())
                        .await
                {
                    info!(log, "serial data: {:?}", sd);
                }
            }

            /*
             * XXX Monitor the health of the SMF service that houses the
             * propolis-standalone process in the zone.
             */
            let mut give_up = false;
            mon.with_problems(|problems| {
                if !problems.is_empty() {
                    warn!(log, "services weird: {problems:#?}");
                    give_up = true;
                }
            });

            if give_up {
                warn!(log, "zone {zn}: giving up on zone!");
                c.db.instance_new_state(&id, InstanceState::Destroying)?;
            }

            /*
             * We'll spend out delay in this state waiting for serial data.
             */
            Ok(DoNext::Immediate)
        }
        InstanceState::Destroying => {
            let t = c.config.for_instance_in_slot(i.slot, &id)?;

            if let Some(cur) = mon.as_ref() {
                tokio::task::block_in_place(|| cur.shutdown());
                *mon = None;
            }

            if let Some(ser) = ser.as_mut() {
                ser.cancel();
                /*
                 * XXX actually drain the serial line...
                 */
            }

            /*
             * Unwind the zone, regardless of how far we got in the original
             * provisioning process.
             *  - make sure zone is halted
             *  - uninstall zone
             *  - delete zone configuration
             *  - delete VNIC for slot
             */
            let zones = zone::Adm::list()
                .await?
                .into_iter()
                .filter(|z| z.name() == zn)
                .collect::<Vec<_>>();

            if zones.len() > 1 {
                bail!("found more than one matching zone: {zones:?}");
            }

            if !zones.is_empty() {
                info!(log, "zone {zn}: state {:?}", zones[0].state());

                match zones[0].state() {
                    zone::State::Running => {
                        /*
                         * Halt the zone.
                         */
                        info!(log, "zone {zn}: halting");
                        zone::Adm::new(&zn).halt().await?;
                    }
                    zone::State::ShuttingDown => {
                        /*
                         * Wait.
                         */
                    }
                    zone::State::Configured => {
                        /*
                         * Delete the zone configuration.
                         */
                        info!(log, "zone {zn}: deleting");
                        zone::Config::new(&zn).delete(true).run().await?;
                    }
                    zone::State::Incomplete | zone::State::Installed => {
                        /*
                         * Uninstall the zone.
                         */
                        info!(log, "zone {zn}: uninstalling");
                        zone::Adm::new(&zn).uninstall(true).await?;
                    }
                    zone::State::Mounted
                    | zone::State::Ready
                    | zone::State::Down => {
                        bail!(
                            "zone {zn} in weird state {:?}",
                            zones[0].state(),
                        );
                    }
                }

                return Ok(DoNext::Sleep);
            }

            info!(log, "zone {zn} no longer present");

            /*
             * Destroy the vnic as well.
             */
            if dladm_vnic_get(&t.vnic())?.is_some() {
                info!(log, "zone {zn} removing VNIC");
                dladm_delete_vnic(&t.vnic())?;
            }

            /*
             * Destroy the zvol if we created one.  File-based images will just
             * be removed with the zone root.
             */
            if let ImageSource::Zvol(_) = targ.source()? {
                info!(log, "zone {zn} removing zvol");
                let out = Command::new(ZFS)
                    .env_clear()
                    .arg("destroy")
                    .arg(t.zvol_name())
                    .output()?;

                if !out.status.success() {
                    let e = String::from_utf8_lossy(&out.stderr);
                    if !e.contains("dataset does not exist") {
                        bail!("zfs destroy failed: {}", out.info());
                    }
                }
            }

            /*
             * Remove the now-vestigial zone path.  The brand actually creates
             * child datasets, so look it up and remove it.
             */
            let mp = Command::new(ZFS)
                .env_clear()
                .arg("get")
                .args(["-Ho", "value,name"])
                .arg("mountpoint")
                .arg(t.zonepath())
                .output()?;
            if !mp.status.success() {
                let e = String::from_utf8_lossy(&mp.stderr);
                if !e.contains("No such file or directory") {
                    bail!("zfs get mountpoint failed: {}", mp.info());
                }
            } else {
                let mp = String::from_utf8(mp.stdout)?;
                let lines = mp.lines().collect::<Vec<_>>();
                if lines.len() != 1 {
                    bail!("unusual zfs get mountpoint output: {lines:?}");
                }

                let terms = lines[0].split('\t').collect::<Vec<_>>();
                if terms.len() != 2 {
                    bail!("unusual zfs get mountpoint output line: {terms:?}");
                }

                if terms[0] == t.zonepath() {
                    /*
                     * The dataset still exists, and we need to destroy it.
                     */
                    info!(log, "destroying zone {zn} dataset {:?}", terms[1]);
                    let out = Command::new(ZFS)
                        .env_clear()
                        .arg("destroy")
                        .arg(terms[1])
                        .output()?;
                    if !out.status.success() {
                        bail!("zfs destroy failed: {}", out.info());
                    }
                } else {
                    /*
                     * We really expect a dataset to be here, but we can drive
                     * on, in case we crashed earlier.
                     */
                    warn!(log, "unexpected dataset situation: {terms:?}");
                }
            }

            /*
             * Make sure we actually cleaned up the zonepath:
             */
            if PathBuf::from(t.zonepath()).exists() {
                bail!("unexpected detritus remains: {:?}", t.zonepath());
            }

            c.db.instance_new_state(&id, InstanceState::Destroyed)?;
            Ok(DoNext::Immediate)
        }
        InstanceState::Destroyed => {
            info!(log, "zone {zn} is completely destroyed");
            Ok(DoNext::Shutdown)
        }
    }
}
