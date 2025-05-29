/*
 * Copyright 2025 Oxide Computer Company
 */

use std::{
    collections::HashMap,
    ops::Deref,
    os::unix::{fs::OpenOptionsExt, io::AsRawFd},
    path::Path,
    process::{Command, Stdio},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{anyhow, bail, Result};
use debug_parser::ValueKind;
use iddqd::{id_upcast, IdHashItem};

use crate::{
    config::{ConfigFileTools, ConfigHost},
    disks::Slot,
};
use crate::{
    humility::{self, HiffyCaller, PathStep, ValueExt},
    HostId,
};
use buildomat_common::*;

#[derive(Debug)]
pub struct Host {
    probe: String,
    archive: String,

    tools: ConfigFileTools,
    host: ConfigHost,
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

    pub fn power_off(&self) -> Result<()> {
        if self.power_state()? == "A2" {
            println!("already powered off");
            return Ok(());
        }

        println!("powering off");
        self.hiffy("Sequencer.set_state").arg("state", "A2").call()?.unit()?;
        Ok(())
    }

    pub fn power_on(&self) -> Result<()> {
        if self.power_state()? == "A0" {
            println!("already powered on");
            return Ok(());
        }

        println!("powering on");
        self.hiffy("Sequencer.set_state").arg("state", "A0").call()?.unit()?;
        Ok(())
    }

    pub fn boot_net(&self) -> Result<()> {
        println!("setting startup options to boot from network");
        self.hiffy("ControlPlaneAgent.set_startup_options")
            .arg("startup_options", "0x80")
            .call()?
            .unit()?;
        Ok(())
    }

    pub fn pick_bsu(&self, bsu: u32) -> Result<()> {
        println!("picking BSU {bsu}");
        self.hiffy("HostFlash.set_dev")
            .arg("dev", &format!("Flash{bsu}"))
            .call()?
            .unit()?;
        Ok(())
    }

    pub fn write_rom(&self, os_dir: &str) -> Result<()> {
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

    pub fn boot_server(&self, os_dir: &str) -> Result<()> {
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
            .arg(&self.host.config.mac)
            .stdout(Stdio::inherit())
            .stdin(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()?;

        if !res.success() {
            bail!("boot server failed");
        }

        Ok(())
    }

    pub fn ssh_options() -> Vec<String> {
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

        cmd.arg(&self.host.config.ip);

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

    pub fn open(
        tools: &ConfigFileTools,
        host: &ConfigHost,
    ) -> Result<Self> {
        let tools = tools.clone();
        let host = host.clone();
        let slot = &host.config.slot;

        let output =
            Command::new(&tools.gimlets).env_clear().arg(slot).output()?;
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

        if serial != host.config.serial {
            bail!(
                "system in slot {slot} has serial {serial:?} != {:?}",
                host.config.serial,
            );
        }

        if partno != host.config.partno {
            bail!(
                "system in slot {slot} has partno {partno:?} != {:?}",
                host.config.partno,
            );
        }

        if revision != host.config.rev {
            bail!(
                "system in slot {slot} has revision {revision:?} != {:?}",
                host.config.rev,
            );
        }

        Ok(Self { probe, archive, tools, host })
    }
}

/*
 * As distinct from the per-instance state tracked in the database, this state
 * is purely kept in memory.  If the factory is interrupted, we'll effectively
 * be back in the "unknown" state until we're told to do something else.
 */
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum HostState {
    /**
     * At factory startup, we do not know what is going on with the host; e.g.,
     * an existing job (which we do not wish to interrupt) may be running, or we
     * may previously have cleaned the host and it is idle.  We remain in this
     * state until instructed to clean the host for some reason.
     */
    Unknown,

    /**
     * We have decided to take control of the host.  This is destructive: we
     * will power cycle the host and upon reboot from a known image we will
     * clear out any detritus left by prior job runs.
     */
    Cleaning,

    /**
     * We have finished cleaning and the host is ready to execute a job.
     */
    Ready,

    /**
     * We are attempting to start the agent and allow it to complete bootstrap.
     * Once we have successfully installed the agent, we move back to the
     * Unknown state.
     */
    Starting,
}

#[derive(Clone, Debug)]
pub struct HostManager(Arc<Inner>);

impl Deref for HostManager {
    type Target = Inner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl IdHashItem for HostManager {
    type Key<'a> = &'a HostId;

    fn key(&self) -> Self::Key<'_> {
        &self.0.host.id
    }

    id_upcast!();
}

#[derive(Debug)]
pub struct Inner {
    host: ConfigHost,
    tools: ConfigFileTools,

    locked: Mutex<Locked>,
}

#[derive(Debug)]
struct Locked {
    state: HostState,
    clean_needed: bool,
    start_needed: bool,
}

impl HostManager {
    pub fn new(
        host: ConfigHost,
        tools: ConfigFileTools,
    ) -> Self {
        let hm0 = Self(Arc::new(Inner {
            host,
            tools,
            locked: Mutex::new(Locked {
                state: HostState::Unknown,
                clean_needed: false,
                start_needed: false,
            }),
        }));

        let hm = hm0.clone();
        std::thread::spawn(move || {
            hm.thread_noerr();
        });

        hm0
    }

    pub fn id(&self) -> &HostId {
        &self.host.id
    }

    pub fn clean(&self) {
        /*
         * XXX
         */
        self.locked.lock().unwrap().clean_needed = true;
    }

    pub fn is_ready(&self) -> bool {
        self.locked.lock().unwrap().state == HostState::Ready
    }

    pub fn start(&self) {
        /*
         * XXX
         */
        self.locked.lock().unwrap().start_needed = true;
    }

    fn thread_noerr(&self) {
        loop {
            if let Err(e) = self.thread() {
                eprintln!("ERROR: thread: {e}");
            }

            std::thread::sleep(Duration::from_secs(1));
        }
    }

    fn thread(&self) -> Result<()> {
        let st = self.locked.lock().unwrap().state;

        match st {
            HostState::Unknown => {
                let mut l = self.locked.lock().unwrap();

                if !l.clean_needed {
                    /*
                     * Don't touch the host until a clean is requested.
                     */
                    return Ok(());
                }

                /*
                 * Move to the cleaning state.  The actual work is performed in
                 * the handler for that state.
                 */
                println!("INFO: host moving to CLEANING...");
                l.state = HostState::Cleaning;
                l.clean_needed = false;
                l.start_needed = false;
                Ok(())
            }
            HostState::Cleaning => {
                /*
                 * XXX power cycle the machine, then wait until we can SSH in to
                 * clean up the disks etc.
                 */
                todo!()
            }
            HostState::Ready => {
                let mut l = self.locked.lock().unwrap();

                if !l.start_needed {
                    /*
                     * Don't touch the host until a start is requested.
                     */
                    return Ok(());
                }

                /*
                 * Move to the starting state.  The actual work is performed in
                 * the handler for that state.
                 */
                println!("INFO: host moving to STARTING...");
                l.state = HostState::Starting;
                l.clean_needed = false;
                l.start_needed = false;
                Ok(())
            }
            HostState::Starting => {
                /*
                 * XXX ssh to the machine and install the agent service
                 */
                todo!()
            }
        }
    }
}
