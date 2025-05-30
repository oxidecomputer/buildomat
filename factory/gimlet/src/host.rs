/*
 * Copyright 2025 Oxide Computer Company
 */

use std::{
    collections::HashMap,
    io::Write,
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
use slog::{error, info, o, warn, Logger};

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
    log: Logger,

    probe: String,
    archive: String,

    tools: ConfigFileTools,
    host: ConfigHost,
}

impl Host {
    fn hiffy(&self, cmd: &str) -> HiffyCaller {
        HiffyCaller {
            log: self.log.new(o!("hiffy" => true)),
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
            info!(&self.log, "already powered off");
            return Ok(());
        }

        info!(&self.log, "powering off");
        self.hiffy("Sequencer.set_state").arg("state", "A2").call()?.unit()?;
        Ok(())
    }

    pub fn power_on(&self) -> Result<()> {
        if self.power_state()? == "A0" {
            info!(&self.log, "already powered on");
            return Ok(());
        }

        info!(&self.log, "powering on");
        self.hiffy("Sequencer.set_state").arg("state", "A0").call()?.unit()?;
        Ok(())
    }

    pub fn boot_net(&self) -> Result<()> {
        info!(&self.log, "setting startup options to boot from network");
        self.hiffy("ControlPlaneAgent.set_startup_options")
            .arg("startup_options", "0x80")
            .call()?
            .unit()?;
        Ok(())
    }

    pub fn pick_bsu(&self, bsu: u32) -> Result<()> {
        info!(&self.log, "picking BSU {bsu}");
        self.hiffy("HostFlash.set_dev")
            .arg("dev", &format!("Flash{bsu}"))
            .call()?
            .unit()?;
        Ok(())
    }

    pub fn write_rom(&self, os_dir: &str) -> Result<()> {
        let file = format!("{os_dir}/rom");
        info!(&self.log, "writing ROM image {file:?}");

        let res = Command::new("/usr/bin/pfexec")
            .env_clear()
            .env("HUMILITY_ARCHIVE", &self.archive)
            .env("HUMILITY_PROBE", &self.probe)
            .arg(&self.tools.humility)
            .arg("qspi")
            .arg("-D")
            .arg(file)
            .output()?;

        if !res.status.success() {
            let e = String::from_utf8_lossy(&res.stderr).trim().to_string();

            bail!("rom write failed: {e}");
        }

        Ok(())
    }

    pub fn boot_server(&self, os_dir: &str) -> Result<()> {
        let file = format!("{os_dir}/zfs.img");
        info!(&self.log, "running boot server for {file:?}");

        let res = Command::new("/usr/bin/pfexec")
            .env_clear()
            .arg(&self.tools.bootserver)
            .arg("-s")
            .arg("-t")
            .arg("300")
            .arg(&self.host.config.control_nic)
            .arg(file)
            .arg(&self.host.config.mac)
            .output()?;

        if !res.status.success() {
            let e = String::from_utf8_lossy(&res.stderr).trim().to_string();

            bail!("boot server failed: {e}");
        }

        Ok(())
    }

    pub fn ssh_options() -> Vec<String> {
        vec![
            "StrictHostKeyChecking=no".to_string(),
            "UserKnownHostsFile=/dev/null".to_string(),
            "ConnectTimeout=5".to_string(),
            "LogLevel=error".to_string(),
        ]
    }

    pub fn ssh(&self) -> Command {
        let mut cmd = Command::new("/usr/bin/ssh");
        cmd.env_clear();

        for opt in Self::ssh_options() {
            cmd.arg("-o").arg(opt);
        }

        cmd.arg(format!("root@{}", &self.host.config.ip));

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
        log: Logger,
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
            log: log.new(o!("hiffy" => true)),
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

        Ok(Self { log, probe, archive, tools, host })
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

/*
 * What is the current intended end state to drive towards for this host.
 */
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum HostGoal {
    Clean,
    Start { bootstrap: String, url: String },
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

    log: Logger,
    locked: Mutex<Locked>,
}

#[derive(Debug)]
struct Locked {
    state: HostState,
    goal: Option<HostGoal>,
}

impl HostManager {
    pub fn new(log: Logger, host: ConfigHost, tools: ConfigFileTools) -> Self {
        let hm0 = Self(Arc::new(Inner {
            host,
            tools,
            log,
            locked: Mutex::new(Locked {
                state: HostState::Unknown,
                goal: None,
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

    pub fn ip(&self) -> &str {
        &self.host.config.ip
    }

    pub fn clean(&self) {
        let log = &self.log;
        let mut l = self.locked.lock().unwrap();

        match l.state {
            HostState::Unknown => {
                match l.goal {
                    /*
                     * A clean has already been requested.
                     */
                    Some(HostGoal::Clean) => (),
                    Some(_) | None => {
                        info!(log, "host in unknown state marked for cleaning");
                        l.goal = Some(HostGoal::Clean);
                    }
                }
            }
            HostState::Cleaning => {
                match l.goal {
                    /*
                     * Already cleaning.
                     */
                    Some(HostGoal::Clean) | None => (),
                    /*
                     * We do not allow a host to be marked for agent start until
                     * it is in the ready state.
                     */
                    Some(HostGoal::Start { .. }) => unreachable!(),
                }
            }
            HostState::Ready | HostState::Starting => {
                match l.goal {
                    /*
                     * A clean has already been requested.
                     */
                    Some(HostGoal::Clean) => (),
                    Some(_) | None => {
                        info!(log, "previously ready host marked for cleaning");
                        l.goal = Some(HostGoal::Clean);
                    }
                }
            }
        }
    }

    pub fn start(&self, url: &str, bootstrap: &str) -> Result<()> {
        let log = &self.log;
        let mut l = self.locked.lock().unwrap();

        let new_goal = Some(HostGoal::Start {
            bootstrap: bootstrap.to_string(),
            url: url.to_string(),
        });

        match l.state {
            HostState::Unknown | HostState::Starting | HostState::Cleaning => {
                bail!("cannot start a host in the {:?} state", l.state);
            }
            HostState::Ready => {
                if new_goal == l.goal {
                    /*
                     * We're already geared up to do this.
                     */
                    return Ok(());
                } else if l.goal.is_some() {
                    bail!(
                        "second attempt to start host with different \
                        parameters"
                    );
                }

                info!(log, "starting host";
                    "url" => url, "bootstrap" => bootstrap);
                l.goal = new_goal;
                Ok(())
            }
        }
    }

    pub fn is_ready(&self) -> bool {
        let l = self.locked.lock().unwrap();

        l.state == HostState::Ready && l.goal.is_none()
    }

    fn thread_noerr(&self) {
        loop {
            if let Err(e) = self.thread() {
                error!(&self.log, "thread error: {e}");
            }

            std::thread::sleep(Duration::from_secs(1));
        }
    }

    fn thread(&self) -> Result<()> {
        let log = &self.log;
        let st = self.locked.lock().unwrap().state;

        match st {
            HostState::Unknown => {
                let mut l = self.locked.lock().unwrap();

                match l.goal {
                    Some(HostGoal::Clean) => {
                        info!(log, "cleaning host");
                        l.state = HostState::Cleaning;
                        l.goal = None;
                        Ok(())
                    }
                    Some(HostGoal::Start { .. }) => unreachable!(),
                    None => return Ok(()),
                }
            }
            HostState::Cleaning => {
                /*
                 * XXX power cycle the machine, then wait until we can SSH in to
                 * clean up the disks etc.
                 */
                let sys =
                    Host::open(self.log.clone(), &self.tools, &self.host)?;
                sys.power_off()?;
                sys.pick_bsu(0)?;
                sys.write_rom(&self.host.config.cleaning_os_dir)?;
                sys.boot_net()?;
                sys.power_on()?;
                sys.boot_server(&self.host.config.cleaning_os_dir)?;

                let ip = &self.host.config.ip;
                info!(log, "waiting for system to come up at {ip}");

                let phase = Instant::now();
                let out = loop {
                    let dur = Instant::now().saturating_duration_since(phase);
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

                let dur = Instant::now().saturating_duration_since(phase);
                info!(log,"got pilot local info";
                    "info" => out,
                    "seconds" => dur.as_secs(),
                );

                /*
                 * Copy the cleanup program to the remote system.
                 */
                info!(log, "copying cleanup program to system");
                let phase = Instant::now();
                let exe = std::env::current_exe()?;
                loop {
                    let dur = Instant::now().saturating_duration_since(phase);
                    if dur.as_secs() > 300 {
                        bail!("gave up after 300 seconds");
                    }

                    let out = sys
                        .scp()
                        .arg(&exe)
                        .arg(&format!("root@{ip}:/tmp/cleanup"))
                        .output()?;
                    if !out.status.success() {
                        warn!(log, "could not scp {exe:?} to {ip}");
                        std::thread::sleep(Duration::from_secs(1));
                        continue;
                    }

                    break;
                }

                /*
                 * Run the cleanup program on the remote system.
                 */
                info!(log, "running cleaning program on system");
                let out = sys.ssh().arg("/tmp/cleanup -C").output()?;
                if !out.status.success() {
                    let e =
                        String::from_utf8_lossy(&out.stderr).trim().to_string();

                    bail!("could not invoke cleanup program on {ip}: {e}");
                }

                let dur = Instant::now().saturating_duration_since(phase);
                let out = String::from_utf8(out.stdout)?;
                info!(log,"got cleanup output";
                    "info" => out,
                    "seconds" => dur.as_secs(),
                );

                let mut l = self.locked.lock().unwrap();
                match l.goal {
                    Some(HostGoal::Start { .. }) => unreachable!(),
                    Some(HostGoal::Clean) | None => {
                        info!(log, "host is now ready");
                        l.state = HostState::Ready;
                        l.goal = None;
                        Ok(())
                    }
                }
            }
            HostState::Ready => {
                let mut l = self.locked.lock().unwrap();

                match l.goal {
                    Some(HostGoal::Clean) => {
                        info!(log, "cleaning host");
                        l.state = HostState::Cleaning;
                        Ok(())
                    }
                    Some(HostGoal::Start { .. }) => {
                        info!(log, "starting host");
                        l.state = HostState::Starting;
                        Ok(())
                    }
                    None => {
                        /*
                         * Otherwise, don't touch the host until a start is
                         * requested.
                         */
                        Ok(())
                    }
                }
            }
            HostState::Starting => {
                /*
                 * Get the bootstrap token we should use:
                 */
                let program = {
                    let template =
                        include_str!("../../propolis/scripts/user_data.sh");

                    let mut l = self.locked.lock().unwrap();
                    match &l.goal {
                        Some(HostGoal::Clean) => {
                            info!(log, "cleaning host");
                            l.state = HostState::Cleaning;
                            return Ok(());
                        }
                        Some(HostGoal::Start { bootstrap, url }) => template
                            .replace("%STRAP%", &bootstrap)
                            .replace("%URL%", &url),
                        None => {
                            info!(log, "host state unknown");
                            l.state = HostState::Unknown;
                            return Ok(());
                        }
                    }
                };

                let sys =
                    Host::open(self.log.clone(), &self.tools, &self.host)?;

                /*
                 * Copy the agent startup program to the remote system.
                 */
                info!(log, "copying agent startup program to system");
                let phase = Instant::now();
                let exe = std::env::current_exe()?;
                loop {
                    let dur = Instant::now().saturating_duration_since(phase);
                    if dur.as_secs() > 300 {
                        bail!("gave up after 300 seconds");
                    }

                    let mut child = sys
                        .ssh()
                        .arg("tee /tmp/install.sh >/dev/null")
                        .stdin(Stdio::piped())
                        .spawn()?;

                    let mut stdin = child.stdin.take().unwrap();
                    stdin.write_all(program.as_bytes())?;

                    let out = child.wait_with_output()?;

                    if !out.status.success() {
                        warn!(log, "could not send startup program");
                        std::thread::sleep(Duration::from_secs(1));
                        continue;
                    }

                    break;
                }

                /*
                 * Run the agent startup program on the remote system.
                 */
                info!(log, "installing agent on system");
                let out = sys
                    .ssh()
                    .arg(format!("bash -x /tmp/install.sh 2>&1"))
                    .output()?;
                if !out.status.success() {
                    let e =
                        String::from_utf8_lossy(&out.stdout).trim().to_string();

                    bail!("could not invoke startup program: {e}");
                }

                let dur = Instant::now().saturating_duration_since(phase);
                let out = String::from_utf8(out.stdout)?;
                info!(log,"got install output";
                    "info" => out,
                    "seconds" => dur.as_secs(),
                );

                let mut l = self.locked.lock().unwrap();
                match l.goal {
                    Some(HostGoal::Start { .. }) => {
                        info!(log, "host is started");
                        l.state = HostState::Unknown;
                        l.goal = None;
                        Ok(())
                    }
                    Some(HostGoal::Clean) => {
                        info!(log, "cleaning host");
                        l.state = HostState::Cleaning;
                        Ok(())
                    }
                    None => {
                        info!(log, "host state unknown");
                        l.state = HostState::Unknown;
                        Ok(())
                    }
                }
            }
        }
    }
}
