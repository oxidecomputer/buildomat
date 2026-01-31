/*
 * Copyright 2025 Oxide Computer Company
 */

use std::{
    collections::HashMap,
    io::Write,
    ops::Deref,
    process::{Command, Stdio},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{anyhow, bail, Result};
use buildomat_types::metadata::FactoryAddresses;
use iddqd::{id_upcast, IdHashItem};
use slog::{error, info, o, warn, Logger};

use crate::config::{ConfigFileTools, ConfigHost};
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

pub struct ExecResult {
    pub out: String,
    #[allow(unused)]
    pub err: String,
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
        let res =
            self.hiffy("Sequencer.set_state").arg("state", "A2").call()?;
        if res.value.to_string().trim().to_string() != "Changed" {
            bail!("unexpected power off result: {:?}", res.stdout);
        }

        Ok(())
    }

    pub fn power_on(&self) -> Result<()> {
        if self.power_state()? == "A0" {
            info!(&self.log, "already powered on");
            return Ok(());
        }

        info!(&self.log, "powering on");
        let res =
            self.hiffy("Sequencer.set_state").arg("state", "A0").call()?;
        if res.value.to_string().trim().to_string() != "Changed" {
            bail!("unexpected power on result: {:?}", res.stdout);
        }

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
        let file = format!("{os_dir}/gimlet.rom");
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

    pub fn ssh_exec(&self, script: &str) -> Result<ExecResult> {
        let ip = &self.host.config.ip;

        let mut cmd = Command::new("/usr/bin/ssh");
        cmd.env_clear();

        for opt in Self::ssh_options() {
            cmd.arg("-o").arg(opt);
        }

        cmd.arg(format!("root@{}", &self.host.config.ip));
        cmd.arg(script);

        let res = cmd.output()?;

        if !res.status.success() {
            let e = String::from_utf8_lossy(&res.stderr).trim().to_string();

            bail!("ssh {ip} {script:?} failed: {e}");
        }

        Ok(ExecResult {
            err: String::from_utf8_lossy(&res.stderr).to_string(),
            out: String::from_utf8(res.stdout)?,
        })
    }

    pub fn ssh_send_file(&self, path: &str, contents: &[u8]) -> Result<()> {
        let ip = &self.host.config.ip;

        let mut cmd = Command::new("/usr/bin/ssh");
        cmd.env_clear();

        for opt in Self::ssh_options() {
            cmd.arg("-o").arg(opt);
        }

        cmd.arg(format!("root@{ip}"));
        cmd.arg(format!("tee {path} >/dev/null"));

        cmd.stdin(Stdio::piped());

        let mut child = cmd.spawn()?;

        /*
         * Write the contents of the file to stdin of the tee(1) process on the
         * remote end, then close the file descriptor by dropping it:
         */
        let mut stdin = child.stdin.take().unwrap();
        stdin.write_all(contents)?;
        drop(stdin);

        let out = child.wait_with_output()?;

        if !out.status.success() {
            let e = String::from_utf8_lossy(&out.stderr).trim().to_string();

            bail!("could not send file to {ip} at {path}: {e}");
        }

        Ok(())
    }

    pub fn scp_to(&self, local: &str, remote: &str) -> Result<()> {
        let ip = &self.host.config.ip;

        let mut cmd = Command::new("/usr/bin/scp");
        cmd.env_clear();

        for opt in Self::ssh_options() {
            cmd.arg("-o").arg(opt);
        }

        cmd.arg(local);
        cmd.arg(format!("{ip}:{remote}"));

        let res = cmd.output()?;

        if !res.status.success() {
            let e = String::from_utf8_lossy(&res.stderr).trim().to_string();

            bail!("scp {local} {ip}:{remote} failed: {e}");
        }

        Ok(())
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
                PathStep::Name("OxideIdentity".into()),
                PathStep::Map("serial".into()),
            ],
        )?
        .from_bytes_utf8()?;

        let partno = humility::locate_term(
            &res.value,
            &[
                PathStep::Name("OxideIdentity".into()),
                PathStep::Map("part_number".into()),
            ],
        )?
        .from_bytes_utf8()?;

        let revision = humility::locate_term(
            &res.value,
            &[
                PathStep::Name("OxideIdentity".into()),
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

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum HostStatus {
    /**
     * Cleaned and ready to be attached to an instance.
     */
    Ready,

    /**
     * An instance has been started.
     */
    Active,
}

/*
 * As distinct from the per-instance state tracked in the database, this state
 * is purely kept in memory.  If the factory is interrupted, we'll effectively
 * be back in the "unknown" state until we're told to do something else.
 */
#[derive(Debug, PartialEq, Eq, Clone)]
enum HostState {
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
    Cleaning(CleaningState),

    /**
     * We have finished cleaning and the host is ready to execute a job.
     */
    Ready,

    /**
     * We are attempting to start the agent and allow it to complete bootstrap.
     * Once we have successfully installed the agent, we move to the Started
     * state.
     */
    Starting(StartingState, HostGoalStart),

    /**
     * Allow the host to execute until we are instructed to reset and clean the
     * host again.
     */
    Started,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum CleaningState {
    WriteRom,
    Boot,
    BootWait,
    Cleanup,
    Ready,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum StartingState {
    WriteRom,
    Boot,
    BootWait,
    Setup,
    InstallAgent,
    Ready,
}

/*
 * What is the current intended end state to drive towards for this host.
 */
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum HostGoal {
    Clean,
    Start(HostGoalStart),
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct HostGoalStart {
    bootstrap: String,
    url: String,
    os_dir: String,
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
    goal: Option<HostGoal>,
    status: Option<HostStatus>,
}

impl HostManager {
    pub fn new(log: Logger, host: ConfigHost, tools: ConfigFileTools) -> Self {
        let hm0 = Self(Arc::new(Inner {
            host,
            tools,
            log,
            locked: Mutex::new(Locked { goal: None, status: None }),
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

    pub fn extra_ips(&self) -> Vec<FactoryAddresses> {
        self.host
            .config
            .extra_ips
            .as_ref()
            .map(|eip| eip.with_gateway("extra", &self.host.config.gateway))
            .into_iter()
            .collect()
    }

    pub fn start(
        &self,
        url: &str,
        bootstrap: &str,
        os_dir: &str,
    ) -> Result<()> {
        let mut l = self.locked.lock().unwrap();

        if matches!(l.status, Some(HostStatus::Ready)) && l.goal.is_none() {
            l.goal = Some(HostGoal::Start(HostGoalStart {
                bootstrap: bootstrap.to_string(),
                url: url.to_string(),
                os_dir: os_dir.to_string(),
            }));

            Ok(())
        } else {
            bail!(
                "cannot start a host with status {:?} and goal {:?}",
                l.status,
                l.goal,
            );
        }
    }

    /**
     * Used to check if a host is currently ready to be used for a new job. This
     * routine is obviously racy in contexts where goals are also being issued,
     * and only exists to be used when a host is not currently attached to an
     * instance.
     */
    pub fn is_ready(&self) -> bool {
        let l = self.locked.lock().unwrap();

        matches!(l.status, Some(HostStatus::Ready)) && l.goal.is_none()
    }

    pub fn make_ready(&self) -> Result<bool> {
        let mut l = self.locked.lock().unwrap();

        match &l.status {
            Some(HostStatus::Ready) => {
                if l.goal.is_some() {
                    /*
                     * If another goal is currently issued but not acknowledged,
                     * we want to wait for the task to take it and decide what
                     * to do before reporting status.
                     */
                    Ok(false)
                } else {
                    /*
                     * The host is ready to go!
                     */
                    Ok(true)
                }
            }
            Some(HostStatus::Active) => {
                /*
                 * The host is active and running a job.  We need to request
                 * that it be cleaned.
                 */
                l.goal = Some(HostGoal::Clean);
                Ok(false)
            }
            None => {
                /*
                 * The host status is not currently known.  Drive it towards
                 * cleaning.
                 */
                l.goal = Some(HostGoal::Clean);
                Ok(false)
            }
        }
    }

    fn thread_noerr(&self) {
        let mut st = HostState::Unknown;

        loop {
            if let Err(e) = self.thread(&mut st) {
                error!(&self.log, "thread error: {e}");
            }

            std::thread::sleep(Duration::from_secs(1));
        }
    }

    fn thread(&self, st: &mut HostState) -> Result<()> {
        let log = &self.log;

        match st {
            HostState::Unknown => {
                let mut l = self.locked.lock().unwrap();

                /*
                 * We don't know what the status of the host is.
                 */
                l.status = None;

                match l.goal.take() {
                    Some(HostGoal::Clean) => {
                        info!(log, "cleaning host");
                        *st = HostState::Cleaning(CleaningState::WriteRom);
                    }
                    Some(HostGoal::Start { .. }) => {
                        warn!(
                            log,
                            "ignoring request to start from unknown state",
                        );
                    }
                    None => (),
                }

                Ok(())
            }
            HostState::Cleaning(cst) => {
                let mut new_cst = cst.clone();

                {
                    let mut l = self.locked.lock().unwrap();

                    /*
                     * We don't know what the status of the host is.
                     */
                    l.status = None;

                    match l.goal.take() {
                        Some(HostGoal::Clean) => (),
                        Some(HostGoal::Start { .. }) => {
                            warn!(
                                log,
                                "ignoring request to start from cleaning state",
                            );
                        }
                        None => (),
                    }
                }

                if let Err(e) = self.thread_cleaning(&mut new_cst) {
                    error!(log, "cleaning error: {e}");
                }

                if &new_cst != cst {
                    info!(log, "cleaning state: {cst:?} -> {new_cst:?}");
                    *cst = new_cst;
                }

                if matches!(cst, CleaningState::Ready) {
                    info!(log, "cleaning complete; host is now ready!");
                    *st = HostState::Ready;
                }

                Ok(())
            }
            HostState::Ready => {
                let mut l = self.locked.lock().unwrap();

                match l.goal.take() {
                    Some(HostGoal::Clean) => {
                        if !matches!(l.status, Some(HostStatus::Ready)) {
                            /*
                             * We've not yet reported that the host was ready,
                             * so we can drop this clean request and just say we
                             * did it.
                             */
                            l.status = Some(HostStatus::Ready);
                        } else {
                            info!(log, "cleaning host from the ready state");
                            *st = HostState::Cleaning(CleaningState::WriteRom);
                            l.status = None;
                        }
                    }
                    Some(HostGoal::Start(hgs)) => {
                        info!(log, "starting host";
                            "url" => &hgs.url,
                            "os_dir" => &hgs.os_dir,
                        );
                        *st = HostState::Starting(StartingState::WriteRom, hgs);
                        l.status = None;
                    }
                    None => {
                        /*
                         * Only report that the host is ready if we are not
                         * about to perform some other action.
                         */
                        l.status = Some(HostStatus::Ready);
                    }
                }

                Ok(())
            }
            HostState::Starting(sst, hgs) => {
                let mut new_sst = sst.clone();

                {
                    let mut l = self.locked.lock().unwrap();

                    /*
                     * We don't know what the status of the host is.
                     */
                    l.status = None;

                    match l.goal.take() {
                        Some(HostGoal::Clean) => {
                            info!(log, "cleaning host from the starting state");
                            *st = HostState::Cleaning(CleaningState::WriteRom);
                            return Ok(());
                        }
                        Some(HostGoal::Start { .. }) => {
                            warn!(
                                log,
                                "ignoring request to start from starting state",
                            );
                        }
                        None => (),
                    }
                }

                if let Err(e) = self.thread_starting(&mut new_sst, &hgs) {
                    error!(log, "starting error: {e}");
                }

                if &new_sst != sst {
                    info!(log, "starting state: {sst:?} -> {new_sst:?}");
                    *sst = new_sst;
                }

                if matches!(sst, StartingState::Ready) {
                    info!(log, "starting complete; host is now started!");
                    *st = HostState::Started;
                }

                Ok(())
            }
            HostState::Started => {
                let mut l = self.locked.lock().unwrap();

                /*
                 * We have started the agent on the host.  Report that status
                 * and come to rest until we're told to clean the host again.
                 */
                l.status = Some(HostStatus::Active);

                match l.goal.take() {
                    Some(HostGoal::Clean) => {
                        l.status = None;
                        info!(log, "cleaning host from the started state");
                        *st = HostState::Cleaning(CleaningState::WriteRom);
                    }
                    Some(HostGoal::Start { .. }) => {
                        warn!(
                            log,
                            "ignoring request to start from starting state",
                        );
                    }
                    None => (),
                }

                Ok(())
            }
        }
    }

    fn thread_cleaning(&self, cst: &mut CleaningState) -> Result<()> {
        let log = &self.log;
        let sys = Host::open(self.log.clone(), &self.tools, &self.host)?;

        match cst {
            CleaningState::WriteRom => {
                sys.power_off()?;
                sys.pick_bsu(0)?;
                sys.write_rom(&self.host.config.cleaning_os_dir)?;
                *cst = CleaningState::Boot;
            }
            CleaningState::Boot => {
                sys.boot_net()?;
                sys.power_on()?;
                sys.boot_server(&self.host.config.cleaning_os_dir)?;
                *cst = CleaningState::BootWait;
            }
            CleaningState::BootWait => {
                let info = sys.ssh_exec("pilot local info -j")?;
                info!(log, "got pilot local info"; "info" => info.out);

                let pb = sys
                    .ssh_exec("svcs -Ho sta,nsta svc:/site/postboot:default")?;
                let t = pb.out.trim().split_whitespace().collect::<Vec<_>>();
                if t.len() != 2 || t[0] == "ON" || t[1] == "-" {
                    bail!("postboot not ready: {t:?}");
                }

                *cst = CleaningState::Cleanup;
            }
            CleaningState::Cleanup => {
                /*
                 * Copy cleanup program to system and run it.
                 */
                let exe = std::env::current_exe()?;

                sys.scp_to(exe.to_str().unwrap(), "/tmp/cleanup")?;
                let res = sys.ssh_exec("/tmp/cleanup -C")?;

                info!(log, "got cleanup output"; "info" => res.out);

                *cst = CleaningState::Ready;
            }
            CleaningState::Ready => (),
        }

        Ok(())
    }

    fn thread_starting(
        &self,
        sst: &mut StartingState,
        hgs: &HostGoalStart,
    ) -> Result<()> {
        let log = &self.log;
        let sys = Host::open(self.log.clone(), &self.tools, &self.host)?;

        match sst {
            StartingState::WriteRom => {
                sys.power_off()?;
                sys.pick_bsu(1)?;
                sys.write_rom(&hgs.os_dir)?;
                *sst = StartingState::Boot;
            }
            StartingState::Boot => {
                sys.boot_net()?;
                sys.power_on()?;
                sys.boot_server(&hgs.os_dir)?;
                *sst = StartingState::BootWait;
            }
            StartingState::BootWait => {
                let info = sys.ssh_exec("pilot local info -j")?;
                info!(log, "got pilot local info"; "info" => info.out);

                let pb = sys
                    .ssh_exec("svcs -Ho sta,nsta svc:/site/postboot:default")?;
                let t = pb.out.trim().split_whitespace().collect::<Vec<_>>();
                if t.len() != 2 || t[0] == "ON" || t[1] == "-" {
                    bail!("postboot not ready: {t:?}");
                }

                *sst = StartingState::Setup;
            }
            StartingState::Setup => {
                /*
                 * Copy setup program to system and run it.
                 */
                let exe = std::env::current_exe()?;

                sys.scp_to(exe.to_str().unwrap(), "/tmp/setup")?;
                let res = sys.ssh_exec("/tmp/setup -S")?;

                info!(log, "got setup output"; "info" => res.out);

                *sst = StartingState::InstallAgent;
            }
            StartingState::InstallAgent => {
                /*
                 * Create a shell program to bootstrap the agent:
                 */
                let program = include_str!("../../gimlet/scripts/install.sh")
                    .replace("%STRAP%", &hgs.bootstrap)
                    .replace("%URL%", &hgs.url);

                sys.ssh_send_file("/tmp/install.sh", program.as_bytes())?;
                let res = sys.ssh_exec("bash -x /tmp/install.sh 2>&1")?;

                info!(log, "got install output"; "info" => res.out);

                *sst = StartingState::Ready;
            }
            StartingState::Ready => (),
        }

        Ok(())
    }
}
