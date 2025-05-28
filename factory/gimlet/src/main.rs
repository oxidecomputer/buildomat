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
use getopts::Options;
use humility::{HiffyCaller, PathStep, ValueExt};

mod cleanup;
mod config;
mod disks;
mod efi;
mod host;
mod humility;
mod pipe;

#[tokio::main(worker_threads = 4)]
async fn main() -> Result<()> {
    usdt::register_probes().unwrap();
    let exe = std::env::current_exe()?;

    let mut opts = Options::new();

    opts.optflag("C", "", "cleanup the Gimlet (destructive!");
    opts.optopt("f", "", "configuration file", "CONFIG");
    // opts.optopt("d", "", "database file", "FILE");

    let p = getopts::Options::new()
        .parsing_style(getopts::ParsingStyle::FloatingFrees)
        .parse(std::env::args_os().skip(1))?;

    if !p.free.is_empty() {
        bail!("unexpected arguments");
    }

    if p.opt_present("C") {
        return cleanup::cleanup();
    }

    let log = make_log("factory-gimlet");

    let config: config::ConfigFile = if let Some(f) = p.opt_str("f").as_deref()
    {
        config::load(f)?
    } else {
        bail!("must specify configuration file (-f)");
    };

    let client = buildomat_client::ClientBuilder::new(&config.general.baseurl)
        .bearer_token(&config.factory.token)
        .build()?;

    /*
     * Install a custom panic hook that will try to exit the process after a
     * short delay.  This is unfortunate, but I am not sure how else to avoid a
     * panicked worker thread leaving the process stuck without some of its
     * functionality.
     */
    let orig_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        orig_hook(info);
        eprintln!("FATAL: THREAD PANIC DETECTED; EXITING IN 5 SECONDS...");
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_secs(5));
            std::process::exit(101);
        });
    }));

    /*
     * Interaction with the buildomat server is performed in this task:
     * XXX
     */

    /*
     * Each host is managed by a separate thread.  It's possible that not all
     * hosts will be available at startup, so we keep trying to start them until
     * we've been able to start all of them.
     *
     * XXX Hosts are in several states:
     *
     *          UNKNOWN         at factory startup, we don't know what
     *                          is going on with the host
     *                          
     *                          (remain in this state until told by the
     *                           worker manager to move to CLEANING)
     *
     *          CLEANING        we have decided to take control of the host
     *                          and clear out any detritus that might be
     *                          left from prior jobs
     *
     *          READY           we have finished cleaning and the host is
     *                          ready for a job
     *
     *          STARTING        attempting to start the agent and bootstrap
     *                          (when complete we return to UNKNOWN)
     */
    todo!()

    //    let sys = System::open(&gcfg, &scfg)?;
    //
    //    println!("sys = {sys:#?}");
    //
    //    let start = Instant::now();
    //    sys.power_off()?;
    //    sys.boot_net()?;
    //    sys.pick_bsu(0)?;
    //    sys.write_rom()?;
    //    sys.power_on()?;
    //    sys.boot_server()?;
    //
    //    println!("waiting for system to come up at {}", sys.scfg.ip);
    //    let phase = Instant::now();
    //    let out = loop {
    //        let now = Instant::now();
    //
    //        let dur = now.saturating_duration_since(phase);
    //        if dur.as_secs() > 300 {
    //            bail!("gave up after 300 seconds");
    //        }
    //
    //        let out = sys.ssh().arg("pilot local info -j").output()?;
    //        if !out.status.success() {
    //            std::thread::sleep(Duration::from_secs(1));
    //            continue;
    //        }
    //
    //        let Ok(out) = String::from_utf8(out.stdout) else {
    //            std::thread::sleep(Duration::from_secs(1));
    //            continue;
    //        };
    //
    //        break out;
    //    };
    //
    //    println!("got output: | {out} |");
    //
    //    let dur = Instant::now().saturating_duration_since(start);
    //    println!("duration = {} seconds", dur.as_secs());
    //
    //    /*
    //     * Copy the cleanup program to the remote system.
    //     */
    //    let out = sys
    //        .scp()
    //        .arg(&exe)
    //        .arg(&format!("{}:/tmp/cleanup", sys.scfg.ip))
    //        .output()?;
    //    if !out.status.success() {
    //        bail!("could not scp {exe:?} to {}", sys.scfg.ip);
    //    }
    //
    //    /*
    //     * Run the cleanup program on the remote system.
    //     */
    //    let out = sys.ssh().arg("/tmp/cleanup cleanup").output()?;
    //    if !out.status.success() {
    //        let e = String::from_utf8_lossy(&out.stderr).trim().to_string();
    //
    //        bail!("could not scp {exe:?} to {}: {e}", sys.scfg.ip);
    //    }
    //
    //    let out = String::from_utf8(out.stdout)?;
    //    println!("output from cleanup = |{out}|");
    //
    //    Ok(())
}
