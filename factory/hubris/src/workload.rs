/*
 * Copyright 2024 Oxide Computer Company
 */

use std::{
    collections::HashSet,
    ffi::CString,
    fs::set_permissions,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    process::Command,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{db::types::*, Central};
use anyhow::{anyhow, bail, Result};
use buildomat_common::OutputExt;
use slog::{debug, error, info, o, warn, Logger};

const FMRI: &str = "svc:/site/buildomat/hubris-agent:default";
const SERVICE: &str = "site/buildomat/hubris-agent";

pub(crate) async fn workload_worker(c: Arc<Central>) -> Result<()> {
    let log = c.log.new(o!("component" => "worker"));

    /*
     * Keep track of the instances for which we have kicked off the per-instance
     * worker task:
     */
    let mut instances_with_workers: HashSet<InstanceId> = Default::default();

    loop {
        if let Err(e) =
            workload_worker_one(&log, &c, &mut instances_with_workers).await
        {
            error!(log, "worker error: {:?}", e);
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn workload_worker_one(
    log: &Logger,
    c: &Arc<Central>,
    instances_with_workers: &mut HashSet<InstanceId>,
) -> Result<()> {
    debug!(log, "locating SMF service instance...");

    let mut scf = smf::Scf::new()?;
    let loc = scf.scope_local()?;

    /*
     * We expect the service to exist all the time, as it is imported from the
     * service bundle.
     */
    let svc = loc
        .get_service("site/buildomat/hubris-agent")?
        .ok_or_else(|| anyhow!("could not locate service {SERVICE:?}"))?;

    /*
     * We create an instance for each worker, named "bmat-{seq:08x}".  Make sure
     * we have an instance record for each SMF service instance found on the
     * system.
     */
    let mut insts = svc.instances()?;
    while let Some(i) = insts.next().transpose()? {
        let name = i.name()?;

        let id = match InstanceId::from_service_instance(
            &c.config.nodename,
            &name,
        ) {
            Ok(id) => id,
            Err(e) => {
                /*
                 * XXX We should remove this SMF service instance.
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
        error!(log, "SMF service instance for {id} has no instance");
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

async fn instance_worker(c: Arc<Central>, id: InstanceId) {
    let log = c.log.new(o!(
        "component" => "instance_worker",
        "instance" => id.to_string(),
    ));

    info!(log, "instance worker starting");

    loop {
        match instance_worker_one(&log, &c, &id, BUILD_USER).await {
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

enum DoNext {
    Sleep,
    Immediate,
    Shutdown,
}

/*
 * XXX This user is created manually outside the management of the process right
 * now.
 */
const BUILD_USER: &str = "build";

/*
 * XXX These datasets are created and destroyed to ensure data does not persist
 * between runs.
 */
const BUILD_USER_DATASET: &str = "rpool/home/build";
const WORK_DATASET: &str = "rpool/work";
const INPUT_DATASET: &str = "rpool/input";

fn kill_all(log: &Logger, user: &str) -> Result<()> {
    let u = crate::unix::get_passwd_by_name(user)?
        .ok_or_else(|| anyhow!("could not locate user {user:?}"))?;

    let id = crate::unix::SigSendId::UserId(u.uid);
    if crate::unix::sigsend_maybe(id, libc::SIGKILL)? {
        info!(log, "killed processes for user {user:?} (uid {})", u.uid);

        /*
         * There are a number of reasons that processes might linger for a
         * little while in the table after we terminate them.  Wait for a bit to
         * make sure they're all gone:
         */
        let start = Instant::now();
        while Instant::now().saturating_duration_since(start).as_secs() < 10 {
            /*
             * Try with the 0 argument, which can be used to confirm that no
             * processes exist.
             */
            if !crate::unix::sigsend_maybe(id, 0)? {
                return Ok(());
            }

            std::thread::sleep(Duration::from_millis(100));
        }

        bail!("processes for user {user:?} (uid {}) still exist?", u.uid);
    } else {
        info!(log, "no processes found for user {user:?} (uid {})", u.uid);
    }

    Ok(())
}

fn scrub_tmp(log: &Logger, _user: &str) -> Result<()> {
    /*
     * XXX
     */
    warn!(log, "really should scrub /tmp and /var/tmp ...");
    Ok(())
}

fn zfs_create(dataset: &str, mountpoint: Option<&str>) -> Result<()> {
    let mut cmd = Command::new("/sbin/zfs");
    cmd.env_clear();
    cmd.arg("create");
    if let Some(mp) = mountpoint {
        cmd.arg("-o");
        cmd.arg(format!("mountpoint={mp}"));
    }
    cmd.arg(dataset);

    let out = cmd.output()?;

    if !out.status.success() {
        bail!("zfs create {dataset} failed: {}", out.info());
    }

    Ok(())
}

fn zfs_destroy(dataset: &str) -> Result<()> {
    let mut cmd = Command::new("/sbin/zfs");
    cmd.env_clear();
    cmd.arg("destroy");
    cmd.arg(dataset);

    let out = cmd.output()?;

    if !out.status.success() {
        let e = String::from_utf8_lossy(&out.stderr);
        if !e.contains("dataset does not exist") {
            bail!("zfs destroy {dataset} failed: {}", out.info());
        }
    }

    Ok(())
}

fn dataset_destroy(_log: &Logger, _user: &str) -> Result<()> {
    zfs_destroy(BUILD_USER_DATASET)?;
    zfs_destroy(WORK_DATASET)?;
    zfs_destroy(INPUT_DATASET)?;

    Ok(())
}

fn dataset_create(_log: &Logger, user: &str) -> Result<()> {
    let u = crate::unix::get_passwd_by_name(user)?
        .ok_or_else(|| anyhow!("could not locate user {user:?}"))?;
    let home = u.dir.as_deref().ok_or_else(|| {
        anyhow!("could not locate home directory for {user:?}")
    })?;

    zfs_create(BUILD_USER_DATASET, None)?;
    zfs_create(WORK_DATASET, Some("/work"))?;
    zfs_create(INPUT_DATASET, Some("/input"))?;

    for dir in [home, "/work", "/input"] {
        let f = PathBuf::from(dir);

        let md = f.metadata()?;
        if md.is_symlink() || !md.is_dir() {
            bail!("{dir} is not a directory?!");
        }

        let cstr = CString::new(dir).unwrap();

        if unsafe { libc::chown(cstr.as_ptr(), u.uid, u.gid) } != 0 {
            let e = std::io::Error::last_os_error();

            bail!("chown {dir:?}: {e}");
        }

        if unsafe { libc::chmod(cstr.as_ptr(), 0o755) } != 0 {
            let e = std::io::Error::last_os_error();

            bail!("chmod {dir:?}: {e}");
        }
    }

    Ok(())
}

fn make_device_available(
    _log: &Logger,
    user: &str,
    dev: &crate::usb::UsbDevice,
) -> Result<()> {
    let u = crate::unix::get_passwd_by_name(user)?
        .ok_or_else(|| anyhow!("could not locate user {user:?}"))?;
    let home = u.dir.as_deref().ok_or_else(|| {
        anyhow!("could not locate home directory for {user:?}")
    })?;

    for l in dev.links.iter() {
        let cstr = CString::new(l.clone()).unwrap();

        if unsafe { libc::chown(cstr.as_ptr(), u.uid, u.gid) } != 0 {
            let e = std::io::Error::last_os_error();

            bail!("chown {l:?}: {e}");
        }

        if unsafe { libc::chmod(cstr.as_ptr(), 0o755) } != 0 {
            let e = std::io::Error::last_os_error();

            bail!("chmod {l:?}: {e}");
        }
    }

    Ok(())
}

async fn instance_worker_one(
    log: &Logger,
    c: &Central,
    id: &InstanceId,
    build_user: &str,
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

    let svcname = i.id().service_instance_name();
    let mut scf = smf::Scf::new()?;
    let loc = scf.scope_local()?;
    let svc = loc
        .get_service("site/buildomat/hubris-agent")?
        .ok_or_else(|| anyhow!("could not locate service {SERVICE:?}"))?;

    match i.state {
        InstanceState::Unconfigured => {
            /*
             * Before we get started, attempt to make sure there are no
             * lingering processes or files owned by the build user.
             */
            kill_all(log, build_user)?;
            dataset_destroy(log, build_user)?;
            scrub_tmp(log, build_user)?;

            /*
             * XXX Make sure we have an MCU-Link available.
             */
            let mculink =
                c.usb.mculink().ok_or_else(|| anyhow!("no MCU-Link found?"))?;
            make_device_available(log, build_user, &mculink)?;
            info!(log, "making USB device available: {mculink:?}");

            /*
             * Create the /home/build and /work datasets for the build user:
             */
            dataset_create(log, build_user)?;

            /*
             * Create the SMF service instance...
             */
            let smfi = if let Some(smfi) = svc.get_instance(&svcname)? {
                info!(log, "SMF instance {svcname:?} existed already");
                smfi
            } else {
                svc.add_instance(&svcname)?
            };

            let fmri = smfi.fmri()?;
            info!(log, "created SMF instance {fmri}");

            let pg = if let Some(pg) = smfi.get_pg("buildomat")? {
                pg
            } else {
                smfi.add_pg("buildomat", "application")?
            };

            let tx = pg.transaction()?;
            tx.start()?;
            tx.property_ensure(
                "url",
                smf::scf_type_t::SCF_TYPE_ASTRING,
                &c.config.general.baseurl,
            )?;
            tx.property_ensure(
                "strap",
                smf::scf_type_t::SCF_TYPE_ASTRING,
                &i.bootstrap,
            )?;
            match tx.commit()? {
                smf::CommitResult::Success => (),
                smf::CommitResult::OutOfDate => {
                    bail!("out of date?!");
                }
            }

            if smfi.get_running_snapshot().is_err() {
                /*
                 * XXX workaround for SMF bullshit
                 */
                smfi.disable(false)?;
            }

            smfi.enable(true)?;
            info!(log, "enabled SMF instance {fmri}");

            c.db.instance_new_state(&id, InstanceState::Configured)?;
            Ok(DoNext::Immediate)
        }
        InstanceState::Configured => {
            let mut scf = smf::Scf::new()?;
            let loc = scf.scope_local()?;
            let svc =
                loc.get_service("site/buildomat/hubris-agent")?.ok_or_else(
                    || anyhow!("could not locate service {SERVICE:?}"),
                )?;

            let smfi = svc.get_instance(&svcname)?.ok_or_else(|| {
                anyhow!("could not locate SMF instance {svcname:?}")
            })?;

            match smfi.states().map_err(|e| anyhow!("states: {e}"))? {
                (None, None) => {
                    warn!(log, "no states at all?!");
                    Ok(DoNext::Sleep)
                }
                trans @ (Some(_), Some(_)) => {
                    warn!(log, "in transition... {trans:?}");
                    Ok(DoNext::Sleep)
                }
                (Some(smf::State::Uninitialized), None) => {
                    /*
                     * This occurs briefly prior to svc.startd(8) picking up the
                     * new instance.
                     */
                    Ok(DoNext::Sleep)
                }
                (Some(smf::State::Maintenance), None) => {
                    /*
                     * This is a terminal failure state that means the worker is
                     * not running anymore.
                     */
                    warn!(log, "service in maintenance; giving up!");
                    c.db.instance_new_state(&id, InstanceState::Destroying)?;
                    Ok(DoNext::Immediate)
                }
                (Some(smf::State::Online), None) => {
                    /*
                     * Service is still OK.
                     */
                    Ok(DoNext::Sleep)
                }
                other => bail!("odd service state? {other:?}"),
            }
        }
        InstanceState::Destroying => {
            /*
             * Tear down the SMF service instance:
             */
            if let Some(smfi) = svc.get_instance(&svcname)? {
                match smfi.states()? {
                    (
                        Some(smf::State::Disabled | smf::State::Maintenance),
                        None,
                    ) => {
                        /*
                         * This is a terminal state and we can delete the
                         * service instance.
                         */
                    }
                    other => {
                        /*
                         * Try to disable the service.
                         */
                        warn!(log, "SMF state: {other:?} --> disable!");
                        smfi.disable(true)?;
                        return Ok(DoNext::Sleep);
                    }
                }
            } else {
                info!(log, "service instance {svcname:?} already deleted");
            };

            /*
             * XXX we need to ensure all processes owned by the builder
             * uid are killed...  see: sigsend(2) / sigsendset(2)
             */
            kill_all(log, build_user)?;
            dataset_destroy(log, build_user)?;
            scrub_tmp(log, build_user)?;

            c.db.instance_new_state(&id, InstanceState::Destroyed)?;
            Ok(DoNext::Immediate)
        }
        InstanceState::Destroyed => {
            info!(log, "service instance {svcname:?} is completely destroyed");
            Ok(DoNext::Shutdown)
        }
    }
}
