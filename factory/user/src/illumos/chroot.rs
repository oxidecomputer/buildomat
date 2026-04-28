/*
 * Copyright 2026 Oxide Computer Company
 */

use crate::db::types::WorkerId;
use crate::Central;
use anyhow::{anyhow, bail, Context as _, Result};
use buildomat_common::unix::{Passwd, Uid};
use buildomat_common::OutputExt as _;
use std::fs::Permissions;
use std::os::unix::fs::{chown, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Command;

const ZFS: &str = "/usr/sbin/zfs";
const MOUNT: &str = "/usr/sbin/mount";
const UMOUNT: &str = "/usr/sbin/umount";

/*
 * Create a directory suitable to be chroot'd into, to execute the job.
 *
 * The goal of this factory is to run jobs in a multi-user host, but even with
 * the isolation provided by UNIX users we can't let the jobs run with regular
 * access to the host's filesystem:
 *
 * - One job's temporary files shouldn't interfere with temporary files from
 *   other jobs or the host, so we'd need isolated /tmp and /var/tmp.
 *
 * - Buildomat commits to providing the /work and /input directories to the job,
 *   but providing them on the host filesystem would mean only one job can run
 *   in each host at the time, which is not really efficient.
 *
 * - As defense-in-depth, it'd be better to enforce on a filesystem level that
 *   everything except the few directories dedicated to the build are read-only,
 *   to protect from misconfigured permissions.
 *
 * This function prepares a directory with a collection of nested mount points
 * to provide a read-only view of the filesystem with just a few allowlisted
 * paths that are writeable.
 */
pub(super) fn prepare(
    central: &Central,
    worker: WorkerId,
    user: Uid,
) -> Result<PathBuf> {
    let passwd = Passwd::by_uid(user)?
        .ok_or_else(|| anyhow!("could not locate user {user:?}"))?;
    let home = passwd
        .dir
        .as_deref()
        .ok_or_else(|| {
            anyhow!("could not locate home directory of user {user:?}")
        })?
        .to_str()
        .ok_or_else(|| anyhow!("non-UTF-8 home directory for user {user:?}"))?;

    cleanup(central, worker)
        .context("failed to cleanup before preparing the chroot")?;

    /*
     * Create the directory that will contain the chroot.  This is owned by the
     * ephemeral user with mode 0700 to prevent anyone else from accessing it.
     */
    let worker_dir = root_dir(worker);
    std::fs::create_dir_all(&worker_dir)?;
    chown(&worker_dir, Some(passwd.uid.0), Some(passwd.gid.0))
        .with_context(|| format!("failed to chown {worker_dir:?}"))?;
    std::fs::set_permissions(&worker_dir, Permissions::from_mode(0o700))
        .with_context(|| format!("failed to chmod {worker_dir:?}"))?;

    /*
     * Create the loopback virtual file system, mapping the root filesystem into
     * the chroot as a read-only device.  We will add writeable mount points on
     * top of it to let the build write data.  The root dir's ownership and
     * permissions match the ones outside the chroot.
     */
    let root = worker_dir.join("root");
    std::fs::create_dir_all(&root)?;
    chown(&root, Some(0), Some(0))
        .with_context(|| format!("failed to chown {root:?}"))?;
    std::fs::set_permissions(&worker_dir, Permissions::from_mode(0o755))
        .with_context(|| format!("failed to chmod {root:?}"))?;
    mount("lofs", Path::new("/"), &root, &["-o", "ro"])?;

    /*
     * The ZFS dataset to store written data is created with sync=disabled to
     * improve build performance at the cost of data integrity in case of a
     * sudden power loss.  We don't care about that property for jobs.
     */
    let dataset = zfs_dataset_name(central, worker);
    run(Command::new(ZFS)
        .args(["create", "-p", "-osync=disabled"])
        .arg(&dataset))
    .with_context(|| format!("failed to create ZFS dataset {dataset:?}"))?;

    /*
     * All temporary directories will be backed by the ZFS dataset created
     * earlier rather than a tmpfs, to avoid exhausting RAM.
     */
    for tmp in ["/tmp", "/var/tmp"] {
        let src = zfs_dataset_mount(central, worker)
            .join(tmp.trim_start_matches('/'));
        std::fs::create_dir_all(&src)?;
        chown(&src, Some(0), Some(0))?;
        std::fs::set_permissions(&src, Permissions::from_mode(0o777))?;

        let dest = root.join(tmp.trim_start_matches('/'));
        mount("lofs", &src, &dest, &[])?;
    }

    for writable in ["/input", "/work", "/var/run/buildomat", home] {
        let src = zfs_dataset_mount(central, worker)
            .join(writable.trim_start_matches('/'));
        std::fs::create_dir_all(&src)?;
        chown(&src, Some(passwd.uid.0), Some(passwd.gid.0))?;

        /*
         * Mounting requires the mount point to be an existing directory.  This
         * is trickier than it sounds, as we cannot create the directory inside
         * of the chroot (the loopback mount is read-only!).  We thus have to
         * create the directory in the root filesystem.
         */
        std::fs::create_dir_all(writable)?;

        let dest = root.join(writable.trim_start_matches('/'));
        mount("lofs", &src, &dest, &[])?;
    }

    Ok(root)
}

pub(super) fn cleanup(central: &Central, worker: WorkerId) -> Result<()> {
    /*
     * When changing this function, keep in mind that it needs to succeed even
     * when it was interrupted halfway through in the past, or when there is
     * nothing left to clean.
     */

    let root = root_dir(worker);
    let dataset = zfs_dataset_name(central, worker);

    /*
     * The chroot contains a read-only loopback mount of / and other mountpoints
     * nested into it, providing write access to specific directories.  We need
     * to delete the nested mount points before we delete the parent mount, or
     * the unmounting will fail.
     *
     * Thankfully /etc/mnttab lists mount points ordered by mount time, so if we
     * process them in reverse we'll do the correct thing.
     */
    for path in paths_with_mounts()?.iter().rev() {
        if path.starts_with(&root) {
            run(Command::new(UMOUNT).arg(path))
                .with_context(|| format!("failed to unmount {path:?}"))?;
        }
    }

    if list_zfs_datasets()?.contains(&dataset) {
        run(Command::new(ZFS).arg("destroy").arg(&dataset)).with_context(
            || format!("failed to destroy ZFS dataset {dataset:?}"),
        )?;
    }

    if root.exists() {
        std::fs::remove_dir_all(&root)
            .with_context(|| format!("failed to remove {root:?}"))?;
    }

    Ok(())
}

fn root_dir(worker: WorkerId) -> PathBuf {
    Path::new("/var/run/buildomat/worker").join(worker.to_string())
}

fn zfs_dataset_name(central: &Central, worker: WorkerId) -> String {
    let parent = central
        .config
        .illumos
        .parent_zfs_dataset
        .as_deref()
        .unwrap_or("rpool/buildomat-workers");
    format!("{parent}/{worker}")
}

fn zfs_dataset_mount(central: &Central, worker: WorkerId) -> PathBuf {
    Path::new("/").join(zfs_dataset_name(central, worker))
}

fn mount(fstype: &str, from: &Path, to: &Path, opts: &[&str]) -> Result<()> {
    run(Command::new(MOUNT).args(["-F", fstype]).args(opts).arg(from).arg(to))
        .with_context(|| {
            format!("failed to mount from {from:?} to {to:?} with {fstype}")
        })?;
    Ok(())
}

fn list_zfs_datasets() -> Result<Vec<String>> {
    Ok(run(Command::new(ZFS).args(["list", "-H", "-o", "name"]))
        .context("failed to list ZFS datasets")?
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| line.into())
        .collect())
}

fn paths_with_mounts() -> Result<Vec<PathBuf>> {
    /*
     * The format of /etc/mnttab is described in mnttab(5).
     */
    Ok(std::fs::read_to_string("/etc/mnttab")
        .context("failed to read mnttab")?
        .lines()
        .filter_map(|line| line.split('\t').nth(1))
        .map(PathBuf::from)
        .collect())
}

fn run(cmd: &mut Command) -> Result<String> {
    let name = cmd.get_program().to_string_lossy().to_string();
    let output = cmd
        .output()
        .with_context(|| format!("failed to spawn command {name:?}"))?;
    if !output.status.success() {
        bail!("{name:?} failed: {}", output.info());
    }
    output.stdout_string()
}
