/*
 * Copyright 2026 Oxide Computer Company
 */

use crate::control::Stuff;
use anyhow::{anyhow, bail, Error, Result};
use openssl::sha::Sha256;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use tokio::task::spawn_blocking;

pub async fn restore(stuff: &mut Stuff, cargo_toml: &Path) -> Result<()> {
    let cargo_toml = cargo_toml.to_path_buf();
    let cache_key = spawn_blocking(move || {
        let metadata = cargo_metadata(&cargo_toml)?;
        cache_key(&metadata)
    })
    .await??;

    super::restore(stuff, &cache_key).await
}

pub async fn save(stuff: &mut Stuff, cargo_toml: &Path) -> Result<()> {
    let cargo_toml = cargo_toml.to_path_buf();
    let (cache_key, files_to_cache) = spawn_blocking(move || {
        let metadata = cargo_metadata(&cargo_toml)?;
        Ok::<_, Error>((cache_key(&metadata)?, files_to_cache(&metadata)?))
    })
    .await??;

    super::save(stuff, &cache_key, files_to_cache).await
}

fn files_to_cache(metadata: &CargoMetadata) -> Result<Vec<PathBuf>> {
    /*
     * Caching the build artefacts of path dependencies has diminishing results,
     * as those (for example within a workspace) tend to change a lot.  The
     * default behavior of Rust caches in buildomat is thus to only cache the
     * artefacts of third-party dependencies.
     */
    let mut cacheable_packages = HashSet::new();
    let mut cacheable_targets = HashSet::new();
    for package in &metadata.packages {
        if is_cacheable_package(package)? {
            cacheable_packages.insert(package.name.to_string());
            cacheable_targets.insert(package.name.replace('-', "_"));
            for target in &package.targets {
                cacheable_targets.insert(target.name.replace('-', "_"));
            }
        }
    }

    let mut ftc = FilesToCache {
        files: Vec::new(),
        cacheable_packages,
        cacheable_targets,
    };

    /*
     * Cargo 1.91 introduced the split between "target directories" (containing
     * the final artifacts, like executables) and "build directories" (dedicated
     * to the intermediate artifacts).  As we only want to cache intermediate
     * artifacts we look within the build directory.
     *
     * Previous versions of Cargo didn't emit build_directory in the metadata
     * though, so in those cases we have to look within the target directory.
     */
    if let Some(build_dir) = &metadata.build_directory {
        ftc.find_profile_directories(build_dir)?;
    } else {
        ftc.find_profile_directories(&metadata.target_directory)?;
    }

    /*
     * The buildomat cache implementation requires cached paths to be relative,
     * but the code above generates absolute paths (we base our search on the
     * path returned by `cargo metadata`, which is absolute).
     */
    let current_dir = std::env::current_dir()?;
    for file in &mut ftc.files {
        *file = file
            .strip_prefix(&current_dir)
            .map_err(|_| anyhow!("path {file:?} is not in the cwd"))?
            .into();
    }

    Ok(ftc.files)
}

struct FilesToCache {
    files: Vec<PathBuf>,
    cacheable_packages: HashSet<String>,
    cacheable_targets: HashSet<String>,
}

impl FilesToCache {
    /*
     * "Profile directories" are the directories containing the build artefacts
     * to cache.  They often are target/debug and target/release, but when cross
     * compiling they become target/$host/debug and target/$host/release.
     *
     * To handle all cases well, we recursively search within target/ for all
     * directories that look like a profile directory.
     */
    fn find_profile_directories(&mut self, path: &Path) -> Result<()> {
        /*
         * There is not really a "this is a profile directory" indicator, other
         * than looking whether it has the structure of one.  We thus check for
         * the presence of the .fingerprint/ directory within it.
         */
        if path.join(".fingerprint").exists() {
            self.handle_profile_directory(path)?;
        } else {
            for entry in path.read_dir()? {
                let entry = entry?;
                if entry.file_type()?.is_dir() {
                    self.find_profile_directories(&entry.path())?;
                }
            }
        }
        Ok(())
    }

    fn handle_profile_directory(&mut self, path: &Path) -> Result<()> {
        /*
         * .fingerprint/ and build/ contain an entry per package.
         */
        self.collect_directory_with_packages(&path.join(".fingerprint"))?;
        self.collect_directory_with_packages(&path.join("build"))?;
        /*
         * deps/ contains an entry per package *and* one for each "target"
         * included within the package.  Targets are, e.g., libraries, binaries,
         * proc macros, tests, or examples.
         */
        self.collect_directory_with_targets(&path.join("deps"))?;
        Ok(())
    }

    fn collect_directory_with_packages(&mut self, path: &Path) -> Result<()> {
        for entry in path.read_dir()? {
            let entry = entry?;
            let file_name = entry.file_name();
            let Some((name, _hash)) = file_name
                .to_str()
                .ok_or_else(|| anyhow!("non-UTF-8 file name"))?
                .rsplit_once('-')
            else {
                continue;
            };
            if self.cacheable_packages.contains(name) {
                self.collect_recursive(&entry.path())?;
            }
        }
        Ok(())
    }

    fn collect_directory_with_targets(&mut self, path: &Path) -> Result<()> {
        for entry in path.read_dir()? {
            let entry = entry?;
            let file_name = entry.file_name();
            let Some((name, _hash)) = file_name
                .to_str()
                .ok_or_else(|| anyhow!("non-UTF-8 file name"))?
                .rsplit_once('-')
            else {
                continue;
            };
            /*
             * Directories with targets contains both files starting with the
             * dependency name, and files starting with `lib${dependency}`.
             *
             * Note that we cannot use `name.trim_end_matches` to strip the lib
             * prefix, as that method removes one *or more* occurrences, not
             * just one.  That causes problems for crates called "libc", as then
             * "liblibc" would be trimmed to "c", rather than "libc".
             */
            if self.cacheable_targets.iter().any(|ct| {
                ct == name || Some(ct.as_str()) == name.strip_prefix("lib")
            }) {
                self.collect_recursive(&entry.path())?;
            }
        }
        Ok(())
    }

    fn collect_recursive(&mut self, path: &Path) -> Result<()> {
        if path.is_dir() {
            for child in path.read_dir()? {
                self.collect_recursive(&child?.path())?;
            }
        } else {
            self.files.push(path.into());
        }
        Ok(())
    }
}

/**
 * Check whether a Cargo package (as returned by "cargo metadata") should be
 * cached.  We currently cache every package outside of the local workspace.
 *
 * https://doc.rust-lang.org/cargo/reference/pkgid-spec.html
 */
fn is_cacheable_package(package: &CargoPackage) -> Result<bool> {
    let kind = package.id.split_once('+').ok_or_else(|| {
        anyhow!("package id {} is not fully qualified", package.id)
    })?;
    match kind.0 {
        "git" => Ok(true),
        "registry" => Ok(true),
        "path" => Ok(false),
        other => bail!("unknown package kind: {other}"),
    }
}

/*
 * The cache key of a Rust project needs to include the rustc version (caches
 * are version-specific), which host it's being built on (as some caches depend
 * on host-specific resources), and what is actually being cached.  Since we are
 * only caching third-party dependencies, "what's actually being cached" is
 * neatly described by Cargo.lock, which we hash.
 */
fn cache_key(metadata: &CargoMetadata) -> Result<String> {
    let rustc_version = rustc_version()?;

    let lockfile_path = metadata.workspace_root.join("Cargo.lock");
    let mut lockfile_digest = Sha256::new();
    lockfile_digest.update(&std::fs::read(&lockfile_path)?);
    let lockfile_digest = hex::encode(lockfile_digest.finish());

    Ok(format!("rust-deps-{rustc_version}-{lockfile_digest}"))
}

fn rustc_version() -> Result<String> {
    eprintln!("discovering the rustc version...");
    let rustc = Command::new("rustc")
        .arg("-vV")
        .stdout(Stdio::piped())
        .spawn()?
        .wait_with_output()?;
    if !rustc.status.success() {
        bail!("invoking rustc failed with {}", rustc.status);
    }
    let stdout = std::str::from_utf8(&rustc.stdout)?;

    let kv = stdout
        .lines()
        .filter_map(|line| line.split_once(": "))
        .collect::<HashMap<_, _>>();

    let Some(release) = kv.get("release") else {
        bail!("missing release field in \"rustc -vV\"");
    };
    let Some(commit_date) = kv.get("commit-date") else {
        bail!("missing commit-date field in \"rustc -vV\"");
    };
    let Some(host) = kv.get("host") else {
        bail!("missing host field in \"rustc -vV\"");
    };

    if release.ends_with("-nightly") {
        Ok(format!("nightly-{commit_date}-{host}"))
    } else if release.ends_with("-beta") {
        Ok(format!("beta-{commit_date}-{host}"))
    } else {
        Ok(format!("{release}-{host}"))
    }
}

fn cargo_metadata(cargo_toml: &Path) -> Result<CargoMetadata> {
    eprintln!("discovering metadata about the Cargo project...");
    let cargo = Command::new("cargo")
        .arg("metadata")
        .arg("--format-version=1")
        .arg("--manifest-path")
        .arg(cargo_toml)
        .stdout(Stdio::piped())
        .spawn()?
        .wait_with_output()?;
    if !cargo.status.success() {
        bail!("invoking cargo failed with {}", cargo.status);
    }
    Ok(serde_json::from_slice(&cargo.stdout)?)
}

#[derive(Deserialize)]
struct CargoMetadata {
    packages: Vec<CargoPackage>,
    workspace_root: PathBuf,
    target_directory: PathBuf,
    /*
     * The build_directory field was added in Rust 1.91.0, older versions won't
     * return it.  We thus have to make it optional.
     */
    #[serde(default)]
    build_directory: Option<PathBuf>,
}

#[derive(Deserialize)]
struct CargoPackage {
    id: String,
    name: String,
    targets: Vec<CargoTarget>,
}

#[derive(Deserialize)]
struct CargoTarget {
    name: String,
}
