/*
 * Copyright 2026 Oxide Computer Company
 */

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::ffi::{CStr, CString};
use std::io::{Error as IoError, ErrorKind};
use std::path::PathBuf;

pub struct Passwd {
    pub uid: Uid,
    pub gid: Gid,
    pub name: Option<String>,
    pub dir: Option<PathBuf>,
}

impl Passwd {
    pub fn current_user() -> Result<Self> {
        Self::by_uid(getuid())?
            .ok_or_else(|| anyhow!("missing passwd entry for the current user"))
    }

    pub fn by_name(name: &str) -> Result<Option<Self>> {
        let name = CString::new(name.to_string())?;
        Self::from_libc(catch_errno(|| unsafe {
            libc::getpwnam(name.as_ptr())
        })?)
    }

    pub fn by_uid(uid: Uid) -> Result<Option<Self>> {
        Self::from_libc(catch_errno(|| unsafe { libc::getpwuid(uid.0) })?)
    }

    fn from_libc(ptr: *const libc::passwd) -> Result<Option<Self>> {
        let passwd = match unsafe { ptr.as_ref() } {
            Some(p) => p,
            None => return Ok(None),
        };

        Ok(Some(Passwd {
            uid: Uid(passwd.pw_uid),
            gid: Gid(passwd.pw_gid),
            name: if passwd.pw_name.is_null() {
                None
            } else {
                let cstr = unsafe { CStr::from_ptr(passwd.pw_name) };
                Some(cstr.to_str()?.into())
            },
            dir: if passwd.pw_dir.is_null() {
                None
            } else {
                let cstr = unsafe { CStr::from_ptr(passwd.pw_dir) };
                Some(cstr.to_str()?.into())
            },
        }))
    }
}

pub fn getuid() -> Uid {
    Uid(unsafe { libc::getuid() })
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct Uid(pub u32);

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct Gid(pub u32);

fn catch_errno<T, F: Fn() -> T>(f: F) -> Result<T, IoError> {
    loop {
        clear_errno();
        let result = f();
        let err = IoError::last_os_error();
        if err.raw_os_error() == Some(0) {
            return Ok(result);
        } else if let ErrorKind::Interrupted = err.kind() {
            continue;
        } else {
            return Err(err);
        }
    }
}

#[cfg(target_os = "illumos")]
fn clear_errno() {
    unsafe {
        let errno = libc::___errno();
        *errno = 0;
    }
}

#[cfg(target_os = "linux")]
fn clear_errno() {
    unsafe {
        let errno = libc::__errno_location();
        *errno = 0;
    }
}
