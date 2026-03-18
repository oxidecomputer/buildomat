/*
 * Copyright 2023 Oxide Computer Company
 */

use std::os::fd::AsRawFd;

use anyhow::{bail, Result};
#[cfg(target_os = "illumos")]
use libc::{getpeerucred, ucred_free, ucred_getzoneid, ucred_t};

use crate::zones::zoneid_t;

pub trait PeerUCred: AsRawFd {
    #[cfg(target_os = "illumos")]
    fn peer_ucred(&self) -> Result<UCred> {
        let fd = self.as_raw_fd();

        let mut uc: *mut ucred_t = std::ptr::null_mut();
        if unsafe { getpeerucred(fd, &mut uc) } != 0 {
            let e = std::io::Error::last_os_error();
            bail!("getpeerucred failure: {e}");
        }
        assert!(!uc.is_null());

        Ok(UCred { uc })
    }

    #[cfg(not(target_os = "illumos"))]
    fn peer_ucred(&self) -> Result<UCred> {
        bail!("only works on illumos systems");
    }
}

impl PeerUCred for tokio::net::UnixStream {}

pub struct UCred {
    #[cfg(target_os = "illumos")]
    uc: *mut ucred_t,
}

#[cfg(target_os = "illumos")]
impl Drop for UCred {
    fn drop(&mut self) {
        assert!(!self.uc.is_null());

        unsafe { ucred_free(self.uc) };
    }
}

impl UCred {
    pub fn zoneid(&self) -> Option<zoneid_t> {
        #[cfg(target_os = "illumos")]
        let zoneid = unsafe { ucred_getzoneid(self.uc) };
        #[cfg(not(target_os = "illumos"))]
        let zoneid = -1;

        if zoneid < 0 {
            None
        } else {
            Some(zoneid)
        }
    }
}
