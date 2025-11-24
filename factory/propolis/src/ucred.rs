/*
 * Copyright 2023 Oxide Computer Company
 */

use std::os::fd::AsRawFd;

use anyhow::{Result, bail};
use libc::{getpeerucred, ucred_free, ucred_getzoneid, ucred_t, zoneid_t};

pub trait PeerUCred: AsRawFd {
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
}

impl PeerUCred for tokio::net::UnixStream {}

pub struct UCred {
    uc: *mut ucred_t,
}

impl Drop for UCred {
    fn drop(&mut self) {
        assert!(!self.uc.is_null());

        unsafe { ucred_free(self.uc) };
    }
}

impl UCred {
    pub fn zoneid(&self) -> Option<zoneid_t> {
        let zoneid = unsafe { ucred_getzoneid(self.uc) };
        if zoneid < 0 { None } else { Some(zoneid) }
    }
}
