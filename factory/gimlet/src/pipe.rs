#![allow(unused)]

/*
 * Copyright 2025 Oxide Computer Company
 */

use std::sync::{
    Arc,
    atomic::{AtomicI32, Ordering},
};

use anyhow::{Result, bail};

#[derive(Clone)]
pub struct PipeEnd(Arc<AtomicI32>);

pub fn mkpipe() -> Result<(PipeEnd, PipeEnd)> {
    let mut fildes: [libc::c_int; 2] = [-1, -1];

    if unsafe { libc::pipe(fildes.as_mut_ptr()) } != 0 {
        let e = std::io::Error::last_os_error();
        bail!("pipe(): {e}");
    }

    Ok((
        PipeEnd(Arc::new(AtomicI32::new(fildes[0]))),
        PipeEnd(Arc::new(AtomicI32::new(fildes[1]))),
    ))
}

impl PipeEnd {
    pub fn close(&self) {
        let fd = self.0.swap(-1, Ordering::Relaxed);
        assert!(unsafe { libc::close(fd) } == 0, "close fd {fd} failed");
    }

    pub fn take(&self) -> libc::c_int {
        let fd = self.0.swap(-1, Ordering::Relaxed);
        assert!(fd != -1);
        fd
    }
}

impl Drop for PipeEnd {
    fn drop(&mut self) {
        let fd = self.0.swap(-1, Ordering::Relaxed);
        if fd != -1 {
            assert!(unsafe { libc::close(fd) } == 0, "close fd {fd} failed");
        }
    }
}

mod sys {
    extern "C" {
        pub fn closefrom(lowfd: libc::c_int);
    }
}

pub fn closefrom(lowfd: libc::c_int) {
    unsafe { sys::closefrom(lowfd) };
}
