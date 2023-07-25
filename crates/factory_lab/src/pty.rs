use std::{os::unix::prelude::*, ptr};

use anyhow::{bail, Result};
use libc::c_int;

pub struct Pty {
    manager: Option<RawFd>,
    subsidiary: Option<RawFd>,
}

impl Pty {
    pub fn new() -> Result<Pty> {
        let mut m: c_int = -1;
        let mut s: c_int = -1;

        let r = unsafe {
            libc::openpty(
                &mut m,
                &mut s,
                ptr::null_mut(),
                ptr::null(),
                ptr::null(),
            )
        };
        if r != 0 {
            let ose = std::io::Error::last_os_error();
            bail!("openpty: {:?}", ose);
        }

        Ok(Pty { manager: Some(m), subsidiary: Some(s) })
    }

    pub fn manager(&mut self) -> std::fs::File {
        let fd = self.manager.take().unwrap();
        unsafe { std::fs::File::from_raw_fd(fd) }
    }

    pub fn subsidiary(&mut self) -> i32 {
        //std::process::Stdio::from_raw_fd(self.subsidiary.take().unwrap())
        self.subsidiary.unwrap()
    }

    pub fn close_subsidiary(&mut self) {
        let fd = self.subsidiary.take().unwrap();
        assert_eq!(unsafe { libc::close(fd) }, 0);
    }
}

impl Drop for Pty {
    fn drop(&mut self) {
        if let Some(fd) = self.manager.take() {
            assert_eq!(unsafe { libc::close(fd) }, 0);
        }
        if let Some(fd) = self.subsidiary.take() {
            assert_eq!(unsafe { libc::close(fd) }, 0);
        }
    }
}
