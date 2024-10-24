/*
 * Copyright 2024 Oxide Computer Company
 */

use anyhow::{bail, Result};
use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};
use std::process::exit;

pub fn errno() -> i32 {
    unsafe {
        let enp = libc::___errno();
        *enp
    }
}

pub fn clear_errno() {
    unsafe {
        let enp = libc::___errno();
        *enp = 0;
    }
}

#[allow(unused)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Passwd {
    pub name: Option<String>,
    pub passwd: Option<String>,
    pub uid: u32,
    pub gid: u32,
    #[cfg(target_os = "illumos")]
    pub age: Option<String>,
    #[cfg(target_os = "illumos")]
    pub comment: Option<String>,
    pub gecos: Option<String>,
    pub dir: Option<String>,
    pub shell: Option<String>,
}

impl Passwd {
    fn from(p: *const libc::passwd) -> Result<Passwd> {
        fn cs(lpsz: *const c_char) -> Result<Option<String>> {
            if lpsz.is_null() {
                Ok(None)
            } else {
                let cstr = unsafe { CStr::from_ptr(lpsz) };
                Ok(Some(cstr.to_str()?.to_string()))
            }
        }

        Ok(Passwd {
            name: cs(unsafe { (*p).pw_name })?,
            passwd: cs(unsafe { (*p).pw_passwd })?,
            uid: unsafe { (*p).pw_uid },
            gid: unsafe { (*p).pw_gid },
            #[cfg(target_os = "illumos")]
            age: cs(unsafe { (*p).pw_age })?,
            #[cfg(target_os = "illumos")]
            comment: cs(unsafe { (*p).pw_comment })?,
            gecos: cs(unsafe { (*p).pw_gecos })?,
            dir: cs(unsafe { (*p).pw_dir })?,
            shell: cs(unsafe { (*p).pw_shell })?,
        })
    }
}

pub fn get_passwd_by_name(name: &str) -> Result<Option<Passwd>> {
    clear_errno();
    let name = CString::new(name.to_owned())?;
    let p = unsafe { libc::getpwnam(name.as_ptr()) };
    let e = errno();
    if p.is_null() {
        if e == 0 {
            Ok(None)
        } else {
            bail!("getpwnam: errno {}", e);
        }
    } else {
        Ok(Some(Passwd::from(p)?))
    }
}

mod sys {
    use libc::{c_int, id_t, idtype_t};

    pub const P_UID: idtype_t = 5;

    extern "C" {
        pub fn sigsend(idtype: idtype_t, id: id_t, sig: c_int) -> c_int;
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SigSendId {
    UserId(u32),
}

pub fn sigsend_maybe(id: SigSendId, sig: i32) -> Result<bool> {
    let (idtype, id) = match id {
        SigSendId::UserId(uid) => (sys::P_UID, uid as i32),
    };

    if unsafe { sys::sigsend(idtype, id, sig) } == 0 {
        return Ok(true);
    }

    match errno() {
        libc::ESRCH => {
            /*
             * No processes were found that matched the criteria.
             */
            Ok(false)
        }
        e => bail!(
            "sigsend({idtype}, {id}, {sig})): {}",
            std::io::Error::from_raw_os_error(e),
        ),
    }
}
