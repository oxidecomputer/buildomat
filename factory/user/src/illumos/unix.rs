/*
 * Copyright 2026 Oxide Computer Company
 */

use anyhow::{bail, Result};

#[cfg(target_os = "illumos")]
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

#[cfg(target_os = "illumos")]
pub fn sigsend_maybe(id: SigSendId, sig: i32) -> Result<bool> {
    let (idtype, id) = match id {
        SigSendId::UserId(uid) => (sys::P_UID, uid as i32),
    };

    if unsafe { sys::sigsend(idtype, id, sig) } == 0 {
        return Ok(true);
    }
    let error = std::io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::ESRCH) => {
            /*
             * No processes were found that matched the criteria.
             */
            Ok(false)
        }
        _ => bail!("sigsend({idtype}, {id}, {sig})): {error}"),
    }
}

#[cfg(not(target_os = "illumos"))]
pub fn sigsend_maybe(_id: SigSendId, _sig: i32) -> Result<bool> {
    bail!("only works on illumos systems");
}
