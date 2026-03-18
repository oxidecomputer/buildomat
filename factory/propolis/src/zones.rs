/*
 * Copyright 2024 Oxide Computer Company
 */

use anyhow::{bail, Result};

#[cfg(not(target_os = "illumos"))]
pub use libc::c_int as zoneid_t;
#[cfg(target_os = "illumos")]
pub use libc::zoneid_t;

#[link(name = "c")]
#[cfg(target_os = "illumos")]
extern "C" {
    fn getzoneidbyname(name: *const libc::c_char) -> zoneid_t;
}

#[cfg(target_os = "illumos")]
pub fn zone_name_to_id(name: &str) -> Result<Option<zoneid_t>> {
    let cs = std::ffi::CString::new(name)?;

    let id = unsafe { getzoneidbyname(cs.as_ptr()) };
    if id < 0 {
        let e = unsafe { *libc::___errno() };
        if e == libc::EINVAL {
            /*
             * According to the documentation, this actually means the zone does
             * not exist on the system.
             */
            return Ok(None);
        }

        let e = std::io::Error::from_raw_os_error(e);
        bail!("getzoneidbyname({name}): {e}");
    }

    Ok(Some(id))
}

#[cfg(not(target_os = "illumos"))]
pub fn zone_name_to_id(_name: &str) -> Result<Option<zoneid_t>> {
    bail!("only works on illumos systems");
}

pub fn zone_exists(name: &str) -> Result<bool> {
    Ok(zone_name_to_id(name)?.is_some())
}
