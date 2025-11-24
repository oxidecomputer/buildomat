/*
 * Copyright 2024 Oxide Computer Company
 */

use std::ffi::CString;

use anyhow::{Result, bail};
pub use libc::zoneid_t;

#[link(name = "c")]
extern "C" {
    fn getzoneidbyname(name: *const libc::c_char) -> zoneid_t;
}

pub fn zone_name_to_id(name: &str) -> Result<Option<zoneid_t>> {
    let cs = CString::new(name)?;

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

pub fn zone_exists(name: &str) -> Result<bool> {
    Ok(zone_name_to_id(name)?.is_some())
}
