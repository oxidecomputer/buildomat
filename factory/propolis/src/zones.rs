/*
 * Copyright 2024 Oxide Computer Company
 */

use anyhow::{bail, Result};

#[cfg(target_os = "illumos")]
type ZoneIdInner = libc::zoneid_t;
/*
 * "Infallible" is the stable equivalent of the never type (!).  It can not be
 * instantiated, ensuring that we never construct a ZoneId outside illumos.
 */
#[cfg(not(target_os = "illumos"))]
type ZoneIdInner = std::convert::Infallible;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ZoneId(pub ZoneIdInner);

impl std::fmt::Display for ZoneId {
    #[cfg(target_os = "illumos")]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }

    #[cfg(not(target_os = "illumos"))]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ZoneId")
    }
}

#[cfg(target_os = "illumos")]
pub fn zone_name_to_id(name: &str) -> Result<Option<ZoneId>> {
    #[link(name = "c")]
    extern "C" {
        fn getzoneidbyname(name: *const libc::c_char) -> zoneid_t;
    }

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

    Ok(Some(ZoneId(id)))
}

#[cfg(not(target_os = "illumos"))]
pub fn zone_name_to_id(_name: &str) -> Result<Option<ZoneId>> {
    bail!("only works on illumos systems");
}

pub fn zone_exists(name: &str) -> Result<bool> {
    Ok(zone_name_to_id(name)?.is_some())
}
