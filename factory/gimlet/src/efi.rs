use std::os::fd::{AsRawFd, RawFd};

use anyhow::{Result, bail};

#[allow(non_camel_case_types)]
#[allow(unused)]
pub mod sys {
    use libc::{c_char, c_int, c_uint, c_ushort};

    pub type diskaddr_t = libc::c_ulonglong;

    /*
     * Partition identification tags
     */
    pub const V_UNASSIGNED: c_ushort = 0x00;
    pub const V_BOOT: c_ushort = 0x01;
    pub const V_ROOT: c_ushort = 0x02;
    pub const V_SWAP: c_ushort = 0x03;
    pub const V_USR: c_ushort = 0x04;
    pub const V_BACKUP: c_ushort = 0x05;
    pub const V_STAND: c_ushort = 0x06;
    pub const V_VAR: c_ushort = 0x07;
    pub const V_HOME: c_ushort = 0x08;
    pub const V_ALTSCTR: c_ushort = 0x09;
    pub const V_CACHE: c_ushort = 0x0a;
    pub const V_RESERVED: c_ushort = 0x0b;
    pub const V_SYSTEM: c_ushort = 0x0c;
    pub const V_VXVM_PUB: c_ushort = 0x0e;
    pub const V_VXVM_PRIV: c_ushort = 0x0f;
    pub const V_BIOS_BOOT: c_ushort = 0x18;
    pub const V_NETBSD_FFS: c_ushort = 0xff;

    pub const EFI_MIN_ARRAY_SIZE: c_uint = 16 * 1024;

    #[derive(Debug, Clone)]
    #[repr(C)]
    pub struct uuid {
        pub time_low: u32,
        pub time_mid: u16,
        pub time_hi_and_version: u16,
        pub clock_seq_hi_and_reserved: u8,
        pub clock_seq_low: u8,
        pub node_addr: [u8; 6],
    }

    #[derive(Debug, Clone)]
    #[repr(C)]
    pub struct dk_part {
        pub p_start: diskaddr_t,
        pub p_size: diskaddr_t,
        pub p_guid: uuid,
        pub p_tag: c_ushort,
        pub p_flag: c_ushort,
        pub p_name: [c_char; 36],
        pub p_uguid: uuid,
        pub p_resv: [c_uint; 8],
    }

    #[repr(C)]
    pub struct dk_gpt_t {
        /*** Set to EFI_VERSION_CURRENT */
        pub efi_version: c_uint,
        /*** number of partitions */
        pub efi_nparts: c_uint,
        /*** size of each entry (unused?!) */
        pub efi_part_size: c_uint,
        /*** size of block in bytes */
        pub efi_lbasize: c_uint,
        /*** last block on disk */
        pub efi_last_lba: diskaddr_t,
        /*** first block after labels */
        pub efi_first_u_lba: diskaddr_t,
        /*** last block before backup labels */
        pub efi_last_u_lba: diskaddr_t,
        pub efi_disk_uguid: uuid,
        pub efi_flags: c_uint,
        pub efi_reserved1: c_uint,
        pub efi_altern_lba: diskaddr_t,
        pub efi_reserved: [c_uint; 12],
        pub efi_parts: [dk_part; 1],
    }

    #[link(name = "efi")]
    unsafe extern "C" {
        pub fn efi_alloc_and_init(
            fd: c_int,
            nparts: u32,
            vtoc: *mut *mut dk_gpt_t,
        ) -> c_int;
        pub fn efi_alloc_and_read(fd: c_int, vtoc: *mut *mut dk_gpt_t)
        -> c_int;
        pub fn efi_write(fd: c_int, vtoc: *mut dk_gpt_t) -> c_int;
        pub fn efi_free(vtoc: *mut dk_gpt_t);
        pub fn efi_reserved_sectors(vtoc: *mut dk_gpt_t) -> c_uint;
    }
}

pub struct Gpt {
    vtoc: *mut sys::dk_gpt_t,
}

impl Drop for Gpt {
    fn drop(&mut self) {
        unsafe { sys::efi_free(self.vtoc) };
    }
}

impl Gpt {
    pub fn init_from_fd(f: RawFd) -> Result<Gpt> {
        let fd = f.as_raw_fd();

        let mut vtoc = std::ptr::null_mut();
        let r = unsafe { sys::efi_alloc_and_init(fd, 9, &mut vtoc) };
        if r < 0 {
            bail!("efi_alloc_and_init(3EXT) failed with {r}");
        }
        if vtoc.is_null() {
            bail!("NULL vtoc?!");
        }

        Ok(Gpt { vtoc })
    }

    #[allow(unused)]
    pub fn read_from_fd(f: RawFd) -> Result<Gpt> {
        let fd = f.as_raw_fd();

        let mut vtoc = std::ptr::null_mut();
        let r = unsafe { sys::efi_alloc_and_read(fd, &mut vtoc) };
        if r < 0 {
            bail!("efi_alloc_and_read(3EXT) failed with {r}");
        }
        if vtoc.is_null() {
            bail!("NULL vtoc?!");
        }

        Ok(Gpt { vtoc })
    }

    pub fn write_to_fd(&mut self, f: RawFd) -> Result<()> {
        let fd = f.as_raw_fd();

        let r = unsafe { sys::efi_write(fd, self.vtoc) };
        if r < 0 {
            bail!("efi_write(3EXT) failed with {r}");
        }

        Ok(())
    }

    pub fn block_size(&self) -> u64 {
        u64::from(unsafe { (*self.vtoc).efi_lbasize })
    }

    pub fn reserved_sectors(&self) -> u32 {
        unsafe { sys::efi_reserved_sectors(self.vtoc) }
    }

    pub fn parts(&self) -> &[sys::dk_part] {
        let a = unsafe { &(*self.vtoc).efi_parts }.as_ptr();
        let n: usize = unsafe { (*self.vtoc).efi_nparts }.try_into().unwrap();
        unsafe { std::slice::from_raw_parts(a, n) }
    }

    pub fn parts_mut(&mut self) -> &mut [sys::dk_part] {
        let a = unsafe { &mut (*self.vtoc).efi_parts }.as_mut_ptr();
        let n: usize = unsafe { (*self.vtoc).efi_nparts }.try_into().unwrap();
        unsafe { std::slice::from_raw_parts_mut(a, n) }
    }

    pub fn lba_first(&self) -> u64 {
        unsafe { (*self.vtoc).efi_first_u_lba }
    }

    pub fn lba_last(&self) -> u64 {
        unsafe { (*self.vtoc).efi_last_u_lba }
    }
}

#[cfg(test)]
mod test {
    use std::mem::{offset_of, size_of};

    use super::sys::*;

    #[test]
    fn shapes() {
        assert_eq!(offset_of!(dk_gpt_t, efi_version), 0x0);
        assert_eq!(offset_of!(dk_gpt_t, efi_nparts), 0x4);
        assert_eq!(offset_of!(dk_gpt_t, efi_lbasize), 0xc);
        assert_eq!(offset_of!(dk_gpt_t, efi_disk_uguid), 0x28);
        assert_eq!(offset_of!(dk_gpt_t, efi_parts), 0x78);
        assert_eq!(size_of::<dk_gpt_t>(), 0xf0);

        assert_eq!(offset_of!(dk_part, p_tag), 0x20);
        assert_eq!(offset_of!(dk_part, p_uguid), 0x48);
        assert_eq!(size_of::<dk_part>(), 0x78);
    }
}
