/*
 * Copyright 2021 Oxide Computer Company
 */

#![allow(dead_code)]

mod c {
    pub const A_REBOOT: i32 = 1;
    pub const A_SHUTDOWN: i32 = 2;
    pub const A_DUMP: i32 = 5;

    pub const AD_HALT: i32 = 0;
    pub const AD_BOOT: i32 = 1;
    pub const AD_POWEROFF: i32 = 6;

    extern "C" {
        pub fn uadmin(cmd: i32, fcn: i32, mdep: usize) -> i32;
    }
}

pub enum Action {
    Reboot(Next),
    Shutdown(Next),
    Dump(Next),
}

impl Action {
    fn cmd(&self) -> i32 {
        match self {
            Action::Reboot(_) => c::A_REBOOT,
            Action::Shutdown(_) => c::A_SHUTDOWN,
            Action::Dump(_) => c::A_DUMP,
        }
    }

    fn fcn(&self) -> i32 {
        match self {
            Action::Reboot(n) | Action::Shutdown(n) | Action::Dump(n) => {
                match n {
                    Next::Halt => c::AD_HALT,
                    Next::Boot => c::AD_BOOT,
                    Next::PowerOff => c::AD_POWEROFF,
                }
            }
        }
    }
}

pub enum Next {
    Halt,
    Boot,
    PowerOff,
}

pub fn uadmin(action: Action) -> std::io::Result<()> {
    let cmd = action.cmd();
    let fcn = action.fcn();

    if unsafe { c::uadmin(cmd, fcn, 0) } == -1 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}
