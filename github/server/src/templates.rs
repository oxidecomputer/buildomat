/*
 * Copyright 2024 Oxide Computer Company
 */

use std::path::PathBuf;
use std::result::Result as SResult;

use anyhow::Result;
use dropshot::HttpError;
use slog::{Logger, error};

pub struct Templates {
    log: Logger,
    dir: Option<PathBuf>,
}

impl Templates {
    pub fn new(log: Logger) -> Result<Self> {
        /*
         * We deploy this program in "/opt/buildomat/lib" and the templates, if
         * present, are alongside in "/opt/buildomat/share".
         */
        let dir = {
            let dir = std::env::current_exe()?;
            if let Some(lib) = dir.parent() {
                if lib.file_name() == Some(std::ffi::OsStr::new("lib")) {
                    Some(lib.parent().unwrap().join("share"))
                } else {
                    None
                }
            } else {
                None
            }
        };

        Ok(Self { log, dir })
    }

    pub fn load(&self, name: &str) -> SResult<String, HttpError> {
        let log = &self.log;

        if let Some(dir) = &self.dir {
            let file = dir.join(name);
            match std::fs::read_to_string(&file) {
                Ok(data) => return Ok(data),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
                Err(e) => {
                    error!(log, "opening template {name:?}: {e}");
                }
            }
        }

        match name {
            "variety/basic/www/style.css" => {
                Ok(include_str!("../../../variety/basic/www/style.css").into())
            }
            _ => Err(HttpError::for_internal_error(format!(
                "could not locate template {name:?}"
            ))),
        }
    }
}
