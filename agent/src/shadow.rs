/*
 * Copyright 2024 Oxide Computer Company
 */

use std::{
    io::{Read, Write},
    path::Path,
};

use anyhow::{Result, bail};

#[derive(Clone, PartialEq)]
pub struct ShadowFile {
    entries: Vec<Vec<String>>,
}

impl ShadowFile {
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut f = std::fs::File::open(path.as_ref())?;
        let mut data = String::new();
        f.read_to_string(&mut data)?;

        let entries = data
            .lines()
            .enumerate()
            .map(|(i, l)| {
                let fields =
                    l.split(':').map(str::to_string).collect::<Vec<_>>();
                if fields.len() != 9 {
                    bail!("invalid shadow line {}: {:?}", i, fields);
                }
                Ok(fields)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(ShadowFile { entries })
    }

    pub fn password_set(&mut self, user: &str, password: &str) -> Result<()> {
        /*
         * First, make sure the username appears exactly once in the shadow
         * file.
         */
        let mc = self.entries.iter().filter(|e| e[0] == user).count();
        if mc != 1 {
            bail!("found {} matches for user {} in shadow file", mc, user);
        }

        self.entries.iter_mut().for_each(|e| {
            if e[0] == user {
                e[1] = password.to_string();
            }
        });
        Ok(())
    }

    pub fn write<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let mut f = std::fs::OpenOptions::new()
            .create(false)
            .truncate(true)
            .write(true)
            .open(path.as_ref())?;

        let mut data = self
            .entries
            .iter()
            .map(|e| e.join(":"))
            .collect::<Vec<_>>()
            .join("\n");
        data.push('\n');

        f.write_all(data.as_bytes())?;
        f.flush()?;
        Ok(())
    }
}
