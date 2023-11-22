use std::process::Command;

use anyhow::{anyhow, bail, Result};
use buildomat_common::*;

const DLADM: &str = "/usr/sbin/dladm";

struct Terms {
    terms: Vec<String>,
    buf: Option<String>,
}

impl Terms {
    fn append(&mut self, c: char) {
        if self.buf.is_none() {
            self.buf = Some(String::new());
        }
        self.buf.as_mut().unwrap().push(c);
    }

    fn commit(&mut self) {
        if let Some(val) = &self.buf {
            self.terms.push(val.to_string());
        }
        self.buf = None;
    }

    fn result(&self) -> Vec<String> {
        self.terms.to_owned()
    }

    fn new() -> Terms {
        Terms { terms: Vec::new(), buf: Some(String::new()) }
    }
}

fn parse_net_adm(stdout: Vec<u8>) -> Result<Vec<Vec<String>>> {
    let stdout = String::from_utf8(stdout)?;
    let mut out = Vec::new();

    for l in stdout.lines() {
        let mut terms = Terms::new();
        let mut escape = false;

        for c in l.chars() {
            if escape {
                terms.append(c);
                escape = false;
            } else if c == '\\' {
                escape = true;
            } else if c == ':' {
                terms.commit();
            } else {
                terms.append(c);
            }
        }
        terms.commit();

        out.push(terms.result());
    }

    Ok(out)
}

fn mac_sanitise(input: &str) -> String {
    let mac = input.split(':').fold(String::new(), |mut buf, octet| {
        if !buf.is_empty() {
            /*
             * Put the separating colon back between octets:
             */
            buf.push(':');
        }

        assert!(octet.len() == 1 || octet.len() == 2);
        if octet.len() < 2 {
            /*
             * Use a leading zero to pad any single-digit octets:
             */
            buf.push('0');
        }
        buf.push_str(octet);

        buf
    });

    assert_eq!(mac.len(), 17);
    mac
}

pub struct Vnic {
    pub name: String,
    pub physical: String,
    pub mac: String,
    pub vlan: Option<u16>,
}

pub fn dladm_vnic_get(vnic: &str) -> Result<Option<Vnic>> {
    let output = Command::new(DLADM)
        .env_clear()
        .arg("show-vnic")
        .arg("-p")
        .arg("-o")
        .arg("link,over,macaddress,vid")
        .arg(vnic)
        .output()?;

    if !output.status.success() {
        let e = String::from_utf8_lossy(&output.stderr);
        if e.contains("invalid vnic name") && e.contains("object not found") {
            return Ok(None);
        }

        bail!("dladm failed: {}", output.info());
    }

    let vnics = parse_net_adm(output.stdout)?
        .into_iter()
        .map(|l| {
            Ok(Vnic {
                name: l[0].to_string(),
                physical: l[1].to_string(),
                mac: mac_sanitise(&l[2]),
                vlan: {
                    let vid = l[3]
                        .parse()
                        .map_err(|e| anyhow!("parsing VLAN ID: {e}"))?;

                    if vid == 0 {
                        None
                    } else {
                        Some(vid)
                    }
                },
            })
        })
        .collect::<Result<Vec<_>>>()?;

    if vnics.len() > 1 {
        bail!("found more than 1 vnic?!");
    }

    Ok(vnics.into_iter().next())
}

pub fn dladm_delete_vnic(vnic: &str) -> Result<()> {
    let output = Command::new(DLADM)
        .env_clear()
        .arg("delete-vnic")
        .arg(vnic)
        .output()?;

    if !output.status.success() {
        bail!("dladm delete-vnic failed: {}", output.info());
    }

    Ok(())
}

pub fn dladm_create_vnic(
    vnic: &str,
    over: &str,
    vlan: Option<u16>,
) -> Result<()> {
    let mut cmd = Command::new(DLADM);
    cmd.env_clear();
    cmd.arg("create-vnic");
    cmd.arg("-t");
    cmd.arg("-l").arg(over);
    if let Some(vlan) = vlan {
        cmd.arg("-v").arg(vlan.to_string());
    }
    cmd.arg(vnic);
    let output = cmd.output()?;

    if !output.status.success() {
        bail!("dladm create-vnic failed: {}", output.info());
    }

    Ok(())
}
