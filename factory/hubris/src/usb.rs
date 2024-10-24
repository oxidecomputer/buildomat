/*
 * Copyright 2024 Oxide Computer
 */

use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use anyhow::{bail, anyhow, Result};
use devinfo::{DevInfo, DevLinks};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsbDevice {
    pub physpath: String,
    pub vendor: u32,
    pub product: u32,
    pub vendor_name: Option<String>,
    pub product_name: Option<String>,
    pub serialno: Option<String>,
    pub minors: Vec<String>,
    pub links: Vec<String>,
}

impl UsbDevice {
    fn mk(
        n: &devinfo::Node,
        usbvend: Option<&devinfo::Property>,
        usbprod: Option<&devinfo::Property>,
        usbser: Option<&devinfo::Property>,
        usbvendname: Option<&devinfo::Property>,
        usbprodname: Option<&devinfo::Property>,
    ) -> Option<UsbDevice> {
        let physpath = n.devfs_path().ok()?;
        let vendor: u32 = usbvend?.as_i32()?.try_into().unwrap();
        let product: u32 = usbprod?.as_i32()?.try_into().unwrap();
        let serialno = usbser.map(|p| p.to_str()).flatten();
        let vendor_name = usbvendname.map(|p| p.to_str()).flatten();
        let product_name = usbprodname.map(|p| p.to_str()).flatten();

        Some(UsbDevice {
            physpath,
            vendor,
            product,
            vendor_name,
            product_name,
            serialno,
            links: Default::default(),
            minors: Default::default(),
        })
    }

    //fn path(&self) -> PathBuf {
    //    PathBuf::from(&format!("/devices{}:keyboard", self.physpath)) /* XXX */
    //}

    // fn is_barcode_scanner(&self) -> bool {
    //     self.vendor == 0x5e0
    //         && self
    //             .product_name
    //             .as_deref()
    //             .map(|s| s.contains("Bar Code"))
    //             .unwrap_or(false)
    // }
}

#[derive(Clone)]
pub struct Usb(Arc<Mutex<Inner>>);

pub struct Inner {
    error: Option<String>,
    devices: Vec<UsbDevice>,
    gen: u64,
    //looms: BTreeMap<String, crate::ConfigLoom>,
}

impl Usb {
    pub fn new(/*looms: BTreeMap<String, crate::ConfigLoom>*/) -> Result<Usb> {
        let usb = Usb(Arc::new(Mutex::new(Inner {
            devices: Vec::new(),
            error: None,
            gen: 1,
            /*looms,*/
        })));

        let usb0 = usb.clone();
        std::thread::Builder::new()
            .name("usb".into())
            .spawn(move || usb0.thread())?;

        Ok(usb)
    }

    fn poll(&self) -> Result<Vec<UsbDevice>> {
        let mut list = Vec::new();

        let mut di = DevInfo::new()?;
        let mut dl = DevLinks::new(true)?;
        let mut w = di.walk_node();
        while let Some(n) = w.next().transpose()? {
            if n.driver_name() == Some("hubd".into()) {
                /*
                 * Ignore USB hubs.
                 */
                continue;
            }

            let mut usbvend = None;
            let mut usbprod = None;
            let mut usbprodname = None;
            let mut usbvendname = None;
            let mut usbser = None;

            let mut pw = n.props();
            while let Some(p) = pw.next().transpose()? {
                match p.name().as_str() {
                    "usb-vendor-id" => {
                        usbvend = Some(p);
                    }
                    "usb-product-id" => {
                        usbprod = Some(p);
                    }
                    "usb-vendor-name" => {
                        usbvendname = Some(p);
                    }
                    "usb-product-name" => {
                        usbprodname = Some(p);
                    }
                    "usb-serialno" => {
                        usbser = Some(p);
                    }
                    _ => {}
                }
            }

            if let Some(mut ud) = UsbDevice::mk(
                &n,
                usbvend.as_ref(),
                usbprod.as_ref(),
                usbser.as_ref(),
                usbvendname.as_ref(),
                usbprodname.as_ref(),
            ) {
                let mut minorwalk = n.minors();
                while let Some(mn) = minorwalk.next().transpose()? {
                    let p = mn.devfs_path()?;
                    for dl in dl.links_for_path(&p)? {
                        ud.links.push(dl.path().to_str().unwrap().to_string());
                    }
                    ud.minors.push(p);
                }

                list.push(ud);
            }
        }

        Ok(list)
    }

    fn thread(&self) {
        loop {
            match self.poll() {
                Ok(devices) => {
                    let mut i = self.0.lock().unwrap();
                    if i.devices != devices || i.error.is_some() {
                        i.devices.clear();
                        i.devices.extend(devices);
                        i.error = None;
                        i.gen += 1;
                    }
                }
                Err(e) => {
                    let mut i = self.0.lock().unwrap();
                    i.error = Some(e.to_string());
                    i.gen += 1;
                }
            }

            std::thread::sleep(std::time::Duration::from_secs(30));
        }
    }

    pub fn gen(&self) -> u64 {
        self.0.lock().unwrap().gen
    }

    pub fn info(&self) -> Result<()> {
        let i = self.0.lock().unwrap();
        for dev in i.devices.iter() {
            println!("{dev:#?}");
        }
        Ok(())
    }

    pub fn mculink(&self) -> Option<UsbDevice> {
        let i = self.0.lock().unwrap();
        i.devices.iter().find(|ud| {
            ud.vendor == 0x1fc9 && ud.product == 0x143
        }).cloned()
    }
}
