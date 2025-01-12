/*
 * Copyright 2023 Oxide Computer Company
 */

use std::path::{Path, PathBuf};
use std::result::Result as SResult;
use std::sync::Arc;

use anyhow::{anyhow, Result};

use buildomat_download::RequestContextEx;
use dropshot::{
    Body, ClientErrorStatusCode, HttpError, Path as TypedPath,
    Query as TypedQuery,
};
use schemars::JsonSchema;
use serde::Deserialize;
use slog::info;

use super::{Central, MARKER_BOOT, MARKER_HOLD};

type HResult<T> = SResult<T, dropshot::HttpError>;

struct FormatScript {
    coreurl: String,
    host_config: super::config::ConfigFileHost,
    key: Option<String>,
    marker: Option<String>,
    bootstrap: Option<String>,
    debug: bool,
}

impl FormatScript {
    fn new(
        cf: &super::config::ConfigFile,
        hc: &super::config::ConfigFileHost,
    ) -> FormatScript {
        FormatScript {
            coreurl: cf.general.baseurl.clone(),
            host_config: hc.clone(),
            key: None,
            marker: None,
            bootstrap: None,
            debug: false,
        }
    }

    fn marker(&mut self, s: &str) -> &mut Self {
        self.marker = Some(s.to_string());
        self
    }

    fn instance(&mut self, i: Option<&super::db::Instance>) -> &mut Self {
        if let Some(i) = i {
            self.key = Some(i.key.to_string());
            self.bootstrap = Some(i.bootstrap.to_string());
        }
        self
    }

    fn debug(&mut self, debug: bool) -> &mut Self {
        self.debug = debug;
        self
    }

    fn format(&self, s: &str) -> String {
        let bootargs = self
            .host_config
            .debug_boot_args
            .as_deref()
            .unwrap_or(if self.debug { "-kd" } else { "" });

        let mut s = s
            .replace("%BASEURL%", &self.host_config.lab_baseurl)
            .replace("%HOST%", &self.host_config.nodename)
            .replace("%CONSOLE%", &self.host_config.console)
            .replace("%BOOTARGS%", &bootargs)
            .replace("%COREURL%", &self.coreurl);

        if let Some(key) = self.key.as_deref() {
            s = s.replace("%KEY%", key);
        }

        if let Some(bootstrap) = self.bootstrap.as_deref() {
            s = s.replace("%STRAP%", bootstrap);
        }

        if let Some(marker) = self.marker.as_deref() {
            s = s.replace("%MARKER%", marker);
        }

        s
    }
}

trait MakeInternalError<T> {
    fn or_500(self) -> HResult<T>;
}

impl<T> MakeInternalError<T> for std::result::Result<T, anyhow::Error> {
    fn or_500(self) -> SResult<T, HttpError> {
        self.map_err(|e| {
            let msg = format!("internal error: {:?}", e);
            HttpError::for_internal_error(msg)
        })
    }
}

impl<T> MakeInternalError<T> for std::io::Result<T> {
    fn or_500(self) -> SResult<T, HttpError> {
        self.map_err(|e| {
            let msg = format!("internal error: {:?}", e);
            HttpError::for_internal_error(msg)
        })
    }
}

impl<T> MakeInternalError<T> for buildomat_database::DBResult<T> {
    fn or_500(self) -> SResult<T, HttpError> {
        self.map_err(|e| {
            use buildomat_database::DatabaseError;

            match e {
                DatabaseError::Conflict(msg) => HttpError::for_client_error(
                    Some("conflict".to_string()),
                    ClientErrorStatusCode::CONFLICT,
                    msg,
                ),
                _ => {
                    let msg = format!("internal error: {:?}", e);
                    HttpError::for_internal_error(msg)
                }
            }
        })
    }
}

trait CentralExt {
    fn host_config(
        &self,
        name: &str,
    ) -> HResult<&super::config::ConfigFileHost>;

    fn target_os_dir(&self, target: &str) -> HResult<PathBuf>;
}

impl CentralExt for Central {
    fn host_config(
        &self,
        name: &str,
    ) -> HResult<&super::config::ConfigFileHost> {
        if let Some(h) = self.hosts.get(&name.to_string()) {
            Ok(&h.config)
        } else {
            Err(dropshot::HttpError::for_client_error(
                None,
                ClientErrorStatusCode::BAD_REQUEST,
                "invalid host".to_string(),
            ))
        }
    }

    fn target_os_dir(&self, target: &str) -> HResult<PathBuf> {
        if let Some(t) = self.config.target.get(target) {
            Ok(PathBuf::from(&t.os_dir))
        } else {
            Err(dropshot::HttpError::for_client_error(
                None,
                ClientErrorStatusCode::BAD_REQUEST,
                "host has invalid target".to_string(),
            ))
        }
    }
}

#[derive(JsonSchema, Deserialize)]
struct IpxePath {
    host: String,
}

#[derive(JsonSchema, Deserialize)]
struct KeyQuery {
    key: String,
}

#[derive(JsonSchema, Deserialize)]
struct HostFilePath {
    host: String,
    file: Vec<String>,
}

#[dropshot::endpoint {
    method = GET,
    path = "/os/{host}/{file:.*}",
    unpublished = true,
}]
async fn os_file(
    ctx: dropshot::RequestContext<Arc<Central>>,
    path: TypedPath<HostFilePath>,
) -> HResult<hyper::Response<Body>> {
    let c = ctx.context();
    let path = path.into_inner();
    let pr = ctx.range();

    info!(ctx.log, "[{}] os file request for {:?}", path.host, path.file);

    let hc = c.host_config(&path.host)?;
    let (booting, target) = if hc.debug_os_dir.is_some() {
        (true, None)
    } else if let Some(i) = c.db.instance_for_host(&path.host).or_500()? {
        (i.is_preboot(), Some(i.target().to_string()))
    } else {
        (false, None)
    };

    if !booting {
        return Err(HttpError::for_bad_request(
            None,
            "access to boot media denied except when booting".to_string(),
        ));
    }

    let mut fp = if let Some(debug) = &hc.debug_os_dir {
        info!(ctx.log, "[{}] using debug OS bits at {:?}", path.host, debug);
        PathBuf::from(debug)
    } else {
        c.target_os_dir(target.as_deref().unwrap())?
    };
    for component in path.file.iter() {
        assert!(!component.contains('/'));
        if component == "." || component == ".." {
            return Err(HttpError::for_bad_request(
                None,
                "no relative path".to_string(),
            ));
        }

        fp.push(component);
    }

    let f = std::fs::File::open(&fp)
        .map_err(|e| HttpError::for_internal_error(e.to_string()))?;

    buildomat_download::stream_from_file(
        &ctx.log,
        format!("os file for {}: {:?}", path.host, path.file),
        f,
        pr,
        false,
    )
    .await
    .map_err(|e| HttpError::for_internal_error(e.to_string()))
}

#[dropshot::endpoint {
    method = POST,
    path = "/signal/{host}",
    unpublished = true,
}]
async fn signal(
    ctx: dropshot::RequestContext<Arc<Central>>,
    query: TypedQuery<KeyQuery>,
    path: TypedPath<IpxePath>,
) -> HResult<hyper::Response<Body>> {
    let c = ctx.context();
    let path = path.into_inner();
    let query = query.into_inner();

    /*
     * Mark instance as having booted the OS.  Once we reach this point, we do
     * not want to offer the IPXE script anymore.
     */
    let _hc = c.host_config(&path.host)?;
    if let Some(i) = c.db.instance_for_host(&path.host).or_500()? {
        if i.key != query.key {
            /*
             * The key is not correct for this instance.
             */
            return Err(HttpError::for_bad_request(
                None,
                "invalid key".to_string(),
            ));
        }

        if i.should_teardown() {
            return Err(HttpError::for_bad_request(
                None,
                "instance is tearing down".to_string(),
            ));
        }

        if i.is_preboot() {
            /*
             * If the system is not yet marked as booted, mark it booted now.
             */
            c.db.instance_boot(&i).or_500()?;
        }

        Ok(hyper::Response::builder().body("".into())?)
    } else {
        Err(HttpError::for_bad_request(
            None,
            "no instance for this host".to_string(),
        ))
    }
}

#[dropshot::endpoint {
    method = GET,
    path = "/postboot/{host}",
    unpublished = true,
}]
async fn postboot_script(
    ctx: dropshot::RequestContext<Arc<Central>>,
    path: TypedPath<IpxePath>,
) -> HResult<hyper::Response<Body>> {
    let c = ctx.context();
    let path = path.into_inner();

    let hc = c.host_config(&path.host)?;
    let i = c.db.instance_for_host(&path.host).or_500()?;
    let booting = i.as_ref().map(|i| i.is_preboot()).unwrap_or(false);

    if !booting && hc.debug_os_dir.is_none() {
        return Err(HttpError::for_bad_request(
            None,
            "access to boot media denied except when booting".to_string(),
        ));
    }

    let script = if let Some(path) = hc.debug_os_postboot_sh.as_deref() {
        /*
         * If we have been given a local file path for a debug postboot script,
         * load it from the file system now.  We still need to expand tokens
         * like %HOST% that appear in the script.
         */
        FormatScript::new(&c.config, hc)
            .instance(i.as_ref())
            .debug(hc.debug_os_dir.is_some())
            .format(&std::fs::read_to_string(path).or_500()?)
    } else if hc.debug_os_dir.is_some() {
        /*
         * Otherwise, if we are booting debug media, provide a blank postboot
         * script:
         */
        "".to_string()
    } else {
        FormatScript::new(&c.config, hc)
            .instance(i.as_ref())
            .format(include_str!("../scripts/postboot.sh"))
    };

    Ok(hyper::Response::builder().body(script.into())?)
}

#[dropshot::endpoint {
    method = GET,
    path = "/ipxe/{host}",
    unpublished = true,
}]
async fn ipxe_script(
    ctx: dropshot::RequestContext<Arc<Central>>,
    path: TypedPath<IpxePath>,
) -> HResult<hyper::Response<Body>> {
    let c = ctx.context();
    let path = path.into_inner();

    let hc = c.host_config(&path.host)?;
    let i = c.db.instance_for_host(&path.host).or_500()?;
    let booting = i.as_ref().map(|i| i.is_preboot()).unwrap_or(false);

    /*
     * If we are in the Preboot state, pass the regular boot script that will
     * instruct iPXE to load the OS.  Otherwise, return the dialtone script.
     */
    let script = FormatScript::new(&c.config, hc)
        .instance(i.as_ref())
        .marker(if booting { MARKER_BOOT } else { MARKER_HOLD })
        .debug(hc.debug_os_dir.is_some())
        .format(if hc.debug_os_dir.is_some() {
            include_str!("../scripts/debug.ipxe")
        } else if booting {
            include_str!("../scripts/regular.ipxe")
        } else {
            include_str!("../scripts/hold.ipxe")
        });

    Ok(hyper::Response::builder().body(script.into())?)
}

fn make_api() -> Result<dropshot::ApiDescription<Arc<Central>>> {
    let mut ad = dropshot::ApiDescription::new();
    ad.register(ipxe_script)?;
    ad.register(postboot_script)?;
    ad.register(signal)?;
    ad.register(os_file)?;
    Ok(ad)
}

pub fn dump_api<P: AsRef<Path>>(p: P) -> Result<()> {
    let p = p.as_ref();
    let ad = make_api()?;
    let mut f =
        std::fs::OpenOptions::new().create_new(true).write(true).open(p)?;
    ad.openapi("Minder", semver::Version::new(1, 0, 0)).write(&mut f)?;
    Ok(())
}

pub(crate) async fn server(
    central: &Arc<Central>,
    bind_address: std::net::SocketAddr,
) -> Result<dropshot::HttpServerStarter<Arc<Central>>> {
    let ad = make_api()?;
    let log = central.log.clone();

    let server = dropshot::HttpServerStarter::new(
        &dropshot::ConfigDropshot { bind_address, ..Default::default() },
        ad,
        Arc::clone(central),
        &log,
    )
    .map_err(|e| anyhow!("server startup failure: {:?}", e))?;

    Ok(server)
}
