/*
 * Copyright 2021 Oxide Computer Company
 */

#![allow(clippy::many_single_char_names)]

use std::path::PathBuf;
use std::process::exit;
use std::result::Result as SResult;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, bail, Context, Result};
use dropshot::{
    endpoint, ApiDescription, ConfigDropshot, HttpError, HttpServerStarter,
    RequestContext,
};
use getopts::Options;
#[allow(unused_imports)]
use hyper::{
    header::AUTHORIZATION, header::CONTENT_LENGTH, Body, Request, Response,
    StatusCode,
};
use hyper_staticfile::FileBytesStream;
use rusty_ulid::Ulid;
use serde::Deserialize;
#[allow(unused_imports)]
use slog::{error, info, warn, Logger};
#[macro_use]
extern crate diesel;
use buildomat_common::*;

mod api;
mod archive;
mod aws;
mod chunks;
mod db;
mod jobs;
mod workers;

use db::{JobId, JobOutputId};

pub(crate) trait MakeInternalError<T> {
    fn or_500(self) -> SResult<T, HttpError>;
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

impl<T> MakeInternalError<T> for db::OResult<T> {
    fn or_500(self) -> SResult<T, HttpError> {
        self.map_err(|e| {
            use db::OperationError;

            match e {
                OperationError::Conflict(msg) => HttpError::for_client_error(
                    Some("conflict".to_string()),
                    StatusCode::CONFLICT,
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

impl<T> MakeInternalError<T>
    for std::result::Result<
        T,
        rusoto_core::RusotoError<rusoto_s3::GetObjectError>,
    >
{
    fn or_500(self) -> SResult<T, HttpError> {
        self.map_err(|e| {
            let msg = format!("object store get error: {:?}", e);
            HttpError::for_internal_error(msg)
        })
    }
}

pub(crate) trait ApiResultEx {
    fn api_check(&self) -> Result<()>;
    fn note(&self, n: &str) -> Result<()>;
}

impl ApiResultEx for std::result::Result<(), String> {
    fn api_check(&self) -> Result<()> {
        self.as_ref()
            .map_err(|e| anyhow!("API registration failure: {}", e))?;
        Ok(())
    }

    fn note(&self, n: &str) -> Result<()> {
        self.as_ref().map_err(|e| anyhow!("{}: {}", n, e))?;
        Ok(())
    }
}

#[derive(Deserialize, Debug)]
struct ConfigFile {
    pub aws: ConfigFileAws,
    pub admin: ConfigFileAdmin,
    pub general: ConfigFileGeneral,
    pub storage: ConfigFileStorage,
    pub sqlite: ConfigFileSqlite,
}

#[derive(Deserialize, Debug)]
struct ConfigFileGeneral {
    pub baseurl: String,
}

#[derive(Deserialize, Debug)]
struct ConfigFileSqlite {
    #[serde(default)]
    pub cache_kb: Option<u32>,
}

#[derive(Deserialize, Debug)]
struct ConfigFileAdmin {
    pub token: String,
    /**
     * Should we hold off on new VM creation by default at startup?
     */
    pub hold: bool,
}

#[derive(Deserialize, Debug)]
struct ConfigFileStorage {
    access_key_id: String,
    secret_access_key: String,
    bucket: String,
    prefix: String,
    region: String,
}

#[derive(Deserialize, Debug)]
struct ConfigFileAws {
    access_key_id: String,
    secret_access_key: String,
    region: String,
    vpc: String,
    subnet: String,
    tag: String,
    key: String,
    instance_type: String,
    root_size_gb: i64,
    ami: String,
    security_group: String,
    limit_spares: usize,
    limit_total: usize,
    max_runtime: u64,
}

struct CentralInner {
    hold: bool,
    needfree: usize,
}

struct Central {
    config: ConfigFile,
    db: db::Database,
    datadir: PathBuf,
    inner: Mutex<CentralInner>,
    s3: rusoto_s3::S3Client,
}

pub(crate) fn unauth_response<T>() -> SResult<T, HttpError> {
    Err(HttpError::for_client_error(
        None,
        StatusCode::UNAUTHORIZED,
        "not authorised".into(),
    ))
}

impl Central {
    async fn _int_auth_token(
        &self,
        log: &Logger,
        req: &Request<Body>,
    ) -> SResult<String, HttpError> {
        let v = if let Some(h) = req.headers().get(AUTHORIZATION) {
            if let Ok(v) = h.to_str() {
                Some(v.to_string())
            } else {
                None
            }
        } else {
            None
        };

        if let Some(v) = v {
            let t = v.split_whitespace().map(|s| s.trim()).collect::<Vec<_>>();

            if t.len() == 2
                && t.iter().all(|s| !s.is_empty())
                && t[0].to_lowercase().trim() == "bearer"
            {
                let b = t[1].trim();

                if b.len() >= 3 {
                    return Ok(b.to_string());
                }
            }
            warn!(log, "invalid authorisation header?");
        } else {
            warn!(log, "no authorisation header?");
        }

        unauth_response()
    }

    async fn require_admin(
        &self,
        log: &Logger,
        req: &Request<Body>,
    ) -> SResult<(), HttpError> {
        let t = self._int_auth_token(log, req).await?;
        if t == self.config.admin.token {
            Ok(())
        } else {
            warn!(log, "admin auth failure");
            unauth_response()
        }
    }

    async fn require_user(
        &self,
        log: &Logger,
        req: &Request<Body>,
    ) -> SResult<db::User, HttpError> {
        let t = self._int_auth_token(log, req).await?;
        match self.db.user_auth(&t) {
            Ok(u) => Ok(u),
            Err(e) => {
                warn!(log, "user auth failure: {:?}", e);
                unauth_response()
            }
        }
    }

    async fn require_worker(
        &self,
        log: &Logger,
        req: &Request<Body>,
    ) -> SResult<db::Worker, HttpError> {
        let t = self._int_auth_token(log, req).await?;
        match self.db.worker_auth(&t) {
            Ok(u) => Ok(u),
            Err(e) => {
                warn!(log, "worker auth failure: {:?}", e);
                unauth_response()
            }
        }
    }

    fn chunk_dir(&self) -> Result<PathBuf> {
        let mut p = self.datadir.clone();
        p.push("chunk");
        std::fs::create_dir_all(&p)?;
        Ok(p)
    }

    fn chunk_path(&self, job: &JobId, chunk: &Ulid) -> Result<PathBuf> {
        let mut p = self.chunk_dir()?;
        p.push(job.to_string());
        std::fs::create_dir_all(&p)?;
        p.push(chunk.to_string());
        Ok(p)
    }

    fn output_dir(&self) -> Result<PathBuf> {
        let mut p = self.datadir.clone();
        p.push("output");
        std::fs::create_dir_all(&p)?;
        Ok(p)
    }

    fn output_path(
        &self,
        job: &JobId,
        output: &JobOutputId,
    ) -> Result<PathBuf> {
        let mut p = self.output_dir()?;
        p.push(job.to_string());
        std::fs::create_dir_all(&p)?;
        p.push(output.to_string());
        Ok(p)
    }

    fn output_object_key(&self, job: &JobId, output: &JobOutputId) -> String {
        /*
         * Object keys begin with a prefix string so that we can have more than
         * one scheme, or more than one buildomat, using the same bucket without
         * conflicts.
         */
        format!("{}/output/{}/{}", self.config.storage.prefix, job, output)
    }
}

#[endpoint {
    method = GET,
    path = "/file/agent",
}]
async fn file_agent(
    _rqctx: Arc<RequestContext<Arc<Central>>>,
) -> SResult<Response<Body>, HttpError> {
    let f = tokio::fs::File::open("buildomat-agent").await.or_500()?;
    let fbs = FileBytesStream::new(f);

    Ok(Response::builder().body(fbs.into_body())?)
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut opts = Options::new();

    opts.optopt("b", "", "bind address:port", "BIND_ADDRESS");
    opts.optopt("f", "", "configuration file", "CONFIG");
    opts.optopt("S", "", "dump OpenAPI schema", "FILE");

    let p = match opts.parse(std::env::args().skip(1)) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("ERROR: usage: {}", e);
            eprintln!("       {}", opts.usage("usage"));
            exit(1);
        }
    };

    let mut ad = ApiDescription::new();
    ad.register(api::admin::control_hold).api_check()?;
    ad.register(api::admin::control_resume).api_check()?;
    ad.register(api::admin::users_list).api_check()?;
    ad.register(api::admin::user_create).api_check()?;
    ad.register(api::admin::workers_list).api_check()?;
    ad.register(api::admin::workers_recycle).api_check()?;
    ad.register(api::user::job_events_get).api_check()?;
    ad.register(api::user::job_outputs_get).api_check()?;
    ad.register(api::user::job_output_download).api_check()?;
    ad.register(api::user::job_get).api_check()?;
    ad.register(api::user::job_submit).api_check()?;
    ad.register(api::user::jobs_get).api_check()?;
    ad.register(api::user::whoami).api_check()?;
    ad.register(api::worker::worker_bootstrap).api_check()?;
    ad.register(api::worker::worker_ping).api_check()?;
    ad.register(api::worker::worker_job_append).api_check()?;
    ad.register(api::worker::worker_job_complete).api_check()?;
    ad.register(api::worker::worker_job_upload_chunk).api_check()?;
    ad.register(api::worker::worker_job_add_output).api_check()?;
    ad.register(api::worker::worker_task_append).api_check()?;
    ad.register(api::worker::worker_task_complete).api_check()?;

    if let Some(s) = p.opt_str("S") {
        let mut f = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&s)?;
        ad.openapi("Buildomat", "1.0").write(&mut f)?;
        return Ok(());
    }

    /*
     * These should not presently appear in the OpenAPI definition, so we
     * register them after it is generated:
     */
    ad.register(file_agent).api_check()?;

    let bind_address =
        p.opt_str("b").as_deref().unwrap_or("127.0.0.1:9979").parse()?;

    let config: ConfigFile = if let Some(f) = p.opt_str("f").as_deref() {
        read_toml(f)?
    } else {
        bail!("must specify configuration file (-f)");
    };

    let log = make_log("buildomat");

    let mut datadir = std::env::current_dir()?;
    datadir.push("data");
    if !datadir.is_dir() {
        bail!("{:?} must be a directory", datadir);
    }

    let mut dbfile = datadir.clone();
    dbfile.push("data.sqlite3");
    let db = db::Database::new(
        log.clone(),
        dbfile,
        config.sqlite.cache_kb.clone(),
    )?;

    let credprov = rusoto_credential::StaticProvider::new_minimal(
        config.storage.access_key_id.clone(),
        config.storage.secret_access_key.clone(),
    );
    let s3 = rusoto_s3::S3Client::new_with(
        rusoto_core::HttpClient::new()?,
        credprov,
        rusoto_core::Region::from_str(&config.storage.region)?,
    );

    let c = Arc::new(Central {
        inner: Mutex::new(CentralInner {
            hold: config.admin.hold,
            needfree: 0,
        }),
        config,
        datadir,
        db,
        s3,
    });

    let c0 = Arc::clone(&c);
    let log0 = log.clone();
    let t_aws = tokio::task::spawn(async move {
        aws::aws_worker(log0, c0).await.context("AWS worker task failure")
    });

    let c0 = Arc::clone(&c);
    let log0 = log.clone();
    let t_assign = tokio::task::spawn(async move {
        jobs::job_assignment(log0, c0)
            .await
            .context("job assignment task failure")
    });

    let c0 = Arc::clone(&c);
    let log0 = log.clone();
    let t_chunks = tokio::task::spawn(async move {
        chunks::chunk_cleanup(log0, c0)
            .await
            .context("chunk cleanup task failure")
    });

    let c0 = Arc::clone(&c);
    let log0 = log.clone();
    let t_archive = tokio::task::spawn(async move {
        archive::archive_outputs(log0, c0).await.context("archive task failure")
    });

    let c0 = Arc::clone(&c);
    let log0 = log.clone();
    let t_workers = tokio::task::spawn(async move {
        workers::worker_cleanup(log0, c0)
            .await
            .context("worker cleanup task failure")
    });

    let server = HttpServerStarter::new(
        #[allow(clippy::needless_update)]
        &ConfigDropshot {
            request_body_max_bytes: 10 * 1024 * 1024,
            bind_address,
            ..Default::default()
        },
        ad,
        c,
        &log,
    )
    .context("server startup failure")?;

    let server_task = server.start();

    loop {
        tokio::select! {
            _ = t_aws => bail!("AWS task stopped early"),
            _ = t_assign => bail!("task assignment task stopped early"),
            _ = t_chunks => bail!("chunk cleanup task stopped early"),
            _ = t_archive => bail!("archive task stopped early"),
            _ = t_workers => bail!("worker cleanup task stopped early"),
            _ = server_task => bail!("server stopped early"),
        }
    }
}
