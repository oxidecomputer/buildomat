/*
 * Copyright 2021 Oxide Computer Company
 */

#![allow(clippy::many_single_char_names)]
#![allow(clippy::too_many_arguments)]

use std::io::Write;
use std::path::PathBuf;
use std::process::exit;
use std::result::Result as SResult;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, bail, Context, Result};
use dropshot::{
    endpoint, ApiDescription, ConfigDropshot, HttpError, HttpServerStarter,
    Query as TypedQuery, RequestContext, RequestInfo,
};
use getopts::Options;
#[allow(unused_imports)]
use hyper::{
    header::AUTHORIZATION, header::CONTENT_LENGTH, Body, Response, StatusCode,
};
use hyper_staticfile::FileBytesStream;
use rusoto_s3::S3;
use rusty_ulid::Ulid;
use schemars::JsonSchema;
use serde::Deserialize;
#[allow(unused_imports)]
use slog::{error, info, warn, Logger};
#[macro_use]
extern crate diesel;
use buildomat_common::*;

mod api;
mod archive;
mod chunks;
mod db;
mod jobs;
mod workers;

use db::{JobFileId, JobId};

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
    for std::result::Result<T, rusty_ulid::DecodingError>
{
    fn or_500(self) -> SResult<T, HttpError> {
        self.map_err(|e| {
            let msg = format!("ID decode error: {:?}", e);
            HttpError::for_internal_error(msg)
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

struct FileResponse {
    pub info: String,
    pub body: Body,
    pub size: u64,
}

#[derive(Deserialize, Debug)]
struct ConfigFile {
    pub admin: ConfigFileAdmin,
    #[allow(dead_code)]
    pub general: ConfigFileGeneral,
    pub storage: ConfigFileStorage,
    pub sqlite: ConfigFileSqlite,
    pub job: ConfigFileJob,
}

#[derive(Deserialize, Debug)]
struct ConfigFileGeneral {
    #[allow(dead_code)]
    pub baseurl: String,
}

#[derive(Deserialize, Debug)]
struct ConfigFileJob {
    pub max_runtime: u64,
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

struct CentralInner {
    hold: bool,
    leases: jobs::Leases,
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
    fn _int_delegate_username(
        &self,
        _log: &Logger,
        req: &RequestInfo,
    ) -> SResult<Option<String>, HttpError> {
        Ok(if let Some(h) = req.headers().get("x-buildomat-delegate") {
            if let Ok(v) = h.to_str() {
                Some(v.trim().to_string())
            } else {
                None
            }
        } else {
            None
        })
    }

    fn _int_auth_token(
        &self,
        log: &Logger,
        req: &RequestInfo,
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
        req: &RequestInfo,
        privname: &str,
    ) -> SResult<(), HttpError> {
        let t = self._int_auth_token(log, req)?;

        if t == self.config.admin.token {
            /*
             * If the bearer token matches the configured global admin token, we
             * can proceed immediately.
             */
            return Ok(());
        }

        /*
         * Otherwise, try to use the bearer token to authenticate as a regular
         * user and we will check if they have been delegated the specific
         * administrative privilege needed.
         */
        assert!(!privname.starts_with("admin."));
        let want = format!("admin.{}", privname);
        let u = match self.db.user_auth(&t) {
            Ok(u) => u,
            Err(e) => {
                warn!(log, "admin auth failure: {:?}", e);
                return unauth_response();
            }
        };

        if !u.has_privilege(&want) {
            warn!(log, "user {} does not have privilege {}", u.name, want);
            return unauth_response();
        }

        info!(log, "user {} used delegated admin privilege {}", u.name, want);
        Ok(())
    }

    async fn require_user(
        &self,
        log: &Logger,
        req: &RequestInfo,
    ) -> SResult<db::AuthUser, HttpError> {
        /*
         * First, use the bearer token to authenticate the user making the
         * request:
         */
        let t = self._int_auth_token(log, req)?;
        let u = match self.db.user_auth(&t) {
            Ok(u) => u,
            Err(e) => {
                warn!(log, "user auth failure: {:?}", e);
                return unauth_response();
            }
        };

        /*
         * Now check to see if the authenticated user is requesting delegated
         * authentication to act as another user:
         */
        if let Some(delegate) = self._int_delegate_username(log, req)? {
            if u.has_privilege("delegate") {
                /*
                 * The authenticated user is allowed to impersonate other users
                 * in the system, and if the requested user does not exist we
                 * will create it for them.  This is used by Wollongong to
                 * create a user per GitHub repository to house the jobs for
                 * that repository.
                 */
                info!(log, "user {} delegated auth as {:?}", u.name, delegate);
                Ok(self.db.user_ensure(&delegate).or_500()?)
            } else {
                /*
                 * This user is not allowed to act as another user.
                 */
                warn!(
                    log,
                    "user {} tried to use delegated auth as {:?}",
                    u.name,
                    delegate
                );
                unauth_response()
            }
        } else {
            Ok(u)
        }
    }

    async fn require_worker(
        &self,
        log: &Logger,
        req: &RequestInfo,
    ) -> SResult<db::Worker, HttpError> {
        let t = self._int_auth_token(log, req)?;
        match self.db.worker_auth(&t) {
            Ok(u) => Ok(u),
            Err(e) => {
                warn!(log, "worker auth failure: {:?}", e);
                unauth_response()
            }
        }
    }

    async fn require_factory(
        &self,
        log: &Logger,
        req: &RequestInfo,
    ) -> SResult<db::Factory, HttpError> {
        let t = self._int_auth_token(log, req)?;
        match self.db.factory_auth(&t) {
            Ok(u) => Ok(u),
            Err(e) => {
                warn!(log, "factory auth failure: {:?}", e);
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

    fn chunk_path(&self, job: JobId, chunk: Ulid) -> Result<PathBuf> {
        let mut p = self.chunk_dir()?;
        p.push(job.to_string());
        std::fs::create_dir_all(&p)?;
        p.push(chunk.to_string());
        Ok(p)
    }

    fn file_dir(&self) -> Result<PathBuf> {
        let mut p = self.datadir.clone();
        p.push("output");
        std::fs::create_dir_all(&p)?;
        Ok(p)
    }

    fn file_path(&self, job: JobId, file: JobFileId) -> Result<PathBuf> {
        let mut p = self.file_dir()?;
        p.push(job.to_string());
        std::fs::create_dir_all(&p)?;
        p.push(file.to_string());
        Ok(p)
    }

    fn file_object_key(&self, job: JobId, file: JobFileId) -> String {
        /*
         * Object keys begin with a prefix string so that we can have more than
         * one scheme, or more than one buildomat, using the same bucket without
         * conflicts.
         */
        format!("{}/output/{}/{}", self.config.storage.prefix, job, file)
    }

    fn write_chunk(&self, job: JobId, chunk: &[u8]) -> Result<Ulid> {
        /*
         * Assign an ID for this chunk and determine where will store it in the
         * file system.
         */
        let cid = Ulid::generate();
        let p = self.chunk_path(job, cid)?;
        let f = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&p)?;
        let mut bw = std::io::BufWriter::new(f);
        bw.write_all(chunk).or_500()?;
        bw.flush()?;

        Ok(cid)
    }

    fn commit_file(
        &self,
        job: JobId,
        chunks: &[Ulid],
        expected_size: u64,
    ) -> Result<JobFileId> {
        /*
         * Check that all of the chunks the client wants to use exist, and that
         * the sum of their sizes matches the total size.
         */
        let files = chunks
            .iter()
            .map(|cid| {
                let f = self.chunk_path(job, *cid)?;
                let md = f.metadata()?;
                Ok((f, md.len()))
            })
            .collect::<Result<Vec<_>>>()
            .or_500()?;
        let chunksize: u64 = files.iter().map(|(_, sz)| *sz).sum();
        if chunksize != expected_size {
            bail!(
                "job {} file: expected size {} != chunk size {}",
                job,
                expected_size,
                chunksize,
            );
        }

        /*
         * Assign an ID for this file and determine where we will store it in
         * the file system.
         */
        let fid = db::JobFileId::generate();
        let fp = self.file_path(job, fid)?;
        let mut fout = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&fp)?;
        {
            let mut bw = std::io::BufWriter::new(&mut fout);
            for (ip, _) in files.iter() {
                let fin = std::fs::File::open(&ip).or_500()?;
                let mut br = std::io::BufReader::new(fin);

                std::io::copy(&mut br, &mut bw).or_500()?;
            }
            bw.flush()?;
        }
        fout.flush()?;
        fout.sync_all()?;

        /*
         * Confirm again that file size is as expected.
         */
        let md = fout.metadata()?;
        if md.len() != expected_size {
            bail!(
                "job {} file {}: expected size {} != copied total {}",
                job,
                fid,
                expected_size,
                md.len(),
            );
        }

        Ok(fid)
    }

    async fn file_response(
        &self,
        job: JobId,
        file: JobFileId,
    ) -> Result<FileResponse> {
        let op = self.file_path(job, file)?;

        Ok(if op.is_file() {
            /*
             * The file exists locally.
             */
            let info = format!("local file system at {:?}", op);
            let f = tokio::fs::File::open(op).await?;
            let md = f.metadata().await?;
            assert!(md.is_file());
            let fbs = FileBytesStream::new(f);

            FileResponse { info, body: fbs.into_body(), size: md.len() }
        } else {
            /*
             * Otherwise, try to get it from the object store.
             *
             * XXX We could conceivably 302 redirect people to the actual object
             * store with a presigned request?
             */
            let key = self.file_object_key(job, file);
            let info = format!("object store at {}", key);
            let obj = self
                .s3
                .get_object(rusoto_s3::GetObjectRequest {
                    bucket: self.config.storage.bucket.to_string(),
                    key,
                    ..Default::default()
                })
                .await?;

            if let Some(body) = obj.body {
                FileResponse {
                    info,
                    body: Body::wrap_stream(body),
                    size: obj.content_length.unwrap() as u64,
                }
            } else {
                bail!("no body on object request for {}/{}", job, file);
            }
        })
    }
}

#[allow(dead_code)]
#[derive(Deserialize, JsonSchema, Debug)]
pub(crate) struct FileAgentQuery {
    kernel: Option<String>,
    proc: Option<String>,
    mach: Option<String>,
    plat: Option<String>,
    id: Option<String>,
    id_like: Option<String>,
    version_id: Option<String>,
}

impl FileAgentQuery {
    fn is_linux(&self) -> bool {
        match self.kernel.as_deref() {
            Some("Linux") => true,
            Some(_) | None => false,
        }
    }
}

#[endpoint {
    method = GET,
    path = "/file/agent",
}]
async fn file_agent(
    rqctx: RequestContext<Arc<Central>>,
    query: TypedQuery<FileAgentQuery>,
) -> SResult<Response<Body>, HttpError> {
    let log = &rqctx.log;
    let q = query.into_inner();

    info!(log, "agent request; query = {:?}", q);

    let filename =
        if q.is_linux() { "buildomat-agent-linux" } else { "buildomat-agent" };
    info!(log, "using agent file {:?}", filename);

    let f = tokio::fs::File::open(filename).await.or_500()?;
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
    ad.register(api::admin::user_get).api_check()?;
    ad.register(api::admin::user_create).api_check()?;
    ad.register(api::admin::user_privilege_grant).api_check()?;
    ad.register(api::admin::user_privilege_revoke).api_check()?;
    ad.register(api::admin::workers_list).api_check()?;
    ad.register(api::admin::workers_recycle).api_check()?;
    ad.register(api::admin::worker_recycle).api_check()?;
    ad.register(api::admin::admin_job_get).api_check()?;
    ad.register(api::admin::admin_jobs_get).api_check()?;
    ad.register(api::admin::factory_create).api_check()?;
    ad.register(api::admin::target_create).api_check()?;
    ad.register(api::admin::targets_list).api_check()?;
    ad.register(api::admin::target_require_privilege).api_check()?;
    ad.register(api::admin::target_require_no_privilege).api_check()?;
    ad.register(api::admin::target_redirect).api_check()?;
    ad.register(api::admin::target_rename).api_check()?;
    ad.register(api::user::job_events_get).api_check()?;
    ad.register(api::user::job_outputs_get).api_check()?;
    ad.register(api::user::job_output_download).api_check()?;
    ad.register(api::user::job_output_publish).api_check()?;
    ad.register(api::user::job_get).api_check()?;
    ad.register(api::user::job_submit).api_check()?;
    ad.register(api::user::job_upload_chunk).api_check()?;
    ad.register(api::user::job_add_input).api_check()?;
    ad.register(api::user::job_cancel).api_check()?;
    ad.register(api::user::jobs_get).api_check()?;
    ad.register(api::user::whoami).api_check()?;
    ad.register(api::worker::worker_bootstrap).api_check()?;
    ad.register(api::worker::worker_ping).api_check()?;
    ad.register(api::worker::worker_job_append).api_check()?;
    ad.register(api::worker::worker_job_complete).api_check()?;
    ad.register(api::worker::worker_job_upload_chunk).api_check()?;
    ad.register(api::worker::worker_job_add_output).api_check()?;
    ad.register(api::worker::worker_job_input_download).api_check()?;
    ad.register(api::worker::worker_task_append).api_check()?;
    ad.register(api::worker::worker_task_complete).api_check()?;
    ad.register(api::factory::factory_workers).api_check()?;
    ad.register(api::factory::factory_worker_get).api_check()?;
    ad.register(api::factory::factory_ping).api_check()?;
    ad.register(api::factory::factory_worker_create).api_check()?;
    ad.register(api::factory::factory_worker_append).api_check()?;
    ad.register(api::factory::factory_worker_flush).api_check()?;
    ad.register(api::factory::factory_worker_associate).api_check()?;
    ad.register(api::factory::factory_worker_destroy).api_check()?;
    ad.register(api::factory::factory_lease).api_check()?;
    ad.register(api::factory::factory_lease_renew).api_check()?;
    ad.register(api::public::public_file_download).api_check()?;

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
    let db = db::Database::new(log.clone(), dbfile, config.sqlite.cache_kb)?;

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
            leases: Default::default(),
        }),
        config,
        datadir,
        db,
        s3,
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
        archive::archive_files(log0, c0).await.context("archive task failure")
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
    .map_err(|e| anyhow!("server startup failure: {:?}", e))?;

    let server_task = server.start();

    loop {
        tokio::select! {
            _ = t_assign => bail!("task assignment task stopped early"),
            _ = t_chunks => bail!("chunk cleanup task stopped early"),
            _ = t_archive => bail!("archive task stopped early"),
            _ = t_workers => bail!("worker cleanup task stopped early"),
            _ = server_task => bail!("server stopped early"),
        }
    }
}
