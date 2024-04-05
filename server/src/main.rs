/*
 * Copyright 2024 Oxide Computer Company
 */

#![allow(clippy::many_single_char_names)]
#![allow(clippy::too_many_arguments)]

use std::collections::VecDeque;
use std::io::{Seek, Write};
use std::path::PathBuf;
use std::process::exit;
use std::result::Result as SResult;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use aws_sdk_s3::primitives::ByteStream;
use buildomat_common::*;
use buildomat_download::unruin_content_length;
use dropshot::{
    endpoint, ApiDescription, ConfigDropshot, HttpError, HttpServerStarter,
    Query as TypedQuery, RequestContext, RequestInfo,
};
use getopts::Options;
use hyper::{header::AUTHORIZATION, Body, Response, StatusCode};
use rusty_ulid::Ulid;
use schemars::JsonSchema;
use serde::Deserialize;
#[allow(unused_imports)]
use slog::{error, info, o, warn, Logger};

mod api;
mod archive;
mod chunks;
mod config;
mod db;
mod files;
mod jobs;
mod workers;

use db::{
    AuthUser, Job, JobEvent, JobFileId, JobId, JobOutput, JobOutputAndFile,
    Worker, WorkerEvent,
};

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

impl<T> MakeInternalError<T> for buildomat_database::DBResult<T> {
    fn or_500(self) -> SResult<T, HttpError> {
        self.map_err(|e| {
            use buildomat_database::DatabaseError;

            match e {
                DatabaseError::Conflict(msg) => HttpError::for_client_error(
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

impl<T> MakeInternalError<T> for serde_json::Result<T> {
    fn or_500(self) -> SResult<T, HttpError> {
        self.map_err(|e| {
            let msg = format!("serde JSON error: {:?}", e);
            HttpError::for_internal_error(msg)
        })
    }
}

pub(crate) trait ApiResultEx {
    fn api_check(&self) -> Result<()>;
}

impl ApiResultEx for std::result::Result<(), String> {
    fn api_check(&self) -> Result<()> {
        self.as_ref()
            .map_err(|e| anyhow!("API registration failure: {}", e))?;
        Ok(())
    }
}

struct FilePresignedUrl {
    pub info: String,
    pub url: String,
}

struct CentralInner {
    hold: bool,
    leases: jobs::Leases,
    archive_queue: VecDeque<JobId>,
}

struct Central {
    config: config::ConfigFile,
    db: db::Database,
    datadir: PathBuf,
    files: files::Files,
    inner: Mutex<CentralInner>,
    s3: aws_sdk_s3::Client,
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
    ) -> SResult<AuthUser, HttpError> {
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

    fn archive_dir(&self) -> Result<PathBuf> {
        let mut p = self.datadir.clone();
        p.push("archive");
        std::fs::create_dir_all(&p)?;
        Ok(p)
    }

    fn archive_path(&self, job: JobId) -> Result<PathBuf> {
        let mut p = self.archive_dir()?;
        p.push(format!("{job}.tlvc"));
        Ok(p)
    }

    fn object_key(&self, collection: &str, suffix: &str) -> String {
        /*
         * Object keys begin with a prefix string so that we can have more than
         * one scheme, or more than one buildomat, using the same bucket without
         * conflicts.
         */
        format!("{}/{collection}/{suffix}", self.config.storage.prefix)
    }

    fn archive_object_key(&self, job: JobId, version: u32) -> String {
        self.object_key("job", &format!("{version}/{job}.tlvc"))
    }

    async fn archive_store(
        &self,
        log: &Logger,
        job: JobId,
        archive_version: u32,
        body: std::fs::File,
        body_len: usize,
    ) -> Result<()> {
        let start = Instant::now();
        let akey = self.archive_object_key(job, archive_version);
        let bucket = &self.config.storage.bucket;

        let body = ByteStream::read_from()
            .file(body.into())
            .offset(0)
            .buffer_size(256 * 1024)
            .build()
            .await?;

        self.s3
            .put_object()
            .bucket(bucket)
            .key(&akey)
            .content_length(body_len.try_into().unwrap())
            .body(body)
            .send()
            .await?;

        let dur = Instant::now().saturating_duration_since(start);
        info!(log, "uploaded job archive from job {job} at {bucket}:{akey}";
            "duration_msec" => dur.as_millis());

        Ok(())
    }

    async fn archive_load(
        &self,
        log: &Logger,
        job: JobId,
    ) -> Result<archive::jobs::LoadedArchivedJob> {
        /*
         * First, check for the archive locally.  If we have already retrieved
         * it from the object store we do not need to do so again.
         */
        let apath = self.archive_path(job)?;
        match std::fs::File::open(&apath) {
            Ok(f) => {
                let aj = archive::jobs::LoadedArchivedJob::load_from_file(f)?;
                return Ok(aj);
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                /*
                 * The file does not exist locally; we need to fetch it from the
                 * object store.
                 */
            }
            Err(e) => bail!("archived job {job} path {apath:?} error: {e}"),
        };

        let start = Instant::now();
        let akey =
            self.archive_object_key(job, archive::jobs::JOB_ARCHIVE_VERSION);
        let bucket = &self.config.storage.bucket;

        let mut res =
            self.s3.get_object().bucket(bucket).key(&akey).send().await?;
        let clen = unruin_content_length(res.content_length())?;

        /*
         * Download the data from S3 into a temporary file in the local file
         * system.
         */
        let mut tf = tempfile::NamedTempFile::new_in(self.archive_dir()?)?;
        while let Some(b) = res.body.try_next().await? {
            tf.write_all(&b)?;
        }

        /*
         * Syncing the file to disk and then loading the contents of the file
         * may take a while and hold up other async tasks.
         */
        tokio::task::block_in_place(|| {
            tf.flush()?;
            tf.as_file_mut().sync_all()?;
            tf.rewind()?;
            let tflen = tf.as_file().metadata()?.len();
            if clen != tflen {
                bail!("expected {clen} bytes, but saved {tflen}?");
            }

            /*
             * Make sure the data we read from S3 is valid:
             */
            archive::jobs::LoadedArchivedJob::load_from_file(
                /*
                 * It's alright to use a cloned open file here, because we have
                 * been careful to use only pread() (i.e., read with an explicit
                 * offset) in the archive loader code.
                 */
                tf.as_file().try_clone()?,
            )?;
            let dur = Instant::now().saturating_duration_since(start);
            info!(log, "loaded archive of job {job} from {bucket}:{akey}";
                "duration_msec" => dur.as_millis());

            /*
             * Rename the file so that it can be found on the next lookup for
             * this job ID.
             */
            let mut f = tf.persist(self.archive_path(job)?)?;
            f.rewind()?;
            archive::jobs::LoadedArchivedJob::load_from_file(f)
        })
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

    fn file_path(
        &self,
        job: JobId,
        file: JobFileId,
        create_parent: bool,
    ) -> Result<PathBuf> {
        let mut p = self.file_dir()?;
        p.push(job.to_string());
        if create_parent {
            std::fs::create_dir_all(&p)?;
        }
        p.push(file.to_string());
        Ok(p)
    }

    fn file_object_key(&self, job: JobId, file: JobFileId) -> String {
        /*
         * Object keys begin with a prefix string so that we can have more than
         * one scheme, or more than one buildomat, using the same bucket without
         * conflicts.
         */
        self.object_key("output", &format!("{job}/{file}"))
    }

    fn write_chunk(&self, job: JobId, chunk: &[u8]) -> Result<Ulid> {
        /*
         * Assign an ID for this chunk and determine where will store it in the
         * file system.
         */
        let cid = Ulid::generate();
        let p = self.chunk_path(job, cid)?;
        let f =
            std::fs::OpenOptions::new().create_new(true).write(true).open(p)?;
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
        let fp = self.file_path(job, fid, true)?;
        let mut fout = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(fp)?;
        {
            let mut bw = std::io::BufWriter::new(&mut fout);
            for (ip, _) in files.iter() {
                let fin = std::fs::File::open(ip).or_500()?;
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

    async fn file_presigned_url(
        &self,
        job: JobId,
        file: JobFileId,
        expiry_seconds: u64,
        content_type: Option<&str>,
        content_disposition: Option<&str>,
    ) -> Result<FilePresignedUrl> {
        if expiry_seconds > 3600 {
            bail!("expiry too long");
        }

        /*
         * Presigned URLs always come from the object store!
         */
        let key = self.file_object_key(job, file);
        let info = format!("object store at {}", key);

        let mut obj =
            self.s3.get_object().bucket(&self.config.storage.bucket).key(key);

        /*
         * We may be asked to override some of the headers that S3 provides in
         * the response.
         */
        if let Some(val) = content_type {
            obj = obj.response_content_type(val);
        }
        if let Some(val) = content_disposition {
            obj = obj.response_content_disposition(val);
        }

        let obj = obj
            .presigned(
                aws_sdk_s3::presigning::PresigningConfig::builder()
                    .expires_in(Duration::from_secs(expiry_seconds))
                    .build()?,
            )
            .await?;

        Ok(FilePresignedUrl { info, url: obj.uri().to_string() })
    }

    /**
     * Generate a response for a file request.  Handles a range GET if requested
     * by the client.
     */
    async fn file_response(
        &self,
        log: &Logger,
        info: String,
        job: JobId,
        file: JobFileId,
        range: Option<buildomat_download::PotentialRange>,
        head: bool,
    ) -> api::DSResult<Response<Body>> {
        let op = self.file_path(job, file, false).or_500()?;

        if op.is_file() {
            /*
             * The file exists locally.
             */
            let f = std::fs::File::open(op).or_500()?;

            buildomat_download::stream_from_file(log, info, f, range, head)
                .await
                .or_500()
        } else {
            /*
             * Otherwise, try to get it from the object store.
             */
            buildomat_download::stream_from_s3(
                log,
                info,
                &self.s3,
                &self.config.storage.bucket,
                self.file_object_key(job, file),
                range,
                head,
            )
            .await
            .or_500()
        }
    }

    fn complete_job(
        &self,
        log: &Logger,
        job: JobId,
        failed: bool,
    ) -> Result<bool> {
        if let Err(e) = self.files.mark_job_completed(job) {
            warn!(log, "job {job} cannot be completed yet: {e}");
            bail!("{}", e);
        }

        let res = self.db.job_complete(job, failed)?;

        self.files.forget_job(job);

        Ok(res)
    }

    /**
     * Load a job record on behalf of an authenticated user.  If the user does
     * not have the right to see the record, we'll return an appropriate HTTP
     * error that should be passed back as the response to the request.
     */
    async fn load_job_for_user(
        &self,
        log: &Logger,
        user: &AuthUser,
        id: JobId,
    ) -> SResult<Job, HttpError> {
        /*
         * Job records are either resident in the database or unknown to the
         * system.  If the database is damaged, job records will need to be
         * repopulated by importing from the archive.
         */
        let job = self.db.job(id).or_500()?;

        let readpriv = "admin.job.read";
        if job.owner == user.id {
            /*
             * Users are always allowed to see their own job records.
             */
            Ok(job)
        } else if user.has_privilege("admin.job.read") {
            /*
             * Users may be granted the right to view all jobs in the system,
             * regardless of who owns them.
             */
            info!(
                log,
                "user {} used delegated admin privilege {readpriv}", user.name,
            );
            Ok(job)
        } else {
            Err(HttpError::for_client_error(
                None,
                StatusCode::FORBIDDEN,
                "not your job".into(),
            ))
        }
    }

    /**
     * Load a job output record, either from the live database or the
     * archive.
     */
    async fn load_job_output(
        &self,
        log: &Logger,
        job: &Job,
        output: JobFileId,
    ) -> Result<JobOutput> {
        if job.is_archived() {
            let aj = self.archive_load(log, job.id).await?;

            aj.job_output(output)
        } else {
            Ok(self.db.job_output(job.id, output)?)
        }
    }

    /**
     * Load all job output records for a particular job, either from the live
     * database or the archive.
     */
    async fn load_job_outputs(
        &self,
        log: &Logger,
        job: &Job,
    ) -> Result<Vec<JobOutputAndFile>> {
        if job.is_archived() {
            let aj = self.archive_load(log, job.id).await?;

            aj.job_outputs()
        } else {
            Ok(self.db.job_outputs(job.id)?)
        }
    }

    /**
     * Load job event records for a particular job, either from the live
     * database or the archive.  Records are sorted by sequence number in
     * ascending order.
     *
     * The minseq argument determines the sequence number of the first event to
     * be returned.  Event sequence numbers begin at 0 and increase
     * monotonically without holes.  If a number higher than that of the most
     * recently stored event is specified, an empty list is returned.
     */
    async fn load_job_events(
        &self,
        log: &Logger,
        job: &Job,
        minseq: usize,
        limit: u64,
    ) -> Result<Vec<JobEvent>> {
        if job.is_archived() {
            let aj = self.archive_load(log, job.id).await?;

            aj.job_events(minseq, limit)
        } else {
            Ok(self.db.job_events(job.id, minseq, limit)?)
        }
    }

    async fn load_worker_events(
        &self,
        _log: &Logger,
        worker: &Worker,
        minseq: usize,
        limit: u64,
    ) -> Result<Vec<WorkerEvent>> {
        Ok(self.db.worker_events(worker.id, minseq, limit)?)
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

async fn file_agent_common(
    log: &Logger,
    q: &FileAgentQuery,
    gzip: bool,
    head_only: bool,
) -> SResult<Response<Body>, HttpError> {
    let pfx = if gzip { "compressed " } else { "" };
    info!(log, "{pfx}agent request; query = {:?}", q);

    let filename = {
        let mut p = PathBuf::from(if q.is_linux() {
            "buildomat-agent-linux"
        } else {
            "buildomat-agent"
        });
        if gzip {
            assert!(p.set_extension("gz"));
        }
        p
    };

    if !filename.is_file() {
        error!(log, "missing agent file {filename:?}");
        return Err(HttpError::for_not_found(
            None,
            "missing agent file".into(),
        ));
    }

    info!(log, "using agent file {filename:?}");

    buildomat_download::stream_from_file(
        log,
        format!("agent file: {filename:?}"),
        std::fs::File::open(&filename).or_500()?,
        None,
        head_only,
    )
    .await
    .or_500()
}

#[endpoint {
    method = GET,
    path = "/file/agent",
    unpublished = true,
}]
async fn file_agent(
    rqctx: RequestContext<Arc<Central>>,
    query: TypedQuery<FileAgentQuery>,
) -> SResult<Response<Body>, HttpError> {
    let log = &rqctx.log;

    file_agent_common(log, &query.into_inner(), false, false).await
}

#[endpoint {
    method = HEAD,
    path = "/file/agent",
    unpublished = true,
}]
async fn head_file_agent(
    rqctx: RequestContext<Arc<Central>>,
    query: TypedQuery<FileAgentQuery>,
) -> SResult<Response<Body>, HttpError> {
    let log = &rqctx.log;

    file_agent_common(log, &query.into_inner(), false, true).await
}

#[endpoint {
    method = GET,
    path = "/file/agent.gz",
    unpublished = true,
}]
async fn file_agent_gz(
    rqctx: RequestContext<Arc<Central>>,
    query: TypedQuery<FileAgentQuery>,
) -> SResult<Response<Body>, HttpError> {
    let log = &rqctx.log;

    file_agent_common(log, &query.into_inner(), true, false).await
}

#[endpoint {
    method = HEAD,
    path = "/file/agent.gz",
    unpublished = true,
}]
async fn head_file_agent_gz(
    rqctx: RequestContext<Arc<Central>>,
    query: TypedQuery<FileAgentQuery>,
) -> SResult<Response<Body>, HttpError> {
    let log = &rqctx.log;

    file_agent_common(log, &query.into_inner(), true, true).await
}

#[tokio::main]
async fn main() -> Result<()> {
    usdt::register_probes().unwrap();

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
    ad.register(api::admin::worker).api_check()?;
    ad.register(api::admin::workers_list).api_check()?;
    ad.register(api::admin::workers_list_old).api_check()?;
    ad.register(api::admin::workers_recycle).api_check()?;
    ad.register(api::admin::worker_recycle).api_check()?;
    ad.register(api::admin::worker_hold_mark).api_check()?;
    ad.register(api::admin::worker_hold_release).api_check()?;
    ad.register(api::admin::worker_events_get).api_check()?;
    ad.register(api::admin::admin_job_get).api_check()?;
    ad.register(api::admin::admin_job_archive_request).api_check()?;
    ad.register(api::admin::admin_jobs_get).api_check()?;
    ad.register(api::admin::admin_jobs_list).api_check()?;
    ad.register(api::admin::factory_create).api_check()?;
    ad.register(api::admin::factory_enable).api_check()?;
    ad.register(api::admin::factory_disable).api_check()?;
    ad.register(api::admin::factories_list).api_check()?;
    ad.register(api::admin::target_create).api_check()?;
    ad.register(api::admin::targets_list).api_check()?;
    ad.register(api::admin::target_require_privilege).api_check()?;
    ad.register(api::admin::target_require_no_privilege).api_check()?;
    ad.register(api::admin::target_redirect).api_check()?;
    ad.register(api::admin::target_rename).api_check()?;
    ad.register(api::user::job_events_get).api_check()?;
    ad.register(api::user::job_watch).api_check()?;
    ad.register(api::user::job_outputs_get).api_check()?;
    ad.register(api::user::job_output_download).api_check()?;
    ad.register(api::user::job_output_head).api_check()?;
    ad.register(api::user::job_output_signed_url).api_check()?;
    ad.register(api::user::job_output_publish).api_check()?;
    ad.register(api::user::job_get).api_check()?;
    ad.register(api::user::job_store_get_all).api_check()?;
    ad.register(api::user::job_store_put).api_check()?;
    ad.register(api::user::job_submit).api_check()?;
    ad.register(api::user::job_upload_chunk).api_check()?;
    ad.register(api::user::job_add_input).api_check()?;
    ad.register(api::user::job_add_input_sync).api_check()?;
    ad.register(api::user::job_cancel).api_check()?;
    ad.register(api::user::jobs_list).api_check()?;
    ad.register(api::user::jobs_get_old).api_check()?;
    ad.register(api::user::quota).api_check()?;
    ad.register(api::user::whoami).api_check()?;
    ad.register(api::worker::worker_bootstrap).api_check()?;
    ad.register(api::worker::worker_ping).api_check()?;
    ad.register(api::worker::worker_fail).api_check()?;
    ad.register(api::worker::worker_diagnostics_enable).api_check()?;
    ad.register(api::worker::worker_diagnostics_complete).api_check()?;
    ad.register(api::worker::worker_append).api_check()?;
    ad.register(api::worker::worker_job_append).api_check()?;
    ad.register(api::worker::worker_job_append_one).api_check()?;
    ad.register(api::worker::worker_job_complete).api_check()?;
    ad.register(api::worker::worker_job_upload_chunk).api_check()?;
    ad.register(api::worker::worker_job_quota).api_check()?;
    ad.register(api::worker::worker_job_add_output).api_check()?;
    ad.register(api::worker::worker_job_add_output_sync).api_check()?;
    ad.register(api::worker::worker_job_input_download).api_check()?;
    ad.register(api::worker::worker_job_store_get).api_check()?;
    ad.register(api::worker::worker_job_store_put).api_check()?;
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
    ad.register(api::public::public_file_head).api_check()?;
    ad.register(file_agent).api_check()?;
    ad.register(head_file_agent).api_check()?;
    ad.register(file_agent_gz).api_check()?;
    ad.register(head_file_agent_gz).api_check()?;

    if let Some(s) = p.opt_str("S") {
        let mut f =
            std::fs::OpenOptions::new().create_new(true).write(true).open(s)?;
        ad.openapi("Buildomat", "1.0").write(&mut f)?;
        return Ok(());
    }

    let bind_address =
        p.opt_str("b").as_deref().unwrap_or("127.0.0.1:9979").parse()?;

    let config = if let Some(f) = p.opt_str("f").as_deref() {
        config::load(f)?
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

    let awscfg = aws_config::ConfigLoader::default()
        .region(config.storage.region())
        .credentials_provider(config.storage.creds())
        .behavior_version(aws_config::BehaviorVersion::v2023_11_09())
        .load()
        .await;
    let s3 = aws_sdk_s3::Client::new(&awscfg);

    let files = files::Files::new(log.new(o!("component" => "files")));

    let c = Arc::new(Central {
        inner: Mutex::new(CentralInner {
            hold: config.admin.hold,
            leases: Default::default(),
            archive_queue: Default::default(),
        }),
        config,
        datadir,
        db,
        s3,
        files,
    });

    c.files.start(&c, 4);

    let c0 = Arc::clone(&c);
    let log0 = log.new(o!("component" => "job_assignment"));
    let t_assign = tokio::task::spawn(async move {
        jobs::job_assignment(log0, c0)
            .await
            .context("job assignment task failure")
    });

    let c0 = Arc::clone(&c);
    let log0 = log.new(o!("component" => "chunk_cleanup"));
    let t_chunks = tokio::task::spawn(async move {
        chunks::chunk_cleanup(log0, c0)
            .await
            .context("chunk cleanup task failure")
    });

    let c0 = Arc::clone(&c);
    let log0 = log.new(o!("component" => "archive_files"));
    let t_archive_files = tokio::task::spawn(async move {
        archive::files::archive_files(log0, c0)
            .await
            .context("archive files task failure")
    });

    let c0 = Arc::clone(&c);
    let log0 = log.new(o!("component" => "archive_jobs"));
    let t_archive_jobs = tokio::task::spawn(async move {
        archive::jobs::archive_jobs(log0, c0)
            .await
            .context("archive jobs task failure")
    });

    let c0 = Arc::clone(&c);
    let log0 = log.new(o!("component" => "worker_cleanup"));
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
            log_headers: vec!["X-Forwarded-For".into()],
            ..Default::default()
        },
        ad,
        c,
        &log,
    )
    .map_err(|e| anyhow!("server startup failure: {:?}", e))?;

    let server_task = server.start();

    tokio::select! {
        _ = t_assign => bail!("task assignment task stopped early"),
        _ = t_chunks => bail!("chunk cleanup task stopped early"),
        _ = t_archive_files => bail!("archive files task stopped early"),
        _ = t_archive_jobs => bail!("archive jobs task stopped early"),
        _ = t_workers => bail!("worker cleanup task stopped early"),
        _ = server_task => bail!("server stopped early"),
    }
}
