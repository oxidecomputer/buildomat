/*
 * Copyright 2024 Oxide Computer Company
 */

use super::prelude::*;

trait JobOwns {
    fn owns(&self, log: &Logger, job: &db::Job) -> DSResult<()>;
}

impl JobOwns for db::Worker {
    fn owns(&self, log: &Logger, job: &db::Job) -> DSResult<()> {
        if let Some(owner) = job.worker.as_ref() {
            if owner == &self.id {
                return Ok(());
            }
        }

        warn!(log, "job {} owned by {:?}, not {}", job.id, job.worker, self.id);

        Err(HttpError::for_client_error(
            None,
            ClientErrorStatusCode::FORBIDDEN,
            "not your job".into(),
        ))
    }
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct JobPath {
    job: String,
}

impl JobPath {
    fn job(&self) -> DSResult<db::JobId> {
        self.job.parse::<db::JobId>().or_500()
    }
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct JobInputPath {
    job: String,
    input: String,
}

impl JobInputPath {
    fn job(&self) -> DSResult<db::JobId> {
        self.job.parse::<db::JobId>().or_500()
    }

    fn input(&self) -> DSResult<db::JobFileId> {
        self.input.parse::<db::JobFileId>().or_500()
    }
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct JobTaskPath {
    job: String,
    task: u32,
}

impl JobTaskPath {
    fn job(&self) -> DSResult<db::JobId> {
        self.job.parse::<db::JobId>().or_500()
    }
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct JobStorePath {
    job: String,
    name: String,
}

impl JobStorePath {
    fn job(&self) -> DSResult<db::JobId> {
        self.job.parse::<db::JobId>().or_500()
    }
}

#[derive(Deserialize, Serialize, JsonSchema)]
pub(crate) struct WorkerJobStoreValue {
    value: String,
    secret: bool,
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct WorkerPingTask {
    id: u32,
    name: String,
    script: String,
    env_clear: bool,
    env: HashMap<String, String>,
    uid: u32,
    gid: u32,
    workdir: String,
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct WorkerPingInput {
    name: String,
    id: String,
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct WorkerPingOutputRule {
    rule: String,
    ignore: bool,
    size_change_ok: bool,
    require_match: bool,
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct WorkerPingJob {
    id: String,
    name: String,
    output_rules: Vec<WorkerPingOutputRule>,
    tasks: Vec<WorkerPingTask>,
    inputs: Vec<WorkerPingInput>,
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct WorkerPingFactoryInfo {
    id: String,
    name: String,
    private: Option<String>,
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct WorkerPingResult {
    poweroff: bool,
    job: Option<WorkerPingJob>,
    factory_metadata: Option<metadata::FactoryMetadata>,
    factory_info: Option<WorkerPingFactoryInfo>,
}

#[endpoint {
    method = GET,
    path = "/0/worker/ping",
}]
pub(crate) async fn worker_ping(
    rqctx: RequestContext<Arc<Central>>,
) -> DSResult<HttpResponseOk<WorkerPingResult>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let w = c.require_worker(log, &rqctx.request).await?;

    info!(log, "worker ping!"; "id" => w.id.to_string());

    c.db.worker_ping(w.id).or_500()?;

    let factory_metadata = w.factory_metadata().or_500()?;

    let factory = c.db.factory(w.factory()).or_500()?;

    let job = if w.wait_for_flush {
        /*
         * The factory may have event records (e.g., boot time console logs or
         * other information about provisioning) to flush before we give the job
         * to the agent.
         */
        None
    } else {
        let job = c.db.worker_job(w.id).or_500()?;
        if let Some(job) = job {
            Some(WorkerPingJob {
                id: job.id.to_string(),
                name: job.name,
                output_rules: c
                    .db
                    .job_output_rules(job.id)
                    .or_500()?
                    .iter()
                    .map(|jor| WorkerPingOutputRule {
                        rule: jor.rule.to_string(),
                        ignore: jor.ignore,
                        size_change_ok: jor.size_change_ok,
                        require_match: jor.require_match,
                    })
                    .collect::<Vec<_>>(),
                tasks: c
                    .db
                    .job_tasks(job.id)
                    .or_500()?
                    .iter()
                    .enumerate()
                    .map(|(i, t)| WorkerPingTask {
                        id: i as u32,
                        name: t.name.to_string(),
                        script: t.script.to_string(),
                        env_clear: t.env_clear,
                        env: t.env.clone().into(),
                        uid: t.user_id.map(|x| x.0).unwrap_or(0),
                        gid: t.group_id.map(|x| x.0).unwrap_or(0),
                        workdir: t
                            .workdir
                            .as_deref()
                            .unwrap_or("/")
                            .to_string(),
                    })
                    .collect::<Vec<_>>(),
                inputs: c
                    .db
                    .job_inputs(job.id)
                    .or_500()?
                    .iter()
                    .filter(|jif| jif.input.id.is_some())
                    .map(|jif| WorkerPingInput {
                        name: jif.input.name.to_string(),
                        id: jif.input.id.unwrap().to_string(),
                    })
                    .collect::<Vec<_>>(),
            })
        } else {
            None
        }
    };

    let poweroff = if w.is_held() || w.diagnostics {
        /*
         * If the worker is on hold, we don't want to power it off as this would
         * destroy at least some of the evidence we are trying to preserve for
         * investigation.
         */
        false
    } else {
        w.recycle || w.deleted
    };

    let res = WorkerPingResult {
        poweroff,
        job,
        factory_metadata,
        factory_info: Some(WorkerPingFactoryInfo {
            id: factory.id.to_string(),
            name: factory.name,
            private: w.factory_private.clone(),
        }),
    };

    Ok(HttpResponseOk(res))
}

#[derive(Deserialize, Serialize, JsonSchema)]
pub(crate) struct WorkerFail {
    reason: Option<String>,
}

#[endpoint {
    method = POST,
    path = "/0/worker/fail",
}]
pub(crate) async fn worker_fail(
    rqctx: RequestContext<Arc<Central>>,
    body: TypedBody<WorkerFail>,
) -> DSResult<HttpResponseUpdatedNoContent> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let w = c.require_worker(log, &rqctx.request).await?;
    let b = body.into_inner();

    let reason = b.reason.as_deref().unwrap_or("agent reported failure");
    warn!(log, "worker failed!"; "id" => w.id.to_string(),
        "reason" => reason);

    /*
     * Record in the database that the worker has failed.  This routine will
     * take care of reporting failure in any assigned jobs, marking the worker
     * as held, etc.
     */
    let failed_jobs = c.db.worker_mark_failed(w.id, reason).or_500()?;
    if !failed_jobs.is_empty() {
        let jobs = failed_jobs
            .into_iter()
            .map(|j| j.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        warn!(log, "worker {} failing caused jobs {jobs} to fail", w.id);
    }

    Ok(HttpResponseUpdatedNoContent())
}

#[endpoint {
    method = GET,
    path = "/0/worker/job/{job}/inputs/{input}",
}]
pub(crate) async fn worker_job_input_download(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<JobInputPath>,
) -> DSResult<Response<Body>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let w = c.require_worker(log, &rqctx.request).await?;

    let p = path.into_inner();
    let pr = rqctx.range();
    let j = c.db.job(p.job()?).or_500()?;
    w.owns(log, &j)?;

    let i = c.db.job_input(p.job()?, p.input()?).or_500()?;

    let info = format!(
        "worker {} job {} input {} name {:?}",
        w.id,
        j.id,
        i.id.as_ref().unwrap(),
        i.name
    );
    c.file_response(
        log,
        info,
        i.other_job.unwrap_or(i.job),
        i.id.unwrap(),
        pr,
        false,
    )
    .await
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct WorkerAppendJob {
    stream: String,
    time: DateTime<Utc>,
    payload: String,
}

#[endpoint {
    method = POST,
    path = "/0/worker/job/{job}/append",
    unpublished = true,
}]
pub(crate) async fn worker_job_append_one(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<JobPath>,
    append: TypedBody<WorkerAppendJob>,
) -> DSResult<HttpResponseUpdatedNoContent> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let w = c.require_worker(log, &rqctx.request).await?;

    let a = append.into_inner();
    let j = c.db.job(path.into_inner().job()?).or_500()?; /* XXX */
    w.owns(log, &j)?;

    info!(log, "worker {} append to job {} stream {}", w.id, j.id, a.stream);

    c.db.job_append_event(
        j.id,
        None,
        &a.stream,
        Utc::now(),
        Some(a.time),
        &a.payload,
    )
    .or_500()?;

    Ok(HttpResponseUpdatedNoContent())
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct WorkerAppendJobOrTask {
    stream: String,
    time: DateTime<Utc>,
    payload: String,
    task: Option<u32>,
}

#[endpoint {
    method = POST,
    path = "/1/worker/job/{job}/append",
}]
pub(crate) async fn worker_job_append(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<JobPath>,
    append: TypedBody<Vec<WorkerAppendJobOrTask>>,
) -> DSResult<HttpResponseUpdatedNoContent> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let w = c.require_worker(log, &rqctx.request).await?;

    let a = append.into_inner();
    let j = c.db.job(path.into_inner().job()?).or_500()?; /* XXX */
    w.owns(log, &j)?;

    info!(log, "worker {} append {} events to job {}", w.id, a.len(), j.id);

    c.db.job_append_events(
        j.id,
        a.into_iter().map(|a| db::CreateJobEvent {
            task: a.task,
            stream: a.stream,
            time: Utc::now(),
            time_remote: Some(a.time),
            payload: a.payload,
        }),
    )
    .or_500()?;

    Ok(HttpResponseUpdatedNoContent())
}

#[endpoint {
    method = POST,
    path = "/0/worker/job/{job}/task/{task}/append",
}]
pub(crate) async fn worker_task_append(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<JobTaskPath>,
    append: TypedBody<WorkerAppendJob>,
) -> DSResult<HttpResponseUpdatedNoContent> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let w = c.require_worker(log, &rqctx.request).await?;

    let a = append.into_inner();
    let p = path.into_inner();
    let j = c.db.job(p.job()?).or_500()?; /* XXX */
    w.owns(log, &j)?;

    info!(
        log,
        "worker {} append to job {} task {} stream {}",
        w.id,
        j.id,
        p.task,
        a.stream
    );

    c.db.job_append_event(
        j.id,
        Some(p.task),
        &a.stream,
        Utc::now(),
        Some(a.time),
        &a.payload,
    )
    .or_500()?;

    Ok(HttpResponseUpdatedNoContent())
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct WorkerCompleteTask {
    failed: bool,
}

#[endpoint {
    method = POST,
    path = "/0/worker/job/{job}/task/{task}/complete",
}]
pub(crate) async fn worker_task_complete(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<JobTaskPath>,
    body: TypedBody<WorkerCompleteTask>,
) -> DSResult<HttpResponseUpdatedNoContent> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let w = c.require_worker(log, &rqctx.request).await?;

    let b = body.into_inner();
    let p = path.into_inner();
    let j = c.db.job(p.job()?).or_500()?; /* XXX */
    w.owns(log, &j)?;

    info!(log, "worker {} complete job {} task {}", w.id, j.id, p.task);
    c.db.task_complete(j.id, p.task, b.failed).or_500()?;

    Ok(HttpResponseUpdatedNoContent())
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct WorkerJobStoreGet {
    value: Option<WorkerJobStoreValue>,
}

#[endpoint {
    method = GET,
    path = "/0/worker/job/{job}/store/{name}",
}]
pub(crate) async fn worker_job_store_get(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<JobStorePath>,
) -> DSResult<HttpResponseOk<WorkerJobStoreGet>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let w = c.require_worker(log, &rqctx.request).await?;

    let p = path.into_inner();
    let j = c.db.job(p.job()?).or_500()?; /* XXX */
    w.owns(log, &j)?;

    info!(log, "worker {} job {} get store value {}", w.id, j.id, p.name);

    let store = c.db.job_store(j.id).or_500()?;

    Ok(HttpResponseOk(WorkerJobStoreGet {
        value: store.get(&p.name).map(|v| WorkerJobStoreValue {
            value: v.value.to_string(),
            secret: v.secret,
        }),
    }))
}

#[endpoint {
    method = PUT,
    path = "/0/worker/job/{job}/store/{name}",
}]
pub(crate) async fn worker_job_store_put(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<JobStorePath>,
    body: TypedBody<WorkerJobStoreValue>,
) -> DSResult<HttpResponseUpdatedNoContent> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let w = c.require_worker(log, &rqctx.request).await?;

    let b = body.into_inner();
    let p = path.into_inner();
    let j = c.db.job(p.job()?).or_500()?; /* XXX */
    w.owns(log, &j)?;

    info!(log, "worker {} job {} put store value {}", w.id, j.id, p.name);

    c.db.job_store_put(j.id, &p.name, &b.value, b.secret, "worker").or_500()?;

    Ok(HttpResponseUpdatedNoContent())
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct WorkerCompleteJob {
    failed: bool,
}

#[endpoint {
    method = POST,
    path = "/0/worker/job/{job}/complete",
}]
pub(crate) async fn worker_job_complete(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<JobPath>,
    body: TypedBody<WorkerCompleteJob>,
) -> DSResult<HttpResponseUpdatedNoContent> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let w = c.require_worker(log, &rqctx.request).await?;

    let b = body.into_inner();
    let p = path.into_inner();
    let j = c.db.job(p.job()?).or_500()?; /* XXX */
    w.owns(log, &j)?;

    if let Err(e) = c.complete_job(log, j.id, b.failed) {
        error!(log, "worker {} cannot complete job {}: {e}", w.id, j.id);
        return Err(HttpError::for_client_error(
            None,
            ClientErrorStatusCode::CONFLICT,
            format!("cannot complete job: {e}"),
        ));
    }

    info!(log, "worker {} complete job {}", w.id, j.id);

    Ok(HttpResponseUpdatedNoContent())
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct UploadedChunk {
    pub id: String,
}

#[endpoint {
    method = POST,
    path = "/0/worker/job/{job}/chunk",
}]
pub(crate) async fn worker_job_upload_chunk(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<JobPath>,
    chunk: UntypedBody,
) -> DSResult<HttpResponseCreated<UploadedChunk>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let w = c.require_worker(log, &rqctx.request).await?;
    let j = c.db.job(path.into_inner().job()?).or_500()?; /* XXX */
    w.owns(log, &j)?;

    let cid = c.write_chunk(j.id, chunk.as_bytes()).or_500()?;
    info!(
        log,
        "worker {} wrote chunk {} for job {}, size {}",
        w.id,
        cid,
        j.id,
        chunk.as_bytes().len(),
    );

    Ok(HttpResponseCreated(UploadedChunk { id: cid.to_string() }))
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct WorkerJobQuota {
    max_bytes_per_output: u64,
}

#[endpoint {
    method = GET,
    path = "/0/worker/job/{job}/quota",
}]
pub(crate) async fn worker_job_quota(
    rqctx: RequestContext<Arc<Central>>,
    _path: TypedPath<JobPath>,
) -> DSResult<HttpResponseOk<WorkerJobQuota>> {
    let c = rqctx.context();

    /*
     * For now, this request just presents statically configured quota
     * information.  In the future, we should have the server examine the set of
     * outputs the job has presently produced and furnish the agent with the
     * number of bytes that remain in the per-job output quota.
     */
    Ok(HttpResponseOk(WorkerJobQuota {
        max_bytes_per_output: c.config.job.max_bytes_per_output(),
    }))
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct WorkerAddOutput {
    path: String,
    size: u64,
    chunks: Vec<String>,
    commit_id: String,
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct WorkerAddOutputResult {
    complete: bool,
    error: Option<String>,
}

#[endpoint {
    method = POST,
    path = "/1/worker/job/{job}/output",
}]
pub(crate) async fn worker_job_add_output(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<JobPath>,
    add: TypedBody<WorkerAddOutput>,
) -> DSResult<HttpResponseOk<WorkerAddOutputResult>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let w = c.require_worker(log, &rqctx.request).await?;
    let j = c.db.job(path.into_inner().job()?).or_500()?; /* XXX */
    w.owns(log, &j)?;

    let add = add.into_inner();
    let chunks = add
        .chunks
        .iter()
        .map(|f| Ok(Ulid::from_str(f.as_str())?))
        .collect::<Result<Vec<_>>>()
        .or_500()?;
    let commit_id = Ulid::from_str(add.commit_id.as_str()).or_500()?;

    let max = c.config.job.max_bytes_per_output();
    if add.size > max {
        return Err(HttpError::for_client_error(
            None,
            ClientErrorStatusCode::BAD_REQUEST,
            format!(
                "output file size {} bigger than allowed maximum {max} bytes",
                add.size,
            ),
        ));
    }

    let res = c.files.commit_file(
        j.id,
        commit_id,
        crate::files::FileKind::Output { path: add.path.to_string() },
        add.size,
        chunks,
    );

    match res {
        Ok(Some(Ok(()))) => Ok(HttpResponseOk(WorkerAddOutputResult {
            complete: true,
            error: None,
        })),
        Ok(Some(Err(msg))) => Ok(HttpResponseOk(WorkerAddOutputResult {
            complete: true,
            error: Some(msg.to_string()),
        })),
        Ok(None) => {
            /*
             * This job is either queued or active, but not yet complete.
             */
            Ok(HttpResponseOk(WorkerAddOutputResult {
                complete: false,
                error: None,
            }))
        }
        Err(e) => {
            /*
             * This is a failure to _submit_ the job; e.g., invalid arguments,
             * or arguments inconsistent with a prior call using the same commit
             * ID.
             */
            warn!(
                log,
                "worker {} job {} upload {} commit {} size {}: {:?}",
                w.id,
                j.id,
                add.path,
                add.commit_id,
                add.size,
                e,
            );
            Err(HttpError::for_client_error(
                Some("invalid".to_string()),
                ClientErrorStatusCode::BAD_REQUEST,
                format!("{}", e),
            ))
        }
    }
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct WorkerAddOutputSync {
    path: String,
    size: i64,
    chunks: Vec<String>,
}

#[endpoint {
    method = POST,
    path = "/0/worker/job/{job}/output",
    unpublished = true,
}]
pub(crate) async fn worker_job_add_output_sync(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<JobPath>,
    add: TypedBody<WorkerAddOutputSync>,
) -> DSResult<HttpResponseUpdatedNoContent> {
    let c = rqctx.context();
    let log = &rqctx.log;

    /*
     * Individual outputs using the old blocking entrypoint are capped at 1GB to
     * avoid request timeouts.  Larger outputs are possible using the new
     * asynchronous job mechanism.
     */
    let add = add.into_inner();
    let addsize = if add.size < 0 || add.size > 1024 * 1024 * 1024 {
        return Err(HttpError::for_client_error(
            Some("invalid".to_string()),
            ClientErrorStatusCode::BAD_REQUEST,
            format!("size {} must be between 0 and 1073741824", add.size),
        ));
    } else {
        add.size as u64
    };
    let w = c.require_worker(log, &rqctx.request).await?;
    let j = c.db.job(path.into_inner().job()?).or_500()?; /* XXX */
    w.owns(log, &j)?;

    let chunks = add
        .chunks
        .iter()
        .map(|f| Ok(Ulid::from_str(f.as_str())?))
        .collect::<Result<Vec<_>>>()
        .or_500()?;

    let fid = match c.commit_file(j.id, &chunks, addsize) {
        Ok(fid) => fid,
        Err(e) => {
            warn!(
                log,
                "worker {} job {} upload {} size {}: {:?}",
                w.id,
                j.id,
                add.path,
                addsize,
                e,
            );
            return Err(HttpError::for_client_error(
                Some("invalid".to_string()),
                ClientErrorStatusCode::BAD_REQUEST,
                format!("{:?}", e),
            ));
        }
    };

    /*
     * Insert a record in the database for this output object and report
     * success.
     */
    c.db.job_add_output(j.id, &add.path, fid, addsize).or_500()?;

    Ok(HttpResponseUpdatedNoContent())
}

#[derive(Debug, Deserialize, JsonSchema)]
pub(crate) struct WorkerBootstrap {
    bootstrap: String,
    token: String,
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct WorkerBootstrapResult {
    id: String,
}

#[endpoint {
    method = POST,
    path = "/0/worker/bootstrap",
}]
pub(crate) async fn worker_bootstrap(
    rqctx: RequestContext<Arc<Central>>,
    strap: TypedBody<WorkerBootstrap>,
) -> DSResult<HttpResponseCreated<WorkerBootstrapResult>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let s = strap.into_inner();
    info!(log, "bootstrap request: {:?}", s);

    if let Some(w) = c.db.worker_bootstrap(&s.bootstrap, &s.token).or_500()? {
        Ok(HttpResponseCreated(WorkerBootstrapResult { id: w.id.to_string() }))
    } else {
        unauth_response()
    }
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct WorkerAppend {
    stream: String,
    time: DateTime<Utc>,
    payload: String,
}

#[endpoint {
    method = POST,
    path = "/0/worker/append",
}]
pub(crate) async fn worker_append(
    rqctx: RequestContext<Arc<Central>>,
    append: TypedBody<Vec<WorkerAppend>>,
) -> DSResult<HttpResponseUpdatedNoContent> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let w = c.require_worker(log, &rqctx.request).await?;

    let a = append.into_inner();

    info!(log, "worker {} append {} self events", w.id, a.len());

    c.db.worker_append_events(
        w.id,
        a.into_iter().map(|a| db::CreateWorkerEvent {
            stream: a.stream,
            time: Utc::now(),
            time_remote: Some(a.time),
            payload: a.payload,
        }),
    )
    .or_500()?;

    Ok(HttpResponseUpdatedNoContent())
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct WorkerCompleteDiagnostics {
    hold: bool,
}

#[endpoint {
    method = POST,
    path = "/0/worker/diagnostics/complete",
}]
pub(crate) async fn worker_diagnostics_complete(
    rqctx: RequestContext<Arc<Central>>,
    body: TypedBody<WorkerCompleteDiagnostics>,
) -> DSResult<HttpResponseUpdatedNoContent> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let w = c.require_worker(log, &rqctx.request).await?;

    let b = body.into_inner();

    c.db.worker_diagnostics_complete(w.id, b.hold).or_500()?;

    info!(log, "worker {} post-job diagnostics complete", w.id);

    Ok(HttpResponseUpdatedNoContent())
}

#[endpoint {
    method = POST,
    path = "/0/worker/diagnostics/enable",
}]
pub(crate) async fn worker_diagnostics_enable(
    rqctx: RequestContext<Arc<Central>>,
) -> DSResult<HttpResponseUpdatedNoContent> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let w = c.require_worker(log, &rqctx.request).await?;

    c.db.worker_diagnostics_enable(w.id).or_500()?;

    info!(log, "worker {} post-job diagnostics enabled", w.id);

    Ok(HttpResponseUpdatedNoContent())
}
