/*
 * Copyright 2022 Oxide Computer Company
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
            StatusCode::FORBIDDEN,
            "not your job".into(),
        ))
    }
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct JobPath {
    job: String,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct JobInputPath {
    job: String,
    input: String,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct JobTaskPath {
    job: String,
    task: u32,
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
pub(crate) struct WorkerPingResult {
    poweroff: bool,
    job: Option<WorkerPingJob>,
}

#[endpoint {
    method = GET,
    path = "/0/worker/ping",
}]
pub(crate) async fn worker_ping(
    rqctx: Arc<RequestContext<Arc<Central>>>,
) -> DSResult<HttpResponseOk<WorkerPingResult>> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    let w = c.require_worker(log, &req).await?;

    info!(log, "worker ping!"; "id" => w.id.to_string());

    c.db.worker_ping(w.id).or_500()?;

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
                    .filter(|(ji, _)| ji.id.is_some())
                    .map(|(ji, _)| WorkerPingInput {
                        name: ji.name.to_string(),
                        id: ji.id.unwrap().to_string(),
                    })
                    .collect::<Vec<_>>(),
            })
        } else {
            None
        }
    };

    let res = WorkerPingResult { poweroff: w.recycle || w.deleted, job };

    Ok(HttpResponseOk(res))
}

#[endpoint {
    method = GET,
    path = "/0/worker/job/{job}/inputs/{input}",
}]
pub(crate) async fn worker_job_input_download(
    rqctx: Arc<RequestContext<Arc<Central>>>,
    path: TypedPath<JobInputPath>,
) -> DSResult<Response<Body>> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    let w = c.require_worker(log, &req).await?;

    let p = path.into_inner();
    let j = c.db.job_by_str(&p.job).or_500()?;
    w.owns(log, &j)?;

    let i = c.db.job_input_by_str(&p.job, &p.input).or_500()?;

    let mut res = Response::builder();
    res = res.header(CONTENT_TYPE, "application/octet-stream");

    let fr = c
        .file_response(i.other_job.unwrap_or(i.job), i.id.unwrap())
        .await
        .or_500()?;
    info!(
        log,
        "worker {} job {} input {} name {:?} is in the {}",
        w.id,
        j.id,
        i.id.as_ref().unwrap(),
        i.name,
        fr.info
    );

    res = res.header(CONTENT_LENGTH, fr.size);
    Ok(res.body(fr.body)?)
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
}]
pub(crate) async fn worker_job_append(
    rqctx: Arc<RequestContext<Arc<Central>>>,
    path: TypedPath<JobPath>,
    append: TypedBody<WorkerAppendJob>,
) -> DSResult<HttpResponseUpdatedNoContent> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    let w = c.require_worker(log, &req).await?;

    let a = append.into_inner();
    let j = c.db.job_by_str(&path.into_inner().job).or_500()?; /* XXX */
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

#[endpoint {
    method = POST,
    path = "/0/worker/job/{job}/task/{task}/append",
}]
pub(crate) async fn worker_task_append(
    rqctx: Arc<RequestContext<Arc<Central>>>,
    path: TypedPath<JobTaskPath>,
    append: TypedBody<WorkerAppendJob>,
) -> DSResult<HttpResponseUpdatedNoContent> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    let w = c.require_worker(log, &req).await?;

    let a = append.into_inner();
    let p = path.into_inner();
    let j = c.db.job_by_str(&p.job).or_500()?; /* XXX */
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
    rqctx: Arc<RequestContext<Arc<Central>>>,
    path: TypedPath<JobTaskPath>,
    body: TypedBody<WorkerCompleteTask>,
) -> DSResult<HttpResponseUpdatedNoContent> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    let w = c.require_worker(log, &req).await?;

    let b = body.into_inner();
    let p = path.into_inner();
    let j = c.db.job_by_str(&p.job).or_500()?; /* XXX */
    w.owns(log, &j)?;

    info!(log, "worker {} complete job {} task {}", w.id, j.id, p.task);
    c.db.task_complete(j.id, p.task, b.failed).or_500()?;

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
    rqctx: Arc<RequestContext<Arc<Central>>>,
    path: TypedPath<JobPath>,
    body: TypedBody<WorkerCompleteJob>,
) -> DSResult<HttpResponseUpdatedNoContent> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    let w = c.require_worker(log, &req).await?;

    let b = body.into_inner();
    let p = path.into_inner();
    let j = c.db.job_by_str(&p.job).or_500()?; /* XXX */
    w.owns(log, &j)?;

    info!(log, "worker {} complete job {}", w.id, j.id);
    c.db.job_complete(j.id, b.failed).or_500()?;

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
    rqctx: Arc<RequestContext<Arc<Central>>>,
    path: TypedPath<JobPath>,
    chunk: UntypedBody,
) -> DSResult<HttpResponseCreated<UploadedChunk>> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    let w = c.require_worker(log, &req).await?;
    let j = c.db.job_by_str(&path.into_inner().job).or_500()?; /* XXX */
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

#[derive(Deserialize, JsonSchema)]
pub(crate) struct WorkerAddOutput {
    path: String,
    size: i64,
    chunks: Vec<String>,
}

#[endpoint {
    method = POST,
    path = "/0/worker/job/{job}/output",
}]
pub(crate) async fn worker_job_add_output(
    rqctx: Arc<RequestContext<Arc<Central>>>,
    path: TypedPath<JobPath>,
    add: TypedBody<WorkerAddOutput>,
) -> DSResult<HttpResponseUpdatedNoContent> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    let add = add.into_inner();
    let addsize = if add.size < 0 {
        return Err(HttpError::for_client_error(
            Some("invalid".to_string()),
            StatusCode::BAD_REQUEST,
            format!("size {} must be >=0", add.size),
        ));
    } else {
        add.size as u64
    };
    let w = c.require_worker(log, &req).await?;
    let j = c.db.job_by_str(&path.into_inner().job).or_500()?; /* XXX */
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
                StatusCode::BAD_REQUEST,
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
    rqctx: Arc<RequestContext<Arc<Central>>>,
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
