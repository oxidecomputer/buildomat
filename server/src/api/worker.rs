/*
 * Copyright 2021 Oxide Computer Company
 */

use super::prelude::*;

trait JobOwns {
    fn owns(&self, log: &Logger, job: &db::Job) -> SResult<(), HttpError>;
}

impl JobOwns for db::Worker {
    fn owns(&self, log: &Logger, job: &db::Job) -> SResult<(), HttpError> {
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
pub(crate) struct WorkerPingJob {
    id: String,
    name: String,
    output_rules: Vec<String>,
    tasks: Vec<WorkerPingTask>,
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
) -> SResult<HttpResponseOk<WorkerPingResult>, HttpError> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    let w = c.require_worker(log, &req).await?;

    info!(log, "worker ping!"; "id" => w.id.to_string());

    c.db.worker_ping(&w.id).or_500()?;

    let job = c.db.worker_job(&w.id).or_500()?;
    let job = if let Some(job) = job {
        let output_rules = c.db.job_output_rules(&job.id).or_500()?;
        let tasks =
            c.db.job_tasks(&job.id)
                .or_500()?
                .iter()
                .enumerate()
                .map(|(i, t)| WorkerPingTask {
                    id: i as u32,
                    name: t.name.to_string(),
                    script: t.script.to_string(),
                    env_clear: t.env_clear,
                    env: t.env.clone(),
                    uid: t.user_id.map(|x| x.0).unwrap_or(0),
                    gid: t.group_id.map(|x| x.0).unwrap_or(0),
                    workdir: t.workdir.as_deref().unwrap_or("/").to_string(),
                })
                .collect::<Vec<_>>();
        Some(WorkerPingJob {
            id: job.id.to_string(),
            name: job.name,
            output_rules,
            tasks,
        })
    } else {
        None
    };

    let res = WorkerPingResult { poweroff: w.recycle || w.deleted, job };

    Ok(HttpResponseOk(res))
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
) -> SResult<HttpResponseCreated<()>, HttpError> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    let w = c.require_worker(log, &req).await?;

    let a = append.into_inner();
    let j = c.db.job_by_str(&path.into_inner().job).or_500()?; /* XXX */
    w.owns(log, &j)?;

    info!(log, "worker {} append to job {} stream {}", w.id, j.id, a.stream);

    c.db.job_append_event(&j.id, None, &a.stream, a.time, &a.payload)
        .or_500()?;

    Ok(HttpResponseCreated(()))
}

#[endpoint {
    method = POST,
    path = "/0/worker/job/{job}/task/{task}/append",
}]
pub(crate) async fn worker_task_append(
    rqctx: Arc<RequestContext<Arc<Central>>>,
    path: TypedPath<JobTaskPath>,
    append: TypedBody<WorkerAppendJob>,
) -> SResult<HttpResponseCreated<()>, HttpError> {
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

    c.db.job_append_event(&j.id, Some(p.task), &a.stream, a.time, &a.payload)
        .or_500()?;

    Ok(HttpResponseCreated(()))
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
) -> SResult<HttpResponseOk<()>, HttpError> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    let w = c.require_worker(log, &req).await?;

    let b = body.into_inner();
    let p = path.into_inner();
    let j = c.db.job_by_str(&p.job).or_500()?; /* XXX */
    w.owns(log, &j)?;

    info!(log, "worker {} complete job {} task {}", w.id, j.id, p.task);
    c.db.task_complete(&j.id, p.task, b.failed).or_500()?;

    Ok(HttpResponseOk(()))
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
) -> SResult<HttpResponseOk<()>, HttpError> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    let w = c.require_worker(log, &req).await?;

    let b = body.into_inner();
    let p = path.into_inner();
    let j = c.db.job_by_str(&p.job).or_500()?; /* XXX */
    w.owns(log, &j)?;

    info!(log, "worker {} complete job {}", w.id, j.id);
    c.db.job_complete(&j.id, b.failed).or_500()?;

    Ok(HttpResponseOk(()))
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct UploadedChunk {
    id: String,
}

#[endpoint {
    method = POST,
    path = "/0/worker/job/{job}/chunk",
}]
pub(crate) async fn worker_job_upload_chunk(
    rqctx: Arc<RequestContext<Arc<Central>>>,
    path: TypedPath<JobPath>,
    chunk: UntypedBody,
) -> SResult<HttpResponseCreated<UploadedChunk>, HttpError> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    let w = c.require_worker(log, &req).await?;
    let j = c.db.job_by_str(&path.into_inner().job).or_500()?; /* XXX */
    w.owns(log, &j)?;

    /*
     * Assign an ID for this chunk and determine where will store it in the file
     * system.
     */
    let cid = Ulid::generate();
    let p = c.chunk_path(&j.id, &cid).or_500()?;
    let f = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&p)
        .or_500()?;
    let mut bw = io::BufWriter::new(f);
    bw.write_all(chunk.as_bytes()).or_500()?;
    bw.flush().or_500()?;

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
) -> SResult<HttpResponseCreated<()>, HttpError> {
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

    /*
     * Check that all of the chunks the client wants to use exist, and that the
     * sum of their sizes matches the total size.
     */
    let files = add
        .chunks
        .iter()
        .map(|f| {
            let cid = Ulid::from_str(f.as_str())?;
            let f = c.chunk_path(&j.id, &cid)?;
            let md = f.metadata()?;
            Ok((f, md.len()))
        })
        .collect::<Result<Vec<_>>>()
        .or_500()?;
    let chunksize: u64 = files.iter().map(|(_, sz)| *sz).sum();
    if chunksize != addsize {
        warn!(
            log,
            "worker {} job {} upload {} size {} != chunk total {}",
            w.id,
            j.id,
            add.path,
            addsize,
            chunksize
        );
        return Err(HttpError::for_client_error(
            Some("invalid".to_string()),
            StatusCode::BAD_REQUEST,
            format!("size {} != chunk total {}", addsize, chunksize),
        ));
    }

    /*
     * Assign an ID for this output and determine where we will store it in the
     * file system.
     */
    let oid = db::JobOutputId::generate();
    let op = c.output_path(&j.id, &oid).or_500()?;
    let mut fout = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&op)
        .or_500()?;
    let mut bw = io::BufWriter::new(&mut fout);
    for (ip, _) in files.iter() {
        let fin = fs::File::open(&ip).or_500()?;
        let mut br = io::BufReader::new(fin);

        io::copy(&mut br, &mut bw).or_500()?;
    }
    bw.flush().or_500()?;
    drop(bw);

    /*
     * Confirm again that file size is as expected.
     */
    let md = fout.metadata().or_500()?;
    if md.len() != addsize {
        return Err(HttpError::for_client_error(
            Some("invalid".to_string()),
            StatusCode::BAD_REQUEST,
            format!("size {} != copied total {}", addsize, md.len()),
        ));
    }

    /*
     * Insert a record in the database for this output object and report
     * success.
     */
    c.db.job_add_output(&j.id, &add.path, &oid, addsize).or_500()?;

    Ok(HttpResponseCreated(()))
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
) -> SResult<HttpResponseCreated<WorkerBootstrapResult>, HttpError> {
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
