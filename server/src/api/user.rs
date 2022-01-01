/*
 * Copyright 2021 Oxide Computer Company
 */

use rusoto_s3::S3;

use super::prelude::*;

#[derive(Serialize, JsonSchema)]
pub(crate) struct JobEvent {
    seq: usize,
    task: Option<u32>,
    stream: String,
    time: DateTime<Utc>,
    time_remote: Option<DateTime<Utc>>,
    payload: String,
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct JobOutput {
    id: String,
    size: u64,
    path: String,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct JobsPath {
    job: String,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct JobsOutputsPath {
    job: String,
    output: String,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct JobsEventsQuery {
    minseq: Option<usize>,
}

#[endpoint {
    method = GET,
    path = "/0/jobs/{job}/events",
}]
pub(crate) async fn job_events_get(
    rqctx: Arc<RequestContext<Arc<Central>>>,
    path: TypedPath<JobsPath>,
    query: TypedQuery<JobsEventsQuery>,
) -> std::result::Result<HttpResponseOk<Vec<JobEvent>>, HttpError> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    let p = path.into_inner();
    let q = query.into_inner();

    let owner = c.require_user(log, &req).await?;

    let j = c.db.job_by_str(&p.job).or_500()?;
    if j.owner != owner.id {
        return Err(HttpError::for_client_error(
            None,
            StatusCode::FORBIDDEN,
            "not your job".into(),
        ));
    }

    let jevs = c.db.job_events(&j.id, q.minseq.unwrap_or(0)).or_500()?;

    Ok(HttpResponseOk(
        jevs.iter()
            .map(|jev| JobEvent {
                seq: jev.seq as usize,
                task: jev.task.map(|n| n as u32),
                stream: jev.stream.to_string(),
                time: jev.time.into(),
                time_remote: jev.time_remote.map(|t| t.into()),
                payload: jev.payload.to_string(),
            })
            .collect(),
    ))
}

#[endpoint {
    method = GET,
    path = "/0/jobs/{job}/outputs",
}]
pub(crate) async fn job_outputs_get(
    rqctx: Arc<RequestContext<Arc<Central>>>,
    path: TypedPath<JobsPath>,
) -> std::result::Result<HttpResponseOk<Vec<JobOutput>>, HttpError> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    let p = path.into_inner();

    let owner = c.require_user(log, &req).await?;

    let j = c.db.job_by_str(&p.job).or_500()?;
    if j.owner != owner.id {
        return Err(HttpError::for_client_error(
            None,
            StatusCode::FORBIDDEN,
            "not your job".into(),
        ));
    }

    let jops = c.db.job_outputs(&j.id).or_500()?;

    Ok(HttpResponseOk(
        jops.iter()
            .map(|jop| JobOutput {
                id: jop.id.to_string(),
                size: jop.size.0,
                path: jop.path.to_string(),
            })
            .collect(),
    ))
}

#[endpoint {
    method = GET,
    path = "/0/jobs/{job}/outputs/{output}",
}]
pub(crate) async fn job_output_download(
    rqctx: Arc<RequestContext<Arc<Central>>>,
    path: TypedPath<JobsOutputsPath>,
) -> std::result::Result<Response<Body>, HttpError> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    let p = path.into_inner();

    let owner = c.require_user(log, &req).await?;

    let t = c.db.job_by_str(&p.job).or_500()?;
    if t.owner != owner.id {
        return Err(HttpError::for_client_error(
            None,
            StatusCode::FORBIDDEN,
            "not your job".into(),
        ));
    }

    let o = c.db.job_output_by_str(&p.job, &p.output).or_500()?;

    let mut res = Response::builder();
    res = res.header(CONTENT_TYPE, "application/octet-stream");

    let op = c.output_path(&t.id, &o.id).or_500()?;

    Ok(if op.is_file() {
        /*
         * The file exists locally.
         */
        info!(
            log,
            "job {} output {} path {:?} is in the local file system",
            t.id,
            o.id,
            o.path
        );
        let f = tokio::fs::File::open(op).await.or_500()?;
        let md = f.metadata().await.or_500()?;
        assert!(md.is_file());
        let fbs = FileBytesStream::new(f);

        res = res.header(CONTENT_LENGTH, md.len());
        res.body(fbs.into_body())?
    } else {
        /*
         * Otherwise, try to get it from the object store.
         *
         * XXX We could conceivably 302 redirect people to the actual object
         * store with a presigned request?
         */
        let key = c.output_object_key(&t.id, &o.id);
        info!(
            log,
            "job {} output {} path {:?} is in the object store at {}",
            t.id,
            o.id,
            o.path,
            key
        );
        let obj =
            c.s3.get_object(rusoto_s3::GetObjectRequest {
                bucket: c.config.storage.bucket.to_string(),
                key,
                ..Default::default()
            })
            .await
            .or_500()?;

        if let Some(body) = obj.body {
            res = res.header(
                CONTENT_LENGTH,
                obj.content_length
                    .ok_or_else(|| {
                        anyhow!("no content length from object store?")
                    })
                    .or_500()?,
            );
            res.body(Body::wrap_stream(body))?
        } else {
            return Err(HttpError::for_internal_error(format!(
                "no body on object request for {}",
                o.id
            )));
        }
    })
}

fn format_task(t: &db::Task) -> Task {
    let state = if t.failed {
        "failed"
    } else if t.complete {
        "completed"
    } else {
        "pending"
    }
    .to_string();

    Task {
        name: t.name.to_string(),
        script: t.script.to_string(),
        env_clear: t.env_clear,
        env: t.env.clone().into(),
        uid: t.user_id.map(|x| x.0),
        gid: t.group_id.map(|x| x.0),
        workdir: t.workdir.clone(),
        state,
    }
}

fn format_job(j: &db::Job, t: &[db::Task], output_rules: Vec<String>) -> Job {
    let state = if j.failed {
        "failed"
    } else if j.complete {
        "completed"
    } else if j.worker.is_some() {
        "running"
    } else {
        "queued"
    }
    .to_string();

    let tasks = t.iter().map(format_task).collect::<Vec<_>>();

    Job {
        id: j.id.to_string(),
        name: j.name.to_string(),
        target: j.target.to_string(),
        tasks,
        output_rules,
        state,
    }
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct JobGetPath {
    job: String,
}

#[endpoint {
    method = GET,
    path = "/0/job/{job}",
}]
pub(crate) async fn job_get(
    rqctx: Arc<RequestContext<Arc<Central>>>,
    path: TypedPath<JobGetPath>,
) -> std::result::Result<HttpResponseOk<Job>, HttpError> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    let owner = c.require_user(log, &req).await?;

    let p = path.into_inner();

    let job = c.db.job_by_str(&p.job).or_500()?;
    if job.owner != owner.id {
        return Err(HttpError::for_client_error(
            None,
            StatusCode::FORBIDDEN,
            "not your job".into(),
        ));
    }

    let tasks = c.db.job_tasks(&job.id).or_500()?;

    Ok(HttpResponseOk(format_job(
        &job,
        &tasks,
        c.db.job_output_rules(&job.id).or_500()?,
    )))
}

#[endpoint {
    method = GET,
    path = "/0/jobs",
}]
pub(crate) async fn jobs_get(
    rqctx: Arc<RequestContext<Arc<Central>>>,
) -> std::result::Result<HttpResponseOk<Vec<Job>>, HttpError> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    let owner = c.require_user(log, &req).await?;

    let jobs =
        c.db.user_jobs(&owner.id)
            .or_500()?
            .iter()
            .map(|j| {
                let output_rules = c.db.job_output_rules(&j.id)?;
                let tasks = c.db.job_tasks(&j.id)?;
                Ok(format_job(j, &tasks, output_rules))
            })
            .collect::<Result<Vec<_>>>()
            .or_500()?;

    Ok(HttpResponseOk(jobs))
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct Job {
    id: String,
    name: String,
    target: String,
    output_rules: Vec<String>,
    tasks: Vec<Task>,
    state: String,
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct Task {
    name: String,
    script: String,
    env_clear: bool,
    env: HashMap<String, String>,
    uid: Option<u32>,
    gid: Option<u32>,
    workdir: Option<String>,
    state: String,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct JobSubmit {
    name: String,
    target: String,
    output_rules: Vec<String>,
    tasks: Vec<TaskSubmit>,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct TaskSubmit {
    name: String,
    script: String,
    env_clear: bool,
    env: HashMap<String, String>,
    uid: Option<u32>,
    gid: Option<u32>,
    workdir: Option<String>,
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct JobSubmitResult {
    id: String,
}

#[endpoint {
    method = POST,
    path = "/0/jobs",
}]
pub(crate) async fn job_submit(
    rqctx: Arc<RequestContext<Arc<Central>>>,
    new_job: TypedBody<JobSubmit>,
) -> std::result::Result<HttpResponseCreated<JobSubmitResult>, HttpError> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    let owner = c.require_user(log, &req).await?;
    let new_job = new_job.into_inner();

    let tasks = new_job
        .tasks
        .iter()
        .map(|ts| db::CreateTask {
            name: ts.name.to_string(),
            script: ts.script.to_string(),
            env_clear: ts.env_clear,
            env: ts.env.clone(),
            user_id: ts.uid,
            group_id: ts.gid,
            workdir: ts.workdir.clone(),
        })
        .collect::<Vec<_>>();

    let t =
        c.db.job_create(
            &owner.id,
            &new_job.name,
            &new_job.target,
            tasks,
            &new_job.output_rules,
        )
        .or_500()?;

    Ok(HttpResponseCreated(JobSubmitResult { id: t.id.to_string() }))
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct WhoamiResult {
    id: String,
    name: String,
}

#[endpoint {
    method = GET,
    path = "/0/whoami",
}]
pub(crate) async fn whoami(
    rqctx: Arc<RequestContext<Arc<Central>>>,
) -> SResult<HttpResponseOk<WhoamiResult>, HttpError> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    let u = c.require_user(log, &req).await?;

    Ok(HttpResponseOk(WhoamiResult { id: u.id.to_string(), name: u.name }))
}
