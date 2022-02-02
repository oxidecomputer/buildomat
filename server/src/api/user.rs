/*
 * Copyright 2021 Oxide Computer Company
 */

use super::prelude::*;

use super::worker::UploadedChunk;

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

    let jevs = c.db.job_events(j.id, q.minseq.unwrap_or(0)).or_500()?;

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

    let jops = c.db.job_outputs(j.id).or_500()?;

    Ok(HttpResponseOk(
        jops.iter()
            .map(|(jop, jf)| JobOutput {
                id: jop.id.to_string(),
                size: jf.size.0,
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

    let fr = c.file_response(t.id, o.id).await.or_500()?;
    info!(
        log,
        "job {} output {} path {:?} is in the {}", t.id, o.id, o.path, fr.info
    );

    res = res.header(CONTENT_LENGTH, fr.size);
    Ok(res.body(fr.body)?)
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

pub(crate) fn format_job_state(j: &db::Job) -> String {
    if j.failed {
        "failed"
    } else if j.complete {
        "completed"
    } else if j.worker.is_some() {
        "running"
    } else if j.waiting {
        "waiting"
    } else {
        "queued"
    }
    .to_string()
}

pub(crate) fn format_job(
    j: &db::Job,
    t: &[db::Task],
    output_rules: Vec<String>,
    tags: HashMap<String, String>,
    target: &db::Target,
) -> Job {
    Job {
        id: j.id.to_string(),
        name: j.name.to_string(),
        target: j.target.to_string(),
        target_real: target.name.to_string(),
        owner: j.owner.to_string(),
        tasks: t.iter().map(format_task).collect::<Vec<_>>(),
        output_rules,
        state: format_job_state(j),
        tags,
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

    let tasks = c.db.job_tasks(job.id).or_500()?;

    Ok(HttpResponseOk(format_job(
        &job,
        &tasks,
        c.db.job_output_rules(job.id).or_500()?,
        c.db.job_tags(job.id).or_500()?,
        &c.db.target_get(job.target()).or_500()?,
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
        c.db.user_jobs(owner.id)
            .or_500()?
            .iter()
            .map(|j| {
                let output_rules = c.db.job_output_rules(j.id)?;
                let tasks = c.db.job_tasks(j.id)?;
                let tags = c.db.job_tags(j.id)?;
                let target = c.db.target_get(j.target())?;
                Ok(format_job(j, &tasks, output_rules, tags, &target))
            })
            .collect::<Result<Vec<_>>>()
            .or_500()?;

    Ok(HttpResponseOk(jobs))
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct Job {
    id: String,
    owner: String,
    name: String,
    target: String,
    target_real: String,
    output_rules: Vec<String>,
    tasks: Vec<Task>,
    state: String,
    tags: HashMap<String, String>,
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
    #[serde(default)]
    inputs: Vec<String>,
    #[serde(default)]
    tags: HashMap<String, String>,
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

    if new_job.tasks.len() > 100 {
        return Err(HttpError::for_client_error(
            None,
            StatusCode::BAD_REQUEST,
            "too many tasks".into(),
        ));
    }

    if new_job.tags.len() > 100 {
        return Err(HttpError::for_client_error(
            None,
            StatusCode::BAD_REQUEST,
            "too many tags".into(),
        ));
    }

    if new_job.tags.iter().map(|(n, v)| n.len() + v.len()).sum::<usize>()
        > 131072
    {
        return Err(HttpError::for_client_error(
            None,
            StatusCode::BAD_REQUEST,
            "total size of all tags is larger than 128KB".into(),
        ));
    }

    for n in new_job.tags.keys() {
        /*
         * Tag names must not be a zero-length string, and all characters must
         * be ASCII: numbers, lowercase letters, periods, hypens, or
         * underscores:
         */
        if n.is_empty()
            || !n.chars().all(|c| {
                c.is_ascii_digit()
                    || c.is_ascii_lowercase()
                    || c == '.'
                    || c == '_'
                    || c == '-'
            })
        {
            return Err(HttpError::for_client_error(
                None,
                StatusCode::BAD_REQUEST,
                "tag names must be [0-9a-z._-]+".into(),
            ));
        }
    }

    /*
     * Resolve the target name to a specific target.  We store both so that it
     * is subsequently clear what we were asked, and what we actually delivered.
     */
    let target = match c.db.target_resolve(&new_job.target).or_500()? {
        Some(target) => target,
        None => {
            info!(log, "could not resolve target name {:?}", new_job.target);
            return Err(HttpError::for_client_error(
                None,
                StatusCode::BAD_REQUEST,
                format!("could not resolve target name {:?}", new_job.target),
            ));
        }
    };
    info!(log, "resolved target name {:?} to {:?}", new_job.target, target,);

    /*
     * Confirm that the authenticated user is allowed to create jobs using the
     * resolved target.
     */
    if let Some(required) = target.privilege.as_deref() {
        if !owner.has_privilege(required) {
            warn!(
                log,
                "user {} denied the use of target {:?} ({:?})",
                owner.id,
                target.name,
                new_job.target,
            );
            return Err(HttpError::for_client_error(
                None,
                StatusCode::FORBIDDEN,
                "you are not allowed to use that target".into(),
            ));
        }
    }

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
            owner.id,
            &new_job.name,
            &new_job.target,
            target.id,
            tasks,
            &new_job.output_rules,
            &new_job.inputs,
            new_job.tags,
        )
        .or_500()?;

    Ok(HttpResponseCreated(JobSubmitResult { id: t.id.to_string() }))
}

#[endpoint {
    method = POST,
    path = "/0/jobs/{job}/chunk",
}]
pub(crate) async fn job_upload_chunk(
    rqctx: Arc<RequestContext<Arc<Central>>>,
    path: TypedPath<JobsPath>,
    chunk: UntypedBody,
) -> SResult<HttpResponseCreated<UploadedChunk>, HttpError> {
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

    if !job.waiting {
        return Err(HttpError::for_client_error(
            None,
            StatusCode::CONFLICT,
            "cannot upload chunks for job that is not waiting".into(),
        ));
    }

    let cid = c.write_chunk(job.id, chunk.as_bytes()).or_500()?;
    info!(
        log,
        "user {} wrote chunk {} for job {}, size {}",
        owner.id,
        cid,
        job.id,
        chunk.as_bytes().len(),
    );

    Ok(HttpResponseCreated(UploadedChunk { id: cid.to_string() }))
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct JobAddInput {
    name: String,
    size: i64,
    chunks: Vec<String>,
}

#[endpoint {
    method = POST,
    path = "/0/jobs/{job}/input",
}]
pub(crate) async fn job_add_input(
    rqctx: Arc<RequestContext<Arc<Central>>>,
    path: TypedPath<JobsPath>,
    add: TypedBody<JobAddInput>,
) -> DSResult<HttpResponseUpdatedNoContent> {
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

    if !job.waiting {
        return Err(HttpError::for_client_error(
            None,
            StatusCode::CONFLICT,
            "cannot add inputs to a job that is not waiting".into(),
        ));
    }

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
    if add.name.contains('/') {
        return Err(HttpError::for_client_error(
            None,
            StatusCode::BAD_REQUEST,
            "name must not be a path".into(),
        ));
    }

    let chunks = add
        .chunks
        .iter()
        .map(|f| Ok(Ulid::from_str(f.as_str())?))
        .collect::<Result<Vec<_>>>()
        .or_500()?;

    let fid = match c.commit_file(job.id, &chunks, addsize) {
        Ok(fid) => fid,
        Err(e) => {
            warn!(
                log,
                "user {} job {} upload {} size {}: {:?}",
                owner.id,
                job.id,
                add.name,
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
     * Insert a record in the database for this input object and report success.
     */
    c.db.job_add_input(job.id, &add.name, fid, addsize).or_500()?;

    Ok(HttpResponseUpdatedNoContent())
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

    Ok(HttpResponseOk(WhoamiResult { id: u.id.to_string(), name: u.user.name }))
}
