/*
 * Copyright 2022 Oxide Computer Company
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
pub(crate) struct JobStorePath {
    job: String,
    name: String,
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
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<JobsPath>,
    query: TypedQuery<JobsEventsQuery>,
) -> DSResult<HttpResponseOk<Vec<JobEvent>>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let p = path.into_inner();
    let q = query.into_inner();

    let owner = c.require_user(log, &rqctx.request).await?;

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
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<JobsPath>,
) -> DSResult<HttpResponseOk<Vec<JobOutput>>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let p = path.into_inner();

    let owner = c.require_user(log, &rqctx.request).await?;

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
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<JobsOutputsPath>,
) -> DSResult<Response<Body>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let p = path.into_inner();

    let owner = c.require_user(log, &rqctx.request).await?;

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

#[derive(Deserialize, JsonSchema)]
pub(crate) struct JobOutputPublish {
    series: String,
    version: String,
    name: String,
}

impl JobOutputPublish {
    fn safe(&self) -> DSResult<()> {
        let Self { series, version, name } = self;
        Self::one_safe(&series)?;
        Self::one_safe(&version)?;
        Self::one_safe(&name)?;
        Ok(())
    }

    fn one_safe(n: &str) -> DSResult<()> {
        if (2..=48).contains(&n.chars().count())
            && n.chars().all(|c| {
                c.is_ascii_digit()
                    || c.is_ascii_alphabetic()
                    || c == '-'
                    || c == '_'
                    || c == '.'
            })
        {
            Ok(())
        } else {
            Err(HttpError::for_client_error(
                None,
                StatusCode::BAD_REQUEST,
                "invalid published file ID".into(),
            ))
        }
    }
}

#[endpoint {
    method = POST,
    path = "/0/jobs/{job}/outputs/{output}/publish",
}]
pub(crate) async fn job_output_publish(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<JobsOutputsPath>,
    body: TypedBody<JobOutputPublish>,
) -> DSResult<HttpResponseUpdatedNoContent> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let p = path.into_inner();

    let b = body.into_inner();
    b.safe()?;

    let owner = c.require_user(log, &rqctx.request).await?;

    let t = c.db.job_by_str(&p.job).or_500()?;
    if t.owner != owner.id {
        return Err(HttpError::for_client_error(
            None,
            StatusCode::FORBIDDEN,
            "not your job".into(),
        ));
    }

    let o = c.db.job_output_by_str(&p.job, &p.output).or_500()?;

    info!(
        log,
        "user {} publishing job {} output {} as {}/{}/{}",
        owner.id,
        t.id,
        o.id,
        &b.series,
        &b.version,
        &b.name
    );

    c.db.job_publish_output(t.id, o.id, &b.series, &b.version, &b.name)
        .or_500()?;

    Ok(HttpResponseUpdatedNoContent())
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
    output_rules: Vec<db::JobOutputRule>,
    tags: HashMap<String, String>,
    target: &db::Target,
    times: HashMap<String, DateTime<Utc>>,
) -> Job {
    /*
     * Job output rules are presently specified as strings with some prefix
     * sigils based on behavioural directives.  We need to reconstruct the
     * string version of this based on the structured version in the database.
     */
    let output_rules = output_rules
        .iter()
        .map(|jor| {
            let mut out = String::with_capacity(jor.rule.capacity() + 3);
            if jor.ignore {
                out.push('!');
            }
            if jor.size_change_ok {
                out.push('%');
            }
            if jor.require_match {
                out.push('=');
            }
            out += &jor.rule;
            out
        })
        .collect::<Vec<_>>();

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
        cancelled: j.cancelled,
        times,
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
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<JobGetPath>,
) -> DSResult<HttpResponseOk<Job>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let owner = c.require_user(log, &rqctx.request).await?;

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
        c.db.job_times(job.id).or_500()?,
    )))
}

#[endpoint {
    method = GET,
    path = "/0/jobs",
}]
pub(crate) async fn jobs_get(
    rqctx: RequestContext<Arc<Central>>,
) -> DSResult<HttpResponseOk<Vec<Job>>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let owner = c.require_user(log, &rqctx.request).await?;

    let jobs =
        c.db.user_jobs(owner.id)
            .or_500()?
            .iter()
            .map(|j| {
                let output_rules = c.db.job_output_rules(j.id)?;
                let tasks = c.db.job_tasks(j.id)?;
                let tags = c.db.job_tags(j.id)?;
                let target = c.db.target_get(j.target())?;
                let times = c.db.job_times(j.id)?;
                Ok(format_job(j, &tasks, output_rules, tags, &target, times))
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
    cancelled: bool,
    #[serde(default)]
    times: HashMap<String, DateTime<Utc>>,
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
    #[serde(default)]
    depends: HashMap<String, DependSubmit>,
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

#[derive(Deserialize, JsonSchema)]
pub(crate) struct DependSubmit {
    prior_job: String,
    copy_outputs: bool,
    on_failed: bool,
    on_completed: bool,
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct JobSubmitResult {
    id: String,
}

fn parse_output_rule(input: &str) -> DSResult<db::CreateOutputRule> {
    enum State {
        Start,
        SlashOrEquals,
        SlashOrPercent,
        Slash,
        Rule,
    }
    let mut s = State::Start;

    let mut rule = String::new();
    let mut ignore = false;
    let mut size_change_ok = false;
    let mut require_match = false;

    for c in input.chars() {
        match s {
            State::Start => match c {
                '/' => {
                    rule.push(c);
                    s = State::Rule;
                }
                '!' => {
                    ignore = true;
                    s = State::Slash;
                }
                '=' => {
                    require_match = true;
                    s = State::SlashOrPercent;
                }
                '%' => {
                    size_change_ok = true;
                    s = State::SlashOrEquals;
                }
                other => {
                    return Err(HttpError::for_client_error(
                        None,
                        StatusCode::BAD_REQUEST,
                        format!("wanted sigil/absolute path, not {:?}", other),
                    ));
                }
            },
            State::SlashOrEquals => match c {
                '/' => {
                    rule.push(c);
                    s = State::Rule;
                }
                '=' => {
                    require_match = true;
                    s = State::Slash;
                }
                other => {
                    return Err(HttpError::for_client_error(
                        None,
                        StatusCode::BAD_REQUEST,
                        format!("{:?} unexpected in output rule", other),
                    ));
                }
            },
            State::SlashOrPercent => match c {
                '/' => {
                    rule.push(c);
                    s = State::Rule;
                }
                '%' => {
                    size_change_ok = true;
                    s = State::Slash;
                }
                other => {
                    return Err(HttpError::for_client_error(
                        None,
                        StatusCode::BAD_REQUEST,
                        format!("{:?} unexpected in output rule", other),
                    ));
                }
            },
            State::Slash => match c {
                '/' => {
                    rule.push(c);
                    s = State::Rule;
                }
                other => {
                    return Err(HttpError::for_client_error(
                        None,
                        StatusCode::BAD_REQUEST,
                        format!("wanted '/', not {:?}, in output rule", other),
                    ));
                }
            },
            State::Rule => rule.push(c),
        }
    }

    if !rule.starts_with("/") {
        return Err(HttpError::for_client_error(
            None,
            StatusCode::BAD_REQUEST,
            format!("output rule pattern must be absolute path"),
        ));
    }

    if ignore {
        assert!(!require_match && !size_change_ok);
    }

    Ok(db::CreateOutputRule { rule, ignore, require_match, size_change_ok })
}

#[endpoint {
    method = POST,
    path = "/0/jobs",
}]
pub(crate) async fn job_submit(
    rqctx: RequestContext<Arc<Central>>,
    new_job: TypedBody<JobSubmit>,
) -> DSResult<HttpResponseCreated<JobSubmitResult>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let owner = c.require_user(log, &rqctx.request).await?;
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

    let depends = new_job
        .depends
        .iter()
        .map(|(name, ds)| {
            Ok(db::CreateDepend {
                name: name.to_string(),
                prior_job: db::JobId::from_str(&ds.prior_job).or_500()?,
                copy_outputs: ds.copy_outputs,
                on_failed: ds.on_failed,
                on_completed: ds.on_completed,
            })
        })
        .collect::<DSResult<Vec<_>>>()?;

    let output_rules = new_job
        .output_rules
        .iter()
        .map(|rule| parse_output_rule(rule.as_str()))
        .collect::<DSResult<Vec<_>>>()?;

    let t =
        c.db.job_create(
            owner.id,
            &new_job.name,
            &new_job.target,
            target.id,
            tasks,
            output_rules,
            &new_job.inputs,
            new_job.tags,
            depends,
        )
        .or_500()?;

    Ok(HttpResponseCreated(JobSubmitResult { id: t.id.to_string() }))
}

#[endpoint {
    method = POST,
    path = "/0/jobs/{job}/chunk",
}]
pub(crate) async fn job_upload_chunk(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<JobsPath>,
    chunk: UntypedBody,
) -> DSResult<HttpResponseCreated<UploadedChunk>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let owner = c.require_user(log, &rqctx.request).await?;

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
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<JobsPath>,
    add: TypedBody<JobAddInput>,
) -> DSResult<HttpResponseUpdatedNoContent> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let owner = c.require_user(log, &rqctx.request).await?;

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

    /*
     * XXX For now, individual upload size is capped at 1GB.
     */
    let add = add.into_inner();
    let addsize = if add.size < 0 || add.size > 1024 * 1024 * 1024 {
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

#[endpoint {
    method = POST,
    path = "/0/jobs/{job}/cancel",
}]
pub(crate) async fn job_cancel(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<JobsPath>,
) -> DSResult<HttpResponseUpdatedNoContent> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let owner = c.require_user(log, &rqctx.request).await?;

    let p = path.into_inner();

    let job = c.db.job_by_str(&p.job).or_500()?;
    if job.owner != owner.id {
        return Err(HttpError::for_client_error(
            None,
            StatusCode::FORBIDDEN,
            "not your job".into(),
        ));
    }

    if job.complete {
        return Err(HttpError::for_client_error(
            None,
            StatusCode::CONFLICT,
            "cannot cancel a job that is already complete".into(),
        ));
    }

    c.db.job_cancel(job.id).or_500()?;
    info!(log, "user {} cancelled job {}", owner.id, job.id);

    Ok(HttpResponseUpdatedNoContent())
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct JobStoreValue {
    value: String,
    secret: bool,
}

#[endpoint {
    method = PUT,
    path = "/0/jobs/{job}/store/{name}",
}]
pub(crate) async fn job_store_put(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<JobStorePath>,
    body: TypedBody<JobStoreValue>,
) -> DSResult<HttpResponseUpdatedNoContent> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let owner = c.require_user(log, &rqctx.request).await?;

    let p = path.into_inner();
    let b = body.into_inner();

    let job = c.db.job_by_str(&p.job).or_500()?;
    if job.owner != owner.id {
        return Err(HttpError::for_client_error(
            None,
            StatusCode::FORBIDDEN,
            "not your job".into(),
        ));
    }

    if job.complete {
        return Err(HttpError::for_client_error(
            None,
            StatusCode::CONFLICT,
            "cannot update the store for a job that is already complete".into(),
        ));
    }

    c.db.job_store_put(job.id, &p.name, &b.value, b.secret, "user").or_500()?;
    info!(
        log,
        "user {} updated job {} store value {}", owner.id, job.id, p.name,
    );

    Ok(HttpResponseUpdatedNoContent())
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct JobStoreValueInfo {
    value: Option<String>,
    secret: bool,
    time_update: DateTime<Utc>,
    source: String,
}

#[endpoint {
    method = GET,
    path = "/0/jobs/{job}/store",
}]
pub(crate) async fn job_store_get_all(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<JobsPath>,
) -> DSResult<HttpResponseOk<HashMap<String, JobStoreValueInfo>>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let owner = c.require_user(log, &rqctx.request).await?;

    let p = path.into_inner();

    let job = c.db.job_by_str(&p.job).or_500()?;
    if job.owner != owner.id {
        return Err(HttpError::for_client_error(
            None,
            StatusCode::FORBIDDEN,
            "not your job".into(),
        ));
    }

    info!(log, "user {} fetch job {} store, all values", owner.id, job.id);

    Ok(HttpResponseOk(
        c.db.job_store(job.id)
            .or_500()?
            .into_iter()
            .map(|(k, v)| {
                (
                    k,
                    JobStoreValueInfo {
                        /*
                         * Do not pass secret values back to the user:
                         */
                        value: if v.secret { None } else { Some(v.value) },
                        secret: v.secret,
                        time_update: v.time_update.0,
                        source: v.source,
                    },
                )
            })
            .collect(),
    ))
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
    rqctx: RequestContext<Arc<Central>>,
) -> DSResult<HttpResponseOk<WhoamiResult>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let u = c.require_user(log, &rqctx.request).await?;

    Ok(HttpResponseOk(WhoamiResult { id: u.id.to_string(), name: u.user.name }))
}

#[cfg(test)]
mod test {
    use super::super::prelude::*;
    use super::parse_output_rule;

    #[test]
    fn test_parse_output_rule() -> Result<()> {
        let cases = vec![
            (
                "/var/log/*.log",
                db::CreateOutputRule {
                    rule: "/var/log/*.log".into(),
                    ignore: false,
                    size_change_ok: false,
                    require_match: false,
                },
            ),
            (
                "!/var/log/*.log",
                db::CreateOutputRule {
                    rule: "/var/log/*.log".into(),
                    ignore: true,
                    size_change_ok: false,
                    require_match: false,
                },
            ),
            (
                "=/var/log/*.log",
                db::CreateOutputRule {
                    rule: "/var/log/*.log".into(),
                    ignore: false,
                    size_change_ok: false,
                    require_match: true,
                },
            ),
            (
                "%/var/log/*.log",
                db::CreateOutputRule {
                    rule: "/var/log/*.log".into(),
                    ignore: false,
                    size_change_ok: true,
                    require_match: false,
                },
            ),
            (
                "=%/var/log/*.log",
                db::CreateOutputRule {
                    rule: "/var/log/*.log".into(),
                    ignore: false,
                    size_change_ok: true,
                    require_match: true,
                },
            ),
            (
                "%=/var/log/*.log",
                db::CreateOutputRule {
                    rule: "/var/log/*.log".into(),
                    ignore: false,
                    size_change_ok: true,
                    require_match: true,
                },
            ),
        ];

        for (rule, want) in cases {
            println!("case {:?} -> {:?}", rule, want);
            let got = parse_output_rule(rule)?;
            assert_eq!(got, want);
        }

        Ok(())
    }

    #[test]
    fn test_parse_output_rule_failures() -> Result<()> {
        let cases = vec![
            "",
            "target/some/file",
            "!var/log/*.log",
            "%var/log/*.log",
            "=var/log/*.log",
            "!!/var/log/*.log",
            "!=/var/log/*.log",
            "!%/var/log/*.log",
            "%!/var/log/*.log",
            "=!/var/log/*.log",
            "==/var/log/*.log",
            "%%/var/log/*.log",
            "=%=/var/log/*.log",
            "%=%/var/log/*.log",
            "=%!/var/log/*.log",
            "%=!/var/log/*.log",
        ];

        for should_fail in cases {
            println!();
            println!("should fail {:?}", should_fail);
            match parse_output_rule(should_fail) {
                Err(e) => println!("  yes, fail! {:?}", e.external_message),
                Ok(res) => panic!("  wanted failure, got {:?}", res),
            }
        }

        Ok(())
    }
}
