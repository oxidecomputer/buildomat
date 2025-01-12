/*
 * Copyright 2024 Oxide Computer Company
 */

use slog::o;

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

impl From<db::JobEvent> for JobEvent {
    fn from(jev: db::JobEvent) -> Self {
        JobEvent {
            seq: jev.seq as usize,
            task: jev.task,
            stream: jev.stream.to_string(),
            time: jev.time.into(),
            time_remote: jev.time_remote.map(|t| t.into()),
            payload: jev.payload.to_string(),
        }
    }
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct JobOutput {
    id: String,
    size: u64,
    path: String,
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
pub(crate) struct JobStorePath {
    job: String,
    name: String,
}

impl JobStorePath {
    fn job(&self) -> DSResult<db::JobId> {
        self.job.parse::<db::JobId>().or_500()
    }
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct JobsOutputsPath {
    job: String,
    output: String,
}

impl JobsOutputsPath {
    fn job(&self) -> DSResult<db::JobId> {
        self.job.parse::<db::JobId>().or_500()
    }

    fn output(&self) -> DSResult<db::JobFileId> {
        self.output.parse::<db::JobFileId>().or_500()
    }
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
    path: TypedPath<JobPath>,
    query: TypedQuery<JobsEventsQuery>,
) -> DSResult<HttpResponseOk<Vec<JobEvent>>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let p = path.into_inner();
    let q = query.into_inner();

    let owner = c.require_user(log, &rqctx.request).await?;
    let j = c.load_job_for_user(log, &owner, p.job()?).await?;

    let jevs = c
        .load_job_events(log, &j, q.minseq.unwrap_or(0), 1000)
        .await
        .or_500()?;

    Ok(HttpResponseOk(jevs.into_iter().map(JobEvent::from).collect()))
}

#[endpoint {
    method = GET,
    path = "/0/jobs/{job}/watch",
}]
pub(crate) async fn job_watch(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<JobPath>,
    query: TypedQuery<JobsEventsQuery>,
) -> DSResult<Response<Body>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let p = path.into_inner();

    let owner = c.require_user(log, &rqctx.request).await?;
    let j = c.load_job_for_user(log, &owner, p.job()?).await?;

    let mut sse = ServerSentEvents::default();
    let Some(mut rx) = c.db.job_subscribe(&j) else {
        /*
         * Tell the client that it is not currently possible to subscribe to
         * this job.  The client should go and check the state of the job with a
         * regular request, and then potentially try again later.
         */
        sse.build_event().event("check").data("-").send().await;

        return sse.to_response().or_500();
    };

    /*
     * Record the last sequence number and job state generation number we have
     * seen, starting with the first number we get from the watch:
     */
    let (mut seq, mut gen) = {
        let jn = rx.borrow();

        (jn.seq, jn.gen)
    };

    /*
     * The "minseq" query parameter be used to seek to a particular stream
     * starting point.  Browsers will provide an initial job event stream offset
     * this way, to resume the watch after the most recent event included in the
     * page rendered by the server.
     */
    let mut resuming = false;
    if let Some(minseq) =
        query.into_inner().minseq.and_then(|n| u32::try_from(n).ok())
    {
        /*
         * The "seq" value refers to the last record we have seen, but the
         * "minseq" parameter specifies the next record we _want_ to see.
         */
        seq = minseq.saturating_sub(1);
        if minseq > 1 {
            resuming = true;
        }
    }

    /*
     * The "Last-Event-ID" header will be sent by a browser when reconnecting,
     * with the "id" field of the last event it saw in the previous stream.  The
     * event stream for this endpoint is a mixture of job events, which have a
     * well-defined sequence number, and status change events, which do not.
     *
     * We include in each ID value the sequence number of the most recently sent
     * job event, so that we can always seek to the right point in the events
     * for the job.  This may lead to more than one event with the same sequence
     * number, but that doesn't appear to be a problem in practice.
     *
     * Note that this value must take precedence over the query parameter, as a
     * resumed stream from the browser will, each time it reconnects, include
     * the original query string we gave to the EventSource.  It will only
     * include the header on subsequent retries once it has seen at least one
     * event.
     */
    if let Some(lei) = rqctx.request.headers().last_event_id() {
        if let Some(num) = lei.strip_prefix("seq-") {
            if let Ok(lei_seq) = num.parse::<u32>() {
                if lei_seq < seq {
                    /*
                     * Resume the event stream from this earlier point.
                     */
                    seq = lei_seq;
                    resuming = true;
                }
            }
        }
    }

    info!(
        log, "{} job {} watch",
        if resuming { "resuming" } else { "starting" }, j.id;
        "seq" => seq
    );

    let log0 = log.new(o!("stream" => true));
    let c0 = Arc::clone(c);
    let res = sse.to_response().or_500()?;
    tokio::task::spawn(async move {
        let c = c0;
        let log = log0;
        let mut check_state = true;
        let mut previous_state = "".to_string();

        loop {
            if check_state {
                /*
                 * Render the job state for the client and send it on if it has
                 * changed.
                 */
                let new_state = format_job_state(&c.db.job(j.id).or_500()?);
                if new_state != previous_state {
                    sse.build_event()
                        .id(&format!("seq-{seq}"))
                        .event("state")
                        .data(&new_state)
                        .send()
                        .await;
                    previous_state = new_state;
                }
                check_state = false;
            }

            /*
             * Attempt to load a small quantity of records from the database.
             */
            let events = tokio::task::block_in_place(|| {
                c.db.job_events(j.id, (seq as usize) + 1, 100)
            })
            .or_500()?;

            if !events.is_empty() {
                /*
                 * Send the events to the client:
                 */
                for ev in events {
                    if ev.seq > seq {
                        seq = ev.seq;
                    }

                    if !sse
                        .build_event()
                        .id(&format!("seq-{seq}"))
                        .event("job")
                        .data(&serde_json::to_string(&JobEvent::from(ev))?)
                        .send()
                        .await
                    {
                        return Ok(());
                    }
                }

                /*
                 * After we deliver a chunk of events to the client, try to make
                 * sure other requests can get time to run.
                 */
                tokio::task::yield_now().await;
                continue;
            }

            /*
             * If there are no events, then either we need to sleep and wait for
             * more or we've reached the end of the job.
             */
            rx.changed().await?;
            if sse.is_closed() {
                return Ok::<(), anyhow::Error>(());
            }

            let complete = {
                let jn = rx.borrow();

                if gen != jn.gen {
                    /*
                     * There has been a job state change.  Go back and check the
                     * state again.
                     */
                    check_state = true;
                    gen = jn.gen;
                    continue;
                }

                if seq < jn.seq {
                    /*
                     * Go back to the database to get more records.
                     */
                    continue;
                }
                jn.complete
            };

            if complete {
                sse.build_event()
                    .id(&format!("seq-{seq}"))
                    .event("complete")
                    .data("-")
                    .send()
                    .await;

                info!(log, "end of job watch stream");
                return Ok(());
            }
        }
    });

    Ok(res)
}

#[endpoint {
    method = GET,
    path = "/0/jobs/{job}/outputs",
}]
pub(crate) async fn job_outputs_get(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<JobPath>,
) -> DSResult<HttpResponseOk<Vec<JobOutput>>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let p = path.into_inner();

    let owner = c.require_user(log, &rqctx.request).await?;
    let j = c.load_job_for_user(log, &owner, p.job()?).await?;

    let jops = c.load_job_outputs(log, &j).await.or_500()?;

    Ok(HttpResponseOk(
        jops.iter()
            .map(|jof| JobOutput {
                id: jof.output.id.to_string(),
                size: jof.file.size.0,
                path: jof.output.path.to_string(),
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
    let pr = rqctx.range();

    let owner = c.require_user(log, &rqctx.request).await?;
    let t = c.load_job_for_user(log, &owner, p.job()?).await?;

    let o = c.load_job_output(log, &t, p.output()?).await.or_500()?;

    let info = format!("job {} output {} path {:?}", t.id, o.id, o.path);
    c.file_response(log, info, t.id, o.id, pr, false).await
}

#[endpoint {
    method = HEAD,
    path = "/0/jobs/{job}/outputs/{output}",
    unpublished = true,
}]
pub(crate) async fn job_output_head(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<JobsOutputsPath>,
) -> DSResult<Response<Body>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let p = path.into_inner();
    let pr = rqctx.range();

    let owner = c.require_user(log, &rqctx.request).await?;
    let t = c.load_job_for_user(log, &owner, p.job()?).await?;

    let o = c.load_job_output(log, &t, p.output()?).await.or_500()?;

    let info = format!("job {} output {} path {:?}", t.id, o.id, o.path);
    c.file_response(log, info, t.id, o.id, pr, true).await
}

#[derive(Deserialize, Debug, JsonSchema)]
pub(crate) struct JobOutputSignedUrl {
    expiry_seconds: u64,
    content_type: Option<String>,
    content_disposition: Option<String>,
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct JobOutputSignedUrlResult {
    url: String,
}

#[endpoint {
    method = POST,
    path = "/0/jobs/{job}/outputs/{output}/sign",
}]
pub(crate) async fn job_output_signed_url(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<JobsOutputsPath>,
    body: TypedBody<JobOutputSignedUrl>,
) -> DSResult<HttpResponseOk<JobOutputSignedUrlResult>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let p = path.into_inner();
    let b = body.into_inner();

    if b.expiry_seconds > 3600 {
        return Err(HttpError::for_client_error(
            None,
            ClientErrorStatusCode::BAD_REQUEST,
            "URLs can last at most one hour (3600 seconds)".into(),
        ));
    }

    let owner = c.require_user(log, &rqctx.request).await?;
    let t = c.load_job_for_user(log, &owner, p.job()?).await?;

    let o = c.load_job_output(log, &t, p.output()?).await.or_500()?;
    let psu = c
        .file_presigned_url(
            t.id,
            o.id,
            b.expiry_seconds,
            b.content_type.as_deref(),
            b.content_disposition.as_deref(),
        )
        .await
        .or_500()?;

    info!(
        log,
        "job {} output {} path {:?} presigned URL is in the {}",
        t.id, o.id, o.path, psu.info; "params" => ?b,
    );

    Ok(HttpResponseOk(JobOutputSignedUrlResult { url: psu.url }))
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
        Self::one_safe(series)?;
        Self::one_safe(version)?;
        Self::one_safe(name)?;
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
                ClientErrorStatusCode::BAD_REQUEST,
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
    let t = c.load_job_for_user(log, &owner, p.job()?).await?;

    let o = c.load_job_output(log, &t, p.output()?).await.or_500()?;

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

#[endpoint {
    method = GET,
    path = "/0/job/{job}",
}]
pub(crate) async fn job_get(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<JobPath>,
) -> DSResult<HttpResponseOk<Job>> {
    let c = rqctx.context();
    let log = &rqctx.log;
    let p = path.into_inner();

    let owner = c.require_user(log, &rqctx.request).await?;
    let job = c.load_job_for_user(log, &owner, p.job()?).await?;

    Ok(HttpResponseOk(Job::load(log, c, &job).await.or_500()?))
}

#[endpoint {
    method = GET,
    path = "/0/jobs",
    unpublished = true,
}]
pub(crate) async fn jobs_get_old(
    rqctx: RequestContext<Arc<Central>>,
) -> DSResult<HttpResponseOk<Vec<Job>>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let owner = c.require_user(log, &rqctx.request).await?;

    /*
     * XXX Consumers should switch to the paginated version of this call, but
     * for now we will load a certain number of jobs into memory ourselves, but
     * in chunks to allow other callers somewhat concurrent access to the
     * database.
     */
    let mut out = Vec::with_capacity(100);
    let mut marker = None;
    while out.len() < 100_000 {
        let page =
            c.db.jobs_page(true, marker, 100, Some(owner.id), None, None, None)
                .or_500()?;
        if page.is_empty() {
            break;
        }

        /*
         * The result set is in ascending order of Job ID, so grab the last ID
         * so we can find the next page of results:
         */
        marker = Some(page.last().unwrap().id);

        for job in page {
            out.push(super::user::Job::load(log, c, &job).await.or_500()?);
        }

        /*
         * Let other people have a turn...
         */
        tokio::task::yield_now().await;
    }

    Ok(HttpResponseOk(out))
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct JobScan {
    #[serde(default)]
    recent_first: bool,
    tag: Option<String>,
}

impl From<JobSelect> for JobScan {
    fn from(sel: JobSelect) -> Self {
        JobScan { recent_first: sel.recent_first, tag: sel.tag }
    }
}

impl JobScan {
    /*
     * XXX Parse the tag filter specifier: NAME=VALUE|NAME=VALUE|...
     */
    fn tag(&self) -> DSResult<Option<Vec<(String, String)>>> {
        if let Some(tag) = self.tag.as_deref() {
            let mut out = Vec::new();

            for s in tag.split('|') {
                if let Some((k, v)) = s.split_once('=') {
                    out.push((k.to_string(), v.to_string()));
                } else {
                    return Err(HttpError::for_bad_request(
                        Some("EINVAL".into()),
                        "invalid tag filter".into(),
                    ));
                }
            }

            Ok(Some(out))
        } else {
            Ok(None)
        }
    }
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub(crate) struct JobSelect {
    id: String,
    #[serde(default)]
    recent_first: bool,
    tag: Option<String>,
}

impl JobSelect {
    fn id(&self) -> DSResult<db::JobId> {
        db::JobId::from_str(&self.id).or_500()
    }

    fn from_scan(scan: &JobScan, id: &str) -> Self {
        JobSelect {
            id: id.to_string(),
            recent_first: scan.recent_first,
            tag: scan.tag.clone(),
        }
    }
}

#[endpoint {
    method = GET,
    path = "/1/jobs",
}]
pub(crate) async fn jobs_list(
    rqctx: RequestContext<Arc<Central>>,
    pag: TypedQuery<PaginationParams<JobScan, JobSelect>>,
) -> DSResult<HttpResponseOk<ResultsPage<JobListEntry>>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let owner = c.require_user(log, &rqctx.request).await?;

    let pag = pag.into_inner();
    let (marker, scan) = match pag.page {
        WhichPage::First(scan) => (None, scan),
        WhichPage::Next(sel) => (Some(sel.id()?), sel.into()),
    };

    Ok(HttpResponseOk(ResultsPage::new(
        c.db.jobs_page(
            /*
             * If the user requests recent jobs first in the result set, we need
             * a descending sort by job ID.  (ULIDs sort properly by creation
             * time.)
             */
            !scan.recent_first,
            marker,
            1000,
            Some(owner.id),
            None,
            None,
            scan.tag()?,
        )
        .or_500()?
        .into_iter()
        .map(JobListEntry::from)
        .collect(),
        &scan,
        |a, scan| JobSelect::from_scan(scan, &a.id),
    )?))
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct JobListEntry {
    pub(crate) id: String,
    owner: String,
    name: String,
    state: String,
    cancelled: bool,
    /**
     * The original target name specified by the user when the job was created.
     */
    target: String,
    /**
     * The resolved ID of the concrete target on which this job ran.
     */
    target_id: String,
}

impl From<db::Job> for JobListEntry {
    fn from(job: db::Job) -> Self {
        JobListEntry {
            state: format_job_state(&job),
            target_id: job.target().to_string(),
            id: job.id.to_string(),
            name: job.name,
            owner: job.owner.to_string(),
            cancelled: job.cancelled,
            target: job.target,
        }
    }
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

impl Job {
    pub(crate) async fn load(
        log: &Logger,
        c: &Central,
        job: &db::Job,
    ) -> Result<Job> {
        let (tasks, output_rules, tags, target, times) = if job.is_archived() {
            let aj = c.archive_load(log, job.id).await?;

            (
                aj.tasks().or_500()?,
                aj.output_rules().or_500()?,
                aj.tags().or_500()?,
                c.db.target(job.target()).or_500()?,
                aj.times().or_500()?,
            )
        } else {
            (
                c.db.job_tasks(job.id).or_500()?,
                c.db.job_output_rules(job.id).or_500()?,
                c.db.job_tags(job.id).or_500()?,
                c.db.target(job.target()).or_500()?,
                c.db.job_times(job.id).or_500()?,
            )
        };

        Ok(format_job(job, &tasks, output_rules, tags, &target, times))
    }
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
                        ClientErrorStatusCode::BAD_REQUEST,
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
                        ClientErrorStatusCode::BAD_REQUEST,
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
                        ClientErrorStatusCode::BAD_REQUEST,
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
                        ClientErrorStatusCode::BAD_REQUEST,
                        format!("wanted '/', not {:?}, in output rule", other),
                    ));
                }
            },
            State::Rule => rule.push(c),
        }
    }

    if !rule.starts_with('/') {
        return Err(HttpError::for_client_error(
            None,
            ClientErrorStatusCode::BAD_REQUEST,
            "output rule pattern must be absolute path".to_string(),
        ));
    }

    if ignore {
        assert!(!require_match && !size_change_ok);
    }

    Ok(db::CreateOutputRule { rule, ignore, require_match, size_change_ok })
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct Quota {
    max_bytes_per_input: u64,
}

#[endpoint {
    method = GET,
    path = "/0/quota",
}]
pub(crate) async fn quota(
    rqctx: RequestContext<Arc<Central>>,
) -> DSResult<HttpResponseOk<Quota>> {
    let c = rqctx.context();

    /*
     * For now, this request just presents statically configured quota
     * information.  These limits are enforced in requests, but we expose them
     * here so that client tools can present better diagnostic information.
     */
    Ok(HttpResponseOk(Quota {
        max_bytes_per_input: c.config.job.max_bytes_per_input(),
    }))
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
            ClientErrorStatusCode::BAD_REQUEST,
            "too many tasks".into(),
        ));
    }

    if new_job.inputs.len() > 25 {
        return Err(HttpError::for_client_error(
            None,
            ClientErrorStatusCode::BAD_REQUEST,
            "too many inputs".into(),
        ));
    }

    if new_job.tags.len() > 100 {
        return Err(HttpError::for_client_error(
            None,
            ClientErrorStatusCode::BAD_REQUEST,
            "too many tags".into(),
        ));
    }

    if new_job.tags.iter().map(|(n, v)| n.len() + v.len()).sum::<usize>()
        > 131072
    {
        return Err(HttpError::for_client_error(
            None,
            ClientErrorStatusCode::BAD_REQUEST,
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
                ClientErrorStatusCode::BAD_REQUEST,
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
                ClientErrorStatusCode::BAD_REQUEST,
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
                ClientErrorStatusCode::FORBIDDEN,
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
    path: TypedPath<JobPath>,
    chunk: UntypedBody,
) -> DSResult<HttpResponseCreated<UploadedChunk>> {
    let c = rqctx.context();
    let log = &rqctx.log;
    let p = path.into_inner();

    let owner = c.require_user(log, &rqctx.request).await?;
    let job = c.load_job_for_user(log, &owner, p.job()?).await?;

    if !job.waiting {
        return Err(HttpError::for_client_error(
            None,
            ClientErrorStatusCode::CONFLICT,
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
    size: u64,
    chunks: Vec<String>,
    commit_id: String,
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct JobAddInputResult {
    complete: bool,
    error: Option<String>,
}

#[endpoint {
    method = POST,
    path = "/1/jobs/{job}/input",
}]
pub(crate) async fn job_add_input(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<JobPath>,
    add: TypedBody<JobAddInput>,
) -> DSResult<HttpResponseOk<JobAddInputResult>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let owner = c.require_user(log, &rqctx.request).await?;

    let p = path.into_inner();

    let add = add.into_inner();
    if add.name.contains('/') {
        return Err(HttpError::for_client_error(
            None,
            ClientErrorStatusCode::BAD_REQUEST,
            "name must not be a path".into(),
        ));
    }

    let max = c.config.job.max_bytes_per_input();
    if add.size > max {
        return Err(HttpError::for_client_error(
            None,
            ClientErrorStatusCode::BAD_REQUEST,
            format!(
                "input file size {} bigger than allowed maximum {max} bytes",
                add.size,
            ),
        ));
    }

    let chunks = add
        .chunks
        .iter()
        .map(|f| Ok(Ulid::from_str(f.as_str())?))
        .collect::<Result<Vec<_>>>()
        .or_500()?;
    let commit_id = Ulid::from_str(add.commit_id.as_str()).or_500()?;

    let job = c.load_job_for_user(log, &owner, p.job()?).await?;

    /*
     * The transition from waiting to queued occurs as soon as the last input is
     * committed.  Clients still need to be able to confirm that previously
     * uploaded inputs have finished committing after this transition occurs.
     *
     * Though this may perhaps seem like a race condition waiting to happen, it
     * is not: a final check is made within a database transaction prior to file
     * commit; this merely allows for a faster failure and better error message.
     */
    if !job.waiting && !c.files.commit_file_exists(job.id, commit_id) {
        return Err(HttpError::for_client_error(
            None,
            ClientErrorStatusCode::CONFLICT,
            "cannot add inputs to a job that is not waiting".into(),
        ));
    }

    let res = c.files.commit_file(
        job.id,
        commit_id,
        crate::files::FileKind::Input { name: add.name.to_string() },
        add.size,
        chunks,
    );

    match res {
        Ok(Some(Ok(()))) => Ok(HttpResponseOk(JobAddInputResult {
            complete: true,
            error: None,
        })),
        Ok(Some(Err(msg))) => Ok(HttpResponseOk(JobAddInputResult {
            complete: true,
            error: Some(msg.to_string()),
        })),
        Ok(None) => {
            /*
             * This job is either queued or active, but not yet complete.
             */
            Ok(HttpResponseOk(JobAddInputResult {
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
                "user {} job {} upload {} commit {} size {}: {:?}",
                owner.id,
                job.id,
                add.name,
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
pub(crate) struct JobAddInputSync {
    name: String,
    size: i64,
    chunks: Vec<String>,
}

#[endpoint {
    method = POST,
    path = "/0/jobs/{job}/input",
    unpublished = true,
}]
pub(crate) async fn job_add_input_sync(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<JobPath>,
    add: TypedBody<JobAddInputSync>,
) -> DSResult<HttpResponseUpdatedNoContent> {
    let c = rqctx.context();
    let log = &rqctx.log;
    let p = path.into_inner();

    let owner = c.require_user(log, &rqctx.request).await?;
    let job = c.load_job_for_user(log, &owner, p.job()?).await?;

    if !job.waiting {
        return Err(HttpError::for_client_error(
            None,
            ClientErrorStatusCode::CONFLICT,
            "cannot add inputs to a job that is not waiting".into(),
        ));
    }

    /*
     * Individual inputs using the old blocking entrypoint are capped at 1GB to
     * avoid request timeouts.  Larger inputs are possible using the new
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
    if add.name.contains('/') {
        return Err(HttpError::for_client_error(
            None,
            ClientErrorStatusCode::BAD_REQUEST,
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
                ClientErrorStatusCode::BAD_REQUEST,
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
    path: TypedPath<JobPath>,
) -> DSResult<HttpResponseUpdatedNoContent> {
    let c = rqctx.context();
    let log = &rqctx.log;
    let p = path.into_inner();

    let owner = c.require_user(log, &rqctx.request).await?;
    let job = c.load_job_for_user(log, &owner, p.job()?).await?;

    if job.complete {
        return Err(HttpError::for_client_error(
            None,
            ClientErrorStatusCode::CONFLICT,
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
    let p = path.into_inner();
    let b = body.into_inner();

    let owner = c.require_user(log, &rqctx.request).await?;
    let job = c.load_job_for_user(log, &owner, p.job()?).await?;

    if job.complete {
        return Err(HttpError::for_client_error(
            None,
            ClientErrorStatusCode::CONFLICT,
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
    path: TypedPath<JobPath>,
) -> DSResult<HttpResponseOk<HashMap<String, JobStoreValueInfo>>> {
    let c = rqctx.context();
    let log = &rqctx.log;
    let p = path.into_inner();

    let owner = c.require_user(log, &rqctx.request).await?;
    let job = c.load_job_for_user(log, &owner, p.job()?).await?;

    info!(log, "user {} fetch job {} store, all values", owner.id, job.id);

    let store = if job.is_archived() {
        let aj = c.archive_load(log, job.id).await.or_500()?;

        aj.store()
            .iter()
            .map(|(k, v)| {
                Ok((
                    k.to_string(),
                    JobStoreValueInfo {
                        /*
                         * Do not pass secret values back to the user:
                         */
                        value: if v.secret() {
                            None
                        } else {
                            v.value().map(str::to_string)
                        },
                        secret: v.secret(),
                        time_update: v.time_update()?.0,
                        source: v.source().to_string(),
                    },
                ))
            })
            .collect::<Result<_>>()
            .or_500()?
    } else {
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
            .collect()
    };

    Ok(HttpResponseOk(store))
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
