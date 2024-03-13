/*
 * Copyright 2024 Oxide Computer Company
 */

use super::prelude::*;

#[derive(Serialize, JsonSchema)]
pub struct User {
    id: String,
    name: String,
    time_create: DateTime<Utc>,
    privileges: Vec<String>,
}

#[derive(Serialize, JsonSchema)]
pub struct Factory {
    id: String,
    name: String,
    last_ping: Option<DateTime<Utc>>,
    enable: bool,
    hold_workers: bool,
}

#[derive(Serialize, JsonSchema)]
pub struct Target {
    id: String,
    name: String,
    desc: String,
    redirect: Option<String>,
    privilege: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
pub struct FactoryPath {
    factory: String,
}

impl FactoryPath {
    fn factory(&self) -> DSResult<db::FactoryId> {
        db::FactoryId::from_str(&self.factory).or_500()
    }
}

#[derive(Deserialize, JsonSchema)]
pub struct JobPath {
    job: String,
}

#[derive(Deserialize, JsonSchema)]
pub struct UserPath {
    user: String,
}

impl UserPath {
    fn user(&self) -> DSResult<db::UserId> {
        db::UserId::from_str(&self.user).or_500()
    }
}

#[derive(Deserialize, JsonSchema)]
pub struct WorkerPath {
    worker: String,
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct WorkerEvent {
    seq: usize,
    stream: String,
    time: DateTime<Utc>,
    time_remote: Option<DateTime<Utc>>,
    payload: String,
}

impl WorkerPath {
    fn worker(&self) -> DSResult<db::WorkerId> {
        db::WorkerId::from_str(&self.worker).or_500()
    }
}

#[derive(Deserialize, JsonSchema)]
pub struct TargetPath {
    target: String,
}

impl TargetPath {
    fn target(&self) -> DSResult<db::TargetId> {
        db::TargetId::from_str(&self.target).or_500()
    }
}

#[derive(Deserialize, JsonSchema)]
pub struct TargetPrivilegePath {
    target: String,
    privilege: String,
}

impl TargetPrivilegePath {
    fn target(&self) -> DSResult<db::TargetId> {
        db::TargetId::from_str(&self.target).or_500()
    }
}

#[derive(Deserialize, JsonSchema)]
pub struct UserPrivilegePath {
    user: String,
    privilege: String,
}

impl UserPrivilegePath {
    fn user(&self) -> DSResult<db::UserId> {
        db::UserId::from_str(&self.user).or_500()
    }
}

#[derive(Deserialize, JsonSchema)]
pub struct UserCreate {
    name: String,
}

#[derive(Serialize, JsonSchema)]
pub struct UserCreateResult {
    id: String,
    name: String,
    token: String,
}

#[endpoint {
    method = POST,
    path = "/0/users",
}]
pub(crate) async fn user_create(
    rqctx: RequestContext<Arc<Central>>,
    new_user: TypedBody<UserCreate>,
) -> DSResult<HttpResponseCreated<UserCreateResult>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    c.require_admin(log, &rqctx.request, "user.create").await?;

    let new_user = new_user.into_inner();
    let u = c.db.user_create(&new_user.name).or_500()?;

    Ok(HttpResponseCreated(UserCreateResult {
        id: u.id.to_string(),
        name: u.name.to_string(),
        token: u.token,
    }))
}

#[derive(Deserialize, JsonSchema)]
pub struct UsersListQuery {
    #[serde(default)]
    name: Option<String>,
}

#[endpoint {
    method = GET,
    path = "/0/users",
}]
pub(crate) async fn users_list(
    rqctx: RequestContext<Arc<Central>>,
    query: TypedQuery<UsersListQuery>,
) -> DSResult<HttpResponseOk<Vec<User>>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    c.require_admin(log, &rqctx.request, "user.read").await?;

    let q = query.into_inner();

    let out =
        c.db.users()
            .or_500()?
            .into_iter()
            .filter_map(|u| {
                if let Some(name) = q.name.as_deref() {
                    if u.name != name {
                        return None;
                    }
                }

                Some(User {
                    id: u.user.id.to_string(),
                    name: u.user.name,
                    time_create: u.user.time_create.into(),
                    privileges: u.privileges,
                })
            })
            .collect::<Vec<_>>();

    Ok(HttpResponseOk(out))
}

#[endpoint {
    method = GET,
    path = "/0/users/{user}",
}]
pub(crate) async fn user_get(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<UserPath>,
) -> DSResult<HttpResponseOk<User>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    c.require_admin(log, &rqctx.request, "user.read").await?;

    if let Some(u) = c.db.user(path.into_inner().user()?).or_500()? {
        Ok(HttpResponseOk(User {
            id: u.user.id.to_string(),
            name: u.user.name,
            time_create: u.user.time_create.into(),
            privileges: u.privileges,
        }))
    } else {
        Err(HttpError::for_not_found(None, "user not found".into()))
    }
}

#[endpoint {
    method = PUT,
    path = "/0/users/{user}/privilege/{privilege}"
}]
pub(crate) async fn user_privilege_grant(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<UserPrivilegePath>,
) -> DSResult<HttpResponseUpdatedNoContent> {
    let c = rqctx.context();
    let log = &rqctx.log;

    c.require_admin(log, &rqctx.request, "privilege.grant").await?;

    let path = path.into_inner();
    let u = path.user()?;

    c.db.user_privilege_grant(u, &path.privilege).or_500()?;

    info!(log, "user {:?} privilege {:?} added", u, path.privilege);

    Ok(HttpResponseUpdatedNoContent())
}

#[endpoint {
    method = DELETE,
    path = "/0/users/{user}/privilege/{privilege}"
}]
pub(crate) async fn user_privilege_revoke(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<UserPrivilegePath>,
) -> DSResult<HttpResponseDeleted> {
    let c = rqctx.context();
    let log = &rqctx.log;

    c.require_admin(log, &rqctx.request, "privilege.revoke").await?;

    let path = path.into_inner();
    let u = path.user()?;

    c.db.user_privilege_revoke(u, &path.privilege).or_500()?;

    info!(log, "user {:?} privilege {:?} removed", u, path.privilege);

    Ok(HttpResponseDeleted())
}

#[derive(Deserialize, JsonSchema)]
pub struct AdminJobsGetQuery {
    #[serde(default)]
    active: bool,
    #[serde(default)]
    completed: Option<u64>,
}

#[endpoint {
    method = GET,
    path = "/0/admin/jobs",
}]
pub(crate) async fn admin_jobs_get(
    rqctx: RequestContext<Arc<Central>>,
    query: TypedQuery<AdminJobsGetQuery>,
) -> DSResult<HttpResponseOk<Vec<super::user::Job>>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    c.require_admin(log, &rqctx.request, "job.read").await?;

    let q = query.into_inner();
    let jobs = if q.active {
        /*
         * We have been asked to list only active jobs:
         */
        let mut jobs = c.db.jobs_active(10_000).or_500()?;
        jobs.extend(c.db.jobs_waiting(10_000).or_500()?);
        jobs
    } else if let Some(n) = &q.completed {
        /*
         * We have been asked to provide some number of recently completed jobs:
         */
        c.db.jobs_completed((*n).try_into().unwrap()).or_500()?
    } else {
        /*
         * We refuse to list jobs without a filter as there are potentially
         * hundreds of thousands of jobs in the database.
         */
        return Err(HttpError::for_bad_request(
            Some("EINVAL".into()),
            "cannot list jobs without a filter".into(),
        ));
    };

    let mut out = Vec::new();
    for job in jobs {
        out.push(super::user::Job::load(log, c, &job).await.or_500()?);
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
    path = "/1/admin/jobs",
}]
pub(crate) async fn admin_jobs_list(
    rqctx: RequestContext<Arc<Central>>,
    pag: TypedQuery<PaginationParams<JobScan, JobSelect>>,
) -> DSResult<HttpResponseOk<ResultsPage<super::user::JobListEntry>>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    c.require_admin(log, &rqctx.request, "job.read").await?;

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
            None,
            None,
            None,
            scan.tag()?,
        )
        .or_500()?
        .into_iter()
        .map(super::user::JobListEntry::from)
        .collect(),
        &scan,
        |a, scan| JobSelect::from_scan(scan, &a.id),
    )?))
}

#[endpoint {
    method = GET,
    path = "/0/admin/jobs/{job}",
}]
pub(crate) async fn admin_job_get(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<JobPath>,
) -> DSResult<HttpResponseOk<super::user::Job>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    c.require_admin(log, &rqctx.request, "job.read").await?;

    let id = path.into_inner().job.parse::<db::JobId>().or_500()?;
    let job = c.db.job(id).or_500()?;

    Ok(HttpResponseOk(super::user::Job::load(log, c, &job).await.or_500()?))
}

#[endpoint {
    method = POST,
    path = "/0/admin/jobs/{job}/archive",
}]
pub(crate) async fn admin_job_archive_request(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<JobPath>,
) -> DSResult<HttpResponseUpdatedNoContent> {
    let c = rqctx.context();
    let log = &rqctx.log;

    c.require_admin(log, &rqctx.request, "job.archive").await?;

    let id = path.into_inner().job.parse::<db::JobId>().or_500()?;
    let job = c.db.job(id).or_500()?;

    if !job.complete {
        return Err(HttpError::for_bad_request(
            None,
            "job cannot be archived until complete".into(),
        ));
    }

    info!(log, "admin: requested archive of job {}", job.id);
    c.inner.lock().unwrap().archive_queue.push_back(job.id);

    Ok(HttpResponseUpdatedNoContent())
}

#[endpoint {
    method = POST,
    path = "/0/control/hold",
}]
pub(crate) async fn control_hold(
    rqctx: RequestContext<Arc<Central>>,
) -> DSResult<HttpResponseUpdatedNoContent> {
    let c = rqctx.context();
    let log = &rqctx.log;

    c.require_admin(log, &rqctx.request, "control").await?;

    info!(log, "ADMIN: HOLD NEW VM CREATION");
    c.inner.lock().unwrap().hold = true;

    Ok(HttpResponseUpdatedNoContent())
}

#[endpoint {
    method = POST,
    path = "/0/control/resume",
}]
pub(crate) async fn control_resume(
    rqctx: RequestContext<Arc<Central>>,
) -> DSResult<HttpResponseUpdatedNoContent> {
    let c = rqctx.context();
    let log = &rqctx.log;

    c.require_admin(log, &rqctx.request, "control").await?;

    info!(log, "ADMIN: RESUME NEW VM CREATION");
    c.inner.lock().unwrap().hold = false;

    Ok(HttpResponseUpdatedNoContent())
}

#[derive(Serialize, JsonSchema)]
struct WorkerJob {
    pub id: String,
    pub name: String,
    pub owner: String,
    pub state: String,
    pub tags: HashMap<String, String>,
}

#[derive(Serialize, JsonSchema)]
struct WorkerHold {
    time: DateTime<Utc>,
    reason: String,
}

impl From<db::Hold> for WorkerHold {
    fn from(db::Hold { time, reason }: db::Hold) -> Self {
        WorkerHold { time, reason }
    }
}

#[derive(Serialize, JsonSchema)]
struct Worker {
    pub id: String,
    pub factory: String,
    pub factory_private: Option<String>,
    pub factory_ip: Option<String>,
    pub target: String,
    pub bootstrap: bool,
    pub deleted: bool,
    pub recycle: bool,
    pub lastping: Option<DateTime<Utc>>,
    pub jobs: Vec<WorkerJob>,
    pub hold: Option<WorkerHold>,
    pub diagnostics: bool,
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct WorkersResult {
    workers: Vec<Worker>,
}

#[derive(Deserialize, JsonSchema)]
pub struct WorkersListQuery {
    #[serde(default)]
    active: bool,
}

#[endpoint {
    method = GET,
    path = "/0/workers",
}]
pub(crate) async fn workers_list_old(
    rqctx: RequestContext<Arc<Central>>,
    query: TypedQuery<WorkersListQuery>,
) -> DSResult<HttpResponseOk<WorkersResult>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    c.require_admin(log, &rqctx.request, "worker.read").await?;

    let w = if query.into_inner().active {
        /*
         * List only active (i.e., not deleted) workers:
         */
        c.db.workers_active().or_500()?
    } else {
        /*
         * List all workers in the database.
         */
        c.db.workers().or_500()?
    };

    let workers = w
        .iter()
        .map(|w| {
            let jobs =
                c.db.worker_jobs(w.id)
                    .unwrap_or_else(|_| vec![])
                    .iter()
                    .map(|j| WorkerJob {
                        id: j.id.to_string(),
                        name: j.name.to_string(),
                        owner: j.owner.to_string(),
                        state: super::user::format_job_state(j),
                        tags: c.db.job_tags(j.id).unwrap_or_default(),
                    })
                    .collect::<Vec<_>>();
            Worker {
                id: w.id.to_string(),
                factory: w.factory().to_string(),
                factory_private: w.factory_private.clone(),
                factory_ip: w.factory_ip.clone(),
                target: w.target().to_string(),
                bootstrap: w.token.is_some(),
                deleted: w.deleted,
                recycle: w.recycle,
                hold: w.hold().map(|h| h.into()),
                lastping: w.lastping.map(|x| x.into()),
                diagnostics: w.diagnostics,
                jobs,
            }
        })
        .collect::<Vec<_>>();

    Ok(HttpResponseOk(WorkersResult { workers }))
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct WorkerScan {
    #[serde(default)]
    active: bool,
    factory: Option<String>,
}

impl From<WorkerSelect> for WorkerScan {
    fn from(sel: WorkerSelect) -> Self {
        WorkerScan { active: sel.active, factory: sel.factory }
    }
}

impl WorkerScan {
    fn factory(&self) -> DSResult<Option<db::FactoryId>> {
        if let Some(factory) = &self.factory {
            Ok(Some(db::FactoryId::from_str(factory).or_500()?))
        } else {
            Ok(None)
        }
    }
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub(crate) struct WorkerSelect {
    id: String,
    #[serde(default)]
    active: bool,
    factory: Option<String>,
}

impl WorkerSelect {
    fn id(&self) -> DSResult<db::WorkerId> {
        db::WorkerId::from_str(&self.id).or_500()
    }

    fn from_scan(scan: &WorkerScan, id: &str) -> Self {
        WorkerSelect {
            id: id.to_string(),
            active: scan.active,
            factory: scan.factory.clone(),
        }
    }
}

#[endpoint {
    method = GET,
    path = "/1/workers",
}]
pub(crate) async fn workers_list(
    rqctx: RequestContext<Arc<Central>>,
    pag: TypedQuery<PaginationParams<WorkerScan, WorkerSelect>>,
) -> DSResult<HttpResponseOk<ResultsPage<Worker>>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    c.require_admin(log, &rqctx.request, "worker.read").await?;

    let pag = pag.into_inner();
    let (marker, scan) = match pag.page {
        WhichPage::First(scan) => (None, scan),
        WhichPage::Next(sel) => (Some(sel.id()?), sel.into()),
    };

    Ok(HttpResponseOk(ResultsPage::new(
        c.db.workers_page(marker, scan.active, scan.factory()?)
            .or_500()?
            .into_iter()
            .map(|w| {
                let jobs =
                    c.db.worker_jobs(w.id)
                        .unwrap_or_else(|_| vec![])
                        .iter()
                        .map(|j| WorkerJob {
                            id: j.id.to_string(),
                            name: j.name.to_string(),
                            owner: j.owner.to_string(),
                            state: super::user::format_job_state(j),
                            tags: c.db.job_tags(j.id).unwrap_or_default(),
                        })
                        .collect::<Vec<_>>();
                Worker {
                    id: w.id.to_string(),
                    factory: w.factory().to_string(),
                    factory_private: w.factory_private.clone(),
                    factory_ip: w.factory_ip.clone(),
                    target: w.target().to_string(),
                    bootstrap: w.token.is_some(),
                    deleted: w.deleted,
                    recycle: w.recycle,
                    hold: w.hold().map(|h| h.into()),
                    lastping: w.lastping.map(|x| x.into()),
                    diagnostics: w.diagnostics,
                    jobs,
                }
            })
            .collect::<Vec<_>>(),
        &scan,
        |a, scan| WorkerSelect::from_scan(scan, &a.id),
    )?))
}

#[endpoint {
    method = POST,
    path = "/0/workers/recycle",
}]
pub(crate) async fn workers_recycle(
    rqctx: RequestContext<Arc<Central>>,
) -> DSResult<HttpResponseUpdatedNoContent> {
    let c = rqctx.context();
    let log = &rqctx.log;

    c.require_admin(log, &rqctx.request, "control").await?;

    c.db.worker_recycle_all().or_500()?;
    info!(log, "ADMIN: recycled all workers");

    Ok(HttpResponseUpdatedNoContent())
}

#[endpoint {
    method = POST,
    path = "/0/admin/worker/{worker}/recycle",
}]
pub(crate) async fn worker_recycle(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<WorkerPath>,
) -> DSResult<HttpResponseUpdatedNoContent> {
    let c = rqctx.context();
    let log = &rqctx.log;

    c.require_admin(log, &rqctx.request, "control").await?;

    let wid = path.into_inner().worker()?;

    c.db.worker_recycle(wid).or_500()?;
    info!(log, "ADMIN: recycled worker {}", wid);

    Ok(HttpResponseUpdatedNoContent())
}

#[derive(Deserialize, JsonSchema)]
pub struct WorkerHoldMark {
    reason: String,
}

#[endpoint {
    method = POST,
    path = "/0/admin/worker/{worker}/hold",
}]
pub(crate) async fn worker_hold_mark(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<WorkerPath>,
    body: TypedBody<WorkerHoldMark>,
) -> DSResult<HttpResponseUpdatedNoContent> {
    let c = rqctx.context();
    let log = &rqctx.log;

    c.require_admin(log, &rqctx.request, "control").await?;

    let wid = path.into_inner().worker()?;
    let reason = body.into_inner().reason;

    c.db.worker_mark_on_hold(wid, &reason).or_500()?;
    info!(log, "ADMIN: marked worker {wid} as on hold ({reason})");

    Ok(HttpResponseUpdatedNoContent())
}

#[endpoint {
    method = POST,
    path = "/0/admin/worker/{worker}/release",
}]
pub(crate) async fn worker_hold_release(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<WorkerPath>,
) -> DSResult<HttpResponseUpdatedNoContent> {
    let c = rqctx.context();
    let log = &rqctx.log;

    c.require_admin(log, &rqctx.request, "control").await?;

    let wid = path.into_inner().worker()?;

    let was_held = c.db.worker_hold_release(wid).or_500()?;
    info!(log, "ADMIN: released hold on worker {wid}"; "was_held" => was_held);

    Ok(HttpResponseUpdatedNoContent())
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct WorkerEventsQuery {
    minseq: Option<usize>,
}

#[endpoint {
    method = GET,
    path = "/0/admin/worker/{worker}/events",
}]
pub(crate) async fn worker_events_get(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<WorkerPath>,
    query: TypedQuery<WorkerEventsQuery>,
) -> DSResult<HttpResponseOk<Vec<WorkerEvent>>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let q = query.into_inner();

    c.require_admin(log, &rqctx.request, "worker.read").await?;

    let w = c.db.worker(path.into_inner().worker()?).or_500()?;

    let wevs = c
        .load_worker_events(log, &w, q.minseq.unwrap_or(0), 1000)
        .await
        .or_500()?;

    Ok(HttpResponseOk(
        wevs.iter()
            .map(|wev| WorkerEvent {
                seq: wev.seq as usize,
                stream: wev.stream.to_string(),
                time: wev.time.into(),
                time_remote: wev.time_remote.map(|t| t.into()),
                payload: wev.payload.to_string(),
            })
            .collect(),
    ))
}

#[endpoint {
    method = GET,
    path = "/0/admin/worker/{worker}",
}]
pub(crate) async fn worker(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<WorkerPath>,
) -> DSResult<HttpResponseOk<Worker>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    c.require_admin(log, &rqctx.request, "worker.read").await?;

    let w = c.db.worker(path.into_inner().worker()?).or_500()?;
    let jobs =
        c.db.worker_jobs(w.id)
            .unwrap_or_else(|_| vec![])
            .iter()
            .map(|j| WorkerJob {
                id: j.id.to_string(),
                name: j.name.to_string(),
                owner: j.owner.to_string(),
                state: super::user::format_job_state(j),
                tags: c.db.job_tags(j.id).unwrap_or_default(),
            })
            .collect::<Vec<_>>();

    Ok(HttpResponseOk(Worker {
        id: w.id.to_string(),
        factory: w.factory().to_string(),
        factory_private: w.factory_private.clone(),
        factory_ip: w.factory_ip.clone(),
        target: w.target().to_string(),
        bootstrap: w.token.is_some(),
        deleted: w.deleted,
        recycle: w.recycle,
        hold: w.hold().map(|h| h.into()),
        lastping: w.lastping.map(|x| x.into()),
        diagnostics: w.diagnostics,
        jobs,
    }))
}

#[derive(Deserialize, JsonSchema)]
pub struct FactoryCreate {
    name: String,
}

#[derive(Serialize, JsonSchema)]
pub struct FactoryCreateResult {
    id: String,
    name: String,
    token: String,
}

#[endpoint {
    method = POST,
    path = "/0/admin/factory",
}]
pub(crate) async fn factory_create(
    rqctx: RequestContext<Arc<Central>>,
    new_fac: TypedBody<FactoryCreate>,
) -> DSResult<HttpResponseCreated<FactoryCreateResult>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    c.require_admin(log, &rqctx.request, "factory.create").await?;

    let new_fac = new_fac.into_inner();
    let f = c.db.factory_create(&new_fac.name).or_500()?;

    Ok(HttpResponseCreated(FactoryCreateResult {
        id: f.id.to_string(),
        name: f.name.to_string(),
        token: f.token,
    }))
}

#[endpoint {
    method = GET,
    path = "/0/admin/factories",
}]
pub(crate) async fn factories_list(
    rqctx: RequestContext<Arc<Central>>,
) -> DSResult<HttpResponseOk<Vec<Factory>>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    c.require_admin(log, &rqctx.request, "factory.read").await?;

    let out =
        c.db.factories()
            .or_500()?
            .into_iter()
            .map(|f| Factory {
                id: f.id.to_string(),
                name: f.name,
                last_ping: f.lastping.map(|d| d.0),
                enable: f.enable,
                hold_workers: f.hold_workers,
            })
            .collect::<Vec<_>>();

    Ok(HttpResponseOk(out))
}

#[endpoint {
    method = POST,
    path = "/0/admin/factory/{factory}/disable",
}]
pub(crate) async fn factory_disable(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<FactoryPath>,
) -> DSResult<HttpResponseUpdatedNoContent> {
    let c = rqctx.context();
    let log = &rqctx.log;

    c.require_admin(log, &rqctx.request, "factory.write").await?;

    /*
     * Make sure this factory exists:
     */
    let f = c.db.factory(path.into_inner().factory()?).or_500()?;

    c.db.factory_enable(f.id, false).or_500()?;
    info!(rqctx.log, "factory {} ({:?}) disabled", f.id, f.name);

    Ok(HttpResponseUpdatedNoContent())
}

#[derive(Deserialize, JsonSchema)]
pub struct FactoryEnable {
    #[serde(default)]
    hold_workers: bool,
}

#[endpoint {
    method = POST,
    path = "/0/admin/factory/{factory}/enable",
}]
pub(crate) async fn factory_enable(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<FactoryPath>,
    body: TypedBody<FactoryEnable>,
) -> DSResult<HttpResponseUpdatedNoContent> {
    let c = rqctx.context();
    let log = &rqctx.log;

    c.require_admin(log, &rqctx.request, "factory.write").await?;

    /*
     * Make sure this factory exists:
     */
    let f = c.db.factory(path.into_inner().factory()?).or_500()?;

    let hold_workers = body.into_inner().hold_workers;

    c.db.factory_enable(f.id, true).or_500()?;
    c.db.factory_hold_workers(f.id, hold_workers).or_500()?;
    info!(rqctx.log, "factory {} ({:?}) enabled", f.id, f.name;
        "hold_workers" => hold_workers);

    Ok(HttpResponseUpdatedNoContent())
}

#[derive(Deserialize, JsonSchema)]
pub struct TargetCreate {
    name: String,
    desc: String,
}

#[derive(Serialize, JsonSchema)]
pub struct TargetCreateResult {
    id: String,
}

impl TargetCreateResult {
    fn new(target: db::TargetId) -> TargetCreateResult {
        TargetCreateResult { id: target.to_string() }
    }
}

#[endpoint {
    method = POST,
    path = "/0/admin/target",
}]
pub(crate) async fn target_create(
    rqctx: RequestContext<Arc<Central>>,
    new_targ: TypedBody<TargetCreate>,
) -> DSResult<HttpResponseCreated<TargetCreateResult>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    c.require_admin(log, &rqctx.request, "target.create").await?;

    let new_targ = new_targ.into_inner();
    let t = c.db.target_create(&new_targ.name, &new_targ.desc).or_500()?;

    Ok(HttpResponseCreated(TargetCreateResult::new(t.id)))
}

#[endpoint {
    method = GET,
    path = "/0/admin/targets",
}]
pub(crate) async fn targets_list(
    rqctx: RequestContext<Arc<Central>>,
) -> DSResult<HttpResponseOk<Vec<Target>>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    c.require_admin(log, &rqctx.request, "target.read").await?;

    let out =
        c.db.targets()
            .or_500()?
            .into_iter()
            .map(|t| Target {
                id: t.id.to_string(),
                name: t.name,
                desc: t.desc,
                redirect: t.redirect.map(|id| id.to_string()),
                privilege: t.privilege,
            })
            .collect::<Vec<_>>();

    Ok(HttpResponseOk(out))
}

#[endpoint {
    method = PUT,
    path = "/0/admin/targets/{target}/require/{privilege}",
}]
pub(crate) async fn target_require_privilege(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<TargetPrivilegePath>,
) -> DSResult<HttpResponseUpdatedNoContent> {
    let c = rqctx.context();
    let log = &rqctx.log;

    c.require_admin(log, &rqctx.request, "target.write").await?;

    let path = path.into_inner();
    let t = c.db.target(path.target()?).or_500()?;

    c.db.target_require(t.id, Some(&path.privilege)).or_500()?;

    Ok(HttpResponseUpdatedNoContent())
}

#[endpoint {
    method = DELETE,
    path = "/0/admin/targets/{target}/require",
}]
pub(crate) async fn target_require_no_privilege(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<TargetPath>,
) -> DSResult<HttpResponseUpdatedNoContent> {
    let c = rqctx.context();
    let log = &rqctx.log;

    c.require_admin(log, &rqctx.request, "target.write").await?;

    let path = path.into_inner();
    let t = c.db.target(path.target()?).or_500()?;

    c.db.target_require(t.id, None).or_500()?;

    Ok(HttpResponseUpdatedNoContent())
}

#[derive(Deserialize, JsonSchema)]
pub struct TargetRedirect {
    redirect: Option<String>,
}

impl TargetRedirect {
    fn redirect(&self) -> DSResult<Option<db::TargetId>> {
        self.redirect
            .as_deref()
            .map(|s| db::TargetId::from_str(s).or_500())
            .transpose()
    }
}

#[endpoint {
    method = PUT,
    path = "/0/admin/targets/{target}/redirect",
}]
pub(crate) async fn target_redirect(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<TargetPath>,
    body: TypedBody<TargetRedirect>,
) -> DSResult<HttpResponseUpdatedNoContent> {
    let c = rqctx.context();
    let log = &rqctx.log;

    c.require_admin(log, &rqctx.request, "target.write").await?;

    let path = path.into_inner();
    let t = c.db.target(path.target()?).or_500()?;

    /*
     * Make sure the redirect target, if specified, exists in the database:
     */
    let redirect = body
        .into_inner()
        .redirect()?
        .map(|t| c.db.target(t).map(|t| t.id))
        .transpose()
        .or_500()?;

    c.db.target_redirect(t.id, redirect).or_500()?;

    Ok(HttpResponseUpdatedNoContent())
}

#[derive(Deserialize, JsonSchema)]
pub struct TargetRename {
    new_name: String,
    signpost_description: String,
}

#[endpoint {
    method = POST,
    path = "/0/admin/targets/{target}/rename",
}]
pub(crate) async fn target_rename(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<TargetPath>,
    body: TypedBody<TargetRename>,
) -> DSResult<HttpResponseCreated<TargetCreateResult>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    c.require_admin(log, &rqctx.request, "target.write").await?;

    let path = path.into_inner();
    let t = c.db.target(path.target()?).or_500()?;
    let body = body.into_inner();

    let t =
        c.db.target_rename(t.id, &body.new_name, &body.signpost_description)
            .or_500()?;

    Ok(HttpResponseCreated(TargetCreateResult::new(t.id)))
}
