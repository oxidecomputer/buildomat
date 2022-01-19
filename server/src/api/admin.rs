/*
 * Copyright 2021 Oxide Computer Company
 */

use super::prelude::*;

#[derive(Serialize, JsonSchema)]
pub struct User {
    id: String,
    name: String,
    time_create: DateTime<Utc>,
}

#[derive(Deserialize, JsonSchema)]
pub struct JobPath {
    job: String,
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
    rqctx: Arc<RequestContext<Arc<Central>>>,
    new_user: TypedBody<UserCreate>,
) -> SResult<HttpResponseCreated<UserCreateResult>, HttpError> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    c.require_admin(log, &req).await?;

    let new_user = new_user.into_inner();
    let u = c.db.user_create(&new_user.name).or_500()?;

    Ok(HttpResponseCreated(UserCreateResult {
        id: u.id.to_string(),
        name: u.name.to_string(),
        token: u.token,
    }))
}

#[endpoint {
    method = GET,
    path = "/0/users",
}]
pub(crate) async fn users_list(
    rqctx: Arc<RequestContext<Arc<Central>>>,
) -> SResult<HttpResponseCreated<Vec<User>>, HttpError> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    c.require_admin(log, &req).await?;

    let out =
        c.db.users()
            .or_500()?
            .iter()
            .map(|u| User {
                id: u.id.to_string(),
                name: u.name.to_string(),
                time_create: u.time_create.into(),
            })
            .collect::<Vec<_>>();

    Ok(HttpResponseCreated(out))
}

#[endpoint {
    method = GET,
    path = "/0/admin/jobs",
}]
pub(crate) async fn admin_jobs_get(
    rqctx: Arc<RequestContext<Arc<Central>>>,
) -> SResult<HttpResponseOk<Vec<super::user::Job>>, HttpError> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    c.require_admin(log, &req).await?;

    Ok(HttpResponseOk(
        c.db.jobs_all()
            .or_500()?
            .iter()
            .map(|job| {
                Ok(super::user::format_job(
                    &job,
                    &c.db.job_tasks(&job.id)?,
                    c.db.job_output_rules(&job.id)?,
                    c.db.job_tags(&job.id)?,
                    &c.db.target_get(job.target())?,
                ))
            })
            .collect::<Result<Vec<_>>>()
            .or_500()?,
    ))
}

#[endpoint {
    method = GET,
    path = "/0/admin/jobs/{job}",
}]
pub(crate) async fn admin_job_get(
    rqctx: Arc<RequestContext<Arc<Central>>>,
    path: TypedPath<JobPath>,
) -> SResult<HttpResponseOk<super::user::Job>, HttpError> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    c.require_admin(log, &req).await?;

    let id = path.into_inner().job.parse::<db::JobId>().or_500()?;
    let job = c.db.job_by_id(&id).or_500()?;

    Ok(HttpResponseOk(super::user::format_job(
        &job,
        &c.db.job_tasks(&job.id).or_500()?,
        c.db.job_output_rules(&job.id).or_500()?,
        c.db.job_tags(&job.id).or_500()?,
        &c.db.target_get(job.target()).or_500()?,
    )))
}
#[endpoint {
    method = POST,
    path = "/0/control/hold",
}]
pub(crate) async fn control_hold(
    rqctx: Arc<RequestContext<Arc<Central>>>,
) -> SResult<HttpResponseOk<()>, HttpError> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    c.require_admin(log, &req).await?;

    info!(log, "ADMIN: HOLD NEW VM CREATION");
    c.inner.lock().unwrap().hold = true;

    Ok(HttpResponseOk(()))
}

#[endpoint {
    method = POST,
    path = "/0/control/resume",
}]
pub(crate) async fn control_resume(
    rqctx: Arc<RequestContext<Arc<Central>>>,
) -> SResult<HttpResponseOk<()>, HttpError> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    c.require_admin(log, &req).await?;

    info!(log, "ADMIN: RESUME NEW VM CREATION");
    c.inner.lock().unwrap().hold = false;

    Ok(HttpResponseOk(()))
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
struct Worker {
    pub id: String,
    pub factory: String,
    pub factory_private: Option<String>,
    pub target: String,
    pub bootstrap: bool,
    pub deleted: bool,
    pub recycle: bool,
    pub lastping: Option<DateTime<Utc>>,
    pub jobs: Vec<WorkerJob>,
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct WorkersResult {
    workers: Vec<Worker>,
}

#[endpoint {
    method = GET,
    path = "/0/workers",
}]
pub(crate) async fn workers_list(
    rqctx: Arc<RequestContext<Arc<Central>>>,
) -> SResult<HttpResponseOk<WorkersResult>, HttpError> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    c.require_admin(log, &req).await?;

    let w = c.db.workers().or_500()?;
    let workers = w
        .iter()
        .filter_map(|w| {
            let jobs =
                c.db.worker_jobs(&w.id)
                    .unwrap_or_else(|_| vec![])
                    .iter()
                    .map(|j| WorkerJob {
                        id: j.id.to_string(),
                        name: j.name.to_string(),
                        owner: j.owner.to_string(),
                        state: super::user::format_job_state(&j),
                        tags: c.db.job_tags(&j.id).unwrap_or_default(),
                    })
                    .collect::<Vec<_>>();
            Some(Worker {
                id: w.id.to_string(),
                factory: w.factory().to_string(),
                factory_private: w.factory_private.clone(),
                target: w.target().to_string(),
                bootstrap: w.token.is_some(),
                deleted: w.deleted,
                recycle: w.recycle,
                lastping: w.lastping.map(|x| x.into()),
                jobs,
            })
        })
        .collect::<Vec<_>>();

    Ok(HttpResponseOk(WorkersResult { workers }))
}

#[endpoint {
    method = POST,
    path = "/0/workers/recycle",
}]
pub(crate) async fn workers_recycle(
    rqctx: Arc<RequestContext<Arc<Central>>>,
) -> SResult<HttpResponseOk<()>, HttpError> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    c.require_admin(log, &req).await?;

    c.db.worker_recycle_all().or_500()?;

    Ok(HttpResponseOk(()))
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
    rqctx: Arc<RequestContext<Arc<Central>>>,
    new_fac: TypedBody<FactoryCreate>,
) -> SResult<HttpResponseCreated<FactoryCreateResult>, HttpError> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    c.require_admin(log, &req).await?;

    let new_fac = new_fac.into_inner();
    let f = c.db.factory_create(&new_fac.name).or_500()?;

    Ok(HttpResponseCreated(FactoryCreateResult {
        id: f.id.to_string(),
        name: f.name.to_string(),
        token: f.token,
    }))
}

#[derive(Deserialize, JsonSchema)]
pub struct TargetCreate {
    name: String,
    desc: String,
    // redirect: Option<String>,
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
    rqctx: Arc<RequestContext<Arc<Central>>>,
    new_targ: TypedBody<TargetCreate>,
) -> SResult<HttpResponseCreated<TargetCreateResult>, HttpError> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    c.require_admin(log, &req).await?;

    let new_targ = new_targ.into_inner();
    let t = c.db.target_create(&new_targ.name, &new_targ.desc).or_500()?;

    Ok(HttpResponseCreated(TargetCreateResult::new(t.id)))
}
