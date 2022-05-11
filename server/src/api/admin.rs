/*
 * Copyright 2022 Oxide Computer Company
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
pub struct Target {
    id: String,
    name: String,
    desc: String,
    redirect: Option<String>,
    privilege: Option<String>,
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
    fn user(&self) -> SResult<db::UserId, HttpError> {
        db::UserId::from_str(&self.user).or_500()
    }
}

#[derive(Deserialize, JsonSchema)]
pub struct TargetPath {
    target: String,
}

impl TargetPath {
    fn target(&self) -> SResult<db::TargetId, HttpError> {
        db::TargetId::from_str(&self.target).or_500()
    }
}

#[derive(Deserialize, JsonSchema)]
pub struct TargetPrivilegePath {
    target: String,
    privilege: String,
}

impl TargetPrivilegePath {
    fn target(&self) -> SResult<db::TargetId, HttpError> {
        db::TargetId::from_str(&self.target).or_500()
    }
}

#[derive(Deserialize, JsonSchema)]
pub struct UserPrivilegePath {
    user: String,
    privilege: String,
}

impl UserPrivilegePath {
    fn user(&self) -> SResult<db::UserId, HttpError> {
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
    rqctx: Arc<RequestContext<Arc<Central>>>,
    new_user: TypedBody<UserCreate>,
) -> SResult<HttpResponseCreated<UserCreateResult>, HttpError> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    c.require_admin(log, &req, "user.create").await?;

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
) -> SResult<HttpResponseOk<Vec<User>>, HttpError> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    c.require_admin(log, &req, "user.read").await?;

    let out =
        c.db.users()
            .or_500()?
            .drain(..)
            .map(|u| User {
                id: u.user.id.to_string(),
                name: u.user.name,
                time_create: u.user.time_create.into(),
                privileges: u.privileges,
            })
            .collect::<Vec<_>>();

    Ok(HttpResponseOk(out))
}

#[endpoint {
    method = GET,
    path = "/0/users/{user}",
}]
pub(crate) async fn user_get(
    rqctx: Arc<RequestContext<Arc<Central>>>,
    path: TypedPath<UserPath>,
) -> SResult<HttpResponseOk<User>, HttpError> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    c.require_admin(log, &req, "user.read").await?;

    if let Some(u) = c.db.user_get_by_id(path.into_inner().user()?).or_500()? {
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
    rqctx: Arc<RequestContext<Arc<Central>>>,
    path: TypedPath<UserPrivilegePath>,
) -> DSResult<HttpResponseUpdatedNoContent> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    c.require_admin(log, &req, "privilege.grant").await?;

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
    rqctx: Arc<RequestContext<Arc<Central>>>,
    path: TypedPath<UserPrivilegePath>,
) -> DSResult<HttpResponseDeleted> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    c.require_admin(log, &req, "privilege.revoke").await?;

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
    rqctx: Arc<RequestContext<Arc<Central>>>,
    query: TypedQuery<AdminJobsGetQuery>,
) -> SResult<HttpResponseOk<Vec<super::user::Job>>, HttpError> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    c.require_admin(log, &req, "job.read").await?;

    let q = query.into_inner();
    let jobs = if q.active {
        /*
         * We have been asked to list only active jobs:
         */
        let mut jobs = c.db.jobs_active().or_500()?;
        jobs.extend(c.db.jobs_waiting().or_500()?);
        jobs
    } else if let Some(n) = &q.completed {
        /*
         * We have been asked to provide some number of recently completed jobs:
         */
        c.db.jobs_completed((*n).try_into().unwrap()).or_500()?
    } else {
        /*
         * By default we list all jobs in the database.
         */
        c.db.jobs_all().or_500()?
    };

    Ok(HttpResponseOk(
        jobs.iter()
            .map(|job| {
                Ok(super::user::format_job(
                    job,
                    &c.db.job_tasks(job.id)?,
                    c.db.job_output_rules(job.id)?,
                    c.db.job_tags(job.id)?,
                    &c.db.target_get(job.target())?,
                    c.db.job_times(job.id)?,
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

    c.require_admin(log, &req, "job.read").await?;

    let id = path.into_inner().job.parse::<db::JobId>().or_500()?;
    let job = c.db.job_by_id(id).or_500()?;

    Ok(HttpResponseOk(super::user::format_job(
        &job,
        &c.db.job_tasks(job.id).or_500()?,
        c.db.job_output_rules(job.id).or_500()?,
        c.db.job_tags(job.id).or_500()?,
        &c.db.target_get(job.target()).or_500()?,
        c.db.job_times(job.id).or_500()?,
    )))
}
#[endpoint {
    method = POST,
    path = "/0/control/hold",
}]
pub(crate) async fn control_hold(
    rqctx: Arc<RequestContext<Arc<Central>>>,
) -> DSResult<HttpResponseUpdatedNoContent> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    c.require_admin(log, &req, "control").await?;

    info!(log, "ADMIN: HOLD NEW VM CREATION");
    c.inner.lock().unwrap().hold = true;

    Ok(HttpResponseUpdatedNoContent())
}

#[endpoint {
    method = POST,
    path = "/0/control/resume",
}]
pub(crate) async fn control_resume(
    rqctx: Arc<RequestContext<Arc<Central>>>,
) -> DSResult<HttpResponseUpdatedNoContent> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    c.require_admin(log, &req, "control").await?;

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

#[derive(Deserialize, JsonSchema)]
pub struct WorkersListQuery {
    #[serde(default)]
    active: bool,
}

#[endpoint {
    method = GET,
    path = "/0/workers",
}]
pub(crate) async fn workers_list(
    rqctx: Arc<RequestContext<Arc<Central>>>,
    query: TypedQuery<WorkersListQuery>,
) -> SResult<HttpResponseOk<WorkersResult>, HttpError> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    c.require_admin(log, &req, "worker.read").await?;

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
                target: w.target().to_string(),
                bootstrap: w.token.is_some(),
                deleted: w.deleted,
                recycle: w.recycle,
                lastping: w.lastping.map(|x| x.into()),
                jobs,
            }
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
) -> DSResult<HttpResponseUpdatedNoContent> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    c.require_admin(log, &req, "control").await?;

    c.db.worker_recycle_all().or_500()?;

    Ok(HttpResponseUpdatedNoContent())
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

    c.require_admin(log, &req, "factory.create").await?;

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

    c.require_admin(log, &req, "target.create").await?;

    let new_targ = new_targ.into_inner();
    let t = c.db.target_create(&new_targ.name, &new_targ.desc).or_500()?;

    Ok(HttpResponseCreated(TargetCreateResult::new(t.id)))
}

#[endpoint {
    method = GET,
    path = "/0/admin/targets",
}]
pub(crate) async fn targets_list(
    rqctx: Arc<RequestContext<Arc<Central>>>,
) -> SResult<HttpResponseOk<Vec<Target>>, HttpError> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    c.require_admin(log, &req, "target.read").await?;

    let out =
        c.db.targets()
            .or_500()?
            .drain(..)
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
    rqctx: Arc<RequestContext<Arc<Central>>>,
    path: TypedPath<TargetPrivilegePath>,
) -> SResult<HttpResponseUpdatedNoContent, HttpError> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    c.require_admin(log, &req, "target.write").await?;

    let path = path.into_inner();
    let t = c.db.target_get(path.target()?).or_500()?;

    c.db.target_require(t.id, Some(&path.privilege)).or_500()?;

    Ok(HttpResponseUpdatedNoContent())
}

#[endpoint {
    method = DELETE,
    path = "/0/admin/targets/{target}/require",
}]
pub(crate) async fn target_require_no_privilege(
    rqctx: Arc<RequestContext<Arc<Central>>>,
    path: TypedPath<TargetPath>,
) -> SResult<HttpResponseUpdatedNoContent, HttpError> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    c.require_admin(log, &req, "target.write").await?;

    let path = path.into_inner();
    let t = c.db.target_get(path.target()?).or_500()?;

    c.db.target_require(t.id, None).or_500()?;

    Ok(HttpResponseUpdatedNoContent())
}

#[derive(Deserialize, JsonSchema)]
pub struct TargetRedirect {
    redirect: Option<String>,
}

impl TargetRedirect {
    fn redirect(&self) -> SResult<Option<db::TargetId>, HttpError> {
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
    rqctx: Arc<RequestContext<Arc<Central>>>,
    path: TypedPath<TargetPath>,
    body: TypedBody<TargetRedirect>,
) -> SResult<HttpResponseUpdatedNoContent, HttpError> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    c.require_admin(log, &req, "target.write").await?;

    let path = path.into_inner();
    let t = c.db.target_get(path.target()?).or_500()?;

    /*
     * Make sure the redirect target, if specified, exists in the database:
     */
    let redirect = body
        .into_inner()
        .redirect()?
        .map(|t| c.db.target_get(t).map(|t| t.id))
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
    rqctx: Arc<RequestContext<Arc<Central>>>,
    path: TypedPath<TargetPath>,
    body: TypedBody<TargetRename>,
) -> SResult<HttpResponseCreated<TargetCreateResult>, HttpError> {
    let c = rqctx.context();
    let req = rqctx.request.lock().await;
    let log = &rqctx.log;

    c.require_admin(log, &req, "target.write").await?;

    let path = path.into_inner();
    let t = c.db.target_get(path.target()?).or_500()?;
    let body = body.into_inner();

    let t =
        c.db.target_rename(t.id, &body.new_name, &body.signpost_description)
            .or_500()?;

    Ok(HttpResponseCreated(TargetCreateResult::new(t.id)))
}
