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
}

#[derive(Serialize, JsonSchema)]
struct Worker {
    pub id: String,
    pub instance_id: Option<String>,
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
                    })
                    .collect::<Vec<_>>();
            Some(Worker {
                id: w.id.to_string(),
                instance_id: w.instance_id.clone(),
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
