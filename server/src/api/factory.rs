/*
 * Copyright 2024 Oxide Computer Company
 */

use super::prelude::*;

trait WorkerOwns {
    fn owns(&self, log: &Logger, worker: &db::Worker) -> DSResult<()>;
}

impl WorkerOwns for db::Factory {
    fn owns(&self, log: &Logger, worker: &db::Worker) -> DSResult<()> {
        if worker.factory() == self.id {
            return Ok(());
        }

        warn!(
            log,
            "worker {} owned by {:?}, not {}",
            worker.id,
            worker.factory,
            self.id
        );

        Err(HttpError::for_client_error(
            None,
            StatusCode::FORBIDDEN,
            "not your worker".into(),
        ))
    }
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct FactoryPingResult {
    ok: bool,
}

#[endpoint {
    method = GET,
    path = "/0/factory/ping",
}]
pub(crate) async fn factory_ping(
    rqctx: RequestContext<Arc<Central>>,
) -> DSResult<HttpResponseOk<FactoryPingResult>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let f = c.require_factory(log, &rqctx.request).await?;

    info!(log, "factory ping!"; "id" => f.id.to_string());

    c.db.factory_ping(f.id).or_500()?;

    let res = FactoryPingResult { ok: true };

    Ok(HttpResponseOk(res))
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct WorkerPath {
    worker: String,
}

impl WorkerPath {
    fn worker(&self) -> DSResult<db::WorkerId> {
        self.worker.parse::<db::WorkerId>().or_500()
    }
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct FactoryWorker {
    id: String,
    private: Option<String>,
    recycle: bool,
    bootstrap: String,
    online: bool,
    hold: bool,
    target: String,
}

impl From<&db::Worker> for FactoryWorker {
    fn from(w: &db::Worker) -> Self {
        FactoryWorker {
            id: w.id.to_string(),
            private: w.factory_private.as_ref().map(|s| s.to_string()),
            /*
             * If a worker is marked on hold, do not tell the factory to recycle
             * that worker even if it is otherwise eligible.  When investigation
             * of the instance is over, the hold can be lifted and recycling can
             * begin.
             */
            recycle: !w.is_held() && w.recycle,
            bootstrap: w.bootstrap.to_string(),
            online: w.token.is_some(),
            hold: w.is_held(),
            target: w.target().to_string(),
        }
    }
}

#[endpoint {
    method = GET,
    path = "/0/factory/workers",
}]
pub(crate) async fn factory_workers(
    rqctx: RequestContext<Arc<Central>>,
) -> DSResult<HttpResponseOk<Vec<FactoryWorker>>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let f = c.require_factory(log, &rqctx.request).await?;
    let workers =
        c.db.workers_for_factory(&f)
            .or_500()?
            .iter()
            .map(|w| {
                assert!(f.owns(log, w).is_ok());
                FactoryWorker::from(w)
            })
            .collect();

    Ok(HttpResponseOk(workers))
}

/*
 * XXX We define this intermediate result type because Option<FactoryWorker> as
 * a return type does not currently seem to work the way we would like with
 * dropshot.
 */
#[derive(Serialize, JsonSchema)]
pub(crate) struct FactoryWorkerResult {
    worker: Option<FactoryWorker>,
}

#[endpoint {
    method = GET,
    path = "/0/factory/worker/{worker}",
}]
pub(crate) async fn factory_worker_get(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<WorkerPath>,
) -> DSResult<HttpResponseOk<FactoryWorkerResult>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let p = path.into_inner();

    let f = c.require_factory(log, &rqctx.request).await?;
    let w = if let Some(w) = c.db.worker_opt(p.worker()?).or_500()? {
        w
    } else {
        return Ok(HttpResponseOk(FactoryWorkerResult { worker: None }));
    };
    f.owns(log, &w)?;

    Ok(HttpResponseOk(FactoryWorkerResult {
        worker: if w.deleted {
            /*
             * This worker has been deleted already.
             */
            None
        } else {
            Some(FactoryWorker::from(&w))
        },
    }))
}

#[derive(Debug, Deserialize, JsonSchema)]
pub(crate) struct FactoryWorkerAppend {
    stream: String,
    time: DateTime<Utc>,
    payload: String,
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct FactoryWorkerAppendResult {
    retry: bool,
}

#[endpoint {
    method = POST,
    path = "/0/factory/worker/{worker}/append",
}]
pub(crate) async fn factory_worker_append(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<WorkerPath>,
    body: TypedBody<FactoryWorkerAppend>,
) -> DSResult<HttpResponseOk<FactoryWorkerAppendResult>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let p = path.into_inner();
    let b = body.into_inner();

    let f = c.require_factory(log, &rqctx.request).await?;

    let w = c.db.worker(p.worker()?).or_500()?;
    f.owns(log, &w)?;

    let job = c.db.worker_job(w.id).or_500()?;

    let retry = if let Some(job) = job {
        if job.complete {
            /*
             * Ignore any console output that arrives after we have closed out
             * the job.
             */
            false
        } else {
            c.db.job_append_event(
                job.id,
                None,
                &b.stream,
                Utc::now(),
                Some(b.time),
                &b.payload,
            )
            .or_500()?;
            info!(
                log,
                "factory {} worker {} job {} append event: {:?}",
                f.id,
                w.id,
                job.id,
                b,
            );
            false
        }
    } else if w.recycle || w.deleted {
        /*
         * This worker has been recycled or deleted without having been assigned
         * a job.  Ignore any console output that arrives.
         */
        false
    } else {
        /*
         * Without out a current job, we do not presently have a context in
         * which to usefully store these event records.  Inform the factory so
         * that it may retain the record and try again later.
         */
        true
    };

    Ok(HttpResponseOk(FactoryWorkerAppendResult { retry }))
}

#[endpoint {
    method = POST,
    path = "/0/factory/worker/{worker}/flush",
}]
pub(crate) async fn factory_worker_flush(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<WorkerPath>,
) -> DSResult<HttpResponseUpdatedNoContent> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let p = path.into_inner();

    let f = c.require_factory(log, &rqctx.request).await?;

    let w = c.db.worker(p.worker()?).or_500()?;
    f.owns(log, &w)?;

    if w.wait_for_flush {
        info!(log, "factory {} worker {} flush boot logs", f.id, w.id);
        c.db.worker_flush(w.id).or_500()?;
    }

    Ok(HttpResponseUpdatedNoContent())
}

#[derive(Debug, Deserialize, JsonSchema)]
pub(crate) struct FactoryWorkerAssociate {
    private: String,
    metadata: Option<metadata::FactoryMetadata>,
}

#[endpoint {
    method = PATCH,
    path = "/0/factory/worker/{worker}",
}]
pub(crate) async fn factory_worker_associate(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<WorkerPath>,
    body: TypedBody<FactoryWorkerAssociate>,
) -> DSResult<HttpResponseUpdatedNoContent> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let p = path.into_inner();
    let b = body.into_inner();

    let f = c.require_factory(log, &rqctx.request).await?;

    let w = c.db.worker(p.worker()?).or_500()?;
    f.owns(log, &w)?;

    if let Err(e) = c.db.worker_associate(w.id, &b.private, b.metadata.as_ref())
    {
        error!(
            log,
            "factory {} worker {} associate failure: {:?}: {:?}",
            f.id,
            w.id,
            b,
            e
        );
        unauth_response()
    } else {
        info!(log, "factory {} worker {} associate: {:?}", f.id, w.id, b);
        Ok(HttpResponseUpdatedNoContent())
    }
}

#[endpoint {
    method = DELETE,
    path = "/0/factory/worker/{worker}",
}]
pub(crate) async fn factory_worker_destroy(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<WorkerPath>,
) -> DSResult<HttpResponseOk<bool>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let p = path.into_inner();

    let f = c.require_factory(log, &rqctx.request).await?;

    let w = c.db.worker(p.worker()?).or_500()?;
    f.owns(log, &w)?;

    if let Err(e) = c.db.worker_destroy(w.id) {
        error!(
            log,
            "factory {} worker {} destroy failure: {:?}", f.id, w.id, e
        );
        unauth_response()
    } else {
        info!(log, "factory {} worker {} destroyed", f.id, w.id);
        Ok(HttpResponseOk(true))
    }
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct FactoryWorkerCreate {
    target: String,
    job: Option<String>,
    #[serde(default)]
    wait_for_flush: bool,
}

impl FactoryWorkerCreate {
    fn job(&self) -> DSResult<Option<db::JobId>> {
        if let Some(job) = self.job.as_deref() {
            Ok(Some(job.parse::<db::JobId>().or_500()?))
        } else {
            Ok(None)
        }
    }

    fn target(&self) -> DSResult<db::TargetId> {
        self.target.parse::<db::TargetId>().or_500()
    }
}

#[endpoint {
    method = POST,
    path = "/0/factory/worker",
}]
pub(crate) async fn factory_worker_create(
    rqctx: RequestContext<Arc<Central>>,
    body: TypedBody<FactoryWorkerCreate>,
) -> DSResult<HttpResponseCreated<FactoryWorker>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let b = body.into_inner();

    let f = c.require_factory(log, &rqctx.request).await?;
    let t = c.db.target(b.target()?).or_500()?;
    let j = b.job()?;

    let hold = if f.hold_workers {
        Some("factory is configured to hold all created workers")
    } else {
        None
    };

    let w = c.db.worker_create(&f, &t, j, b.wait_for_flush, hold).or_500()?;
    info!(log, "factory {} worker {} created (job {:?})", f.id, t.id, j);

    Ok(HttpResponseCreated(FactoryWorker::from(&w)))
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct FactoryWhatsNext {
    supported_targets: Vec<String>,
}

impl FactoryWhatsNext {
    fn supported_targets(&self) -> DSResult<Vec<db::TargetId>> {
        self.supported_targets
            .iter()
            .map(|s| Ok(s.parse()?))
            .collect::<Result<Vec<_>>>()
            .or_500()
    }
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct FactoryLease {
    target: String,
    job: String,
}

impl FactoryLease {
    fn new(job: db::JobId, target: db::TargetId) -> FactoryLease {
        FactoryLease { job: job.to_string(), target: target.to_string() }
    }
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct FactoryLeaseResult {
    lease: Option<FactoryLease>,
}

#[endpoint {
    method = POST,
    path = "/0/factory/lease",
}]
pub(crate) async fn factory_lease(
    rqctx: RequestContext<Arc<Central>>,
    body: TypedBody<FactoryWhatsNext>,
) -> DSResult<HttpResponseOk<FactoryLeaseResult>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let supported_targets = body.into_inner().supported_targets()?;

    let f = c.require_factory(log, &rqctx.request).await?;

    /*
     * Update the last ping time for this factory, whether we are going to issue
     * it a lease or not:
     */
    c.db.factory_ping(f.id).or_500()?;

    if !f.enable || c.inner.lock().unwrap().hold {
        /*
         * The operator has requested that we not create any more workers,
         * either for this factory specifically or for the entire system.
         */
        return Ok(HttpResponseOk(FactoryLeaseResult { lease: None }));
    }

    /*
     * Look at the jobs that are not assigned.
     */
    for j in c.db.jobs_active(10_000).or_500()? {
        assert!(!j.complete);
        assert!(!j.waiting);

        if j.cancelled || j.worker.is_some() {
            continue;
        }

        let t = c.db.target(j.target()).or_500()?;

        if !supported_targets.contains(&t.id) {
            continue;
        }

        if c.inner.lock().unwrap().leases.take_lease(j.id, f.id) {
            info!(log, "factory {}: granted lease for job {}", f.id, j.id);
            return Ok(HttpResponseOk(FactoryLeaseResult {
                lease: Some(FactoryLease::new(j.id, t.id)),
            }));
        }
    }

    Ok(HttpResponseOk(FactoryLeaseResult { lease: None }))
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct FactoryJobPath {
    job: String,
}

impl FactoryJobPath {
    fn job(&self) -> DSResult<db::JobId> {
        self.job.parse::<db::JobId>().or_500()
    }
}

#[endpoint {
    method = POST,
    path = "/0/factory/lease/{job}",
}]
pub(crate) async fn factory_lease_renew(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<FactoryJobPath>,
) -> DSResult<HttpResponseOk<bool>> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let p = path.into_inner();

    let f = c.require_factory(log, &rqctx.request).await?;
    let job = p.job()?;

    if c.inner.lock().unwrap().leases.renew_lease(job, f.id) {
        Ok(HttpResponseOk(true))
    } else {
        warn!(log, "factory {} denied lease renewal for job {}", f.id, job);
        Ok(HttpResponseOk(false))
    }
}
