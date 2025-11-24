/*
 * Copyright 2025 Oxide Computer Company
 */

use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::Result;
use buildomat_common::*;
use buildomat_database::{DBResult, FromRow, Handle, Sqlite, conflict};
use buildomat_types::*;
use chrono::prelude::*;
use sea_query::{
    Alias, Asterisk, Cond, Expr, Iden, IntoTableRef, Keyword, Order, Query,
    SeaRc, SelectStatement, TableRef, all,
};
#[allow(unused_imports)]
use slog::{Logger, debug, error, info, warn};
use tokio::sync::watch;

mod tables;

mod types {
    use buildomat_database::rusqlite;
    use buildomat_database::{sqlite_integer_new_type, sqlite_ulid_new_type};

    sqlite_integer_new_type!(UnixUid, u32, Unsigned);
    sqlite_integer_new_type!(UnixGid, u32, Unsigned);
    sqlite_integer_new_type!(DataSize, u64, BigUnsigned);

    sqlite_ulid_new_type!(UserId);
    sqlite_ulid_new_type!(JobId);
    sqlite_ulid_new_type!(JobFileId);
    sqlite_ulid_new_type!(TaskId);
    sqlite_ulid_new_type!(WorkerId);
    sqlite_ulid_new_type!(FactoryId);
    sqlite_ulid_new_type!(TargetId);

    pub use buildomat_database::{Dictionary, IsoDate, JsonValue};
}

pub use tables::*;
pub use types::*;

pub struct Database {
    log: Logger,
    sql: Sqlite,
    job_subscriptions: Mutex<HashMap<JobId, watch::Sender<JobNotification>>>,
}

pub struct CreateTask {
    pub name: String,
    pub script: String,
    pub env_clear: bool,
    pub env: HashMap<String, String>,
    pub user_id: Option<u32>,
    pub group_id: Option<u32>,
    pub workdir: Option<String>,
}

pub struct CreateDepend {
    pub name: String,
    pub prior_job: JobId,
    pub copy_outputs: bool,
    pub on_failed: bool,
    pub on_completed: bool,
}

#[derive(Debug, PartialEq)]
pub struct CreateOutputRule {
    pub rule: String,
    pub ignore: bool,
    pub size_change_ok: bool,
    pub require_match: bool,
}

pub struct CreateJobEvent {
    pub task: Option<u32>,
    pub stream: String,
    pub time: DateTime<Utc>,
    pub time_remote: Option<DateTime<Utc>>,
    pub payload: String,
}

pub struct CreateWorkerEvent {
    pub stream: String,
    pub time: DateTime<Utc>,
    pub time_remote: Option<DateTime<Utc>>,
    pub payload: String,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct JobNotification {
    pub id: JobId,
    pub seq: u32,
    pub complete: bool,
    pub gen: u32,
}

impl Database {
    pub fn new<P: AsRef<Path>>(
        log: Logger,
        path: P,
        cache_kb: Option<u32>,
    ) -> Result<Database> {
        let sql = Sqlite::setup(
            log.clone(),
            path,
            include_str!("../../schema.sql"),
            cache_kb,
        )?;

        Ok(Database { log, sql, job_subscriptions: Default::default() })
    }

    fn i_worker_for_bootstrap(
        &self,
        h: &mut Handle,
        bs: &str,
    ) -> DBResult<Worker> {
        h.get_row(
            Query::select()
                .from(WorkerDef::Table)
                .columns(Worker::columns())
                .and_where(Expr::col(WorkerDef::Bootstrap).eq(bs))
                .to_owned(),
        )
    }

    fn i_worker(&self, h: &mut Handle, id: WorkerId) -> DBResult<Worker> {
        h.get_row(
            Query::select()
                .from(WorkerDef::Table)
                .columns(Worker::columns())
                .and_where(Expr::col(WorkerDef::Id).eq(id))
                .to_owned(),
        )
    }

    pub fn workers_page(
        &self,
        marker: Option<WorkerId>,
        active: bool,
        factory: Option<FactoryId>,
    ) -> DBResult<Vec<Worker>> {
        let q = Query::select()
            .from(WorkerDef::Table)
            .columns(Worker::columns())
            .order_by(WorkerDef::Id, Order::Asc)
            .conditions(
                active,
                |q| {
                    q.and_where(Expr::col(WorkerDef::Deleted).eq(false));
                },
                |_| {},
            )
            .and_where_option(marker.map(|id| Expr::col(WorkerDef::Id).gt(id)))
            .and_where_option(
                factory.map(|id| Expr::col(WorkerDef::Factory).eq(id)),
            )
            .limit(1000)
            .to_owned();

        self.sql.tx(|h| h.get_rows(q))
    }

    pub fn workers(&self) -> DBResult<Vec<Worker>> {
        self.sql.tx(|h| {
            h.get_rows(
                Query::select()
                    .from(WorkerDef::Table)
                    .columns(Worker::columns())
                    .order_by(WorkerDef::Id, Order::Asc)
                    .to_owned(),
            )
        })
    }

    pub fn workers_active(&self) -> DBResult<Vec<Worker>> {
        self.sql.tx(|h| {
            h.get_rows(
                Query::select()
                    .from(WorkerDef::Table)
                    .columns(Worker::columns())
                    .order_by(WorkerDef::Id, Order::Asc)
                    .and_where(Expr::col(WorkerDef::Deleted).eq(false))
                    .to_owned(),
            )
        })
    }

    pub fn workers_without_pings(
        &self,
        age: Duration,
    ) -> DBResult<Vec<Worker>> {
        /*
         * Create a date in the past, based on the maximum ping age threshold,
         * that we can use for comparison:
         */
        let Some(dt) = Utc::now()
            .checked_sub_signed(chrono::Duration::from_std(age).unwrap())
        else {
            conflict!("could not calculate date in the past");
        };

        self.sql.tx(|h| {
            h.get_rows(
                Query::select()
                    .from(WorkerDef::Table)
                    .columns(Worker::columns())
                    .order_by(WorkerDef::Id, Order::Asc)
                    /*
                     * Once the recycle bit has been set on a worker, it is up
                     * to the factory to get it to the deleted state.  We can
                     * ignore workers in this state where the agents have
                     * stopped phoning in.
                     */
                    .and_where(Expr::col(WorkerDef::Recycle).eq(false))
                    .and_where(Expr::col(WorkerDef::Deleted).eq(false))
                    /*
                     * Look for any worker that has phoned in at least once, but
                     * has then not done so since the threshold has passed:
                     */
                    .and_where(Expr::col(WorkerDef::Lastping).is_not_null())
                    .and_where(Expr::col(WorkerDef::Lastping).lt(IsoDate(dt)))
                    /*
                     * Ignore held workers as they are presumably already being
                     * dealt with by the operator.
                     */
                    .and_where(
                        Expr::col((WorkerDef::Table, WorkerDef::HoldTime))
                            .is_null(),
                    )
                    .to_owned(),
            )
        })
    }

    pub fn workers_for_factory(
        &self,
        factory: &Factory,
    ) -> DBResult<Vec<Worker>> {
        self.sql.tx(|h| {
            h.get_rows(
                Query::select()
                    .from(WorkerDef::Table)
                    .columns(Worker::columns())
                    .order_by(WorkerDef::Id, Order::Asc)
                    .and_where(Expr::col(WorkerDef::Factory).eq(factory.id))
                    .and_where(Expr::col(WorkerDef::Deleted).eq(false))
                    .to_owned(),
            )
        })
    }

    pub fn i_worker_jobs(
        &self,
        h: &mut Handle,
        worker: WorkerId,
    ) -> DBResult<Vec<Job>> {
        h.get_rows(
            Query::select()
                .from(JobDef::Table)
                .columns(Job::columns())
                .and_where(Expr::col(JobDef::Worker).eq(worker))
                .to_owned(),
        )
    }

    pub fn worker_jobs(&self, worker: WorkerId) -> DBResult<Vec<Job>> {
        self.sql.tx(|h| self.i_worker_jobs(h, worker))
    }

    pub fn free_workers(&self) -> DBResult<Vec<Worker>> {
        self.sql.tx(|h| {
            h.get_rows(
                Query::select()
                    .from(WorkerDef::Table)
                    .columns(Worker::columns())
                    .left_join(
                        JobDef::Table,
                        Expr::col((JobDef::Table, JobDef::Worker))
                            .eq(Expr::col((WorkerDef::Table, WorkerDef::Id))),
                    )
                    .order_by((WorkerDef::Table, WorkerDef::Id), Order::Asc)
                    .and_where(
                        Expr::col((JobDef::Table, JobDef::Worker)).is_null(),
                    )
                    .and_where(
                        Expr::col((WorkerDef::Table, WorkerDef::Deleted))
                            .eq(false),
                    )
                    .and_where(
                        Expr::col((WorkerDef::Table, WorkerDef::Recycle))
                            .eq(false),
                    )
                    .and_where(
                        Expr::col((WorkerDef::Table, WorkerDef::Token))
                            .is_not_null(),
                    )
                    /*
                     * Workers that are marked as on hold are not eligible for
                     * job assignment.
                     */
                    .and_where(
                        Expr::col((WorkerDef::Table, WorkerDef::HoldTime))
                            .is_null(),
                    )
                    .to_owned(),
            )
        })
    }

    pub fn worker_recycle_all(&self) -> DBResult<usize> {
        self.sql.tx(|h| {
            h.exec_update(
                Query::update()
                    .table(WorkerDef::Table)
                    .values([(WorkerDef::Recycle, true.into())])
                    .and_where(Expr::col(WorkerDef::Deleted).eq(false))
                    .to_owned(),
            )
        })
    }

    fn i_worker_recycle(&self, h: &mut Handle, id: WorkerId) -> DBResult<bool> {
        Ok(h.exec_update(
            Query::update()
                .table(WorkerDef::Table)
                .values([(WorkerDef::Recycle, true.into())])
                .and_where(Expr::col(WorkerDef::Id).eq(id))
                .and_where(Expr::col(WorkerDef::Deleted).eq(false))
                .to_owned(),
        )? > 0)
    }

    pub fn worker_recycle(&self, id: WorkerId) -> DBResult<bool> {
        self.sql.tx(|h| self.i_worker_recycle(h, id))
    }

    pub fn worker_flush(&self, id: WorkerId) -> DBResult<bool> {
        self.sql.tx(|h| {
            Ok(h.exec_update(
                Query::update()
                    .table(WorkerDef::Table)
                    .values([(WorkerDef::WaitForFlush, false.into())])
                    .and_where(Expr::col(WorkerDef::Id).eq(id))
                    .to_owned(),
            )? > 0)
        })
    }

    pub fn worker_destroy(&self, id: WorkerId) -> DBResult<bool> {
        self.sql.tx(|h| {
            Ok(h.exec_update(
                Query::update()
                    .table(WorkerDef::Table)
                    .value(WorkerDef::Deleted, true)
                    /*
                     * If the diagnostic information has not arrived by now, it
                     * never will:
                     */
                    .value(WorkerDef::Diagnostics, false)
                    .and_where(Expr::col(WorkerDef::Id).eq(id))
                    .to_owned(),
            )? > 0)
        })
    }

    pub fn worker_ping(&self, id: WorkerId) -> DBResult<bool> {
        let now = IsoDate(Utc::now());

        self.sql.tx(|h| {
            Ok(h.exec_update(
                Query::update()
                    .table(WorkerDef::Table)
                    .values([(WorkerDef::Lastping, now.into())])
                    .and_where(Expr::col(WorkerDef::Id).eq(id))
                    .to_owned(),
            )? > 0)
        })
    }

    fn i_worker_mark_on_hold(
        &self,
        h: &mut Handle,
        id: WorkerId,
        reason: &str,
    ) -> DBResult<()> {
        let w = self.i_worker(h, id)?;
        if w.deleted {
            conflict!("worker {id} is deleted; cannot be held");
        }

        if let Some(hold) = w.hold() {
            conflict!("worker {id} is already on hold: {hold:?}");
        }

        let c = h.exec_update(
            Query::update()
                .table(WorkerDef::Table)
                .value(WorkerDef::HoldTime, IsoDate::now())
                .value(WorkerDef::HoldReason, reason)
                .and_where(Expr::col(WorkerDef::Id).eq(w.id))
                .and_where(Expr::col(WorkerDef::Deleted).eq(false))
                .and_where(Expr::col(WorkerDef::HoldTime).is_null())
                .and_where(Expr::col(WorkerDef::HoldReason).is_null())
                .to_owned(),
        )?;
        assert_eq!(c, 1);

        Ok(())
    }

    pub fn worker_mark_on_hold(
        &self,
        id: WorkerId,
        reason: &str,
    ) -> DBResult<()> {
        self.sql.tx_immediate(|h| self.i_worker_mark_on_hold(h, id, reason))
    }

    pub fn worker_hold_release(&self, id: WorkerId) -> DBResult<bool> {
        self.sql.tx_immediate(|h| {
            let w = self.i_worker(h, id)?;
            if !w.is_held() {
                return Ok(false);
            }

            self.i_worker_control_event_insert(h, w.id, "hold released")?;

            let c = h.exec_update(
                Query::update()
                    .table(WorkerDef::Table)
                    .value(WorkerDef::HoldTime, Keyword::Null)
                    .value(WorkerDef::HoldReason, Keyword::Null)
                    .and_where(Expr::col(WorkerDef::Id).eq(w.id))
                    .and_where(Expr::col(WorkerDef::Deleted).eq(false))
                    .and_where(Expr::col(WorkerDef::HoldTime).is_not_null())
                    .and_where(Expr::col(WorkerDef::HoldReason).is_not_null())
                    .to_owned(),
            )?;
            assert_eq!(c, 1);

            Ok(true)
        })
    }

    pub fn worker_mark_failed(
        &self,
        id: WorkerId,
        reason: &str,
    ) -> DBResult<Vec<JobId>> {
        self.sql.tx_immediate(|h| {
            let w = self.i_worker(h, id)?;
            if w.recycle || w.deleted {
                /*
                 * This worker has already been marked for deletion.
                 */
                return Ok(Default::default());
            }

            /*
             * If a worker fails while a job was in flight, we need to report
             * that condition in the job event stream and then mark that job as
             * failed.
             */
            let mut failed_jobs: Vec<Job> = Default::default();
            for job in self.i_worker_jobs(h, id)? {
                if job.complete || job.failed || job.cancelled {
                    /*
                     * The job is already finished one way or another, so don't
                     * report anything here.
                     */
                    continue;
                }

                self.i_job_control_event_insert(
                    h,
                    job.id,
                    "worker agent experienced a fatal error; aborting job",
                )?;

                self.i_job_complete(h, &job, true)?;

                failed_jobs.push(job);
            }

            /*
             * Mark the worker as on hold so that an operator can inspect it.
             */
            let mut reason = reason.to_string();
            if !failed_jobs.is_empty() {
                reason.push_str(" during job execution");
            }
            self.i_worker_mark_on_hold(h, w.id, &reason)?;

            /*
             * Record the hold action in the per-worker event stream:
             */
            self.i_worker_control_event_insert(
                h,
                w.id,
                &format!("marked held: {reason}"),
            )?;

            /*
             * The worker must not be re-used for a new job, so mark it recycled
             * now.  The hold flag will prevent this from taking effect until
             * after the operator releases the hold.
             */
            let recycled = self.i_worker_recycle(h, w.id)?;
            assert!(recycled);

            for j in &failed_jobs {
                let seq = self
                    .i_job_event_max_seq(h, j.id)
                    .ok()
                    .flatten()
                    .unwrap_or(0);
                self.i_job_notify(h, j, seq, true, true);
            }

            Ok(failed_jobs.into_iter().map(|j| j.id).collect())
        })
    }

    pub fn worker_diagnostics_enable(&self, id: WorkerId) -> DBResult<()> {
        self.sql.tx_immediate(|h| {
            let w = self.i_worker(h, id)?;
            if w.deleted {
                /*
                 * This worker has already been marked for deletion.
                 */
                conflict!("worker {id} already deleted");
            }

            if w.diagnostics {
                /*
                 * It's possible that this is a retried request, so just ignore
                 * it.
                 */
                return Ok(());
            }

            self.i_worker_control_event_insert(
                h,
                w.id,
                "post-job diagnostics enabled",
            )?;

            h.exec_update(
                Query::update()
                    .table(WorkerDef::Table)
                    .value(WorkerDef::Diagnostics, true)
                    .and_where(Expr::col(WorkerDef::Id).eq(id))
                    .to_owned(),
            )?;

            Ok(())
        })
    }

    pub fn worker_diagnostics_complete(
        &self,
        id: WorkerId,
        hold: bool,
    ) -> DBResult<()> {
        self.sql.tx_immediate(|h| {
            let w = self.i_worker(h, id)?;
            if w.deleted {
                /*
                 * The "deleted" flag for workers is analogous to the "complete"
                 * flag for jobs; once set, the worker is immutable.
                 */
                conflict!("worker {id} already deleted");
            }

            if !w.diagnostics {
                /*
                 * It's possible that this is a retried request, so just ignore
                 * it.
                 */
                return Ok(());
            }

            let reason = "post-job diagnostics reported a fault";
            if hold && !w.is_held() {
                /*
                 * Mark the worker as on hold so that an operator can inspect
                 * it.
                 */
                self.i_worker_mark_on_hold(h, w.id, reason)?;

                /*
                 * Record the hold action in the per-worker event stream:
                 */
                self.i_worker_control_event_insert(
                    h,
                    w.id,
                    &format!("marked held: {reason}"),
                )?;
            } else if hold {
                /*
                 * Holds do not nest.  If the worker was already held by the
                 * time post-job diagnostics ran, just note the reported fault
                 * in the per-worker event stream.
                 */
                self.i_worker_control_event_insert(h, w.id, reason)?;
            }

            /*
             * Clear the diagnostics flag to allow the worker to be recycled.
             */
            h.exec_update(
                Query::update()
                    .table(WorkerDef::Table)
                    .value(WorkerDef::Diagnostics, false)
                    .and_where(Expr::col(WorkerDef::Id).eq(id))
                    .to_owned(),
            )?;

            Ok(())
        })
    }

    fn i_job_control_event_insert(
        &self,
        h: &mut Handle,
        id: JobId,
        msg: &str,
    ) -> DBResult<u32> {
        self.i_job_event_insert(h, id, None, "control", Utc::now(), None, msg)
    }

    pub fn i_worker_assign_job(
        &self,
        h: &mut Handle,
        w: &Worker,
        jid: JobId,
    ) -> DBResult<()> {
        let j: Job = h.get_row(Job::find(jid))?;
        if let Some(jw) = j.worker.as_ref() {
            conflict!("job {} already assigned to worker {}", j.id, jw);
        }

        let c: usize = {
            h.get_row(
                Query::select()
                    .from(JobDef::Table)
                    .expr(Expr::col(Asterisk).count())
                    .and_where(Expr::col(JobDef::Worker).eq(w.id))
                    .to_owned(),
            )?
        };
        if c > 0 {
            conflict!("worker {} already has {} jobs assigned", w.id, c);
        }

        let uc = h.exec_update(
            Query::update()
                .table(JobDef::Table)
                .values([(JobDef::Worker, w.id.into())])
                .and_where(Expr::col(JobDef::Id).eq(j.id))
                .and_where(Expr::col(JobDef::Worker).is_null())
                .to_owned(),
        )?;
        assert_eq!(uc, 1);

        /*
         * Estimate how long the job was waiting in the queue for a worker.
         */
        self.i_job_time_record(h, j.id, "assigned", Utc::now())?;
        let wait = if let Some(dur) =
            self.i_job_time_delta(h, j.id, "ready", "assigned")?
        {
            format!(" (queued for {})", dur.render())
        } else if let Ok(dur) =
            Utc::now().signed_duration_since(j.id.datetime()).to_std()
        {
            format!(" (queued for {})", dur.render())
        } else {
            "".to_string()
        };

        /*
         * Report the assignment in a message that appears in the job event
         * stream.  Try to include some details about the worker; e.g., the name
         * of the factory that created it and the factory-private identifier for
         * the backing resource.
         */
        let mut extra = Vec::new();
        if let Some(fid) = w.factory {
            let f = self.i_factory(h, fid)?;
            extra.push(format!("factory {}", f.name));
        }
        if let Some(fp) = w.factory_private.as_deref() {
            extra.push(fp.to_string());
        }
        let extra = if extra.is_empty() {
            "".to_string()
        } else {
            format!(" [{}]", extra.join(", "))
        };

        let seq = self.i_job_control_event_insert(
            h,
            j.id,
            &format!("job assigned to worker {}{extra}{wait}", w.id),
        )?;

        self.i_job_notify(h, &j, seq, false, true);
        Ok(())
    }

    pub fn worker_assign_job(&self, wid: WorkerId, jid: JobId) -> DBResult<()> {
        self.sql.tx_immediate(|h| {
            let w = self.i_worker(h, wid)?;
            if w.deleted || w.recycle {
                conflict!("worker {} already deleted, cannot assign job", w.id);
            }

            if w.is_held() {
                conflict!("worker {} is on hold, cannot assign job", w.id);
            }

            self.i_worker_assign_job(h, &w, jid)?;

            Ok(())
        })
    }

    pub fn worker_bootstrap(
        &self,
        bootstrap: &str,
        token: &str,
    ) -> DBResult<Option<Worker>> {
        self.sql.tx_immediate(|h| {
            let w = self.i_worker_for_bootstrap(h, bootstrap)?;
            if w.deleted {
                error!(
                    self.log,
                    "worker {} already deleted, cannot bootstrap", w.id
                );
                return Ok(None);
            }

            if w.factory_private.is_none() {
                error!(
                    self.log,
                    "worker {} has no instance, cannot bootstrap", w.id
                );
                return Ok(None);
            }

            if let Some(current) = w.token.as_deref() {
                if current == token {
                    /*
                     * Everything is as expected.
                     */
                } else {
                    /*
                     * A conflict exists?
                     */
                    error!(
                        self.log,
                        "worker {} already has a token ({} != {})",
                        w.id,
                        current,
                        token
                    );
                    return Ok(None);
                }
            } else {
                let count = h.exec_update(
                    Query::update()
                        .table(WorkerDef::Table)
                        .values([
                            (WorkerDef::Token, token.into()),
                            (WorkerDef::Lastping, IsoDate(Utc::now()).into()),
                        ])
                        .and_where(Expr::col(WorkerDef::Id).eq(w.id))
                        .and_where(
                            Expr::col(WorkerDef::Bootstrap).eq(bootstrap),
                        )
                        .and_where(Expr::col(WorkerDef::Token).is_null())
                        .to_owned(),
                )?;
                assert_eq!(count, 1);
            }

            let out = self.i_worker(h, w.id)?;

            Ok(Some(out))
        })
    }

    pub fn worker_associate(
        &self,
        wid: WorkerId,
        factory_private: &str,
        factory_metadata: Option<&metadata::FactoryMetadata>,
        factory_ip: Option<&str>,
    ) -> DBResult<()> {
        /*
         * First, convert the provided metadata object to a serde JSON value.
         * We'll use this (rather than the concrete string) for comparison with
         * an existing record because the string might not be serialised
         * byte-for-byte the same each time.
         */
        let factory_metadata = factory_metadata
            .as_ref()
            .map(serde_json::to_value)
            .transpose()?
            .map(JsonValue);

        self.sql.tx_immediate(|h| {
            let w: Worker = h.get_row(Worker::find(wid))?;
            if w.deleted {
                conflict!("worker {} already deleted, cannot associate", w.id);
            }

            if let Some(current) = w.factory_private.as_deref() {
                if current == factory_private {
                    /*
                     * Everything is as expected.
                     */
                } else {
                    /*
                     * A conflict exists?
                     */
                    conflict!(
                        "worker {} already associated with instance {} not {}",
                        w.id,
                        current,
                        factory_private
                    );
                }
            } else {
                /*
                 * The worker is not yet associated with an instance ID.
                 */
                let count = h.exec_update(
                    Query::update()
                        .table(WorkerDef::Table)
                        .and_where(Expr::col(WorkerDef::Id).eq(w.id))
                        .value(WorkerDef::FactoryPrivate, factory_private)
                        .to_owned(),
                )?;
                assert_eq!(count, 1);
            }

            /*
             * The IP address of the worker is only present here for diagnostic
             * purposes, so allow it to change if needed without raising an
             * error:
             */
            if let Some(factory_ip) = factory_ip {
                let update_ip = if let Some(current) = w.factory_ip.as_deref() {
                    current != factory_ip
                } else {
                    true
                };

                if update_ip {
                    let count = h.exec_update(
                        Query::update()
                            .table(WorkerDef::Table)
                            .and_where(Expr::col(WorkerDef::Id).eq(w.id))
                            .value(WorkerDef::FactoryIp, factory_ip)
                            .to_owned(),
                    )?;
                    assert_eq!(count, 1);
                }
            }

            if let Some(current) = w.factory_metadata.as_ref() {
                /*
                 * The worker record already has factory metadata.
                 */
                if let Some(new) = factory_metadata.as_ref() {
                    if current.0 == new.0 {
                        /*
                         * The factory metadata in the database matches what we
                         * have already.
                         */
                    } else {
                        conflict!(
                            "worker {} factory metadata mismatch: {:?} != {:?}",
                            w.id,
                            current,
                            factory_metadata,
                        );
                    }
                }
            } else if let Some(factory_metadata) = factory_metadata {
                /*
                 * Store the factory metadata for this worker:
                 */
                let count = h.exec_update(
                    Query::update()
                        .table(WorkerDef::Table)
                        .and_where(Expr::col(WorkerDef::Id).eq(w.id))
                        .value(WorkerDef::FactoryMetadata, factory_metadata)
                        .to_owned(),
                )?;
                assert_eq!(count, 1);
            }

            Ok(())
        })
    }

    pub fn worker(&self, id: WorkerId) -> DBResult<Worker> {
        self.sql.tx(|h| {
            h.get_row(
                Query::select()
                    .from(WorkerDef::Table)
                    .columns(Worker::columns())
                    .and_where(Expr::col(WorkerDef::Id).eq(id))
                    .to_owned(),
            )
        })
    }

    pub fn worker_opt(&self, id: WorkerId) -> DBResult<Option<Worker>> {
        self.sql.tx(|h| {
            h.get_row_opt(
                Query::select()
                    .from(WorkerDef::Table)
                    .columns(Worker::columns())
                    .and_where(Expr::col(WorkerDef::Id).eq(id))
                    .to_owned(),
            )
        })
    }

    pub fn worker_auth(&self, token: &str) -> DBResult<Worker> {
        self.sql.tx(|h| {
            let mut workers = h.get_rows::<Worker>(
                Query::select()
                    .from(WorkerDef::Table)
                    .columns(Worker::columns())
                    .and_where(Expr::col(WorkerDef::Token).eq(token))
                    .and_where(Expr::col(WorkerDef::Deleted).eq(false))
                    .to_owned(),
            )?;

            match (workers.pop(), workers.pop()) {
                (None, _) => conflict!("auth failure"),
                (Some(u), Some(x)) => {
                    conflict!("token error ({}, {})", u.id, x.id)
                }
                (Some(u), None) => {
                    assert_eq!(u.token.as_deref(), Some(token));
                    Ok(u)
                }
            }
        })
    }

    pub fn worker_create(
        &self,
        factory: &Factory,
        target: &Target,
        job: Option<JobId>,
        wait_for_flush: bool,
        hold_reason: Option<&str>,
    ) -> DBResult<Worker> {
        /*
         * Allow a held worker to be created in the held state for debugging
         * purposes.
         */
        let (hold_time, hold_reason) = if let Some(hr) = hold_reason {
            (Some(IsoDate::now()), Some(hr.to_string()))
        } else {
            (None, None)
        };

        let w = Worker {
            id: WorkerId::generate(),
            bootstrap: genkey(64),
            factory_private: None,
            factory_ip: None,
            factory_metadata: None,
            token: None,
            deleted: false,
            recycle: false,
            lastping: None,
            factory: Some(factory.id),
            target: Some(target.id),
            wait_for_flush,
            hold_time,
            hold_reason,
            diagnostics: false,
        };

        self.sql.tx(|h| {
            let count = h.exec_insert(w.insert())?;
            assert_eq!(count, 1);

            /*
             * If this is a concrete target, we will be given the job ID at
             * worker creation time.  This allows the factory to reserve the
             * specific job and use the job configuration for target-specific
             * pre-setup.
             *
             * If no job was specified, this will be an ephemeral target that
             * will be assigned later by the job assignment task.
             */
            if let Some(job) = job {
                self.i_worker_assign_job(h, &w, job)?;
            }

            Ok(w)
        })
    }

    pub fn worker_events(
        &self,
        worker: WorkerId,
        minseq: usize,
        limit: u64,
    ) -> DBResult<Vec<WorkerEvent>> {
        self.sql.tx(|h| {
            h.get_rows(
                Query::select()
                    .from(WorkerEventDef::Table)
                    .columns(WorkerEvent::columns())
                    .order_by(WorkerEventDef::Seq, Order::Asc)
                    .and_where(Expr::col(WorkerEventDef::Worker).eq(worker))
                    .and_where(
                        Expr::col(WorkerEventDef::Seq).gte(minseq as i64),
                    )
                    .limit(limit)
                    .to_owned(),
            )
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn i_worker_event_insert(
        &self,
        h: &mut Handle,
        worker: WorkerId,
        stream: &str,
        time: DateTime<Utc>,
        time_remote: Option<DateTime<Utc>>,
        payload: &str,
    ) -> DBResult<()> {
        let max: Option<u32> = h.get_row(
            Query::select()
                .from(WorkerEventDef::Table)
                .and_where(Expr::col(WorkerEventDef::Worker).eq(worker))
                .expr(Expr::col(WorkerEventDef::Seq).max())
                .to_owned(),
        )?;

        let ic = h.exec_insert(
            WorkerEvent {
                worker,
                seq: max.unwrap_or(0) + 1,
                stream: stream.to_string(),
                time: IsoDate(time),
                time_remote: time_remote.map(IsoDate),
                payload: payload.to_string(),
            }
            .insert(),
        )?;
        assert_eq!(ic, 1);

        Ok(())
    }

    fn i_worker_control_event_insert(
        &self,
        h: &mut Handle,
        id: WorkerId,
        msg: &str,
    ) -> DBResult<()> {
        self.i_worker_event_insert(h, id, "control", Utc::now(), None, msg)
    }

    pub fn worker_append_events(
        &self,
        worker: WorkerId,
        events: impl Iterator<Item = CreateWorkerEvent>,
    ) -> DBResult<()> {
        self.sql.tx_immediate(|h| {
            let w: Worker = h.get_row(Worker::find(worker))?;
            if w.deleted {
                conflict!("worker already deleted, cannot append");
            }

            for e in events {
                self.i_worker_event_insert(
                    h,
                    w.id,
                    &e.stream,
                    e.time,
                    e.time_remote,
                    &e.payload,
                )?;
            }

            Ok(())
        })
    }

    /**
     * Fetch a page of jobs from the database according to the specified
     * filters.
     */
    pub fn jobs_page(
        &self,
        ascending: bool,
        marker: Option<JobId>,
        limit: usize,
        owner: Option<UserId>,
        complete: Option<bool>,
        waiting: Option<bool>,
        tags: Option<Vec<(String, String)>>,
    ) -> DBResult<Vec<Job>> {
        let mut q = Query::select()
            .from(JobDef::Table)
            .columns(Job::columns())
            .and_where_option(
                owner
                    .map(|id| Expr::col((JobDef::Table, JobDef::Owner)).eq(id)),
            )
            .and_where_option(
                complete.map(|v| {
                    Expr::col((JobDef::Table, JobDef::Complete)).eq(v)
                }),
            )
            .and_where_option(
                waiting
                    .map(|v| Expr::col((JobDef::Table, JobDef::Waiting)).eq(v)),
            )
            .conditions(
                ascending,
                |q| {
                    q.and_where_option(marker.map(|id| {
                        Expr::col((JobDef::Table, JobDef::Id)).gt(id)
                    }));
                },
                |q| {
                    q.and_where_option(marker.map(|id| {
                        Expr::col((JobDef::Table, JobDef::Id)).lt(id)
                    }));
                },
            )
            .limit(limit.try_into().unwrap())
            .to_owned();

        let q = if let Some(tags) = tags {
            if tags.is_empty() {
                conflict!("if tags are specified the list must not be empty");
            }

            if tags.len() > 8 {
                conflict!("too many tags specified");
            }

            /*
             * Build up the conjunction of all tag-value queries:
             */
            let mut tagcond = Cond::all();

            /*
             * Querying by tag is complex: each tag-value pair is in its own
             * row, but we only want a single row per job in the output if _all_
             * of the requested tag-value filters match.  To achieve this, we
             * join with the tags table multiple times, as if there were a table
             * per tag name.  Care must be taken to ensure that we can actually
             * use the indexes on the tag table to keep this relatively cheap.
             */
            for (i, (k, v)) in tags.into_iter().enumerate() {
                /*
                 * We need to be able to refer to each alias of the tags table
                 * that appears in the join.  Create a unique alias:
                 */
                let tagn: SeaRc<dyn Iden> = SeaRc::new(Alias::new(format!(
                    "{}{i}",
                    JobTagDef::Table.to_string(),
                )));

                tagcond = tagcond.add(all![
                    Expr::col((tagn.clone(), JobTagDef::Name)).eq(k),
                    Expr::col((tagn.clone(), JobTagDef::Value)).eq(v),
                ]);

                /*
                 * Add the join for this tag-value filter using the alias we
                 * created:
                 */
                q = q
                    .inner_join(
                        TableRef::TableAlias(
                            SeaRc::new(JobTagDef::Table),
                            tagn.clone(),
                        ),
                        Expr::col((tagn.clone(), JobTagDef::Job))
                            .eq(Expr::col((JobDef::Table, JobDef::Id))),
                    )
                    .to_owned();

                /*
                 * If this is the first alias in the list, use the job field to
                 * sort.  Using the job field from the tags table, rather than
                 * the ID field from the jobs table, appears to convince SQLite
                 * that it need not do a temporary sort at the end.
                 */
                if i == 0 {
                    q = q
                        .order_by(
                            (tagn.clone(), JobTagDef::Job),
                            if ascending { Order::Asc } else { Order::Desc },
                        )
                        .to_owned();
                }
            }

            q.cond_where(tagcond).to_owned()
        } else {
            q.order_by(
                (JobDef::Table, JobDef::Id),
                if ascending { Order::Asc } else { Order::Desc },
            )
            .to_owned()
        };

        self.sql.tx(|h| h.get_rows(q))
    }

    /**
     * Enumerate jobs that are active; i.e., not yet complete, but not waiting.
     */
    pub fn jobs_active(&self, limit: usize) -> DBResult<Vec<Job>> {
        self.jobs_page(true, None, limit, None, Some(false), Some(false), None)
    }

    /**
     * Enumerate jobs that are waiting for inputs, or for dependees to complete.
     */
    pub fn jobs_waiting(&self, limit: usize) -> DBResult<Vec<Job>> {
        self.jobs_page(true, None, limit, None, Some(false), Some(true), None)
    }

    /**
     * Enumerate some number of the most recently complete jobs.
     */
    pub fn jobs_completed(&self, limit: usize) -> DBResult<Vec<Job>> {
        let mut res =
            self.jobs_page(false, None, limit, None, Some(true), None, None)?;

        res.reverse();

        Ok(res)
    }

    fn q_job_tasks(&self, job: JobId) -> SelectStatement {
        Query::select()
            .from(TaskDef::Table)
            .columns(Task::columns())
            .order_by(TaskDef::Seq, Order::Asc)
            .and_where(Expr::col(TaskDef::Job).eq(job))
            .to_owned()
    }

    pub fn job_tasks(&self, job: JobId) -> DBResult<Vec<Task>> {
        self.sql.tx(|h| h.get_rows(self.q_job_tasks(job)))
    }

    pub fn job_tags(&self, job: JobId) -> DBResult<HashMap<String, String>> {
        self.sql.tx(|h| {
            Ok(h.get_rows::<JobTag>(
                Query::select()
                    .from(JobTagDef::Table)
                    .columns(JobTag::columns())
                    .and_where(Expr::col(JobTagDef::Job).eq(job))
                    .to_owned(),
            )?
            .into_iter()
            .map(|jt| (jt.name, jt.value))
            .collect())
        })
    }

    pub fn job_output_rules(&self, job: JobId) -> DBResult<Vec<JobOutputRule>> {
        self.sql.tx(|h| {
            h.get_rows(
                Query::select()
                    .from(JobOutputRuleDef::Table)
                    .columns(JobOutputRule::columns())
                    .order_by(JobOutputRuleDef::Seq, Order::Asc)
                    .and_where(Expr::col(JobOutputRuleDef::Job).eq(job))
                    .to_owned(),
            )
        })
    }

    pub fn job_depends(&self, job: JobId) -> DBResult<Vec<JobDepend>> {
        self.sql.tx(|h| {
            h.get_rows(
                Query::select()
                    .from(JobDependDef::Table)
                    .columns(JobDepend::columns())
                    .order_by(JobDependDef::Name, Order::Asc)
                    .and_where(Expr::col(JobDependDef::Job).eq(job))
                    .to_owned(),
            )
        })
    }

    /**
     * When a prior job has completed, we need to mark the dependency record as
     * satisfied.  In the process, if requested, we will copy any output files
     * from the previous job into the new job as input files.
     */
    pub fn job_depend_satisfy(
        &self,
        jid: JobId,
        d: &JobDepend,
    ) -> DBResult<()> {
        self.sql.tx_immediate(|h| {
            /*
             * Confirm that this job exists and is still waiting.
             */
            let j: Job = h.get_row(Job::find(jid))?;
            if !j.waiting {
                conflict!("job not waiting, cannot satisfy dependency");
            }

            /*
             * Confirm that the dependency record still exists and has not yet
             * been satisfied.
             */
            let d: JobDepend = h.get_row(JobDepend::find(j.id, &d.name))?;
            if d.satisfied {
                conflict!("job dependency already satisfied");
            }

            /*
             * Confirm that the prior job exists and is complete.
             */
            let pj: Job = h.get_row(Job::find(d.prior_job))?;
            if !pj.complete {
                conflict!("prior job not complete");
            }

            if d.copy_outputs {
                /*
                 * Resolve the list of output files.
                 */
                let pjouts = self.i_job_outputs(h, pj.id)?;

                /*
                 * For each output file produced by the dependency, create an
                 * input record for this job.
                 */
                for JobOutputAndFile { output: pjo, file: pjf } in pjouts {
                    /*
                     * These input files will be placed under a directory named
                     * for the dependency; e.g., If the dependency produced a
                     * file "/work/file.txt", and the dependency was named
                     * "builds", we want to create an input named
                     * "builds/work/file.txt", which will result in a file at
                     * "/input/builds/work/file.txt" in the job runner.
                     */
                    let mut name = d.name.trim().to_string();
                    if !pjo.path.starts_with('/') {
                        name.push('/');
                    }
                    name.push_str(&pjo.path);

                    /*
                     * Note that job files are named by a (job ID, file ID)
                     * tuple.  In the case of a copied output, we need to use
                     * the "other_job" field to refer to the job which holds the
                     * output file we are referencing.
                     */
                    let ji = JobInput {
                        job: j.id,
                        name,
                        id: Some(pjf.id),
                        other_job: Some(pjf.job),
                    };

                    h.exec_insert(ji.upsert())?;
                }
            }

            let uc = h.exec_update(
                Query::update()
                    .table(JobDependDef::Table)
                    .and_where(Expr::col(JobDependDef::Job).eq(j.id))
                    .and_where(Expr::col(JobDependDef::Name).eq(&d.name))
                    .value(JobDependDef::Satisfied, true)
                    .to_owned(),
            )?;
            assert_eq!(uc, 1);

            Ok(())
        })
    }

    pub fn job_inputs(&self, job: JobId) -> DBResult<Vec<JobInputAndFile>> {
        self.sql.tx(|h| {
            h.get_rows(
                JobInputAndFile::base_query()
                    .and_where(
                        Expr::col((JobInputDef::Table, JobInputDef::Job))
                            .eq(job),
                    )
                    .order_by((JobInputDef::Table, JobInputDef::Id), Order::Asc)
                    .to_owned(),
            )
        })
    }

    fn i_job_outputs(
        &self,
        h: &mut Handle,
        job: JobId,
    ) -> DBResult<Vec<JobOutputAndFile>> {
        h.get_rows(
            JobOutputAndFile::base_query()
                .and_where(
                    Expr::col((JobFileDef::Table, JobFileDef::Job)).eq(job),
                )
                .order_by((JobFileDef::Table, JobFileDef::Id), Order::Asc)
                .to_owned(),
        )
    }

    pub fn job_outputs(&self, job: JobId) -> DBResult<Vec<JobOutputAndFile>> {
        self.sql.tx(|h| self.i_job_outputs(h, job))
    }

    pub fn job_file(&self, job: JobId, file: JobFileId) -> DBResult<JobFile> {
        self.sql.tx(|h| h.get_row(JobFile::find(job, file)))
    }

    pub fn job_file_opt(
        &self,
        job: JobId,
        file: JobFileId,
    ) -> DBResult<Option<JobFile>> {
        self.sql.tx(|h| h.get_row_opt(JobFile::find(job, file)))
    }

    pub fn job_events(
        &self,
        job: JobId,
        minseq: usize,
        limit: u64,
    ) -> DBResult<Vec<JobEvent>> {
        self.sql.tx(|h| {
            h.get_rows(
                Query::select()
                    .from(JobEventDef::Table)
                    .columns(JobEvent::columns())
                    .order_by(JobEventDef::Seq, Order::Asc)
                    .and_where(Expr::col(JobEventDef::Job).eq(job))
                    .and_where(Expr::col(JobEventDef::Seq).gte(minseq as i64))
                    .limit(limit)
                    .to_owned(),
            )
        })
    }

    pub fn job(&self, job: JobId) -> DBResult<Job> {
        self.sql.tx(|h| h.get_row(Job::find(job)))
    }

    pub fn job_opt(&self, job: JobId) -> DBResult<Option<Job>> {
        self.sql.tx(|h| h.get_row_opt(Job::find(job)))
    }

    pub fn job_create<I>(
        &self,
        owner: UserId,
        name: &str,
        target_name: &str,
        target: TargetId,
        tasks: Vec<CreateTask>,
        output_rules: Vec<CreateOutputRule>,
        inputs: &[String],
        tags: I,
        depends: Vec<CreateDepend>,
    ) -> DBResult<Job>
    where
        I: IntoIterator<Item = (String, String)>,
    {
        if tasks.is_empty() {
            conflict!("a job must have at least one task");
        }
        if tasks.len() > 64 {
            conflict!("a job must have 64 or fewer tasks");
        }

        if depends.len() > 8 {
            conflict!("a job must depend on 8 or fewer other jobs");
        }
        for cd in depends.iter() {
            if cd.name.contains('/') || cd.name.trim().is_empty() {
                conflict!("invalid depend name");
            }

            if !cd.on_failed && !cd.on_completed {
                conflict!("depend must have at least one trigger condition");
            }
        }

        if inputs.len() > 32 {
            conflict!("a job must have 32 or fewer input files");
        }
        for ci in inputs.iter() {
            if ci.contains('/') || ci.trim().is_empty() {
                conflict!("invalid input name");
            }
        }

        /*
         * If the job has any input files or any jobs on which it depends, it
         * begins in the "waiting" state.  Otherwise it can begin immediately.
         */
        let waiting = !inputs.is_empty() || !depends.is_empty();

        let j = Job {
            id: JobId::generate(),
            owner,
            name: name.to_string(),
            target: target_name.to_string(),
            target_id: Some(target),
            waiting,
            complete: false,
            failed: false,
            worker: None,
            cancelled: false,
            time_archived: None,
            time_purged: None,
        };

        /*
         * Use the timestamp from the generated ID as the submit time.
         */
        let start = j.id.datetime();

        self.sql.tx_immediate(|h| {
            let ic = h.exec_insert(j.insert())?;
            assert_eq!(ic, 1);

            self.i_job_time_record(h, j.id, "submit", start)?;
            if !waiting {
                /*
                 * If the job is not waiting, record the submit time as the time
                 * at which dependencies are satisfied and the job is ready to
                 * run.
                 */
                self.i_job_time_record(h, j.id, "ready", start)?;
            }

            for (i, ct) in tasks.iter().enumerate() {
                let ic =
                    h.exec_insert(Task::from_create(ct, j.id, i).insert())?;
                assert_eq!(ic, 1);
            }

            for cd in depends.iter() {
                /*
                 * Make sure that this job exists in the system and that its
                 * owner matches the owner of this job.  By requiring prior jobs
                 * to exist already at the time of dependency specification we
                 * can avoid the mess of cycles in the dependency graph.
                 */
                let pj: Option<Job> = h.get_row_opt(Job::find(cd.prior_job))?;

                if !pj.map(|j| j.owner == owner).unwrap_or(false) {
                    /*
                     * Try not to leak information about job IDs from other
                     * users in the process.
                     */
                    conflict!("prior job does not exist");
                }

                let ic =
                    h.exec_insert(JobDepend::from_create(cd, j.id).insert())?;
                assert_eq!(ic, 1);
            }

            for ci in inputs.iter() {
                let ic = h.exec_insert(
                    JobInput::from_create(ci.as_str(), j.id).insert(),
                )?;
                assert_eq!(ic, 1);
            }

            for (i, rule) in output_rules.iter().enumerate() {
                let ic = h.exec_insert(
                    JobOutputRule::from_create(rule, j.id, i).insert(),
                )?;
                assert_eq!(ic, 1);
            }

            for (n, v) in tags {
                let ic = h.exec_insert(
                    Query::insert()
                        .into_table(JobTagDef::Table)
                        .columns([
                            JobTagDef::Job,
                            JobTagDef::Name,
                            JobTagDef::Value,
                        ])
                        .values_panic([j.id.into(), n.into(), v.into()])
                        .to_owned(),
                )?;
                assert_eq!(ic, 1);
            }

            self.i_job_notify(h, &j, 0, false, true);
            Ok(j)
        })
    }

    pub fn job_purge(&self, job: JobId) -> DBResult<()> {
        self.sql.tx_immediate(|h| {
            let j: Job = h.get_row(Job::find(job))?;
            if !j.is_archived() {
                conflict!("job must be archived before being purged");
            }
            assert!(j.complete);

            /*
             * Delete records from ancillary tables that contain data we will
             * subsequently retrieve from the archive instead of the live
             * database.
             *
             * We are intentionally leaving "job" and "published_file" table
             * records in place in the database.
             */
            let deletes = [
                (
                    JobDependDef::Table.into_table_ref(),
                    Expr::col(JobDependDef::Job),
                ),
                (
                    JobEventDef::Table.into_table_ref(),
                    Expr::col(JobEventDef::Job),
                ),
                (
                    JobFileDef::Table.into_table_ref(),
                    Expr::col(JobFileDef::Job),
                ),
                (
                    JobInputDef::Table.into_table_ref(),
                    Expr::col(JobInputDef::Job),
                ),
                (
                    JobOutputDef::Table.into_table_ref(),
                    Expr::col(JobOutputDef::Job),
                ),
                (
                    JobOutputRuleDef::Table.into_table_ref(),
                    Expr::col(JobOutputRuleDef::Job),
                ),
                (
                    JobStoreDef::Table.into_table_ref(),
                    Expr::col(JobStoreDef::Job),
                ),
                (JobTagDef::Table.into_table_ref(), Expr::col(JobTagDef::Job)),
                (
                    JobTimeDef::Table.into_table_ref(),
                    Expr::col(JobTimeDef::Job),
                ),
                (TaskDef::Table.into_table_ref(), Expr::col(TaskDef::Job)),
            ];

            for (tab, col) in deletes {
                h.exec_delete(
                    Query::delete()
                        .from_table(tab)
                        .and_where(col.eq(job))
                        .to_owned(),
                )?;
            }

            /*
             * Mark the job as purged in the same transaction that we used to
             * remove the ancillary records:
             */
            let uc = h.exec_update(
                Query::update()
                    .table(JobDef::Table)
                    .and_where(Expr::col(JobDef::Id).eq(job))
                    .and_where(Expr::col(JobDef::TimePurged).is_null())
                    .value(JobDef::TimePurged, IsoDate::now())
                    .to_owned(),
            )?;
            assert_eq!(uc, 1);

            Ok(())
        })
    }

    pub fn job_input(&self, job: JobId, file: JobFileId) -> DBResult<JobInput> {
        self.sql.tx(|h| {
            h.get_row(
                Query::select()
                    .from(JobInputDef::Table)
                    .columns(JobInput::columns())
                    .and_where(Expr::col(JobInputDef::Job).eq(job))
                    .and_where(Expr::col(JobInputDef::Id).eq(file))
                    .to_owned(),
            )
        })
    }

    pub fn job_output(
        &self,
        job: JobId,
        file: JobFileId,
    ) -> DBResult<JobOutput> {
        self.sql.tx(|h| {
            h.get_row(
                Query::select()
                    .from(JobOutputDef::Table)
                    .columns(JobOutput::columns())
                    .and_where(Expr::col(JobOutputDef::Job).eq(job))
                    .and_where(Expr::col(JobOutputDef::Id).eq(file))
                    .to_owned(),
            )
        })
    }

    pub fn published_file_by_name(
        &self,
        owner: UserId,
        series: &str,
        version: &str,
        name: &str,
    ) -> DBResult<Option<PublishedFile>> {
        self.sql.tx(|h| {
            h.get_row_opt(PublishedFile::find(owner, series, version, name))
        })
    }

    pub fn job_publish_output(
        &self,
        job: JobId,
        file: JobFileId,
        series: &str,
        version: &str,
        name: &str,
    ) -> DBResult<()> {
        self.sql.tx_immediate(|h| {
            let j: Job = h.get_row(Job::find(job))?;
            if !j.complete {
                conflict!("job must be complete before files are published");
            }

            /*
             * Make sure this output exists.
             */
            let _jo: JobOutput = h.get_row(JobOutput::find(job, file))?;

            /*
             * Determine whether this record has been published already or not.
             */
            let pf: Option<PublishedFile> = h.get_row_opt(
                PublishedFile::find(j.owner, series, version, name),
            )?;

            if let Some(pf) = pf {
                if pf.owner == j.owner && pf.job == job && pf.file == file {
                    /*
                     * The target file is the same, so just succeed.
                     */
                    return Ok(());
                } else {
                    conflict!(
                        "that published file already exists with \
                    different contents"
                    );
                }
            }

            let ic = h.exec_insert(
                PublishedFile {
                    owner: j.owner,
                    job,
                    file,
                    series: series.to_string(),
                    version: version.to_string(),
                    name: name.to_string(),
                }
                .insert(),
            )?;
            assert!(ic == 1);

            Ok(())
        })
    }

    pub fn job_add_output(
        &self,
        job: JobId,
        path: &str,
        id: JobFileId,
        size: u64,
    ) -> DBResult<()> {
        self.sql.tx_immediate(|h| {
            let j: Job = h.get_row(Job::find(job))?;
            if j.complete {
                conflict!("job already complete, cannot add more files");
            }

            let ic = h.exec_insert(
                JobFile { job, id, size: DataSize(size), time_archived: None }
                    .insert(),
            )?;
            assert_eq!(ic, 1);

            let ic = h.exec_insert(
                JobOutput { job, path: path.to_string(), id }.insert(),
            )?;
            assert_eq!(ic, 1);

            Ok(())
        })
    }

    pub fn job_add_input(
        &self,
        job: JobId,
        name: &str,
        id: JobFileId,
        size: u64,
    ) -> DBResult<()> {
        if name.contains('/') {
            conflict!("name cannot be a path");
        }

        self.sql.tx_immediate(|h| {
            let j: Job = h.get_row(Job::find(job))?;
            if !j.waiting {
                conflict!("job not waiting, cannot add more inputs");
            }

            let ic = h.exec_insert(
                JobFile { job, id, size: DataSize(size), time_archived: None }
                    .insert(),
            )?;
            assert_eq!(ic, 1);

            let uc = h.exec_update(
                Query::update()
                    .table(JobInputDef::Table)
                    .and_where(Expr::col(JobInputDef::Job).eq(job))
                    .and_where(Expr::col(JobInputDef::Name).eq(name))
                    .value(JobInputDef::Id, id)
                    .to_owned(),
            )?;
            assert_eq!(uc, 1);

            Ok(())
        })
    }

    pub fn job_next_unpurged(&self) -> DBResult<Option<Job>> {
        /*
         * Find the oldest archived job that has not yet been purged from the
         * live database.
         */
        self.sql.tx(|h| {
            h.get_row_opt(
                Query::select()
                    .from(JobDef::Table)
                    .columns(Job::columns())
                    .order_by(JobDef::Id, Order::Asc)
                    .and_where(Expr::col(JobDef::Complete).eq(true))
                    .and_where(Expr::col(JobDef::TimeArchived).is_not_null())
                    .and_where(Expr::col(JobDef::TimePurged).is_null())
                    .limit(1)
                    .to_owned(),
            )
        })
    }

    pub fn job_next_unarchived(&self) -> DBResult<Option<Job>> {
        /*
         * Find the oldest completed job that has not yet been archived to long
         * term storage.
         */
        self.sql.tx(|h| {
            h.get_row_opt(
                Query::select()
                    .from(JobDef::Table)
                    .columns(Job::columns())
                    .order_by(JobDef::Id, Order::Asc)
                    .and_where(Expr::col(JobDef::Complete).eq(true))
                    .and_where(Expr::col(JobDef::TimeArchived).is_null())
                    .limit(1)
                    .to_owned(),
            )
        })
    }

    pub fn job_mark_archived(
        &self,
        job: JobId,
        time: DateTime<Utc>,
    ) -> DBResult<()> {
        self.sql.tx(|h| {
            let uc = h.exec_update(
                Query::update()
                    .table(JobDef::Table)
                    .and_where(Expr::col(JobDef::Id).eq(job))
                    .and_where(Expr::col(JobDef::TimeArchived).is_null())
                    .value(JobDef::TimeArchived, IsoDate(time))
                    .to_owned(),
            )?;
            assert_eq!(uc, 1);

            Ok(())
        })
    }

    pub fn job_file_next_unarchived(&self) -> DBResult<Option<JobFile>> {
        /*
         * Find the most recently uploaded output stored as part of a job that
         * has been completed.
         */
        self.sql.tx(|h| {
            h.get_row_opt(
                Query::select()
                    .from(JobFileDef::Table)
                    .inner_join(
                        JobDef::Table,
                        Expr::col((JobDef::Table, JobDef::Id)).eq(Expr::col((
                            JobFileDef::Table,
                            JobFileDef::Job,
                        ))),
                    )
                    .columns(JobFile::columns())
                    .order_by((JobFileDef::Table, JobFileDef::Id), Order::Asc)
                    .and_where(
                        Expr::col((JobDef::Table, JobDef::Complete)).eq(true),
                    )
                    .and_where(
                        Expr::col((
                            JobFileDef::Table,
                            JobFileDef::TimeArchived,
                        ))
                        .is_null(),
                    )
                    .limit(1)
                    .to_owned(),
            )
        })
    }

    pub fn job_file_mark_archived(
        &self,
        file: &JobFile,
        time: DateTime<Utc>,
    ) -> DBResult<()> {
        self.sql.tx(|h| {
            let uc = h.exec_update(
                Query::update()
                    .table(JobFileDef::Table)
                    .and_where(Expr::col(JobFileDef::Job).eq(file.job))
                    .and_where(Expr::col(JobFileDef::Id).eq(file.id))
                    .and_where(Expr::col(JobFileDef::TimeArchived).is_null())
                    .value(JobFileDef::TimeArchived, IsoDate(time))
                    .to_owned(),
            )?;
            assert_eq!(uc, 1);

            Ok(())
        })
    }

    pub fn job_subscribe(
        &self,
        job: &Job,
    ) -> Option<watch::Receiver<JobNotification>> {
        let js = self.job_subscriptions.lock().unwrap();
        js.get(&job.id).map(|tx| tx.subscribe())
    }

    fn i_job_notify(
        &self,
        _h: &mut Handle,
        job: &Job,
        seq: u32,
        complete: bool,
        state_change: bool,
    ) {
        let mut js = self.job_subscriptions.lock().unwrap();

        if let Some(s) = js.get(&job.id) {
            s.send_if_modified(|cur| {
                assert_eq!(job.id, cur.id);

                let gen = if state_change { cur.gen + 1 } else { cur.gen };

                if cur.seq != seq || cur.complete != complete || cur.gen != gen
                {
                    cur.seq = seq;
                    cur.complete = complete;
                    cur.gen = gen;
                    true
                } else {
                    false
                }
            });
        } else if !complete {
            let (tx, _) = watch::channel(JobNotification {
                id: job.id,
                seq,
                complete,
                gen: 0,
            });

            js.insert(job.id, tx);
        }

        if complete {
            js.remove(&job.id);
        }
    }

    pub fn job_append_events(
        &self,
        job: JobId,
        events: impl Iterator<Item = CreateJobEvent>,
    ) -> DBResult<()> {
        self.sql.tx_immediate(|h| {
            let j: Job = h.get_row(Job::find(job))?;
            if j.complete {
                conflict!("job already complete, cannot append");
            }

            let mut seq = 0;
            for e in events {
                seq = self.i_job_event_insert(
                    h,
                    j.id,
                    e.task,
                    &e.stream,
                    e.time,
                    e.time_remote,
                    &e.payload,
                )?;
            }

            self.i_job_notify(h, &j, seq, false, false);
            Ok(())
        })
    }

    pub fn job_append_event(
        &self,
        job: JobId,
        task: Option<u32>,
        stream: &str,
        time: DateTime<Utc>,
        time_remote: Option<DateTime<Utc>>,
        payload: &str,
    ) -> DBResult<()> {
        self.sql.tx_immediate(|h| {
            let j: Job = h.get_row(Job::find(job))?;
            if j.complete {
                conflict!("job already complete, cannot append");
            }

            let seq = self.i_job_event_insert(
                h,
                j.id,
                task,
                stream,
                time,
                time_remote,
                payload,
            )?;

            self.i_job_notify(h, &j, seq, false, false);
            Ok(())
        })
    }

    pub fn job_wakeup(&self, job: JobId) -> DBResult<()> {
        self.sql.tx_immediate(|h| {
            let j: Job = h.get_row(Job::find(job))?;
            if !j.waiting {
                conflict!("job {} was not waiting, cannot wakeup", j.id);
            }

            /*
             * Record the time at which the job became ready to run.
             */
            self.i_job_time_record(h, j.id, "ready", Utc::now())?;

            /*
             *
             * Estimate the length of time that we were waiting for dependencies
             * to complete running.
             */
            let mut msg = "job dependencies complete; ready to run".to_string();
            if let Some(dur) =
                self.i_job_time_delta(h, j.id, "submit", "ready")?
            {
                msg += &format!(" (waiting for {})", dur.render());
            }
            let seq = self.i_job_control_event_insert(h, j.id, &msg)?;

            let uc = h.exec_update(
                Query::update()
                    .table(JobDef::Table)
                    .and_where(Expr::col(JobDef::Id).eq(j.id))
                    .and_where(Expr::col(JobDef::Waiting).eq(true))
                    .value(JobDef::Waiting, false)
                    .to_owned(),
            )?;
            assert_eq!(uc, 1);

            self.i_job_notify(h, &j, seq, false, true);

            Ok(())
        })
    }

    pub fn job_cancel(&self, job: JobId) -> DBResult<bool> {
        self.sql.tx_immediate(|h| {
            let j: Job = h.get_row(Job::find(job))?;
            if j.complete {
                conflict!("job {} is already complete", j.id);
            }

            if j.cancelled {
                /*
                 * Already cancelled previously.
                 */
                return Ok(false);
            }

            let seq =
                self.i_job_control_event_insert(h, j.id, "job cancelled")?;

            let uc = h.exec_update(
                Query::update()
                    .table(JobDef::Table)
                    .and_where(Expr::col(JobDef::Id).eq(j.id))
                    .and_where(Expr::col(JobDef::Complete).eq(false))
                    .value(JobDef::Cancelled, true)
                    .to_owned(),
            )?;
            assert_eq!(uc, 1);

            self.i_job_notify(h, &j, seq, false, true);
            Ok(true)
        })
    }

    pub fn job_complete(&self, job: JobId, failed: bool) -> DBResult<bool> {
        self.sql.tx_immediate(|h| {
            let j: Job = h.get_row(Job::find(job))?;

            let res = self.i_job_complete(h, &j, failed);

            let seq =
                self.i_job_event_max_seq(h, j.id).ok().flatten().unwrap_or(0);
            self.i_job_notify(h, &j, seq, true, true);

            res
        })
    }

    pub fn i_job_complete(
        &self,
        h: &mut Handle,
        j: &Job,
        failed: bool,
    ) -> DBResult<bool> {
        if j.complete {
            /*
             * This job is already complete.
             */
            return Ok(false);
        }

        if j.cancelled && !failed {
            conflict!("job {} cancelled; cannot succeed", j.id);
        }

        /*
         * Mark any tasks that have not yet completed as failed, as they will
         * not be executed.
         */
        let mut tasks_failed = false;
        let tasks: Vec<Task> = h.get_rows(self.q_job_tasks(j.id))?;
        for t in tasks {
            if t.failed {
                tasks_failed = true;
            }
            if t.failed || t.complete || j.cancelled {
                continue;
            }

            self.i_job_control_event_insert(
                h,
                j.id,
                &format!("task {} was incomplete, marked failed", t.seq),
            )?;

            let uc = h.exec_update(
                Query::update()
                    .table(TaskDef::Table)
                    .and_where(Expr::col(TaskDef::Job).eq(j.id))
                    .and_where(Expr::col(TaskDef::Seq).eq(t.seq))
                    .and_where(Expr::col(TaskDef::Complete).eq(false))
                    .value(TaskDef::Failed, true)
                    .value(TaskDef::Complete, true)
                    .to_owned(),
            )?;
            assert_eq!(uc, 1);
        }

        let failed = if failed {
            true
        } else if tasks_failed {
            /*
             * If a task failed, we must report job-level failure even if the
             * job was for some reason not explicitly failed.
             */
            self.i_job_control_event_insert(
                h,
                j.id,
                "job failed because at least one task failed",
            )?;
            true
        } else {
            false
        };

        let uc = h.exec_update(
            Query::update()
                .table(JobDef::Table)
                .and_where(Expr::col(JobDef::Id).eq(j.id))
                .and_where(Expr::col(JobDef::Complete).eq(false))
                .value(JobDef::Failed, failed)
                .value(JobDef::Complete, true)
                .to_owned(),
        )?;
        assert_eq!(uc, 1);

        self.i_job_time_record(h, j.id, "complete", Utc::now())?;

        Ok(true)
    }

    pub fn i_job_time_record(
        &self,
        h: &mut Handle,
        job: JobId,
        name: &str,
        when: DateTime<Utc>,
    ) -> DBResult<()> {
        h.exec_insert(
            JobTime { job, name: name.to_string(), time: IsoDate(when) }
                .upsert(),
        )?;

        Ok(())
    }

    pub fn i_job_time_delta(
        &self,
        h: &mut Handle,
        job: JobId,
        from: &str,
        until: &str,
    ) -> DBResult<Option<Duration>> {
        let from = if let Some(from) =
            h.get_row_opt::<JobTime>(JobTime::find(job, from))?
        {
            from
        } else {
            return Ok(None);
        };

        let until = if let Some(until) =
            h.get_row_opt::<JobTime>(JobTime::find(job, until))?
        {
            until
        } else {
            return Ok(None);
        };

        let dur = until.time.0.signed_duration_since(from.time.0);
        if dur.num_milliseconds() < 0 {
            return Ok(None);
        }

        Ok(Some(dur.to_std()?))
    }

    pub fn job_times(
        &self,
        job: JobId,
    ) -> DBResult<HashMap<String, DateTime<Utc>>> {
        self.sql.tx(|h| {
            Ok(h.get_rows::<JobTime>(
                Query::select()
                    .from(JobTimeDef::Table)
                    .columns(JobTime::columns())
                    .and_where(Expr::col(JobTimeDef::Job).eq(job))
                    .to_owned(),
            )?
            .into_iter()
            .map(|jt| (jt.name, jt.time.0))
            .collect())
        })
    }

    pub fn job_store(&self, job: JobId) -> DBResult<HashMap<String, JobStore>> {
        self.sql.tx(|h| {
            Ok(h.get_rows::<JobStore>(
                Query::select()
                    .from(JobStoreDef::Table)
                    .columns(JobStore::columns())
                    .and_where(Expr::col(JobStoreDef::Job).eq(job))
                    .to_owned(),
            )?
            .into_iter()
            .map(|js| (js.name.to_string(), js))
            .collect())
        })
    }

    pub fn job_store_put(
        &self,
        job: JobId,
        name: &str,
        value: &str,
        secret: bool,
        source: &str,
    ) -> DBResult<()> {
        if name.is_empty()
            || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            conflict!("invalid store entry name");
        }

        /*
         * Cap the number of values and the size of each value:
         */
        let max_val_count = 100;
        let max_val_kib = 10;
        if value.len() > max_val_kib * 1024 {
            conflict!("maximum value size is {max_val_kib}KiB");
        }

        self.sql.tx_immediate(|h| {
            /*
             * Make sure the job exists and is not yet completed.  We do not
             * allow store updates after the job is finished.
             */
            let j: Job = h.get_row(Job::find(job))?;
            if j.complete {
                conflict!("job {job} already complete; cannot update store");
            }

            /*
             * First, check to see if this value already exists in the store:
             */
            let pre: Option<JobStore> =
                h.get_row_opt(JobStore::find(job, name))?;

            if let Some(pre) = pre {
                /*
                 * Overwrite the existing value.  If the value we are replacing
                 * was already marked as a secret, we mark the updated value as
                 * secret as well to avoid accidents.
                 */
                let new_secret = if pre.secret { true } else { secret };

                let uc = h.exec_update(
                    Query::update()
                        .table(JobStoreDef::Table)
                        .and_where(Expr::col(JobStoreDef::Job).eq(job))
                        .and_where(Expr::col(JobStoreDef::Name).eq(name))
                        .value(JobStoreDef::Value, value)
                        .value(JobStoreDef::Secret, new_secret)
                        .value(JobStoreDef::Source, source)
                        .value(JobStoreDef::TimeUpdate, IsoDate::now())
                        .to_owned(),
                )?;
                assert_eq!(uc, 1);

                return Ok(());
            }

            /*
             * If the value does not already exist, we need to create it.  We
             * also need to make sure we do not allow values to be stored in
             * excess of the value count cap.
             */
            let count: usize = h.get_row(
                Query::select()
                    .from(JobStoreDef::Table)
                    .expr(Expr::col(Asterisk).count())
                    .and_where(Expr::col(JobStoreDef::Job).eq(job))
                    .to_owned(),
            )?;
            if count >= max_val_count {
                conflict!("job {job} already has {count} store values");
            }

            let ic = h.exec_insert(
                JobStore {
                    job,
                    name: name.to_string(),
                    value: value.to_string(),
                    secret,
                    source: source.to_string(),
                    time_update: IsoDate::now(),
                }
                .insert(),
            )?;
            assert_eq!(ic, 1);

            Ok(())
        })
    }

    pub fn task_complete(
        &self,
        job: JobId,
        seq: u32,
        failed: bool,
    ) -> DBResult<bool> {
        self.sql.tx_immediate(|h| {
            let j: Job = h.get_row(Job::find(job))?;
            if j.complete {
                conflict!("job {} is already complete", j.id);
            }

            let t: Task = h.get_row(Task::find(job, seq))?;
            if t.complete {
                return Ok(false);
            }

            let uc = h.exec_update(
                Query::update()
                    .table(TaskDef::Table)
                    .and_where(Expr::col(TaskDef::Job).eq(job))
                    .and_where(Expr::col(TaskDef::Seq).eq(seq))
                    .and_where(Expr::col(TaskDef::Complete).eq(false))
                    .value(TaskDef::Complete, true)
                    .value(TaskDef::Failed, failed)
                    .to_owned(),
            )?;
            assert_eq!(uc, 1);

            Ok(true)
        })
    }

    fn i_job_event_max_seq(
        &self,
        h: &mut Handle,
        job: JobId,
    ) -> DBResult<Option<u32>> {
        h.get_row(
            Query::select()
                .from(JobEventDef::Table)
                .and_where(Expr::col(JobEventDef::Job).eq(job))
                .expr(Expr::col(JobEventDef::Seq).max())
                .to_owned(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn i_job_event_insert(
        &self,
        h: &mut Handle,
        job: JobId,
        task: Option<u32>,
        stream: &str,
        time: DateTime<Utc>,
        time_remote: Option<DateTime<Utc>>,
        payload: &str,
    ) -> DBResult<u32> {
        let max = self.i_job_event_max_seq(h, job)?;
        let seq = max.unwrap_or(0) + 1;

        let ic = h.exec_insert(
            JobEvent {
                job,
                task,
                seq,
                stream: stream.to_string(),
                time: IsoDate(time),
                time_remote: time_remote.map(IsoDate),
                payload: payload.to_string(),
            }
            .insert(),
        )?;
        assert_eq!(ic, 1);

        Ok(seq)
    }

    pub fn worker_job(&self, worker: WorkerId) -> DBResult<Option<Job>> {
        self.sql.tx(|h| {
            h.get_row_opt(
                Query::select()
                    .from(JobDef::Table)
                    .columns(Job::columns())
                    .and_where(Expr::col(JobDef::Worker).eq(worker))
                    .to_owned(),
            )
        })
    }

    pub fn user(&self, id: UserId) -> DBResult<Option<AuthUser>> {
        self.sql.tx(|h| {
            h.get_row_opt::<User>(
                Query::select()
                    .from(UserDef::Table)
                    .columns(User::columns())
                    .and_where(Expr::col(UserDef::Id).eq(id))
                    .to_owned(),
            )?
            .map(|user| {
                Ok(AuthUser {
                    privileges: self.i_user_privileges(h, user.id)?,
                    user,
                })
            })
            .transpose()
        })
    }

    pub fn user_by_name(&self, name: &str) -> DBResult<Option<AuthUser>> {
        self.sql.tx(|h| {
            h.get_row_opt::<User>(
                Query::select()
                    .from(UserDef::Table)
                    .columns(User::columns())
                    .and_where(Expr::col(UserDef::Name).eq(name))
                    .to_owned(),
            )?
            .map(|user| {
                Ok(AuthUser {
                    privileges: self.i_user_privileges(h, user.id)?,
                    user,
                })
            })
            .transpose()
        })
    }

    pub fn users(&self) -> DBResult<Vec<AuthUser>> {
        self.sql.tx(|h| {
            h.get_rows::<User>(
                Query::select()
                    .from(UserDef::Table)
                    .columns(User::columns())
                    .to_owned(),
            )?
            .into_iter()
            .map(|user| {
                Ok(AuthUser {
                    privileges: self.i_user_privileges(h, user.id)?,
                    user,
                })
            })
            .collect::<DBResult<_>>()
        })
    }

    fn i_user_privileges(
        &self,
        h: &mut Handle,
        user: UserId,
    ) -> DBResult<Vec<String>> {
        h.get_strings(
            Query::select()
                .from(UserPrivilegeDef::Table)
                .column(UserPrivilegeDef::Privilege)
                .and_where(Expr::col(UserPrivilegeDef::User).eq(user))
                .order_by(UserPrivilegeDef::Privilege, Order::Asc)
                .to_owned(),
        )
    }

    pub fn user_privilege_grant(
        &self,
        u: UserId,
        privilege: &str,
    ) -> DBResult<bool> {
        if privilege.is_empty()
            || !privilege.chars().all(|c| {
                c.is_ascii_digit()
                    || c.is_ascii_lowercase()
                    || c == '-'
                    || c == '_'
                    || c == '/'
                    || c == '.'
            })
        {
            conflict!("invalid privilege name");
        }

        self.sql.tx_immediate(|h| {
            /*
             * Confirm that the user exists before creating privilege records:
             */
            let u: User = h.get_row(User::find(u))?;

            let ic = h.exec_insert(
                UserPrivilege { user: u.id, privilege: privilege.to_string() }
                    .upsert(),
            )?;
            assert!(ic == 0 || ic == 1);

            Ok(ic != 0)
        })
    }

    pub fn user_privilege_revoke(
        &self,
        u: UserId,
        privilege: &str,
    ) -> DBResult<bool> {
        self.sql.tx_immediate(|h| {
            /*
             * Confirm that the user exists before trying to remove privilege
             * records:
             */
            let u: User = h.get_row(User::find(u))?;

            let dc = h.exec_delete(
                Query::delete()
                    .from_table(UserPrivilegeDef::Table)
                    .and_where(Expr::col(UserPrivilegeDef::User).eq(u.id))
                    .and_where(
                        Expr::col(UserPrivilegeDef::Privilege).eq(privilege),
                    )
                    .to_owned(),
            )?;
            assert!(dc == 0 || dc == 1);

            Ok(dc != 0)
        })
    }

    fn i_user_create(&self, name: &str, h: &mut Handle) -> DBResult<User> {
        let u = User {
            id: UserId::generate(),
            name: name.to_string(),
            token: genkey(48),
            time_create: Utc::now().into(),
        };

        let ic = h.exec_insert(u.insert())?;
        assert_eq!(ic, 1);

        Ok(u)
    }

    pub fn user_create(&self, name: &str) -> DBResult<User> {
        /*
         * Make sure the requested username is not one we might mistake as a
         * ULID later.
         */
        if looks_like_a_ulid(name) {
            conflict!("usernames must not look like ULIDs");
        }

        self.sql.tx_immediate(|h| self.i_user_create(name, h))
    }

    pub fn user_ensure(&self, name: &str) -> DBResult<AuthUser> {
        /*
         * Make sure the requested username is not one we might mistake as a
         * ULID later.
         */
        if looks_like_a_ulid(name) {
            conflict!("usernames must not look like ULIDs");
        }

        self.sql.tx_immediate(|h| {
            let user = if let Some(user) = h.get_row_opt::<User>(
                Query::select()
                    .from(UserDef::Table)
                    .columns(User::columns())
                    .and_where(Expr::col(UserDef::Name).eq(name))
                    .to_owned(),
            )? {
                user
            } else {
                /*
                 * The user does not exist already, so a new one must be
                 * created:
                 */
                self.i_user_create(name, h)?
            };

            let au = AuthUser {
                privileges: self.i_user_privileges(h, user.id)?,
                user,
            };

            Ok(au)
        })
    }

    pub fn user_auth(&self, token: &str) -> DBResult<AuthUser> {
        self.sql.tx(|h| {
            let mut users: Vec<User> = h.get_rows::<User>(
                Query::select()
                    .from(UserDef::Table)
                    .columns(User::columns())
                    .and_where(Expr::col(UserDef::Token).eq(token))
                    .to_owned(),
            )?;

            match (users.pop(), users.pop()) {
                (None, _) => conflict!("auth failure"),
                (Some(u), Some(x)) => {
                    conflict!("token error ({}, {})", u.id, x.id)
                }
                (Some(u), None) => {
                    assert_eq!(&u.token, token);
                    Ok(AuthUser {
                        privileges: self.i_user_privileges(h, u.id)?,
                        user: u,
                    })
                }
            }
        })
    }

    pub fn factory_create(&self, name: &str) -> DBResult<Factory> {
        /*
         * Make sure the requested name is not one we might mistake as a
         * ULID later.
         */
        if looks_like_a_ulid(name) {
            conflict!("factory names must not look like ULIDs");
        }

        let f = Factory {
            id: FactoryId::generate(),
            name: name.to_string(),
            token: genkey(64),
            lastping: None,
            enable: true,
            hold_workers: false,
        };

        self.sql.tx(|h| h.exec_insert(f.insert()))?;

        Ok(f)
    }

    pub fn factories(&self) -> DBResult<Vec<Factory>> {
        self.sql.tx(|h| {
            h.get_rows(
                Query::select()
                    .from(FactoryDef::Table)
                    .columns(Factory::columns())
                    .order_by(FactoryDef::Id, Order::Asc)
                    .to_owned(),
            )
        })
    }

    pub fn factory(&self, id: FactoryId) -> DBResult<Factory> {
        self.sql.tx(|h| self.i_factory(h, id))
    }

    fn i_factory(&self, h: &mut Handle, id: FactoryId) -> DBResult<Factory> {
        if id == Worker::legacy_default_factory_id() {
            /*
             * Factory records for workers that were created prior to the
             * existence of factories do not exist.  Return a fake record.
             */
            return Ok(Factory {
                id,
                name: "legacy".into(),
                /*
                 * Authentication checks are done below in factory_auth() where
                 * we need to guard against accidentally accepting this value:
                 */
                token: "".into(),
                lastping: None,
                enable: true,
                hold_workers: false,
            });
        }

        h.get_row(Factory::find(id))
    }

    pub fn factory_auth(&self, token: &str) -> DBResult<Factory> {
        if token.is_empty() {
            /*
             * Make sure this trivially invalid value used in the legacy
             * sentinel record is never accepted, even if it somehow ends up in
             * the database by accident.
             */
            conflict!("auth failure");
        }

        self.sql.tx(|h| {
            let mut rows = h.get_rows::<Factory>(
                Query::select()
                    .from(FactoryDef::Table)
                    .columns(Factory::columns())
                    .and_where(Expr::col(FactoryDef::Token).eq(token))
                    .to_owned(),
            )?;

            match (rows.pop(), rows.pop()) {
                (None, _) => conflict!("auth failure"),
                (Some(u), Some(x)) => {
                    conflict!("token error ({}, {})", u.id, x.id)
                }
                (Some(u), None) => {
                    assert_eq!(u.token.as_str(), token);
                    Ok(u)
                }
            }
        })
    }

    pub fn factory_ping(&self, id: FactoryId) -> DBResult<bool> {
        self.sql.tx(|h| {
            Ok(h.exec_update(
                Query::update()
                    .table(FactoryDef::Table)
                    .and_where(Expr::col(FactoryDef::Id).eq(id))
                    .value(FactoryDef::Lastping, IsoDate::now())
                    .to_owned(),
            )? > 0)
        })
    }

    pub fn factory_enable(&self, id: FactoryId, enable: bool) -> DBResult<()> {
        self.sql.tx(|h| {
            h.exec_update(
                Query::update()
                    .table(FactoryDef::Table)
                    .and_where(Expr::col(FactoryDef::Id).eq(id))
                    .value(FactoryDef::Enable, enable)
                    .to_owned(),
            )?;

            Ok(())
        })
    }

    pub fn factory_hold_workers(
        &self,
        id: FactoryId,
        hold_workers: bool,
    ) -> DBResult<()> {
        self.sql.tx(|h| {
            h.exec_update(
                Query::update()
                    .table(FactoryDef::Table)
                    .and_where(Expr::col(FactoryDef::Id).eq(id))
                    .value(FactoryDef::HoldWorkers, hold_workers)
                    .to_owned(),
            )?;

            Ok(())
        })
    }

    pub fn targets(&self) -> DBResult<Vec<Target>> {
        self.sql.tx(|h| {
            h.get_rows(
                Query::select()
                    .from(TargetDef::Table)
                    .columns(Target::columns())
                    .order_by(TargetDef::Id, Order::Asc)
                    .to_owned(),
            )
        })
    }

    pub fn target(&self, id: TargetId) -> DBResult<Target> {
        self.sql.tx(|h| h.get_row(Target::find(id)))
    }

    pub fn target_create(&self, name: &str, desc: &str) -> DBResult<Target> {
        /*
         * Make sure the requested name is not one we might mistake as a
         * ULID later.
         */
        if looks_like_a_ulid(name) {
            conflict!("target names must not look like ULIDs");
        }

        if name != name.trim() || name.is_empty() {
            conflict!("invalid target name");
        }

        let t = Target {
            id: TargetId::generate(),
            name: name.to_string(),
            desc: desc.to_string(),
            redirect: None,
            privilege: None,
        };

        self.sql.tx(|h| {
            let ic = h.exec_insert(t.insert())?;
            assert_eq!(ic, 1);

            Ok(t)
        })
    }

    pub fn target_resolve(&self, name: &str) -> DBResult<Option<Target>> {
        self.sql.tx_immediate(|h| {
            /*
             * Use the target name to look up the initial target match:
             */
            let mut target: Target = if let Some(target) =
                h.get_row_opt(Target::find_by_name(name))?
            {
                target
            } else {
                return Ok(None);
            };

            let mut count = 0;
            loop {
                if count > 32 {
                    conflict!(
                        "too many target redirects starting from {:?}",
                        name
                    );
                }
                count += 1;

                if let Some(redirect) = &target.redirect {
                    target = if let Some(target) =
                        h.get_row_opt(Target::find(*redirect))?
                    {
                        target
                    } else {
                        return Ok(None);
                    };
                } else {
                    return Ok(Some(target));
                }
            }
        })
    }

    pub fn target_require(
        &self,
        id: TargetId,
        privilege: Option<&str>,
    ) -> DBResult<()> {
        self.sql.tx(|h| {
            let uc = h.exec_update(
                Query::update()
                    .table(TargetDef::Table)
                    .and_where(Expr::col(TargetDef::Id).eq(id))
                    .value(TargetDef::Privilege, privilege)
                    .to_owned(),
            )?;
            assert!(uc == 1);

            Ok(())
        })
    }

    pub fn target_redirect(
        &self,
        id: TargetId,
        redirect: Option<TargetId>,
    ) -> DBResult<()> {
        self.sql.tx(|h| {
            let uc = h.exec_update(
                Query::update()
                    .table(TargetDef::Table)
                    .and_where(Expr::col(TargetDef::Id).eq(id))
                    .value(TargetDef::Redirect, redirect)
                    .to_owned(),
            )?;
            assert!(uc == 1);

            Ok(())
        })
    }

    /**
     * Rename an existing target.  In the process, create a new target with the
     * old name which redirects to the new target.  In this way, we can turn
     * previously a concrete target like "helios" into a specific version target
     * like "helios-20200101", leaving behind a redirect with the original name
     * for backwards compatibility for existing job files.  We can then, later,
     * redirect the new "helios" target to a new datestamp.  Job status will
     * reflect the fact that they were run against whatever version was current
     * at the time as the IDs will not change.
     */
    pub fn target_rename(
        &self,
        id: TargetId,
        new_name: &str,
        signpost_description: &str,
    ) -> DBResult<Target> {
        /*
         * Make sure the requested name is not one we might mistake as a
         * ULID later.
         */
        if looks_like_a_ulid(new_name) {
            conflict!("target names must not look like ULIDs");
        }

        if new_name != new_name.trim() || new_name.is_empty() {
            conflict!("invalid target name");
        }

        self.sql.tx_immediate(|h| {
            /*
             * First, load the target to rename from the database.
             */
            let t: Target = h.get_row(Target::find(id))?;
            if t.name == new_name {
                conflict!("target {} already has name {new_name:?}", t.id);
            }

            /*
             * Then, make sure a target with the new name does not yet
             * exist.
             */
            let nt: Option<Target> =
                h.get_row_opt(Target::find_by_name(new_name))?;
            if let Some(nt) = nt {
                conflict!(
                    "target {} with name {:?} already exists",
                    nt.id,
                    new_name,
                );
            }

            /*
             * Rename the target:
             */
            let uc = h.exec_update(
                Query::update()
                    .table(TargetDef::Table)
                    .and_where(Expr::col(TargetDef::Id).eq(id))
                    .and_where(Expr::col(TargetDef::Name).eq(&t.name))
                    .value(TargetDef::Name, new_name)
                    .to_owned(),
            )?;
            assert_eq!(uc, 1);

            /*
             * Create the signpost target record.
             */
            let nt = Target {
                id: TargetId::generate(),
                name: t.name.to_string(),
                desc: signpost_description.to_string(),
                redirect: Some(t.id),
                privilege: t.privilege,
            };

            let ic = h.exec_insert(nt.insert())?;
            assert_eq!(ic, 1);

            Ok(nt)
        })
    }
}
