/*
 * Copyright 2023 Oxide Computer Company
 */

use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;

use anyhow::{anyhow, bail, Result};
use buildomat_common::*;
use buildomat_database::sqlite::rusqlite;
use buildomat_types::*;
use chrono::prelude::*;
use rusqlite::Transaction;
use rusty_ulid::Ulid;
use sea_query::{
    Asterisk, Cond, DeleteStatement, Expr, InsertStatement, OnConflict, Order,
    Query, SelectStatement, SqliteQueryBuilder, UpdateStatement,
};
use sea_query_rusqlite::{RusqliteBinder, RusqliteValues};
#[allow(unused_imports)]
use slog::{debug, error, info, warn, Logger};
use thiserror::Error;

mod models;

pub use models::*;

#[derive(Error, Debug)]
pub enum OperationError {
    #[error("conflict: {0}")]
    Conflict(String),
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    OutOfRange(#[from] chrono::OutOfRangeError),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type OResult<T> = std::result::Result<T, OperationError>;

macro_rules! conflict {
    ($msg:expr) => {
        return Err(OperationError::Conflict(format!($msg)))
    };
    ($fmt:expr, $($arg:tt)*) => {
        return Err(OperationError::Conflict(format!($fmt, $($arg)*)))
    }
}

struct Inner {
    conn: rusqlite::Connection,
}

pub struct Database(Logger, Mutex<Inner>);

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

pub struct JobEventToAppend {
    pub task: Option<u32>,
    pub stream: String,
    pub time: DateTime<Utc>,
    pub time_remote: Option<DateTime<Utc>>,
    pub payload: String,
}

impl Database {
    pub fn new<P: AsRef<Path>>(
        log: Logger,
        path: P,
        cache_kb: Option<u32>,
    ) -> Result<Database> {
        let conn = buildomat_database::sqlite::sqlite_setup(
            &log,
            path,
            include_str!("../schema.sql"),
            cache_kb,
        )?;

        Ok(Database(log, Mutex::new(Inner { conn })))
    }

    fn i_worker_for_bootstrap(
        &self,
        tx: &mut Transaction,
        bs: &str,
    ) -> OResult<Worker> {
        self.tx_get_row(
            tx,
            Query::select()
                .from(WorkerDef::Table)
                .columns(Worker::columns())
                .and_where(Expr::col(WorkerDef::Bootstrap).eq(bs))
                .to_owned(),
        )
    }

    fn i_worker(&self, tx: &mut Transaction, id: WorkerId) -> OResult<Worker> {
        self.tx_get_row(
            tx,
            Query::select()
                .from(WorkerDef::Table)
                .columns(Worker::columns())
                .and_where(Expr::col(WorkerDef::Id).eq(id))
                .to_owned(),
        )
    }

    pub fn workers(&self) -> OResult<Vec<Worker>> {
        self.get_rows(
            Query::select()
                .from(WorkerDef::Table)
                .columns(Worker::columns())
                .order_by(WorkerDef::Id, Order::Asc)
                .to_owned(),
        )
    }

    pub fn workers_active(&self) -> OResult<Vec<Worker>> {
        self.get_rows(
            Query::select()
                .from(WorkerDef::Table)
                .columns(Worker::columns())
                .order_by(WorkerDef::Id, Order::Asc)
                .and_where(Expr::col(WorkerDef::Deleted).eq(false))
                .to_owned(),
        )
    }

    pub fn workers_for_factory(
        &self,
        factory: &Factory,
    ) -> OResult<Vec<Worker>> {
        self.get_rows(
            Query::select()
                .from(WorkerDef::Table)
                .columns(Worker::columns())
                .order_by(WorkerDef::Id, Order::Asc)
                .and_where(Expr::col(WorkerDef::Factory).eq(factory.id))
                .and_where(Expr::col(WorkerDef::Deleted).eq(false))
                .to_owned(),
        )
    }

    pub fn worker_jobs(&self, worker: WorkerId) -> OResult<Vec<Job>> {
        self.get_rows(
            Query::select()
                .from(JobDef::Table)
                .columns(Job::columns())
                .and_where(Expr::col(JobDef::Worker).eq(worker))
                .to_owned(),
        )
    }

    pub fn free_workers(&self) -> OResult<Vec<Worker>> {
        self.get_rows(
            Query::select()
                .columns(Worker::columns())
                .from(WorkerDef::Table)
                .left_join(
                    JobDef::Table,
                    Expr::col((JobDef::Table, JobDef::Worker))
                        .eq(Expr::col((WorkerDef::Table, WorkerDef::Id))),
                )
                .order_by((WorkerDef::Table, WorkerDef::Id), Order::Asc)
                .and_where(Expr::col((JobDef::Table, JobDef::Worker)).is_null())
                .and_where(
                    Expr::col((WorkerDef::Table, WorkerDef::Deleted)).eq(false),
                )
                .and_where(
                    Expr::col((WorkerDef::Table, WorkerDef::Recycle)).eq(false),
                )
                .and_where(
                    Expr::col((WorkerDef::Table, WorkerDef::Token))
                        .is_not_null(),
                )
                .to_owned(),
        )
    }

    pub fn worker_recycle_all(&self) -> OResult<usize> {
        self.exec_update(
            Query::update()
                .table(WorkerDef::Table)
                .values([(WorkerDef::Recycle, true.into())])
                .and_where(Expr::col(WorkerDef::Deleted).eq(false))
                .to_owned(),
        )
    }

    pub fn worker_recycle(&self, id: WorkerId) -> OResult<bool> {
        Ok(self.exec_update(
            Query::update()
                .table(WorkerDef::Table)
                .values([(WorkerDef::Recycle, true.into())])
                .and_where(Expr::col(WorkerDef::Id).eq(id))
                .and_where(Expr::col(WorkerDef::Deleted).eq(false))
                .to_owned(),
        )? > 0)
    }

    pub fn worker_flush(&self, id: WorkerId) -> OResult<bool> {
        Ok(self.exec_update(
            Query::update()
                .table(WorkerDef::Table)
                .values([(WorkerDef::WaitForFlush, false.into())])
                .and_where(Expr::col(WorkerDef::Id).eq(id))
                .to_owned(),
        )? > 0)
    }

    pub fn worker_destroy(&self, id: WorkerId) -> OResult<bool> {
        Ok(self.exec_update(
            Query::update()
                .table(WorkerDef::Table)
                .values([(WorkerDef::Deleted, true.into())])
                .and_where(Expr::col(WorkerDef::Id).eq(id))
                .to_owned(),
        )? > 0)
    }

    pub fn worker_ping(&self, id: WorkerId) -> OResult<bool> {
        let now = IsoDate(Utc::now());

        Ok(self.exec_update(
            Query::update()
                .table(WorkerDef::Table)
                .values([(WorkerDef::Lastping, now.into())])
                .and_where(Expr::col(WorkerDef::Id).eq(id))
                .to_owned(),
        )? > 0)
    }

    pub fn i_worker_assign_job(
        &self,
        tx: &mut Transaction,
        w: &Worker,
        jid: JobId,
    ) -> OResult<()> {
        let j: Job = self.tx_get_row(tx, Job::find(jid))?;
        if let Some(jw) = j.worker.as_ref() {
            conflict!("job {} already assigned to worker {}", j.id, jw);
        }

        let c: usize = {
            self.tx_get_row(
                tx,
                Query::select()
                    .expr(Expr::col(Asterisk).count())
                    .from(JobDef::Table)
                    .and_where(Expr::col(JobDef::Worker).eq(w.id))
                    .to_owned(),
            )?
        };
        if c > 0 {
            conflict!("worker {} already has {} jobs assigned", w.id, c);
        }

        let uc = self.tx_exec_update(
            tx,
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
        self.i_job_time_record(tx, j.id, "assigned", Utc::now())?;
        let wait = if let Some(dur) =
            self.i_job_time_delta(tx, j.id, "ready", "assigned")?
        {
            format!(" (queued for {})", dur.render())
        } else if let Ok(dur) =
            Utc::now().signed_duration_since(j.id.datetime()).to_std()
        {
            format!(" (queued for {})", dur.render())
        } else {
            "".to_string()
        };

        self.i_job_event_insert(
            tx,
            j.id,
            None,
            "control",
            Utc::now(),
            None,
            &format!("job assigned to worker {}{}", w.id, wait),
        )?;

        Ok(())
    }

    pub fn worker_assign_job(&self, wid: WorkerId, jid: JobId) -> OResult<()> {
        let c = &mut self.1.lock().unwrap().conn;
        let mut tx = c.transaction_with_behavior(
            rusqlite::TransactionBehavior::Immediate,
        )?;

        let w = self.i_worker(&mut tx, wid)?;
        if w.deleted || w.recycle {
            conflict!("worker {} already deleted, cannot assign job", w.id);
        }

        self.i_worker_assign_job(&mut tx, &w, jid)?;

        tx.commit()?;
        Ok(())
    }

    pub fn worker_bootstrap(
        &self,
        bootstrap: &str,
        token: &str,
    ) -> OResult<Option<Worker>> {
        let log = &self.0;
        let c = &mut self.1.lock().unwrap().conn;
        let mut tx = c.transaction_with_behavior(
            rusqlite::TransactionBehavior::Immediate,
        )?;

        let w = self.i_worker_for_bootstrap(&mut tx, bootstrap)?;
        if w.deleted {
            error!(log, "worker {} already deleted, cannot bootstrap", w.id);
            return Ok(None);
        }

        if w.factory_private.is_none() {
            error!(log, "worker {} has no instance, cannot bootstrap", w.id);
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
                    log,
                    "worker {} already has a token ({} != {})",
                    w.id,
                    current,
                    token
                );
                return Ok(None);
            }
        } else {
            let count = self.tx_exec_update(
                &mut tx,
                Query::update()
                    .table(WorkerDef::Table)
                    .values([
                        (WorkerDef::Token, token.into()),
                        (WorkerDef::Lastping, IsoDate(Utc::now()).into()),
                    ])
                    .and_where(Expr::col(WorkerDef::Id).eq(w.id))
                    .and_where(Expr::col(WorkerDef::Bootstrap).eq(bootstrap))
                    .and_where(Expr::col(WorkerDef::Token).is_null())
                    .to_owned(),
            )?;
            assert_eq!(count, 1);
        }

        let out = self.i_worker(&mut tx, w.id)?;

        tx.commit()?;
        Ok(Some(out))
    }

    pub fn worker_associate(
        &self,
        wid: WorkerId,
        factory_private: &str,
        factory_metadata: Option<&metadata::FactoryMetadata>,
    ) -> OResult<()> {
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

        let c = &mut self.1.lock().unwrap().conn;
        let mut tx = c.transaction_with_behavior(
            rusqlite::TransactionBehavior::Immediate,
        )?;

        let w: Worker = self.tx_get_row(&mut tx, Worker::find(wid))?;
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
            let count = self.tx_exec_update(
                &mut tx,
                Query::update()
                    .table(WorkerDef::Table)
                    .and_where(Expr::col(WorkerDef::Id).eq(w.id))
                    .value(WorkerDef::FactoryPrivate, factory_private)
                    .to_owned(),
            )?;
            assert_eq!(count, 1);
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
            let count = self.tx_exec_update(
                &mut tx,
                Query::update()
                    .table(WorkerDef::Table)
                    .and_where(Expr::col(WorkerDef::Id).eq(w.id))
                    .value(WorkerDef::FactoryMetadata, factory_metadata)
                    .to_owned(),
            )?;
            assert_eq!(count, 1);
        }

        tx.commit()?;
        Ok(())
    }

    pub fn worker(&self, id: WorkerId) -> OResult<Worker> {
        self.get_row(
            Query::select()
                .from(WorkerDef::Table)
                .columns(Worker::columns())
                .and_where(Expr::col(WorkerDef::Id).eq(id))
                .to_owned(),
        )
    }

    pub fn worker_opt(&self, id: WorkerId) -> OResult<Option<Worker>> {
        self.get_row_opt(
            Query::select()
                .from(WorkerDef::Table)
                .columns(Worker::columns())
                .and_where(Expr::col(WorkerDef::Id).eq(id))
                .to_owned(),
        )
    }

    pub fn worker_auth(&self, token: &str) -> OResult<Worker> {
        let mut workers = self.get_rows::<Worker>(
            Query::select()
                .from(WorkerDef::Table)
                .columns(Worker::columns())
                .and_where(Expr::col(WorkerDef::Token).eq(token))
                .and_where(Expr::col(WorkerDef::Deleted).eq(false))
                .to_owned(),
        )?;

        match (workers.pop(), workers.pop()) {
            (None, _) => conflict!("auth failure"),
            (Some(u), Some(x)) => conflict!("token error ({}, {})", u.id, x.id),
            (Some(u), None) => {
                assert_eq!(u.token.as_deref(), Some(token));
                Ok(u)
            }
        }
    }

    pub fn worker_create(
        &self,
        factory: &Factory,
        target: &Target,
        job: Option<JobId>,
        wait_for_flush: bool,
    ) -> OResult<Worker> {
        let w = Worker {
            id: WorkerId::generate(),
            bootstrap: genkey(64),
            factory_private: None,
            factory_metadata: None,
            token: None,
            deleted: false,
            recycle: false,
            lastping: None,
            factory: Some(factory.id),
            target: Some(target.id),
            wait_for_flush,
        };

        let c = &mut self.1.lock().unwrap().conn;
        let mut tx = c.transaction_with_behavior(
            rusqlite::TransactionBehavior::Immediate,
        )?;

        let count = self.tx_exec_insert(&mut tx, w.insert())?;
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
            self.i_worker_assign_job(&mut tx, &w, job)?;
        }

        tx.commit()?;
        Ok(w)
    }

    /**
     * Enumerate all jobs.
     */
    pub fn jobs_all(&self) -> OResult<Vec<Job>> {
        self.get_rows(
            Query::select()
                .from(JobDef::Table)
                .columns(Job::columns())
                .order_by(JobDef::Id, Order::Asc)
                .to_owned(),
        )
    }

    /**
     * Enumerate jobs that are active; i.e., not yet complete, but not waiting.
     */
    pub fn jobs_active(&self) -> OResult<Vec<Job>> {
        self.get_rows(
            Query::select()
                .from(JobDef::Table)
                .columns(Job::columns())
                .order_by(JobDef::Id, Order::Asc)
                .and_where(Expr::col(JobDef::Complete).eq(false))
                .and_where(Expr::col(JobDef::Waiting).eq(false))
                .to_owned(),
        )
    }

    /**
     * Enumerate jobs that are waiting for inputs, or for dependees to complete.
     */
    pub fn jobs_waiting(&self) -> OResult<Vec<Job>> {
        self.get_rows(
            Query::select()
                .from(JobDef::Table)
                .columns(Job::columns())
                .order_by(JobDef::Id, Order::Asc)
                .and_where(Expr::col(JobDef::Complete).eq(false))
                .and_where(Expr::col(JobDef::Waiting).eq(true))
                .to_owned(),
        )
    }

    /**
     * Enumerate some number of the most recently complete jobs.
     */
    pub fn jobs_completed(&self, limit: usize) -> OResult<Vec<Job>> {
        let mut res = self.get_rows(
            Query::select()
                .from(JobDef::Table)
                .columns(Job::columns())
                .order_by(JobDef::Id, Order::Desc)
                .and_where(Expr::col(JobDef::Complete).eq(true))
                .limit(limit.try_into().unwrap())
                .to_owned(),
        )?;

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

    pub fn job_tasks(&self, job: JobId) -> OResult<Vec<Task>> {
        self.get_rows(self.q_job_tasks(job))
    }

    pub fn job_tags(&self, job: JobId) -> OResult<HashMap<String, String>> {
        let (q, v) = Query::select()
            .from(JobTagDef::Table)
            .columns([JobTagDef::Name, JobTagDef::Value])
            .and_where(Expr::col(JobTagDef::Job).eq(job))
            .build_rusqlite(SqliteQueryBuilder);

        let c = &mut self.1.lock().unwrap().conn;

        let mut s = c.prepare(&q)?;
        let out = s.query_map(&*v.as_params(), |row| {
            Ok((row.get_unwrap(0), row.get_unwrap(1)))
        })?;

        Ok(out.collect::<rusqlite::Result<_>>()?)
    }

    pub fn job_output_rules(&self, job: JobId) -> OResult<Vec<JobOutputRule>> {
        self.get_rows(
            Query::select()
                .from(JobOutputRuleDef::Table)
                .columns(JobOutputRule::columns())
                .order_by(JobOutputRuleDef::Seq, Order::Asc)
                .and_where(Expr::col(JobOutputRuleDef::Job).eq(job))
                .to_owned(),
        )
    }

    pub fn job_depends(&self, job: JobId) -> OResult<Vec<JobDepend>> {
        self.get_rows(
            Query::select()
                .from(JobDependDef::Table)
                .columns(JobDepend::columns())
                .order_by(JobDependDef::Name, Order::Asc)
                .and_where(Expr::col(JobDependDef::Job).eq(job))
                .to_owned(),
        )
    }

    /**
     * When a prior job has completed, we need to mark the dependency record as
     * satisfied.  In the process, if requested, we will copy any output files
     * from the previous job into the new job as input files.
     */
    pub fn job_depend_satisfy(&self, jid: JobId, d: &JobDepend) -> OResult<()> {
        let c = &mut self.1.lock().unwrap().conn;
        let mut tx = c.transaction_with_behavior(
            rusqlite::TransactionBehavior::Immediate,
        )?;

        /*
         * Confirm that this job exists and is still waiting.
         */
        let j: Job = self.tx_get_row(&mut tx, Job::find(jid))?;
        if !j.waiting {
            conflict!("job not waiting, cannot satisfy dependency");
        }

        /*
         * Confirm that the dependency record still exists and has not yet
         * been satisfied.
         */
        let d: JobDepend =
            self.tx_get_row(&mut tx, JobDepend::find(j.id, &d.name))?;
        if d.satisfied {
            conflict!("job dependency already satisfied");
        }

        /*
         * Confirm that the prior job exists and is complete.
         */
        let pj: Job = self.tx_get_row(&mut tx, Job::find(d.prior_job))?;
        if !pj.complete {
            conflict!("prior job not complete");
        }

        if d.copy_outputs {
            /*
             * Resolve the list of output files.
             */
            let pjouts = self.i_job_outputs(&mut tx, pj.id)?;

            /*
             * For each output file produced by the dependency, create an
             * input record for this job.
             */
            for (pjo, pjf) in pjouts {
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

                self.tx_exec_insert(&mut tx, ji.upsert())?;
            }
        }

        let uc = self.tx_exec_update(
            &mut tx,
            Query::update()
                .table(JobDependDef::Table)
                .and_where(Expr::col(JobDependDef::Job).eq(j.id))
                .and_where(Expr::col(JobDependDef::Name).eq(&d.name))
                .value(JobDependDef::Satisfied, true)
                .to_owned(),
        )?;
        assert_eq!(uc, 1);

        tx.commit()?;
        Ok(())
    }

    pub fn job_inputs(
        &self,
        job: JobId,
    ) -> OResult<Vec<(JobInput, Option<JobFile>)>> {
        self.get_rows(
            Query::select()
                .columns(<(JobInput, Option<JobFile>) as FromRow>::columns())
                .from(JobInputDef::Table)
                .left_join(
                    JobFileDef::Table,
                    Cond::all()
                        /*
                         * The file ID column must always match:
                         */
                        .add(Expr::col((JobFileDef::Table, JobFileDef::Id)).eq(
                            Expr::col((JobInputDef::Table, JobInputDef::Id)),
                        ))
                        .add(
                            Cond::any()
                                /*
                                 * Either the other_job field is null, and the input
                                 * job ID matches the file job ID directly...
                                 */
                                .add(
                                    Expr::col((
                                        JobInputDef::Table,
                                        JobInputDef::OtherJob,
                                    ))
                                    .is_null()
                                    .and(
                                        Expr::col((
                                            JobFileDef::Table,
                                            JobFileDef::Job,
                                        ))
                                        .eq(Expr::col((
                                            JobInputDef::Table,
                                            JobInputDef::Job,
                                        ))),
                                    ),
                                )
                                /*
                                 * ... or the other_job field is populated and it
                                 * matches the file job ID instead:
                                 */
                                .add(
                                    Expr::col((
                                        JobInputDef::Table,
                                        JobInputDef::OtherJob,
                                    ))
                                    .is_not_null()
                                    .and(
                                        Expr::col((
                                            JobFileDef::Table,
                                            JobFileDef::Job,
                                        ))
                                        .eq(Expr::col((
                                            JobInputDef::Table,
                                            JobInputDef::OtherJob,
                                        ))),
                                    ),
                                ),
                        ),
                )
                .and_where(
                    Expr::col((JobInputDef::Table, JobInputDef::Job)).eq(job),
                )
                .order_by((JobInputDef::Table, JobInputDef::Id), Order::Asc)
                .to_owned(),
        )
    }

    fn q_job_outputs(&self, job: JobId) -> SelectStatement {
        Query::select()
            .columns(<(JobOutput, JobFile) as FromRow>::columns())
            .from(JobOutputDef::Table)
            .inner_join(
                JobFileDef::Table,
                Expr::col((JobFileDef::Table, JobFileDef::Job))
                    .eq(Expr::col((JobOutputDef::Table, JobOutputDef::Job)))
                    .and(Expr::col((JobFileDef::Table, JobFileDef::Id)).eq(
                        Expr::col((JobOutputDef::Table, JobOutputDef::Id)),
                    )),
            )
            .and_where(Expr::col((JobFileDef::Table, JobFileDef::Job)).eq(job))
            .order_by((JobFileDef::Table, JobFileDef::Id), Order::Asc)
            .to_owned()
    }

    fn i_job_outputs(
        &self,
        tx: &mut Transaction,
        job: JobId,
    ) -> OResult<Vec<(JobOutput, JobFile)>> {
        self.tx_get_rows(tx, self.q_job_outputs(job))
    }

    pub fn job_outputs(
        &self,
        job: JobId,
    ) -> OResult<Vec<(JobOutput, JobFile)>> {
        self.get_rows(self.q_job_outputs(job))
    }

    pub fn job_file_opt(
        &self,
        job: JobId,
        file: JobFileId,
    ) -> OResult<Option<JobFile>> {
        self.get_row_opt(JobFile::find(job, file))
    }

    pub fn job_events(
        &self,
        job: JobId,
        minseq: usize,
    ) -> OResult<Vec<JobEvent>> {
        self.get_rows(
            Query::select()
                .from(JobEventDef::Table)
                .columns(JobEvent::columns())
                .order_by(JobEventDef::Seq, Order::Asc)
                .and_where(Expr::col(JobEventDef::Job).eq(job))
                .and_where(Expr::col(JobEventDef::Seq).gte(minseq as i64))
                .to_owned(),
        )
    }

    pub fn job(&self, job: JobId) -> OResult<Job> {
        self.get_row(Job::find(job))
    }

    pub fn job_opt(&self, job: JobId) -> OResult<Option<Job>> {
        self.get_row_opt(Job::find(job))
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
    ) -> OResult<Job>
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
        };

        /*
         * Use the timestamp from the generated ID as the submit time.
         */
        let start = j.id.datetime();

        let c = &mut self.1.lock().unwrap().conn;
        let mut tx = c.transaction_with_behavior(
            rusqlite::TransactionBehavior::Immediate,
        )?;

        let ic = self.tx_exec_insert(&mut tx, j.insert())?;
        assert_eq!(ic, 1);

        self.i_job_time_record(&mut tx, j.id, "submit", start)?;
        if !waiting {
            /*
             * If the job is not waiting, record the submit time as the time
             * at which dependencies are satisfied and the job is ready to
             * run.
             */
            self.i_job_time_record(&mut tx, j.id, "ready", start)?;
        }

        for (i, ct) in tasks.iter().enumerate() {
            let ic = self.tx_exec_insert(
                &mut tx,
                Task::from_create(ct, j.id, i).insert(),
            )?;
            assert_eq!(ic, 1);
        }

        for cd in depends.iter() {
            /*
             * Make sure that this job exists in the system and that its
             * owner matches the owner of this job.  By requiring prior jobs
             * to exist already at the time of dependency specification we
             * can avoid the mess of cycles in the dependency graph.
             */
            let pj: Option<Job> =
                self.tx_get_row_opt(&mut tx, Job::find(cd.prior_job))?;

            if !pj.map(|j| j.owner == owner).unwrap_or(false) {
                /*
                 * Try not to leak information about job IDs from other
                 * users in the process.
                 */
                conflict!("prior job does not exist");
            }

            let ic = self.tx_exec_insert(
                &mut tx,
                JobDepend::from_create(cd, j.id).insert(),
            )?;
            assert_eq!(ic, 1);
        }

        for ci in inputs.iter() {
            let ic = self.tx_exec_insert(
                &mut tx,
                JobInput::from_create(ci.as_str(), j.id).insert(),
            )?;
            assert_eq!(ic, 1);
        }

        for (i, rule) in output_rules.iter().enumerate() {
            let ic = self.tx_exec_insert(
                &mut tx,
                JobOutputRule::from_create(rule, j.id, i).insert(),
            )?;
            assert_eq!(ic, 1);
        }

        for (n, v) in tags {
            let ic = self.tx_exec_insert(
                &mut tx,
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

        tx.commit()?;
        Ok(j)
    }

    pub fn job_input(&self, job: JobId, file: JobFileId) -> OResult<JobInput> {
        self.get_row(
            Query::select()
                .from(JobInputDef::Table)
                .columns(JobInput::columns())
                .and_where(Expr::col(JobInputDef::Job).eq(job))
                .and_where(Expr::col(JobInputDef::Id).eq(file))
                .to_owned(),
        )
    }

    pub fn job_output(
        &self,
        job: JobId,
        file: JobFileId,
    ) -> OResult<JobOutput> {
        self.get_row(
            Query::select()
                .from(JobOutputDef::Table)
                .columns(JobOutput::columns())
                .and_where(Expr::col(JobOutputDef::Job).eq(job))
                .and_where(Expr::col(JobOutputDef::Id).eq(file))
                .to_owned(),
        )
    }

    pub fn published_file_by_name(
        &self,
        owner: UserId,
        series: &str,
        version: &str,
        name: &str,
    ) -> OResult<Option<PublishedFile>> {
        self.get_row_opt(PublishedFile::find(owner, series, version, name))
    }

    pub fn job_publish_output(
        &self,
        job: JobId,
        file: JobFileId,
        series: &str,
        version: &str,
        name: &str,
    ) -> OResult<()> {
        let c = &mut self.1.lock().unwrap().conn;
        let mut tx = c.transaction_with_behavior(
            rusqlite::TransactionBehavior::Immediate,
        )?;

        let j: Job = self.tx_get_row(&mut tx, Job::find(job))?;
        if !j.complete {
            conflict!("job must be complete before files are published");
        }

        /*
         * Make sure this output exists.
         */
        let _jo: JobOutput =
            self.tx_get_row(&mut tx, JobOutput::find(job, file))?;

        /*
         * Determine whether this record has been published already or not.
         */
        let pf: Option<PublishedFile> = self.tx_get_row_opt(
            &mut tx,
            PublishedFile::find(j.owner, series, version, name),
        )?;

        if let Some(pf) = pf {
            if pf.owner == j.owner && pf.job == job && pf.file == file {
                /*
                 * The target file is the same, so just succeed.
                 */
                tx.commit()?;
                return Ok(());
            } else {
                conflict!(
                    "that published file already exists with \
                    different contents"
                );
            }
        }

        let ic = self.tx_exec_insert(
            &mut tx,
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

        tx.commit()?;
        Ok(())
    }

    pub fn job_add_output(
        &self,
        job: JobId,
        path: &str,
        id: JobFileId,
        size: u64,
    ) -> OResult<()> {
        let c = &mut self.1.lock().unwrap().conn;
        let mut tx = c.transaction_with_behavior(
            rusqlite::TransactionBehavior::Immediate,
        )?;

        let j: Job = self.tx_get_row(&mut tx, Job::find(job))?;
        if j.complete {
            conflict!("job already complete, cannot add more files");
        }

        let ic = self.tx_exec_insert(
            &mut tx,
            JobFile { job, id, size: DataSize(size), time_archived: None }
                .insert(),
        )?;
        assert_eq!(ic, 1);

        let ic = self.tx_exec_insert(
            &mut tx,
            JobOutput { job, path: path.to_string(), id }.insert(),
        )?;
        assert_eq!(ic, 1);

        tx.commit()?;
        Ok(())
    }

    pub fn job_add_input(
        &self,
        job: JobId,
        name: &str,
        id: JobFileId,
        size: u64,
    ) -> OResult<()> {
        if name.contains('/') {
            conflict!("name cannot be a path");
        }

        let c = &mut self.1.lock().unwrap().conn;
        let mut tx = c.transaction_with_behavior(
            rusqlite::TransactionBehavior::Immediate,
        )?;

        let j: Job = self.tx_get_row(&mut tx, Job::find(job))?;
        if !j.waiting {
            conflict!("job not waiting, cannot add more inputs");
        }

        let ic = self.tx_exec_insert(
            &mut tx,
            JobFile { job, id, size: DataSize(size), time_archived: None }
                .insert(),
        )?;
        assert_eq!(ic, 1);

        let uc = self.tx_exec_update(
            &mut tx,
            Query::update()
                .table(JobInputDef::Table)
                .and_where(Expr::col(JobInputDef::Job).eq(job))
                .and_where(Expr::col(JobInputDef::Name).eq(name))
                .value(JobInputDef::Id, id)
                .to_owned(),
        )?;
        assert_eq!(uc, 1);

        tx.commit()?;
        Ok(())
    }

    pub fn job_next_unarchived(&self) -> OResult<Option<Job>> {
        /*
         * Find the oldest completed job that has not yet been archived to long
         * term storage.
         */
        self.get_row_opt(
            Query::select()
                .from(JobDef::Table)
                .columns(Job::columns())
                .order_by(JobDef::Id, Order::Asc)
                .and_where(Expr::col(JobDef::Complete).eq(true))
                .and_where(Expr::col(JobDef::TimeArchived).is_null())
                .limit(1)
                .to_owned(),
        )
    }

    pub fn job_mark_archived(
        &self,
        job: JobId,
        time: DateTime<Utc>,
    ) -> OResult<()> {
        let uc = self.exec_update(
            Query::update()
                .table(JobDef::Table)
                .and_where(Expr::col(JobDef::Id).eq(job))
                .and_where(Expr::col(JobDef::TimeArchived).is_null())
                .value(JobDef::TimeArchived, IsoDate(time))
                .to_owned(),
        )?;
        assert_eq!(uc, 1);

        Ok(())
    }

    pub fn job_file_next_unarchived(&self) -> OResult<Option<JobFile>> {
        /*
         * Find the most recently uploaded output stored as part of a job that
         * has been completed.
         */
        self.get_row_opt(
            Query::select()
                .from(JobDef::Table)
                .inner_join(
                    JobFileDef::Table,
                    Expr::col((JobDef::Table, JobDef::Id))
                        .eq(Expr::col((JobFileDef::Table, JobFileDef::Job))),
                )
                .columns(JobFile::columns())
                .order_by((JobFileDef::Table, JobFileDef::Id), Order::Asc)
                .and_where(
                    Expr::col((JobDef::Table, JobDef::Complete)).eq(true),
                )
                .and_where(
                    Expr::col((JobFileDef::Table, JobFileDef::TimeArchived))
                        .is_null(),
                )
                .limit(1)
                .to_owned(),
        )
    }

    pub fn job_file_mark_archived(
        &self,
        file: &JobFile,
        time: DateTime<Utc>,
    ) -> OResult<()> {
        let uc = self.exec_update(
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
    }

    pub fn job_append_events(
        &self,
        job: JobId,
        events: impl Iterator<Item = JobEventToAppend>,
    ) -> OResult<()> {
        let c = &mut self.1.lock().unwrap().conn;
        let mut tx = c.transaction_with_behavior(
            rusqlite::TransactionBehavior::Immediate,
        )?;

        let j: Job = self.tx_get_row(&mut tx, Job::find(job))?;
        if j.complete {
            conflict!("job already complete, cannot append");
        }

        for e in events {
            self.i_job_event_insert(
                &mut tx,
                j.id,
                e.task,
                &e.stream,
                e.time,
                e.time_remote,
                &e.payload,
            )?;
        }

        tx.commit()?;
        Ok(())
    }

    pub fn job_append_event(
        &self,
        job: JobId,
        task: Option<u32>,
        stream: &str,
        time: DateTime<Utc>,
        time_remote: Option<DateTime<Utc>>,
        payload: &str,
    ) -> OResult<()> {
        let c = &mut self.1.lock().unwrap().conn;
        let mut tx = c.transaction_with_behavior(
            rusqlite::TransactionBehavior::Immediate,
        )?;

        let j: Job = self.tx_get_row(&mut tx, Job::find(job))?;
        if j.complete {
            conflict!("job already complete, cannot append");
        }

        self.i_job_event_insert(
            &mut tx,
            j.id,
            task,
            stream,
            time,
            time_remote,
            payload,
        )?;

        tx.commit()?;
        Ok(())
    }

    pub fn job_wakeup(&self, job: JobId) -> OResult<()> {
        let c = &mut self.1.lock().unwrap().conn;
        let mut tx = c.transaction_with_behavior(
            rusqlite::TransactionBehavior::Immediate,
        )?;

        let j: Job = self.tx_get_row(&mut tx, Job::find(job))?;
        if !j.waiting {
            conflict!("job {} was not waiting, cannot wakeup", j.id);
        }

        /*
         * Record the time at which the job became ready to run.
         */
        self.i_job_time_record(&mut tx, j.id, "ready", Utc::now())?;

        /*
         *
         * Estimate the length of time that we were waiting for dependencies
         * to complete running.
         */
        let mut msg = "job dependencies complete; ready to run".to_string();
        if let Some(dur) =
            self.i_job_time_delta(&mut tx, j.id, "submit", "ready")?
        {
            msg += &format!(" (waiting for {})", dur.render());
        }
        self.i_job_event_insert(
            &mut tx,
            j.id,
            None,
            "control",
            Utc::now(),
            None,
            &msg,
        )?;

        let uc = self.tx_exec_update(
            &mut tx,
            Query::update()
                .table(JobDef::Table)
                .and_where(Expr::col(JobDef::Id).eq(j.id))
                .and_where(Expr::col(JobDef::Waiting).eq(true))
                .value(JobDef::Waiting, false)
                .to_owned(),
        )?;
        assert_eq!(uc, 1);

        tx.commit()?;
        Ok(())
    }

    pub fn job_cancel(&self, job: JobId) -> OResult<bool> {
        let c = &mut self.1.lock().unwrap().conn;
        let mut tx = c.transaction_with_behavior(
            rusqlite::TransactionBehavior::Immediate,
        )?;

        let j: Job = self.tx_get_row(&mut tx, Job::find(job))?;
        if j.complete {
            conflict!("job {} is already complete", j.id);
        }

        if j.cancelled {
            /*
             * Already cancelled previously.
             */
            return Ok(false);
        }

        self.i_job_event_insert(
            &mut tx,
            j.id,
            None,
            "control",
            Utc::now(),
            None,
            "job cancelled",
        )?;

        let uc = self.tx_exec_update(
            &mut tx,
            Query::update()
                .table(JobDef::Table)
                .and_where(Expr::col(JobDef::Id).eq(j.id))
                .and_where(Expr::col(JobDef::Complete).eq(false))
                .value(JobDef::Cancelled, true)
                .to_owned(),
        )?;
        assert_eq!(uc, 1);

        tx.commit()?;
        Ok(true)
    }

    pub fn job_complete(&self, job: JobId, failed: bool) -> OResult<bool> {
        let c = &mut self.1.lock().unwrap().conn;
        let mut tx = c.transaction_with_behavior(
            rusqlite::TransactionBehavior::Immediate,
        )?;

        let j: Job = self.tx_get_row(&mut tx, Job::find(job))?;
        if j.complete {
            /*
             * This job is already complete.
             */
            tx.commit()?;
            return Ok(false);
        }

        if j.cancelled && !failed {
            conflict!("job {} cancelled; cannot succeed", j.id);
        }

        /*
         * Mark any tasks that have not yet completed as failed, as they
         * will not be executed.
         */
        let mut tasks_failed = false;
        let tasks: Vec<Task> =
            self.tx_get_rows(&mut tx, self.q_job_tasks(j.id))?;
        for t in tasks {
            if t.failed {
                tasks_failed = true;
            }
            if t.failed || t.complete || j.cancelled {
                continue;
            }

            self.i_job_event_insert(
                &mut tx,
                j.id,
                None,
                "control",
                Utc::now(),
                None,
                &format!("task {} was incomplete, marked failed", t.seq),
            )?;

            let uc = self.tx_exec_update(
                &mut tx,
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
             * If a task failed, we must report job-level failure even if
             * the job was for some reason not explicitly failed.
             */
            self.i_job_event_insert(
                &mut tx,
                j.id,
                None,
                "control",
                Utc::now(),
                None,
                "job failed because at least one task failed",
            )?;
            true
        } else {
            false
        };

        let uc = self.tx_exec_update(
            &mut tx,
            Query::update()
                .table(JobDef::Table)
                .and_where(Expr::col(JobDef::Id).eq(j.id))
                .and_where(Expr::col(JobDef::Complete).eq(false))
                .value(JobDef::Failed, failed)
                .value(JobDef::Complete, true)
                .to_owned(),
        )?;
        assert_eq!(uc, 1);

        self.i_job_time_record(&mut tx, j.id, "complete", Utc::now())?;

        tx.commit()?;
        Ok(true)
    }

    pub fn i_job_time_record(
        &self,
        tx: &mut Transaction,
        job: JobId,
        name: &str,
        when: DateTime<Utc>,
    ) -> OResult<()> {
        self.tx_exec_insert(
            tx,
            JobTime { job, name: name.to_string(), time: IsoDate(when) }
                .upsert(),
        )?;

        Ok(())
    }

    pub fn i_job_time_delta(
        &self,
        tx: &mut Transaction,
        job: JobId,
        from: &str,
        until: &str,
    ) -> OResult<Option<std::time::Duration>> {
        let from = if let Some(from) =
            self.tx_get_row_opt::<JobTime>(tx, JobTime::find(job, from))?
        {
            from
        } else {
            return Ok(None);
        };

        let until = if let Some(until) =
            self.tx_get_row_opt::<JobTime>(tx, JobTime::find(job, until))?
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
    ) -> OResult<HashMap<String, DateTime<Utc>>> {
        Ok(self
            .get_rows::<JobTime>(
                Query::select()
                    .from(JobTimeDef::Table)
                    .and_where(Expr::col(JobTimeDef::Job).eq(job))
                    .to_owned(),
            )?
            .into_iter()
            .map(|jt| (jt.name, jt.time.0))
            .collect())
    }

    pub fn job_store(&self, job: JobId) -> OResult<HashMap<String, JobStore>> {
        Ok(self
            .get_rows::<JobStore>(
                Query::select()
                    .from(JobStoreDef::Table)
                    .and_where(Expr::col(JobStoreDef::Job).eq(job))
                    .to_owned(),
            )?
            .into_iter()
            .map(|js| (js.name.to_string(), js))
            .collect())
    }

    pub fn job_store_put(
        &self,
        job: JobId,
        name: &str,
        value: &str,
        secret: bool,
        source: &str,
    ) -> OResult<()> {
        // use schema::{job, job_store};

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
        if value.as_bytes().len() > max_val_kib * 1024 {
            conflict!("maximum value size is {max_val_kib}KiB");
        }

        let c = &mut self.1.lock().unwrap().conn;
        let mut tx = c.transaction_with_behavior(
            rusqlite::TransactionBehavior::Immediate,
        )?;

        /*
         * Make sure the job exists and is not yet completed.  We do not
         * allow store updates after the job is finished.
         */
        let j: Job = self.tx_get_row(&mut tx, Job::find(job))?;
        if j.complete {
            conflict!("job {job} already complete; cannot update store");
        }

        /*
         * First, check to see if this value already exists in the store:
         */
        let pre: Option<JobStore> =
            self.tx_get_row_opt(&mut tx, JobStore::find(job, name))?;

        if let Some(pre) = pre {
            /*
             * Overwrite the existing value.  If the value we are replacing
             * was already marked as a secret, we mark the updated value as
             * secret as well to avoid accidents.
             */
            let new_secret = if pre.secret { true } else { secret };

            let uc = self.tx_exec_update(
                &mut tx,
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

            tx.commit()?;
            return Ok(());
        }

        /*
         * If the value does not already exist, we need to create it.  We
         * also need to make sure we do not allow values to be stored in
         * excess of the value count cap.
         */
        let count: usize = self.tx_get_row(
            &mut tx,
            Query::select()
                .from(JobStoreDef::Table)
                .expr(Expr::col(Asterisk).count())
                .and_where(Expr::col(JobStoreDef::Job).eq(job))
                .to_owned(),
        )?;
        if count >= max_val_count {
            conflict!("job {job} already has {count} store values");
        }

        let ic = self.tx_exec_insert(
            &mut tx,
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

        tx.commit()?;
        Ok(())
    }

    pub fn task_complete(
        &self,
        job: JobId,
        seq: u32,
        failed: bool,
    ) -> OResult<bool> {
        let c = &mut self.1.lock().unwrap().conn;
        let mut tx = c.transaction_with_behavior(
            rusqlite::TransactionBehavior::Immediate,
        )?;

        let j: Job = self.tx_get_row(&mut tx, Job::find(job))?;
        if j.complete {
            conflict!("job {} is already complete", j.id);
        }

        let t: Task = self.tx_get_row(&mut tx, Task::find(job, seq))?;
        if t.complete {
            tx.commit()?;
            return Ok(false);
        }

        let uc = self.tx_exec_update(
            &mut tx,
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

        tx.commit()?;
        Ok(true)
    }

    #[allow(clippy::too_many_arguments)]
    fn i_job_event_insert(
        &self,
        tx: &mut Transaction,
        job: JobId,
        task: Option<u32>,
        stream: &str,
        time: DateTime<Utc>,
        time_remote: Option<DateTime<Utc>>,
        payload: &str,
    ) -> OResult<()> {
        let max: Option<u32> = self.tx_get_row(
            tx,
            Query::select()
                .from(JobEventDef::Table)
                .and_where(Expr::col(JobEventDef::Job).eq(job))
                .expr(Expr::col(JobEventDef::Seq).max())
                .to_owned(),
        )?;

        let ic = self.tx_exec_insert(
            tx,
            JobEvent {
                job,
                task,
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

    pub fn user_jobs(&self, owner: UserId) -> OResult<Vec<Job>> {
        self.get_rows(
            Query::select()
                .from(JobDef::Table)
                .columns(Job::columns())
                .and_where(Expr::col(JobDef::Owner).eq(owner))
                .to_owned(),
        )
    }

    pub fn worker_job(&self, worker: WorkerId) -> OResult<Option<Job>> {
        self.get_row_opt(
            Query::select()
                .from(JobDef::Table)
                .columns(Job::columns())
                .and_where(Expr::col(JobDef::Worker).eq(worker))
                .to_owned(),
        )
    }

    pub fn user(&self, id: UserId) -> OResult<Option<AuthUser>> {
        self.get_row_opt::<User>(
            Query::select()
                .from(UserDef::Table)
                .columns(User::columns())
                .and_where(Expr::col(UserDef::Id).eq(id))
                .to_owned(),
        )?
        .map(|user| {
            Ok(AuthUser { privileges: self.i_user_privileges(user.id)?, user })
        })
        .transpose()
    }

    pub fn user_by_name(&self, name: &str) -> OResult<Option<AuthUser>> {
        self.get_row_opt::<User>(
            Query::select()
                .from(UserDef::Table)
                .columns(User::columns())
                .and_where(Expr::col(UserDef::Name).eq(name))
                .to_owned(),
        )?
        .map(|user| {
            Ok(AuthUser { privileges: self.i_user_privileges(user.id)?, user })
        })
        .transpose()
    }

    pub fn users(&self) -> OResult<Vec<AuthUser>> {
        self.get_rows::<User>(
            Query::select()
                .from(UserDef::Table)
                .columns(User::columns())
                .to_owned(),
        )?
        .into_iter()
        .map(|user| {
            Ok(AuthUser { privileges: self.i_user_privileges(user.id)?, user })
        })
        .collect::<OResult<_>>()
    }

    fn q_user_privileges(&self, user: UserId) -> SelectStatement {
        Query::select()
            .from(UserPrivilegeDef::Table)
            .column(UserPrivilegeDef::Privilege)
            .and_where(Expr::col(UserPrivilegeDef::User).eq(user))
            .order_by(UserPrivilegeDef::Privilege, Order::Asc)
            .to_owned()
    }

    fn i_user_privileges(&self, user: UserId) -> OResult<Vec<String>> {
        self.get_strings(self.q_user_privileges(user))
    }

    pub fn user_privilege_grant(
        &self,
        u: UserId,
        privilege: &str,
    ) -> OResult<bool> {
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

        let c = &mut self.1.lock().unwrap().conn;
        let mut tx = c.transaction_with_behavior(
            rusqlite::TransactionBehavior::Immediate,
        )?;

        /*
         * Confirm that the user exists before creating privilege records:
         */
        let u: User = self.tx_get_row(&mut tx, User::find(u))?;

        let ic = self.tx_exec_insert(
            &mut tx,
            UserPrivilege { user: u.id, privilege: privilege.to_string() }
                .upsert(),
        )?;
        assert!(ic == 0 || ic == 1);

        tx.commit()?;
        Ok(ic != 0)
    }

    pub fn user_privilege_revoke(
        &self,
        u: UserId,
        privilege: &str,
    ) -> OResult<bool> {
        let c = &mut self.1.lock().unwrap().conn;
        let mut tx = c.transaction_with_behavior(
            rusqlite::TransactionBehavior::Immediate,
        )?;

        /*
         * Confirm that the user exists before trying to remove privilege
         * records:
         */
        let u: User = self.tx_get_row(&mut tx, User::find(u))?;

        let dc = self.tx_exec_delete(
            &mut tx,
            Query::delete()
                .from_table(UserPrivilegeDef::Table)
                .and_where(Expr::col(UserPrivilegeDef::User).eq(u.id))
                .and_where(Expr::col(UserPrivilegeDef::Privilege).eq(privilege))
                .to_owned(),
        )?;
        assert!(dc == 0 || dc == 1);

        tx.commit()?;
        Ok(dc != 0)
    }

    fn i_user_create(&self, name: &str, tx: &mut Transaction) -> OResult<User> {
        let u = User {
            id: UserId::generate(),
            name: name.to_string(),
            token: genkey(48),
            time_create: Utc::now().into(),
        };

        let ic = self.tx_exec_insert(tx, u.insert())?;
        assert_eq!(ic, 1);

        Ok(u)
    }

    pub fn user_create(&self, name: &str) -> OResult<User> {
        /*
         * Make sure the requested username is not one we might mistake as a
         * ULID later.
         */
        if looks_like_a_ulid(name) {
            conflict!("usernames must not look like ULIDs");
        }

        let c = &mut self.1.lock().unwrap().conn;
        let mut tx = c.transaction_with_behavior(
            rusqlite::TransactionBehavior::Immediate,
        )?;

        let out = self.i_user_create(name, &mut tx)?;

        tx.commit()?;
        Ok(out)
    }

    pub fn user_ensure(&self, name: &str) -> OResult<AuthUser> {
        /*
         * Make sure the requested username is not one we might mistake as a
         * ULID later.
         */
        if looks_like_a_ulid(name) {
            conflict!("usernames must not look like ULIDs");
        }

        let c = &mut self.1.lock().unwrap().conn;
        let mut tx = c.transaction_with_behavior(
            rusqlite::TransactionBehavior::Immediate,
        )?;

        let user = if let Some(user) = self.get_row_opt::<User>(
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
            self.i_user_create(name, &mut tx)?
        };

        let au = AuthUser {
            privileges: self
                .tx_get_strings(&mut tx, self.q_user_privileges(user.id))?,
            user,
        };

        tx.commit()?;
        Ok(au)
    }

    pub fn user_auth(&self, token: &str) -> OResult<AuthUser> {
        let mut users: Vec<User> = self.get_rows::<User>(
            Query::select()
                .from(UserDef::Table)
                .columns(User::columns())
                .and_where(Expr::col(UserDef::Token).eq(token))
                .to_owned(),
        )?;

        match (users.pop(), users.pop()) {
            (None, _) => conflict!("auth failure"),
            (Some(u), Some(x)) => conflict!("token error ({}, {})", u.id, x.id),
            (Some(u), None) => {
                assert_eq!(&u.token, token);
                Ok(AuthUser {
                    privileges: self.i_user_privileges(u.id)?,
                    user: u,
                })
            }
        }
    }

    pub fn factory_create(&self, name: &str) -> OResult<Factory> {
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
        };

        self.exec_insert(f.insert())?;

        Ok(f)
    }

    pub fn factory(&self, id: FactoryId) -> OResult<Factory> {
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
            });
        }

        self.get_row(Factory::find(id))
    }

    pub fn factory_auth(&self, token: &str) -> OResult<Factory> {
        if token.is_empty() {
            /*
             * Make sure this trivially invalid value used in the legacy
             * sentinel record is never accepted, even if it somehow ends up in
             * the database by accident.
             */
            conflict!("auth failure");
        }

        let mut rows = self.get_rows::<Factory>(
            Query::select()
                .from(FactoryDef::Table)
                .columns(Factory::columns())
                .and_where(Expr::col(FactoryDef::Token).eq(token))
                .to_owned(),
        )?;

        match (rows.pop(), rows.pop()) {
            (None, _) => conflict!("auth failure"),
            (Some(u), Some(x)) => conflict!("token error ({}, {})", u.id, x.id),
            (Some(u), None) => {
                assert_eq!(u.token.as_str(), token);
                Ok(u)
            }
        }
    }

    pub fn factory_ping(&self, id: FactoryId) -> OResult<bool> {
        Ok(self.exec_update(
            Query::update()
                .table(FactoryDef::Table)
                .and_where(Expr::col(FactoryDef::Id).eq(id))
                .value(FactoryDef::Lastping, IsoDate::now())
                .to_owned(),
        )? > 0)
    }

    pub fn targets(&self) -> OResult<Vec<Target>> {
        self.get_rows(
            Query::select()
                .from(TargetDef::Table)
                .columns(Target::columns())
                .order_by(TargetDef::Id, Order::Asc)
                .to_owned(),
        )
    }

    pub fn target(&self, id: TargetId) -> OResult<Target> {
        self.get_row(Target::find(id))
    }

    pub fn target_create(&self, name: &str, desc: &str) -> OResult<Target> {
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

        let ic = self.exec_insert(t.insert())?;
        assert_eq!(ic, 1);

        Ok(t)
    }

    pub fn target_resolve(&self, name: &str) -> OResult<Option<Target>> {
        /*
         * Use the target name to look up the initial target match:
         */
        let mut target: Target = if let Some(target) =
            self.get_row_opt(Target::find_by_name(name))?
        {
            target
        } else {
            return Ok(None);
        };

        let mut count = 0;
        loop {
            if count > 32 {
                conflict!("too many target redirects starting from {:?}", name);
            }
            count += 1;

            if let Some(redirect) = &target.redirect {
                target = if let Some(target) =
                    self.get_row_opt(Target::find(*redirect))?
                {
                    target
                } else {
                    return Ok(None);
                };
            } else {
                return Ok(Some(target));
            }
        }
    }

    pub fn target_require(
        &self,
        id: TargetId,
        privilege: Option<&str>,
    ) -> OResult<()> {
        let uc = self.exec_update(
            Query::update()
                .table(TargetDef::Table)
                .and_where(Expr::col(TargetDef::Id).eq(id))
                .value(TargetDef::Privilege, privilege)
                .to_owned(),
        )?;
        assert!(uc == 1);

        Ok(())
    }

    pub fn target_redirect(
        &self,
        id: TargetId,
        redirect: Option<TargetId>,
    ) -> OResult<()> {
        let uc = self.exec_update(
            Query::update()
                .table(TargetDef::Table)
                .and_where(Expr::col(TargetDef::Id).eq(id))
                .value(TargetDef::Redirect, redirect)
                .to_owned(),
        )?;
        assert!(uc == 1);

        Ok(())
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
    ) -> OResult<Target> {
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

        let c = &mut self.1.lock().unwrap().conn;
        let mut tx = c.transaction_with_behavior(
            rusqlite::TransactionBehavior::Immediate,
        )?;

        /*
         * First, load the target to rename from the database.
         */
        let t: Target = self.tx_get_row(&mut tx, Target::find(id))?;
        if t.name == new_name {
            conflict!("target {} already has name {new_name:?}", t.id);
        }

        /*
         * Then, make sure a target with the new name does not yet
         * exist.
         */
        let nt: Option<Target> =
            self.tx_get_row_opt(&mut tx, Target::find_by_name(new_name))?;
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
        let uc = self.tx_exec_update(
            &mut tx,
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

        let ic = self.tx_exec_insert(&mut tx, nt.insert())?;
        assert_eq!(ic, 1);

        tx.commit()?;
        Ok(nt)
    }

    /*
     * Helper routines for database access:
     */

    pub fn tx_exec_delete(
        &self,
        tx: &mut Transaction,
        d: DeleteStatement,
    ) -> OResult<usize> {
        let (q, v) = d.build_rusqlite(SqliteQueryBuilder);
        self.tx_exec(tx, q, v)
    }

    pub fn tx_exec_update(
        &self,
        tx: &mut Transaction,
        u: UpdateStatement,
    ) -> OResult<usize> {
        let (q, v) = u.build_rusqlite(SqliteQueryBuilder);
        self.tx_exec(tx, q, v)
    }

    pub fn tx_exec_insert(
        &self,
        tx: &mut Transaction,
        i: InsertStatement,
    ) -> OResult<usize> {
        let (q, v) = i.build_rusqlite(SqliteQueryBuilder);
        self.tx_exec(tx, q, v)
    }

    pub fn tx_exec(
        &self,
        tx: &mut Transaction,
        q: String,
        v: RusqliteValues,
    ) -> OResult<usize> {
        let mut s = tx.prepare(&q)?;
        let out = s.execute(&*v.as_params())?;

        Ok(out)
    }

    #[allow(unused)]
    pub fn exec_delete(&self, d: DeleteStatement) -> OResult<usize> {
        let (q, v) = d.build_rusqlite(SqliteQueryBuilder);
        debug!(self.0, "query: {q}"; "sql" => true);
        self.exec(q, v)
    }

    pub fn exec_update(&self, u: UpdateStatement) -> OResult<usize> {
        let (q, v) = u.build_rusqlite(SqliteQueryBuilder);
        debug!(self.0, "query: {q}"; "sql" => true);
        self.exec(q, v)
    }

    pub fn exec_insert(&self, i: InsertStatement) -> OResult<usize> {
        let (q, v) = i.build_rusqlite(SqliteQueryBuilder);
        debug!(self.0, "query: {q}"; "sql" => true);
        self.exec(q, v)
    }

    pub fn exec(&self, q: String, v: RusqliteValues) -> OResult<usize> {
        let c = &mut self.1.lock().unwrap().conn;

        let out = c.prepare(&q)?.execute(&*v.as_params())?;

        Ok(out)
    }

    pub fn get_strings(&self, s: SelectStatement) -> OResult<Vec<String>> {
        let (q, v) = s.build_rusqlite(SqliteQueryBuilder);
        debug!(self.0, "query: {q}"; "sql" => true);
        let c = &mut self.1.lock().unwrap().conn;

        let mut s = c.prepare(&q)?;
        let out = s.query_map(&*v.as_params(), |row| row.get(0))?;

        Ok(out.collect::<rusqlite::Result<_>>()?)
    }

    pub fn get_rows<T: FromRow>(&self, s: SelectStatement) -> OResult<Vec<T>> {
        let (q, v) = s.build_rusqlite(SqliteQueryBuilder);
        debug!(self.0, "query: {q}"; "sql" => true);
        let c = &mut self.1.lock().unwrap().conn;

        let mut s = c.prepare(&q)?;
        let out = s.query_map(&*v.as_params(), T::from_row)?;

        Ok(out.collect::<rusqlite::Result<_>>()?)
    }

    pub fn get_row<T: FromRow>(&self, s: SelectStatement) -> OResult<T> {
        let (q, v) = s.build_rusqlite(SqliteQueryBuilder);
        debug!(self.0, "query: {q}"; "sql" => true);
        let c = &mut self.1.lock().unwrap().conn;

        let mut s = c.prepare(&q)?;
        let out = s.query_map(&*v.as_params(), T::from_row)?;
        let mut out = out.collect::<rusqlite::Result<Vec<T>>>()?;
        match out.len() {
            0 => conflict!("record not found"),
            1 => Ok(out.pop().unwrap()),
            n => conflict!("found {n} records when we wanted only 1"),
        }
    }

    pub fn get_row_opt<T: FromRow>(
        &self,
        s: SelectStatement,
    ) -> OResult<Option<T>> {
        let (q, v) = s.build_rusqlite(SqliteQueryBuilder);
        debug!(self.0, "query: {q}"; "sql" => true);
        let c = &mut self.1.lock().unwrap().conn;

        let mut s = c.prepare(&q)?;
        let out = s.query_map(&*v.as_params(), T::from_row)?;
        let mut out = out.collect::<rusqlite::Result<Vec<T>>>()?;
        match out.len() {
            0 => Ok(None),
            1 => Ok(Some(out.pop().unwrap())),
            n => conflict!("found {n} records when we wanted only 1"),
        }
    }

    pub fn tx_get_row_opt<T: FromRow>(
        &self,
        tx: &mut Transaction,
        s: SelectStatement,
    ) -> OResult<Option<T>> {
        let (q, v) = s.build_rusqlite(SqliteQueryBuilder);
        debug!(self.0, "query: {q}"; "sql" => true);
        let mut s = tx.prepare(&q)?;
        let out = s.query_map(&*v.as_params(), T::from_row)?;
        let mut out = out.collect::<rusqlite::Result<Vec<T>>>()?;
        match out.len() {
            0 => Ok(None),
            1 => Ok(Some(out.pop().unwrap())),
            n => conflict!("found {n} records when we wanted only 1"),
        }
    }

    pub fn tx_get_strings(
        &self,
        tx: &mut Transaction,
        s: SelectStatement,
    ) -> OResult<Vec<String>> {
        let (q, v) = s.build_rusqlite(SqliteQueryBuilder);
        debug!(self.0, "query: {q}"; "sql" => true);
        let mut s = tx.prepare(&q)?;
        let out = s.query_map(&*v.as_params(), |row| row.get(0))?;

        Ok(out.collect::<rusqlite::Result<_>>()?)
    }

    pub fn tx_get_rows<T: FromRow>(
        &self,
        tx: &mut Transaction,
        s: SelectStatement,
    ) -> OResult<Vec<T>> {
        let (q, v) = s.build_rusqlite(SqliteQueryBuilder);
        debug!(self.0, "query: {q}"; "sql" => true);
        let mut s = tx.prepare(&q)?;
        let out = s.query_map(&*v.as_params(), T::from_row)?;

        Ok(out.collect::<rusqlite::Result<_>>()?)
    }

    pub fn tx_get_row<T: FromRow>(
        &self,
        tx: &mut Transaction,
        s: SelectStatement,
    ) -> OResult<T> {
        let (q, v) = s.build_rusqlite(SqliteQueryBuilder);
        debug!(self.0, "query: {q}"; "sql" => true);
        let mut s = tx.prepare(&q)?;
        let out = s.query_map(&*v.as_params(), T::from_row)?;
        let mut out = out.collect::<rusqlite::Result<Vec<T>>>()?;
        match out.len() {
            0 => conflict!("record not found"),
            1 => Ok(out.pop().unwrap()),
            n => conflict!("found {n} records when we wanted only 1"),
        }
    }
}
