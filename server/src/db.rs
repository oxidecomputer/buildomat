/*
 * Copyright 2023 Oxide Computer Company
 */

use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
use std::sync::Mutex;

use anyhow::{anyhow, bail, Result};
use buildomat_common::*;
use buildomat_types::*;
use chrono::prelude::*;
use diesel::prelude::*;
#[allow(unused_imports)]
use rusty_ulid::Ulid;
#[allow(unused_imports)]
use slog::{error, info, warn, Logger};
use thiserror::Error;

mod models;
mod schema;

pub use models::*;

#[derive(Error, Debug)]
pub enum OperationError {
    #[error("conflict: {0}")]
    Conflict(String),
    #[error(transparent)]
    Sql(#[from] diesel::result::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type OResult<T> = std::result::Result<T, OperationError>;

macro_rules! conflict {
    ($msg:expr) => {
        return Err(OperationError::Conflict($msg.to_string()))
    };
    ($fmt:expr, $($arg:tt)*) => {
        return Err(OperationError::Conflict(format!($fmt, $($arg)*)))
    }
}

struct Inner {
    conn: diesel::sqlite::SqliteConnection,
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

impl Database {
    pub fn new<P: AsRef<Path>>(
        log: Logger,
        path: P,
        cache_kb: Option<u32>,
    ) -> Result<Database> {
        let conn = buildomat_database::sqlite_setup(
            &log,
            path,
            include_str!("../schema.sql"),
            cache_kb,
        )?;

        Ok(Database(log, Mutex::new(Inner { conn })))
    }

    pub fn workers(&self) -> Result<Vec<Worker>> {
        let c = &mut self.1.lock().unwrap().conn;

        use schema::worker::dsl;

        Ok(dsl::worker.order_by(dsl::id.asc()).get_results(c)?)
    }

    pub fn workers_active(&self) -> Result<Vec<Worker>> {
        let c = &mut self.1.lock().unwrap().conn;

        use schema::worker::dsl;

        Ok(dsl::worker
            .filter(dsl::deleted.eq(false))
            .order_by(dsl::id.asc())
            .get_results(c)?)
    }

    pub fn workers_for_factory(
        &self,
        factory: &Factory,
    ) -> Result<Vec<Worker>> {
        let c = &mut self.1.lock().unwrap().conn;

        use schema::worker::dsl;

        Ok(dsl::worker
            .filter(dsl::factory.nullable().eq(factory.id))
            .filter(dsl::deleted.eq(false))
            .order_by(dsl::id.asc())
            .get_results(c)?)
    }

    pub fn worker_jobs(&self, worker: WorkerId) -> Result<Vec<Job>> {
        let c = &mut self.1.lock().unwrap().conn;

        use schema::job::dsl;

        Ok(dsl::job.filter(dsl::worker.eq(worker)).get_results(c)?)
    }

    pub fn free_workers(&self) -> Result<Vec<Worker>> {
        use schema::{job, worker};

        let c = &mut self.1.lock().unwrap().conn;

        let free_workers: Vec<(Worker, Option<Job>)> = worker::dsl::worker
            .left_outer_join(job::table)
            .filter(job::dsl::worker.is_null())
            .filter(worker::dsl::deleted.eq(false))
            .filter(worker::dsl::recycle.eq(false))
            .filter(worker::dsl::token.is_not_null())
            .order_by(worker::dsl::id.asc())
            .get_results(c)?;

        Ok(free_workers.iter().map(|(w, _)| w.clone()).collect())
    }

    pub fn worker_recycle_all(&self) -> Result<usize> {
        use schema::worker::dsl;

        let c = &mut self.1.lock().unwrap().conn;

        Ok(diesel::update(dsl::worker)
            .filter(dsl::deleted.eq(false))
            .set(dsl::recycle.eq(true))
            .execute(c)?)
    }

    pub fn worker_recycle(&self, id: WorkerId) -> Result<bool> {
        use schema::worker::dsl;

        let c = &mut self.1.lock().unwrap().conn;

        Ok(diesel::update(dsl::worker)
            .filter(dsl::id.eq(id))
            .set(dsl::recycle.eq(true))
            .execute(c)?
            > 0)
    }

    pub fn worker_flush(&self, id: WorkerId) -> Result<bool> {
        use schema::worker::dsl;

        let c = &mut self.1.lock().unwrap().conn;

        Ok(diesel::update(dsl::worker)
            .filter(dsl::id.eq(id))
            .set(dsl::wait_for_flush.eq(false))
            .execute(c)?
            > 0)
    }

    pub fn worker_destroy(&self, id: WorkerId) -> Result<bool> {
        use schema::worker::dsl;

        let c = &mut self.1.lock().unwrap().conn;

        Ok(diesel::update(dsl::worker)
            .filter(dsl::id.eq(id))
            .set(dsl::deleted.eq(true))
            .execute(c)?
            > 0)
    }

    pub fn worker_ping(&self, id: WorkerId) -> Result<bool> {
        use schema::worker::dsl;

        let c = &mut self.1.lock().unwrap().conn;

        let now = models::IsoDate(Utc::now());
        Ok(diesel::update(dsl::worker)
            .filter(dsl::id.eq(id))
            .set(dsl::lastping.eq(now))
            .execute(c)?
            > 0)
    }

    pub fn i_worker_assign_job(
        &self,
        tx: &mut SqliteConnection,
        w: &Worker,
        jid: JobId,
    ) -> OResult<()> {
        use schema::job;

        let j: Job = job::dsl::job.find(jid).get_result(tx)?;
        if let Some(jw) = j.worker.as_ref() {
            conflict!("job {} already assigned to worker {}", j.id, jw);
        }

        let c: i64 = job::dsl::job
            .filter(job::dsl::worker.eq(w.id))
            .count()
            .get_result(tx)?;
        if c > 0 {
            conflict!("worker {} already has {} jobs assigned", w.id, c);
        }

        let uc = diesel::update(job::dsl::job)
            .filter(job::dsl::id.eq(j.id))
            .set(job::dsl::worker.eq(w.id))
            .execute(tx)?;
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

        c.immediate_transaction(|tx| {
            use schema::worker;

            let w: Worker = worker::dsl::worker.find(wid).get_result(tx)?;
            if w.deleted || w.recycle {
                conflict!("worker {} already deleted, cannot assign job", w.id);
            }

            self.i_worker_assign_job(tx, &w, jid)
        })
    }

    pub fn worker_bootstrap(
        &self,
        bootstrap: &str,
        token: &str,
    ) -> Result<Option<Worker>> {
        let log = &self.0;
        let c = &mut self.1.lock().unwrap().conn;

        c.immediate_transaction(|tx| {
            use schema::worker;

            let w: Worker = worker::dsl::worker
                .filter(worker::dsl::bootstrap.eq(bootstrap))
                .get_result(tx)?;
            assert_eq!(&w.bootstrap, bootstrap);
            if w.deleted {
                error!(
                    log,
                    "worker {} already deleted, cannot bootstrap", w.id
                );
                return Ok(None);
            }

            if w.factory_private.is_none() {
                error!(
                    log,
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
                        log,
                        "worker {} already has a token ({} != {})",
                        w.id,
                        current,
                        token
                    );
                    return Ok(None);
                }
            } else {
                let count = diesel::update(worker::dsl::worker)
                    .filter(worker::dsl::id.eq(w.id))
                    .filter(worker::dsl::bootstrap.eq(bootstrap))
                    .filter(worker::dsl::token.is_null())
                    .set((
                        worker::dsl::token.eq(token),
                        worker::dsl::lastping.eq(IsoDate(Utc::now())),
                    ))
                    .execute(tx)?;
                assert_eq!(count, 1);
            }

            Ok(Some(worker::dsl::worker.find(w.id).get_result(tx)?))
        })
    }

    pub fn worker_associate(
        &self,
        wid: WorkerId,
        factory_private: &str,
        factory_metadata: Option<&metadata::FactoryMetadata>,
    ) -> OResult<()> {
        let c = &mut self.1.lock().unwrap().conn;

        /*
         * First, convert the provided metadata object to a serde JSON value.
         * We'll use this (rather than the concrete string) for comparison with
         * an existing record because the string might not be serialised
         * byte-for-byte the same each time.
         */
        let factory_metadata = factory_metadata
            .as_ref()
            .map(|md| serde_json::to_value(md))
            .transpose()?
            .map(|j| JsonValue(j));

        c.immediate_transaction(|tx| {
            use schema::worker;

            let w: Worker = worker::dsl::worker.find(wid).get_result(tx)?;
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
                let count = diesel::update(worker::dsl::worker)
                    .filter(worker::dsl::id.eq(w.id))
                    .set(worker::dsl::factory_private.eq(factory_private))
                    .execute(tx)?;
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
                let count = diesel::update(worker::dsl::worker)
                    .filter(worker::dsl::id.eq(w.id))
                    .set(worker::dsl::factory_metadata.eq(factory_metadata))
                    .execute(tx)?;
                assert_eq!(count, 1);
            }

            Ok(())
        })
    }

    pub fn worker_get(&self, id: WorkerId) -> Result<Worker> {
        use schema::worker::dsl;

        let c = &mut self.1.lock().unwrap().conn;
        Ok(dsl::worker.find(id).get_result(c)?)
    }

    pub fn worker_get_opt(&self, id: WorkerId) -> Result<Option<Worker>> {
        use schema::worker::dsl;

        let c = &mut self.1.lock().unwrap().conn;
        Ok(dsl::worker.find(id).get_result(c).optional()?)
    }

    pub fn worker_auth(&self, token: &str) -> Result<Worker> {
        use schema::worker;

        let c = &mut self.1.lock().unwrap().conn;

        let mut workers: Vec<Worker> = worker::dsl::worker
            .filter(worker::dsl::token.eq(token))
            .get_results(c)?;

        match (workers.pop(), workers.pop()) {
            (None, _) => bail!("auth failure"),
            (Some(u), Some(x)) => bail!("token error ({}, {})", u.id, x.id),
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
    ) -> Result<Worker> {
        use schema::worker;

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

        c.immediate_transaction(|tx| {
            let count = diesel::insert_into(worker::dsl::worker)
                .values(&w)
                .execute(tx)?;
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
                self.i_worker_assign_job(tx, &w, job)?;
            }

            Ok(w)
        })
    }

    /**
     * Enumerate all jobs.
     */
    pub fn jobs_all(&self) -> Result<Vec<Job>> {
        use schema::job::dsl;

        let c = &mut self.1.lock().unwrap().conn;
        Ok(dsl::job.order_by(dsl::id.asc()).get_results(c)?)
    }

    /**
     * Enumerate jobs that are active; i.e., not yet complete, but not waiting.
     */
    pub fn jobs_active(&self) -> Result<Vec<Job>> {
        use schema::job::dsl;

        let c = &mut self.1.lock().unwrap().conn;
        Ok(dsl::job
            .filter(dsl::complete.eq(false))
            .filter(dsl::waiting.eq(false))
            .order_by(dsl::id.asc())
            .get_results(c)?)
    }

    /**
     * Enumerate jobs that are waiting for inputs, or for dependees to complete.
     */
    pub fn jobs_waiting(&self) -> Result<Vec<Job>> {
        use schema::job::dsl;

        let c = &mut self.1.lock().unwrap().conn;
        Ok(dsl::job
            .filter(dsl::complete.eq(false))
            .filter(dsl::waiting.eq(true))
            .order_by(dsl::id.asc())
            .get_results(c)?)
    }

    /**
     * Enumerate some number of the most recently complete jobs.
     */
    pub fn jobs_completed(&self, limit: usize) -> Result<Vec<Job>> {
        use schema::job::dsl;

        let c = &mut self.1.lock().unwrap().conn;
        let mut res = dsl::job
            .filter(dsl::complete.eq(true))
            .order_by(dsl::id.desc())
            .limit(limit.try_into().unwrap())
            .get_results(c)?;
        res.reverse();
        Ok(res)
    }

    pub fn job_tasks(&self, job: JobId) -> Result<Vec<Task>> {
        use schema::task::dsl;

        let c = &mut self.1.lock().unwrap().conn;
        Ok(dsl::task
            .filter(dsl::job.eq(job))
            .order_by(dsl::seq.asc())
            .get_results(c)?)
    }

    pub fn job_tags(&self, job: JobId) -> Result<HashMap<String, String>> {
        use schema::job_tag::dsl;

        let c = &mut self.1.lock().unwrap().conn;

        Ok(dsl::job_tag
            .select((dsl::name, dsl::value))
            .filter(dsl::job.eq(job))
            .get_results::<(String, String)>(c)?
            .into_iter()
            .collect())
    }

    pub fn job_output_rules(&self, job: JobId) -> Result<Vec<JobOutputRule>> {
        use schema::job_output_rule::dsl;

        let c = &mut self.1.lock().unwrap().conn;
        Ok(dsl::job_output_rule
            .filter(dsl::job.eq(job))
            .order_by(dsl::seq.asc())
            .get_results(c)?)
    }

    pub fn job_depends(&self, job: JobId) -> Result<Vec<JobDepend>> {
        use schema::job_depend;

        let c = &mut self.1.lock().unwrap().conn;

        Ok(job_depend::dsl::job_depend
            .filter(job_depend::dsl::job.eq(job))
            .order_by(job_depend::dsl::name)
            .get_results(c)?)
    }

    /**
     * When a prior job has completed, we need to mark the dependency record as
     * satisfied.  In the process, if requested, we will copy any output files
     * from the previous job into the new job as input files.
     */
    pub fn job_depend_satisfy(&self, jid: JobId, d: &JobDepend) -> Result<()> {
        use schema::{job, job_depend, job_input};

        let c = &mut self.1.lock().unwrap().conn;

        c.immediate_transaction(|tx| {
            /*
             * Confirm that this job exists and is still waiting.
             */
            let j: Job = job::dsl::job.find(jid).get_result(tx)?;
            if !j.waiting {
                bail!("job not waiting, cannot satisfy dependency");
            }

            /*
             * Confirm that the dependency record still exists and has not yet
             * been satisfied.
             */
            let d: JobDepend = job_depend::dsl::job_depend
                .find((j.id, &d.name))
                .get_result(tx)?;
            if d.satisfied {
                bail!("job dependency already satisfied");
            }

            /*
             * Confirm that the prior job exists and is complete.
             */
            let pj: Job = job::dsl::job.find(d.prior_job).get_result(tx)?;
            if !pj.complete {
                bail!("prior job not complete");
            }

            if d.copy_outputs {
                /*
                 * Resolve the list of output files.
                 */
                let pjouts = self.i_job_outputs(tx, pj.id)?;

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

                    diesel::insert_into(job_input::dsl::job_input)
                        .values(ji)
                        .on_conflict_do_nothing()
                        .execute(tx)?;
                }
            }

            diesel::update(job_depend::dsl::job_depend)
                .set((job_depend::dsl::satisfied.eq(true),))
                .filter(job_depend::dsl::job.eq(j.id))
                .filter(job_depend::dsl::name.eq(&d.name))
                .execute(tx)?;

            Ok(())
        })
    }

    pub fn job_inputs(
        &self,
        job: JobId,
    ) -> Result<Vec<(JobInput, Option<JobFile>)>> {
        use schema::{job_file, job_input};

        let c = &mut self.1.lock().unwrap().conn;

        Ok(job_input::dsl::job_input
            .left_outer_join(
                job_file::table.on(
                    /*
                     * The file ID column must always match:
                     */
                    job_file::dsl::id.nullable().eq(job_input::dsl::id).and(
                        /*
                         * Either the other_job field is null, and the input
                         * job ID matches the file job ID directly...
                         */
                        (job_input::dsl::other_job
                            .is_null()
                            .and(job_file::dsl::job.eq(job_input::dsl::job)))
                        /*
                         * ... or the other_job field is populated and it
                         * matches the file job ID instead:
                         */
                        .or(job_input::dsl::other_job.is_not_null().and(
                            job_file::dsl::job
                                .nullable()
                                .eq(job_input::dsl::other_job),
                        )),
                    ),
                ),
            )
            .filter(job_input::dsl::job.eq(job))
            .order_by(job_input::dsl::id.asc())
            .get_results(c)?)
    }

    fn i_job_outputs(
        &self,
        tx: &mut SqliteConnection,
        job: JobId,
    ) -> Result<Vec<(JobOutput, JobFile)>> {
        use schema::{job_file, job_output};

        Ok(job_output::dsl::job_output
            .inner_join(
                job_file::table.on(job_file::dsl::job
                    .eq(job_output::dsl::job)
                    .and(job_file::dsl::id.eq(job_output::dsl::id))),
            )
            .filter(job_file::dsl::job.eq(job))
            .order_by(job_file::dsl::id.asc())
            .get_results(tx)?)
    }

    pub fn job_outputs(&self, job: JobId) -> Result<Vec<(JobOutput, JobFile)>> {
        let c = &mut self.1.lock().unwrap().conn;

        self.i_job_outputs(c, job)
    }

    pub fn job_file_by_id_opt(
        &self,
        job: JobId,
        file: JobFileId,
    ) -> Result<Option<JobFile>> {
        let c = &mut self.1.lock().unwrap().conn;
        use schema::job_file::dsl;
        Ok(dsl::job_file
            .filter(dsl::job.eq(job))
            .filter(dsl::id.eq(file))
            .get_result(c)
            .optional()?)
    }

    pub fn job_events(
        &self,
        job: JobId,
        minseq: usize,
    ) -> Result<Vec<JobEvent>> {
        use schema::job_event::dsl;

        let c = &mut self.1.lock().unwrap().conn;
        Ok(dsl::job_event
            .filter(dsl::job.eq(job))
            .filter(dsl::seq.ge(minseq as i32))
            .order_by(dsl::seq.asc())
            .get_results(c)?)
    }

    pub fn job_by_str(&self, job: &str) -> Result<Job> {
        let id = JobId(Ulid::from_str(job)?);
        let c = &mut self.1.lock().unwrap().conn;
        use schema::job::dsl;
        Ok(dsl::job.filter(dsl::id.eq(id)).get_result(c)?)
    }

    #[allow(dead_code)]
    pub fn job_by_id(&self, job: JobId) -> Result<Job> {
        let c = &mut self.1.lock().unwrap().conn;
        use schema::job::dsl;
        Ok(dsl::job.filter(dsl::id.eq(job)).get_result(c)?)
    }

    pub fn job_by_id_opt(&self, job: JobId) -> Result<Option<Job>> {
        let c = &mut self.1.lock().unwrap().conn;
        use schema::job::dsl;
        Ok(dsl::job.filter(dsl::id.eq(job)).get_result(c).optional()?)
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
    ) -> Result<Job>
    where
        I: IntoIterator<Item = (String, String)>,
    {
        use schema::{
            job, job_depend, job_input, job_output_rule, job_tag, task,
        };

        if tasks.is_empty() {
            bail!("a job must have at least one task");
        }
        if tasks.len() > 64 {
            bail!("a job must have 64 or fewer tasks");
        }

        if depends.len() > 8 {
            bail!("a job must depend on 8 or fewer other jobs");
        }
        for cd in depends.iter() {
            if cd.name.contains('/') || cd.name.trim().is_empty() {
                bail!("invalid depend name");
            }

            if !cd.on_failed && !cd.on_completed {
                bail!("depend must have at least one trigger condition");
            }
        }

        if inputs.len() > 32 {
            bail!("a job must have 32 or fewer input files");
        }
        for ci in inputs.iter() {
            if ci.contains('/') || ci.trim().is_empty() {
                bail!("invalid input name");
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
        };

        /*
         * Use the timestamp from the generated ID as the submit time.
         */
        let start = j.id.datetime();

        let c = &mut self.1.lock().unwrap().conn;

        c.immediate_transaction(|tx| {
            let ic =
                diesel::insert_into(job::dsl::job).values(&j).execute(tx)?;
            assert_eq!(ic, 1);

            self.i_job_time_record(tx, j.id, "submit", start)?;
            if !waiting {
                /*
                 * If the job is not waiting, record the submit time as the time
                 * at which dependencies are satisfied and the job is ready to
                 * run.
                 */
                self.i_job_time_record(tx, j.id, "ready", start)?;
            }

            for (i, ct) in tasks.iter().enumerate() {
                let ic = diesel::insert_into(task::dsl::task)
                    .values(Task::from_create(ct, j.id, i))
                    .execute(tx)?;
                assert_eq!(ic, 1);
            }

            for cd in depends.iter() {
                /*
                 * Make sure that this job exists in the system and that its
                 * owner matches the owner of this job.  By requiring prior jobs
                 * to exist already at the time of dependency specification we
                 * can avoid the mess of cycles in the dependency graph.
                 */
                let pj: Option<Job> = job::dsl::job
                    .find(cd.prior_job)
                    .get_result(tx)
                    .optional()?;

                if !pj.map(|j| j.owner == owner).unwrap_or(false) {
                    /*
                     * Try not to leak information about job IDs from other
                     * users in the process.
                     */
                    bail!("prior job does not exist");
                }

                let ic = diesel::insert_into(job_depend::dsl::job_depend)
                    .values(JobDepend::from_create(cd, j.id))
                    .execute(tx)?;
                assert_eq!(ic, 1);
            }

            for ci in inputs.iter() {
                let ic = diesel::insert_into(job_input::dsl::job_input)
                    .values(JobInput::from_create(ci.as_str(), j.id))
                    .execute(tx)?;
                assert_eq!(ic, 1);
            }

            for (i, rule) in output_rules.iter().enumerate() {
                let ic =
                    diesel::insert_into(job_output_rule::dsl::job_output_rule)
                        .values(JobOutputRule::from_create(rule, j.id, i))
                        .execute(tx)?;
                assert_eq!(ic, 1);
            }

            for (n, v) in tags {
                let ic = diesel::insert_into(job_tag::dsl::job_tag)
                    .values((
                        job_tag::dsl::job.eq(j.id),
                        job_tag::dsl::name.eq(n),
                        job_tag::dsl::value.eq(v),
                    ))
                    .execute(tx)?;
                assert_eq!(ic, 1);
            }

            Ok(j)
        })
    }

    pub fn job_input_by_str(&self, job: &str, file: &str) -> Result<JobInput> {
        use schema::job_input;

        let job = JobId(Ulid::from_str(job)?);
        let file = JobFileId(Ulid::from_str(file)?);

        let c = &mut self.1.lock().unwrap().conn;

        Ok(job_input::dsl::job_input
            .filter(job_input::dsl::job.eq(job))
            .filter(job_input::dsl::id.eq(file))
            .get_result(c)?)
    }

    pub fn job_output_by_str(
        &self,
        job: &str,
        file: &str,
    ) -> Result<JobOutput> {
        use schema::job_output;

        let job = JobId(Ulid::from_str(job)?);
        let file = JobFileId(Ulid::from_str(file)?);

        let c = &mut self.1.lock().unwrap().conn;

        Ok(job_output::dsl::job_output
            .filter(job_output::dsl::job.eq(job))
            .filter(job_output::dsl::id.eq(file))
            .get_result(c)?)
    }

    pub fn published_file_by_name(
        &self,
        owner: UserId,
        series: &str,
        version: &str,
        name: &str,
    ) -> OResult<Option<PublishedFile>> {
        use schema::published_file;

        let c = &mut self.1.lock().unwrap().conn;

        Ok(published_file::dsl::published_file
            .find((owner, series, version, name))
            .get_result(c)
            .optional()?)
    }

    pub fn job_publish_output(
        &self,
        job: JobId,
        file: JobFileId,
        series: &str,
        version: &str,
        name: &str,
    ) -> OResult<()> {
        use schema::{job, job_output, published_file};

        let c = &mut self.1.lock().unwrap().conn;

        c.immediate_transaction(|tx| {
            let j: Job = job::dsl::job.find(job).get_result(tx)?;
            if !j.complete {
                conflict!("job must be complete before files are published");
            }

            /*
             * Make sure this output exists.
             */
            let _jo: JobOutput = job_output::dsl::job_output
                .filter(job_output::dsl::job.eq(job))
                .filter(job_output::dsl::id.eq(file))
                .get_result(tx)?;

            /*
             * Determine whether this record has been published already or not.
             */
            let pf: Option<PublishedFile> = published_file::dsl::published_file
                .find((j.owner, series, version, name))
                .get_result(tx)
                .optional()?;

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

            let ic = diesel::insert_into(published_file::dsl::published_file)
                .values(PublishedFile {
                    owner: j.owner,
                    job,
                    file,
                    series: series.to_string(),
                    version: version.to_string(),
                    name: name.to_string(),
                })
                .execute(tx)?;
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
    ) -> OResult<()> {
        use schema::{job, job_file, job_output};

        let c = &mut self.1.lock().unwrap().conn;

        c.immediate_transaction(|tx| {
            let j: Job = job::dsl::job.find(job).get_result(tx)?;
            if j.complete {
                conflict!("job already complete, cannot add more files");
            }

            let ic = diesel::insert_into(job_file::dsl::job_file)
                .values(JobFile {
                    job,
                    id,
                    size: DataSize(size),
                    time_archived: None,
                })
                .execute(tx)?;
            assert_eq!(ic, 1);

            let ic = diesel::insert_into(job_output::dsl::job_output)
                .values(JobOutput { job, path: path.to_string(), id })
                .execute(tx)?;
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
    ) -> OResult<()> {
        use schema::{job, job_file, job_input};

        if name.contains('/') {
            return Err(anyhow!("name cannot be a path").into());
        }

        let c = &mut self.1.lock().unwrap().conn;

        c.immediate_transaction(|tx| {
            let j: Job = job::dsl::job.find(job).get_result(tx)?;
            if !j.waiting {
                conflict!("job not waiting, cannot add more inputs");
            }

            let ic = diesel::insert_into(job_file::dsl::job_file)
                .values(JobFile {
                    job,
                    id,
                    size: DataSize(size),
                    time_archived: None,
                })
                .execute(tx)?;
            assert_eq!(ic, 1);

            let uc = diesel::update(job_input::dsl::job_input)
                .filter(job_input::dsl::job.eq(job))
                .filter(job_input::dsl::name.eq(name))
                .set((job_input::dsl::id.eq(id),))
                .execute(tx)?;
            assert_eq!(uc, 1);

            Ok(())
        })
    }

    pub fn job_file_next_unarchived(&self) -> OResult<Option<JobFile>> {
        use schema::{job, job_file};

        let c = &mut self.1.lock().unwrap().conn;

        /*
         * Find the most recently uploaded output stored as part of a job that
         * has been completed.
         */
        let res: Option<(Job, JobFile)> = job::dsl::job
            .inner_join(job_file::table)
            .filter(job::dsl::complete.eq(true))
            .filter(job_file::dsl::time_archived.is_null())
            .order_by(job_file::dsl::id.asc())
            .limit(1)
            .get_result(c)
            .optional()?;

        Ok(res.map(|(_, out)| out))
    }

    pub fn job_file_mark_archived(
        &self,
        file: &JobFile,
        time: DateTime<Utc>,
    ) -> OResult<()> {
        use schema::job_file;

        let c = &mut self.1.lock().unwrap().conn;

        let uc = diesel::update(job_file::dsl::job_file)
            .filter(job_file::dsl::job.eq(&file.job))
            .filter(job_file::dsl::id.eq(&file.id))
            .filter(job_file::dsl::time_archived.is_null())
            .set((job_file::dsl::time_archived.eq(IsoDate(time)),))
            .execute(c)?;
        assert_eq!(uc, 1);

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
        use schema::job;

        let c = &mut self.1.lock().unwrap().conn;

        c.immediate_transaction(|tx| {
            let j: Job = job::dsl::job.find(job).get_result(tx)?;
            if j.complete {
                conflict!("job already complete, cannot append");
            }

            Ok(self.i_job_event_insert(
                tx,
                j.id,
                task,
                stream,
                time,
                time_remote,
                payload,
            )?)
        })
    }

    pub fn job_wakeup(&self, job: JobId) -> OResult<()> {
        use schema::job;

        let c = &mut self.1.lock().unwrap().conn;

        c.immediate_transaction(|tx| {
            let j: Job = job::dsl::job.find(job).get_result(tx)?;
            if !j.waiting {
                conflict!("job {} was not waiting, cannot wakeup", j.id);
            }

            /*
             * Record the time at which the job became ready to run.
             */
            self.i_job_time_record(tx, j.id, "ready", Utc::now())?;

            /*
             *
             * Estimate the length of time that we were waiting for dependencies
             * to complete running.
             */
            let mut msg = "job dependencies complete; ready to run".to_string();
            if let Some(dur) =
                self.i_job_time_delta(tx, j.id, "submit", "ready")?
            {
                msg += &format!(" (waiting for {})", dur.render());
            }
            self.i_job_event_insert(
                tx,
                j.id,
                None,
                "control",
                Utc::now(),
                None,
                &msg,
            )?;

            let uc = diesel::update(job::dsl::job)
                .filter(job::dsl::id.eq(j.id))
                .filter(job::dsl::waiting.eq(true))
                .set((job::dsl::waiting.eq(false),))
                .execute(tx)?;
            assert_eq!(uc, 1);

            Ok(())
        })
    }

    pub fn job_cancel(&self, job: JobId) -> OResult<bool> {
        use schema::job;

        let c = &mut self.1.lock().unwrap().conn;

        c.immediate_transaction(|tx| {
            let j: Job = job::dsl::job.find(job).get_result(tx)?;
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
                tx,
                j.id,
                None,
                "control",
                Utc::now(),
                None,
                "job cancelled",
            )?;

            let uc = diesel::update(job::dsl::job)
                .filter(job::dsl::id.eq(j.id))
                .filter(job::dsl::complete.eq(false))
                .set((job::dsl::cancelled.eq(true),))
                .execute(tx)?;
            assert_eq!(uc, 1);

            Ok(true)
        })
    }

    pub fn job_complete(&self, job: JobId, failed: bool) -> Result<bool> {
        use schema::{job, task};

        let c = &mut self.1.lock().unwrap().conn;

        c.immediate_transaction(|tx| {
            let j: Job = job::dsl::job.find(job).get_result(tx)?;
            if j.complete {
                /*
                 * This job is already complete.
                 */
                return Ok(false);
            }

            if j.cancelled && !failed {
                bail!("job {} cancelled; cannot succeed", j.id);
            }

            /*
             * Mark any tasks that have not yet completed as failed, as they
             * will not be executed.
             */
            let mut tasks_failed = false;
            let tasks: Vec<Task> = task::dsl::task
                .filter(task::dsl::job.eq(j.id))
                .order_by(task::dsl::seq.asc())
                .get_results(tx)?;
            for t in tasks {
                if t.failed {
                    tasks_failed = true;
                }
                if t.failed || t.complete || j.cancelled {
                    continue;
                }

                self.i_job_event_insert(
                    tx,
                    j.id,
                    None,
                    "control",
                    Utc::now(),
                    None,
                    &format!("task {} was incomplete, marked failed", t.seq),
                )?;

                let uc = diesel::update(task::dsl::task)
                    .filter(task::dsl::job.eq(j.id))
                    .filter(task::dsl::seq.eq(t.seq))
                    .filter(task::dsl::complete.eq(false))
                    .set((
                        task::dsl::failed.eq(true),
                        task::dsl::complete.eq(true),
                    ))
                    .execute(tx)?;
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
                    tx,
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

            let uc = diesel::update(job::dsl::job)
                .filter(job::dsl::id.eq(j.id))
                .filter(job::dsl::complete.eq(false))
                .set((job::dsl::failed.eq(failed), job::dsl::complete.eq(true)))
                .execute(tx)?;
            assert_eq!(uc, 1);

            self.i_job_time_record(tx, j.id, "complete", Utc::now())?;

            Ok(true)
        })
    }

    pub fn i_job_time_record(
        &self,
        tx: &mut SqliteConnection,
        job: JobId,
        name: &str,
        when: DateTime<Utc>,
    ) -> Result<()> {
        use schema::job_time;

        diesel::insert_into(job_time::dsl::job_time)
            .values(JobTime {
                job,
                name: name.to_string(),
                time: IsoDate(when),
            })
            .on_conflict_do_nothing()
            .execute(tx)?;
        Ok(())
    }

    pub fn i_job_time_delta(
        &self,
        tx: &mut SqliteConnection,
        job: JobId,
        from: &str,
        until: &str,
    ) -> Result<Option<std::time::Duration>> {
        use schema::job_time;

        let from: JobTime = if let Some(t) = job_time::dsl::job_time
            .find((job, from))
            .get_result(tx)
            .optional()?
        {
            t
        } else {
            return Ok(None);
        };

        let until: JobTime = if let Some(t) = job_time::dsl::job_time
            .find((job, until))
            .get_result(tx)
            .optional()?
        {
            t
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
    ) -> Result<HashMap<String, DateTime<Utc>>> {
        use schema::job_time;

        let c = &mut self.1.lock().unwrap().conn;

        Ok(job_time::dsl::job_time
            .filter(job_time::dsl::job.eq(job))
            .get_results::<JobTime>(c)?
            .drain(..)
            .map(|jt| (jt.name, jt.time.0))
            .collect())
    }

    pub fn job_store(&self, job: JobId) -> Result<HashMap<String, JobStore>> {
        use schema::job_store;

        let c = &mut self.1.lock().unwrap().conn;

        Ok(job_store::dsl::job_store
            .filter(job_store::dsl::job.eq(job))
            .get_results::<JobStore>(c)?
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
        use schema::{job, job_store};

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

        c.immediate_transaction(|tx| {
            /*
             * Make sure the job exists and is not yet completed.  We do not
             * allow store updates after the job is finished.
             */
            let j: Job = job::dsl::job.find(job).get_result(tx)?;
            if j.complete {
                conflict!("job {job} already complete; cannot update store");
            }

            /*
             * First, check to see if this value already exists in the store:
             */
            let pre: Option<JobStore> = job_store::dsl::job_store
                .filter(job_store::dsl::job.eq(job))
                .filter(job_store::dsl::name.eq(name))
                .get_result(tx)
                .optional()?;

            if let Some(pre) = pre {
                /*
                 * Overwrite the existing value.  If the value we are replacing
                 * was already marked as a secret, we mark the updated value as
                 * secret as well to avoid accidents.
                 */
                let new_secret = if pre.secret { true } else { secret };

                let uc = diesel::update(job_store::dsl::job_store)
                    .filter(job_store::dsl::job.eq(job))
                    .filter(job_store::dsl::name.eq(name))
                    .set((
                        job_store::dsl::value.eq(value),
                        job_store::dsl::secret.eq(new_secret),
                        job_store::dsl::source.eq(source),
                        job_store::dsl::time_update.eq(IsoDate::now()),
                    ))
                    .execute(tx)?;
                assert_eq!(uc, 1);
                return Ok(());
            }

            /*
             * If the value does not already exist, we need to create it.  We
             * also need to make sure we do not allow values to be stored in
             * excess of the value count cap.
             */
            let count: i64 = job_store::dsl::job_store
                .filter(job_store::dsl::job.eq(job))
                .count()
                .get_result(tx)?;
            if count >= max_val_count {
                conflict!("job {job} already has {count} store values");
            }

            let ic = diesel::insert_into(job_store::dsl::job_store)
                .values(JobStore {
                    job,
                    name: name.to_string(),
                    value: value.to_string(),
                    secret,
                    source: source.to_string(),
                    time_update: IsoDate::now(),
                })
                .execute(tx)?;
            assert_eq!(ic, 1);

            Ok(())
        })
    }

    pub fn task_complete(
        &self,
        job: JobId,
        seq: u32,
        failed: bool,
    ) -> Result<bool> {
        use schema::task;

        let c = &mut self.1.lock().unwrap().conn;

        c.immediate_transaction(|tx| {
            let t: Task =
                task::dsl::task.find((job, seq as i32)).get_result(tx)?;
            if t.complete {
                return Ok(false);
            }

            let uc = diesel::update(task::dsl::task)
                .filter(task::dsl::job.eq(job))
                .filter(task::dsl::seq.eq(seq as i32))
                .filter(task::dsl::complete.eq(false))
                .set((
                    task::dsl::complete.eq(true),
                    task::dsl::failed.eq(failed),
                ))
                .execute(tx)?;
            assert_eq!(uc, 1);

            Ok(true)
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn i_job_event_insert(
        &self,
        tx: &mut SqliteConnection,
        job: JobId,
        task: Option<u32>,
        stream: &str,
        time: DateTime<Utc>,
        time_remote: Option<DateTime<Utc>>,
        payload: &str,
    ) -> Result<()> {
        use schema::job_event;

        let max: Option<i32> = job_event::dsl::job_event
            .select(diesel::dsl::max(job_event::dsl::seq))
            .filter(job_event::dsl::job.eq(job))
            .get_result(tx)?;

        let ic = diesel::insert_into(job_event::dsl::job_event)
            .values(JobEvent {
                job,
                task: task.map(|n| n as i32),
                seq: max.unwrap_or(0) + 1,
                stream: stream.to_string(),
                time: IsoDate(time),
                time_remote: time_remote.map(IsoDate),
                payload: payload.to_string(),
            })
            .execute(tx)?;
        assert_eq!(ic, 1);

        Ok(())
    }

    pub fn user_jobs(&self, owner: UserId) -> Result<Vec<Job>> {
        use schema::job;

        let c = &mut self.1.lock().unwrap().conn;

        Ok(job::dsl::job.filter(job::dsl::owner.eq(owner)).get_results(c)?)
    }

    pub fn worker_job(&self, worker: WorkerId) -> Result<Option<Job>> {
        use schema::job;

        let c = &mut self.1.lock().unwrap().conn;

        let t: Vec<Job> =
            job::dsl::job.filter(job::dsl::worker.eq(worker)).get_results(c)?;

        match t.len() {
            0 => Ok(None),
            1 => Ok(Some(t[0].clone())),
            n => bail!("found {} jobs for worker {}", n, worker),
        }
    }

    pub fn user_get_by_id(&self, id: UserId) -> Result<Option<AuthUser>> {
        use schema::user::dsl;

        let c = &mut self.1.lock().unwrap().conn;

        dsl::user
            .find(id)
            .get_result::<User>(c)
            .optional()?
            .map::<Result<_>, _>(|u| {
                Ok(AuthUser {
                    privileges: self.user_privileges(u.id, c)?,
                    user: u,
                })
            })
            .transpose()
    }

    pub fn user_get_by_name(&self, name: &str) -> Result<Option<User>> {
        use schema::user::dsl;

        let c = &mut self.1.lock().unwrap().conn;

        Ok(dsl::user
            .filter(dsl::name.eq(name))
            .get_result::<User>(c)
            .optional()?)
    }

    pub fn users(&self) -> Result<Vec<AuthUser>> {
        use schema::user::dsl;

        let c = &mut self.1.lock().unwrap().conn;

        dsl::user
            .get_results::<User>(c)?
            .drain(..)
            .map(|u| {
                Ok(AuthUser {
                    privileges: self.user_privileges(u.id, c)?,
                    user: u,
                })
            })
            .collect::<Result<Vec<_>>>()
    }

    fn user_privileges(
        &self,
        user: UserId,
        tx: &mut SqliteConnection,
    ) -> Result<Vec<String>> {
        use schema::user_privilege::dsl;

        Ok(dsl::user_privilege
            .select((dsl::privilege,))
            .filter(dsl::user.eq(user))
            .order_by(dsl::privilege.asc())
            .get_results::<(String,)>(tx)?
            .drain(..)
            .map(|(s,)| s)
            .collect::<Vec<_>>())
    }

    pub fn user_privilege_grant(
        &self,
        u: UserId,
        privilege: &str,
    ) -> Result<bool> {
        use schema::{user, user_privilege};

        let c = &mut self.1.lock().unwrap().conn;

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
            bail!("invalid privilege name");
        }

        c.immediate_transaction(|tx| {
            /*
             * Confirm that the user exists before creating privilege records:
             */
            let u: User = user::dsl::user.find(u).get_result(tx)?;

            let ic = diesel::insert_into(user_privilege::dsl::user_privilege)
                .values(Privilege {
                    user: u.id,
                    privilege: privilege.to_string(),
                })
                .on_conflict_do_nothing()
                .execute(tx)?;
            assert!(ic == 0 || ic == 1);

            Ok(ic != 0)
        })
    }

    pub fn user_privilege_revoke(
        &self,
        u: UserId,
        privilege: &str,
    ) -> Result<bool> {
        use schema::{user, user_privilege};

        let c = &mut self.1.lock().unwrap().conn;

        c.immediate_transaction(|tx| {
            /*
             * Confirm that the user exists before trying to remove privilege
             * records:
             */
            let u: User = user::dsl::user.find(u).get_result(tx)?;

            let dc = diesel::delete(user_privilege::dsl::user_privilege)
                .filter(user_privilege::dsl::user.eq(u.id))
                .filter(user_privilege::dsl::privilege.eq(privilege))
                .execute(tx)?;
            assert!(dc == 0 || dc == 1);

            Ok(dc != 0)
        })
    }

    fn i_user_create(
        &self,
        name: &str,
        tx: &mut SqliteConnection,
    ) -> Result<User> {
        use schema::user::dsl;

        let u = User {
            id: UserId::generate(),
            name: name.to_string(),
            token: genkey(48),
            time_create: Utc::now().into(),
        };

        diesel::insert_into(dsl::user).values(&u).execute(tx)?;

        Ok(u)
    }

    pub fn user_create(&self, name: &str) -> Result<User> {
        let c = &mut self.1.lock().unwrap().conn;

        /*
         * Make sure the requested username is not one we might mistake as a
         * ULID later.
         */
        if looks_like_a_ulid(name) {
            bail!("usernames must not look like ULIDs");
        }

        self.i_user_create(name, c)
    }

    pub fn user_ensure(&self, name: &str) -> Result<AuthUser> {
        use schema::user::dsl;

        let c = &mut self.1.lock().unwrap().conn;

        c.immediate_transaction(|tx| {
            let user = if let Some(user) = dsl::user
                .filter(dsl::name.eq(name))
                .get_result::<User>(tx)
                .optional()?
            {
                user
            } else {
                /*
                 * The user does not exist already, so a new one must be
                 * created:
                 */
                self.i_user_create(name, tx)?
            };

            Ok(AuthUser {
                privileges: self.user_privileges(user.id, tx)?,
                user,
            })
        })
    }

    pub fn user_auth(&self, token: &str) -> Result<AuthUser> {
        use schema::user;

        let c = &mut self.1.lock().unwrap().conn;

        let mut users: Vec<User> = user::dsl::user
            .filter(user::dsl::token.eq(token))
            .get_results(c)?;

        match (users.pop(), users.pop()) {
            (None, _) => bail!("auth failure"),
            (Some(u), Some(x)) => bail!("token error ({}, {})", u.id, x.id),
            (Some(u), None) => {
                assert_eq!(&u.token, token);
                Ok(AuthUser {
                    privileges: self.user_privileges(u.id, c)?,
                    user: u,
                })
            }
        }
    }

    pub fn factory_create(&self, name: &str) -> Result<Factory> {
        let f = Factory {
            id: FactoryId::generate(),
            name: name.to_string(),
            token: genkey(64),
            lastping: None,
        };

        let c = &mut self.1.lock().unwrap().conn;

        use schema::factory::dsl;

        diesel::insert_into(dsl::factory).values(&f).execute(c)?;

        Ok(f)
    }

    pub fn factory_auth(&self, token: &str) -> Result<Factory> {
        use schema::factory;

        let c = &mut self.1.lock().unwrap().conn;

        let mut rows: Vec<Factory> = factory::dsl::factory
            .filter(factory::dsl::token.eq(token))
            .get_results(c)?;

        match (rows.pop(), rows.pop()) {
            (None, _) => bail!("auth failure"),
            (Some(u), Some(x)) => bail!("token error ({}, {})", u.id, x.id),
            (Some(u), None) => {
                assert_eq!(u.token.as_str(), token);
                Ok(u)
            }
        }
    }

    pub fn factory_ping(&self, id: FactoryId) -> Result<bool> {
        use schema::factory::dsl;

        let c = &mut self.1.lock().unwrap().conn;

        let now = models::IsoDate(Utc::now());
        Ok(diesel::update(dsl::factory)
            .filter(dsl::id.eq(id))
            .set(dsl::lastping.eq(now))
            .execute(c)?
            > 0)
    }

    pub fn targets(&self) -> Result<Vec<Target>> {
        use schema::target::dsl;

        let c = &mut self.1.lock().unwrap().conn;
        Ok(dsl::target.order_by(dsl::id.asc()).get_results(c)?)
    }

    pub fn target_get(&self, id: TargetId) -> Result<Target> {
        use schema::target::dsl;

        let c = &mut self.1.lock().unwrap().conn;
        Ok(dsl::target.find(id).get_result(c)?)
    }

    pub fn target_create(&self, name: &str, desc: &str) -> Result<Target> {
        if name != name.trim() || name.is_empty() {
            bail!("invalid target name");
        }

        let t = Target {
            id: TargetId::generate(),
            name: name.to_string(),
            desc: desc.to_string(),
            redirect: None,
            privilege: None,
        };

        let c = &mut self.1.lock().unwrap().conn;

        use schema::target::dsl;

        diesel::insert_into(dsl::target).values(&t).execute(c)?;

        Ok(t)
    }

    pub fn target_resolve(&self, name: &str) -> Result<Option<Target>> {
        use schema::target::dsl;

        let c = &mut self.1.lock().unwrap().conn;

        /*
         * Use the target name to look up the initial target match:
         */
        let mut target: Target = if let Some(target) =
            dsl::target.filter(dsl::name.eq(name)).get_result(c).optional()?
        {
            target
        } else {
            return Ok(None);
        };

        let mut count = 0;
        loop {
            if count > 32 {
                bail!("too many target redirects starting from {:?}", name);
            }
            count += 1;

            if let Some(redirect) = &target.redirect {
                target = if let Some(target) =
                    dsl::target.find(redirect).get_result(c).optional()?
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
    ) -> Result<()> {
        use schema::target::dsl;

        let c = &mut self.1.lock().unwrap().conn;

        let uc = diesel::update(dsl::target)
            .filter(dsl::id.eq(id))
            .set(dsl::privilege.eq(privilege))
            .execute(c)?;
        assert!(uc == 1);

        Ok(())
    }

    pub fn target_redirect(
        &self,
        id: TargetId,
        redirect: Option<TargetId>,
    ) -> Result<()> {
        use schema::target::dsl;

        let c = &mut self.1.lock().unwrap().conn;

        let uc = diesel::update(dsl::target)
            .filter(dsl::id.eq(id))
            .set(dsl::redirect.eq(redirect))
            .execute(c)?;
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
    ) -> Result<Target> {
        use schema::target::dsl;

        let c = &mut self.1.lock().unwrap().conn;

        if new_name != new_name.trim() || new_name.is_empty() {
            bail!("invalid target name");
        }

        c.immediate_transaction(|tx| {
            /*
             * First, load the target to rename from the database.
             */
            let t: Target = dsl::target.find(id).get_result(tx)?;

            /*
             * Then, make sure a target with the new name does not yet
             * exist.
             */
            let nt: Option<Target> = dsl::target
                .filter(dsl::name.eq(new_name))
                .get_result(tx)
                .optional()?;
            if let Some(nt) = nt {
                bail!(
                    "target {} with name {:?} already exists",
                    nt.id,
                    new_name,
                );
            }

            /*
             * Rename the target:
             */
            let uc = diesel::update(dsl::target)
                .filter(dsl::id.eq(id))
                .set(dsl::name.eq(new_name))
                .execute(tx)?;
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

            let ic =
                diesel::insert_into(dsl::target).values(&nt).execute(tx)?;
            assert_eq!(ic, 1);

            Ok(nt)
        })
    }
}
