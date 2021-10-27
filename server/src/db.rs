/*
 * Copyright 2021 Oxide Computer Company
 */

use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
use std::sync::Mutex;

use anyhow::{bail, Result};
use chrono::prelude::*;
use diesel::prelude::*;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
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

pub fn genkey(len: usize) -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(|c| c as char)
        .collect()
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

impl Database {
    pub fn new<P: AsRef<Path>>(log: Logger, path: P) -> Result<Database> {
        let url = format!("sqlite://{}", path.as_ref().to_str().unwrap());
        let mut conn = diesel::SqliteConnection::establish(&url)?;

        #[derive(QueryableByName)]
        struct UserVersion {
            #[sql_type = "diesel::sql_types::Integer"]
            user_version: i32,
        }

        /*
         * Take the schema file and split it on the special comments we use to
         * split up into statements
         */
        let mut steps: Vec<(i32, String)> = Vec::new();
        let mut version = None;
        let mut statement = String::new();

        for l in include_str!("../schema.sql").lines() {
            if l.starts_with("-- v ") {
                if let Some(version) = version.take() {
                    steps.push((version, statement.trim().to_string()));
                }

                version = Some(l.trim_start_matches("-- v ").parse()?);
                statement.clear();
            } else {
                statement.push_str(l);
                statement.push('\n');
            }
        }
        if let Some(version) = version.take() {
            steps.push((version, statement.trim().to_string()));
        }

        info!(
            log,
            "found user version {} in database",
            diesel::sql_query("PRAGMA user_version")
                .get_result::<UserVersion>(&mut conn)?
                .user_version
        );
        for (version, statement) in steps {
            /*
             * Do some whitespace normalisation.  We would prefer to keep the
             * whitespace-heavy layout of the schema as represented in the file,
             * as SQLite will preserve it in the ".schema" output.
             * Unfortunately, there is no obvious way to ALTER TABLE ADD COLUMN
             * in a way that similarly maintains the whitespace, so we will
             * instead uniformly do without.
             */
            let mut statement = statement.replace('\n', " ");
            while statement.contains("( ") {
                statement = statement.trim().replace("( ", "(");
            }
            while statement.contains(" )") {
                statement = statement.trim().replace(" )", ")");
            }
            while statement.contains("  ") {
                statement = statement.trim().replace("  ", " ");
            }

            /*
             * Determine the current user version.
             */
            let uv = diesel::sql_query("PRAGMA user_version")
                .get_result::<UserVersion>(&mut conn)?
                .user_version;
            if version > uv {
                info!(log, "apply version {}, run {}", version, statement);
                diesel::sql_query(statement).execute(&mut conn)?;
                diesel::sql_query(format!("PRAGMA user_version = {}", version))
                    .execute(&mut conn)?;
            }
        }

        Ok(Database(log, Mutex::new(Inner { conn })))
    }

    pub fn workers(&self) -> Result<Vec<Worker>> {
        let c = &mut self.1.lock().unwrap().conn;

        use schema::worker::dsl;

        Ok(dsl::worker.order_by(dsl::id.asc()).get_results(c)?)
    }

    pub fn worker_jobs(&self, worker: &WorkerId) -> Result<Vec<Job>> {
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

    pub fn worker_recycle(&self, id: &WorkerId) -> Result<bool> {
        use schema::worker::dsl;

        let c = &mut self.1.lock().unwrap().conn;

        Ok(diesel::update(dsl::worker)
            .filter(dsl::id.eq(id))
            .set(dsl::recycle.eq(true))
            .execute(c)?
            > 0)
    }

    pub fn worker_destroy(&self, id: &WorkerId) -> Result<bool> {
        use schema::worker::dsl;

        let c = &mut self.1.lock().unwrap().conn;

        Ok(diesel::update(dsl::worker)
            .filter(dsl::id.eq(id))
            .set(dsl::deleted.eq(true))
            .execute(c)?
            > 0)
    }

    pub fn worker_ping(&self, id: &WorkerId) -> Result<bool> {
        use schema::worker::dsl;

        let c = &mut self.1.lock().unwrap().conn;

        let now = models::IsoDate(Utc::now());
        Ok(diesel::update(dsl::worker)
            .filter(dsl::id.eq(id))
            .set(dsl::lastping.eq(now))
            .execute(c)?
            > 0)
    }

    pub fn worker_assign_job(
        &self,
        wid: &WorkerId,
        jid: &JobId,
    ) -> OResult<()> {
        let c = &mut self.1.lock().unwrap().conn;

        c.immediate_transaction(|tx| {
            use schema::{job, worker};

            let w: Worker = worker::dsl::worker.find(wid).get_result(tx)?;
            if w.deleted || w.recycle {
                conflict!("worker {} already deleted, cannot assign job", w.id);
            }

            let j: Job = job::dsl::job.find(jid).get_result(tx)?;
            if let Some(jw) = j.worker.as_ref() {
                conflict!("job {} already assigned to worker {}", j.id, jw);
            }

            let c: i64 = job::dsl::job
                .filter(job::dsl::worker.eq(w.id))
                .count()
                .get_result(tx)?;
            if c > 0 {
                conflict!("worker {} already has {} jobs assigned", wid, c);
            }

            let uc = diesel::update(job::dsl::job)
                .filter(job::dsl::id.eq(j.id))
                .set(job::dsl::worker.eq(w.id))
                .execute(tx)?;
            assert_eq!(uc, 1);

            /*
             * Estimate how long the job was waiting in the queue for a worker.
             */
            let wait = if let Ok(dur) =
                Utc::now().signed_duration_since(j.id.datetime()).to_std()
            {
                let mut out = String::new();
                let mut secs = dur.as_secs();
                let hours = secs / 3600;
                if hours > 0 {
                    secs -= hours * 3600;
                    out += &format!(" {} h", hours);
                }
                let minutes = secs / 60;
                if minutes > 0 || hours > 0 {
                    secs -= minutes * 60;
                    out += &format!(" {} m", minutes);
                }
                out += &format!(" {} s", secs);

                format!(" (queued for{})", out)
            } else {
                "".to_string()
            };

            self.i_job_event_insert(
                tx,
                &j.id,
                None,
                "control",
                Utc::now(),
                None,
                &format!("job assigned to worker {}{}", w.id, wait),
            )?;

            Ok(())
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

            if w.instance_id.is_none() {
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
        wid: &WorkerId,
        instance_id: &str,
    ) -> OResult<()> {
        let c = &mut self.1.lock().unwrap().conn;

        c.immediate_transaction(|tx| {
            use schema::worker;

            let w: Worker = worker::dsl::worker.find(wid).get_result(tx)?;
            if w.deleted {
                conflict!("worker {} already deleted, cannot associate", w.id);
            }

            if let Some(current) = w.instance_id.as_deref() {
                if current == instance_id {
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
                        instance_id
                    );
                }
            } else {
                /*
                 * The worker is not yet associated with an instance ID.
                 */
                let count = diesel::update(worker::dsl::worker)
                    .filter(worker::dsl::id.eq(w.id))
                    .set(worker::dsl::instance_id.eq(instance_id))
                    .execute(tx)?;
                assert_eq!(count, 1);
            }

            Ok(())
        })
    }

    pub fn worker_get(&self, id: &WorkerId) -> Result<Worker> {
        use schema::worker::dsl;

        let c = &mut self.1.lock().unwrap().conn;
        Ok(dsl::worker.find(id).get_result(c)?)
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

    pub fn worker_create(&self) -> Result<Worker> {
        use schema::worker;

        let w = Worker {
            id: WorkerId::generate(),
            bootstrap: genkey(64),
            instance_id: None,
            token: None,
            deleted: false,
            recycle: false,
            lastping: None,
        };

        let c = &mut self.1.lock().unwrap().conn;

        let count =
            diesel::insert_into(worker::dsl::worker).values(&w).execute(c)?;
        assert_eq!(count, 1);

        Ok(w)
    }

    pub fn jobs(&self) -> Result<Vec<Job>> {
        use schema::job::dsl;

        let c = &mut self.1.lock().unwrap().conn;
        Ok(dsl::job.order_by(dsl::id.asc()).get_results(c)?)
    }

    pub fn job_tasks(&self, job: &JobId) -> Result<Vec<Task>> {
        use schema::task::dsl;

        let c = &mut self.1.lock().unwrap().conn;
        Ok(dsl::task
            .filter(dsl::job.eq(job))
            .order_by(dsl::seq.asc())
            .get_results(c)?)
    }

    pub fn job_output_rules(&self, job: &JobId) -> Result<Vec<String>> {
        use schema::job_output_rule::dsl;

        let c = &mut self.1.lock().unwrap().conn;
        Ok(dsl::job_output_rule
            .select(dsl::rule)
            .filter(dsl::job.eq(job))
            .order_by(dsl::seq.asc())
            .get_results::<String>(c)?)
    }

    pub fn job_outputs(&self, job: &JobId) -> Result<Vec<JobOutput>> {
        use schema::job_output::dsl;

        let c = &mut self.1.lock().unwrap().conn;
        Ok(dsl::job_output
            .filter(dsl::job.eq(job))
            .order_by(dsl::id.asc())
            .get_results(c)?)
    }

    pub fn job_events(
        &self,
        job: &JobId,
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
    pub fn job_by_id(&self, job: &JobId) -> Result<Job> {
        let c = &mut self.1.lock().unwrap().conn;
        use schema::job::dsl;
        Ok(dsl::job.filter(dsl::id.eq(job)).get_result(c)?)
    }

    pub fn job_by_id_opt(&self, job: &JobId) -> Result<Option<Job>> {
        let c = &mut self.1.lock().unwrap().conn;
        use schema::job::dsl;
        Ok(dsl::job.filter(dsl::id.eq(job)).get_result(c).optional()?)
    }

    pub fn job_create(
        &self,
        owner: &UserId,
        name: &str,
        target: &str,
        tasks: Vec<CreateTask>,
        output_rules: &[String],
    ) -> Result<Job> {
        use schema::{job, job_output_rule, task};

        if tasks.is_empty() {
            bail!("a job must have at least one task");
        }

        let j = Job {
            id: JobId::generate(),
            owner: *owner,
            name: name.to_string(),
            target: target.to_string(),
            complete: false,
            failed: false,
            worker: None,
        };

        let c = &mut self.1.lock().unwrap().conn;

        c.immediate_transaction(|tx| {
            let ic =
                diesel::insert_into(job::dsl::job).values(&j).execute(tx)?;
            assert_eq!(ic, 1);

            for (i, ct) in tasks.iter().enumerate() {
                let ic = diesel::insert_into(task::dsl::task)
                    .values(Task::from_create(ct, j.id, i))
                    .execute(tx)?;
                assert_eq!(ic, 1);
            }

            for (i, rule) in output_rules.iter().enumerate() {
                let ic =
                    diesel::insert_into(job_output_rule::dsl::job_output_rule)
                        .values((
                            job_output_rule::dsl::job.eq(j.id),
                            job_output_rule::dsl::seq.eq(i as i32),
                            job_output_rule::dsl::rule.eq(rule),
                        ))
                        .execute(tx)?;
                assert_eq!(ic, 1);
            }

            Ok(j)
        })
    }

    pub fn job_output_by_str(
        &self,
        job: &str,
        output: &str,
    ) -> Result<JobOutput> {
        use schema::job_output;

        let job = JobId(Ulid::from_str(job)?);
        let output = JobOutputId(Ulid::from_str(output)?);

        let c = &mut self.1.lock().unwrap().conn;

        Ok(job_output::dsl::job_output
            .filter(job_output::dsl::job.eq(job))
            .filter(job_output::dsl::id.eq(output))
            .get_result(c)?)
    }

    pub fn job_add_output(
        &self,
        job: &JobId,
        path: &str,
        id: &JobOutputId,
        size: u64,
    ) -> OResult<()> {
        use schema::{job, job_output};

        let c = &mut self.1.lock().unwrap().conn;

        c.immediate_transaction(|tx| {
            let j: Job = job::dsl::job.find(job).get_result(tx)?;
            if j.complete {
                conflict!("job already complete, cannot add more files");
            }

            let ic = diesel::insert_into(job_output::dsl::job_output)
                .values(JobOutput {
                    job: job.clone(),
                    path: path.to_string(),
                    id: id.clone(),
                    size: DataSize(size),
                })
                .execute(tx)?;
            assert_eq!(ic, 1);

            Ok(())
        })
    }

    pub fn job_append_event(
        &self,
        job: &JobId,
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
                &j.id,
                task,
                stream,
                time,
                time_remote,
                payload,
            )?)
        })
    }

    pub fn job_complete(&self, job: &JobId, failed: bool) -> Result<bool> {
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
                if t.failed || t.complete {
                    continue;
                }

                self.i_job_event_insert(
                    tx,
                    &j.id,
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
                    &j.id,
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

            Ok(true)
        })
    }

    pub fn task_complete(
        &self,
        job: &JobId,
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
        job: &JobId,
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
                job: job.clone(),
                task: task.map(|n| n as i32),
                seq: max.unwrap_or(0) + 1,
                stream: stream.to_string(),
                time: IsoDate(time),
                time_remote: time_remote.map(|t| IsoDate(t)),
                payload: payload.to_string(),
            })
            .execute(tx)?;
        assert_eq!(ic, 1);

        Ok(())
    }

    pub fn user_jobs(&self, owner: &UserId) -> Result<Vec<Job>> {
        use schema::job;

        let c = &mut self.1.lock().unwrap().conn;

        Ok(job::dsl::job.filter(job::dsl::owner.eq(owner)).get_results(c)?)
    }

    pub fn worker_job(&self, worker: &WorkerId) -> Result<Option<Job>> {
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

    pub fn users(&self) -> Result<Vec<User>> {
        use schema::user::dsl;

        let c = &mut self.1.lock().unwrap().conn;

        Ok(dsl::user.get_results(c)?)
    }

    pub fn user_create(&self, name: &str) -> Result<User> {
        let u = User {
            id: UserId::generate(),
            name: name.to_string(),
            token: genkey(48),
            time_create: Utc::now().into(),
        };

        let c = &mut self.1.lock().unwrap().conn;

        use schema::user::dsl;

        diesel::insert_into(dsl::user).values(&u).execute(c)?;

        Ok(u)
    }

    pub fn user_auth(&self, token: &str) -> Result<User> {
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
                Ok(u)
            }
        }
    }
}
