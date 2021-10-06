use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
use std::sync::Mutex;

use anyhow::{anyhow, bail, Context, Result};
use chrono::prelude::*;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
#[allow(unused_imports)]
use rusqlite::{
    params, types::ToSql, types::Type, Connection, DropBehavior,
    OptionalExtension, Row, Transaction, TransactionBehavior,
};
use rusty_ulid::Ulid;
use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use slog::{error, info, warn, Logger};
use thiserror::Error;

/* XXX from rusqlite? */
const UNKNOWN_COLUMN: usize = std::usize::MAX;

#[derive(Error, Debug)]
pub enum OperationError {
    #[error("conflict: {0}")]
    Conflict(String),
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
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
    conn: Connection,
}

pub struct Database(Logger, Mutex<Inner>);

pub struct User {
    pub id: Ulid,
    pub name: String,
    pub token: String,
    pub time_create: DateTime<Utc>,
}

#[derive(Clone)]
pub struct Job {
    pub id: Ulid,
    pub owner: Ulid,
    pub name: String,
    pub target: String,
    pub complete: bool,
    pub failed: bool,
    pub worker: Option<Ulid>,
}

impl Job {
    #[allow(dead_code)]
    pub fn time_submit(&self) -> DateTime<Utc> {
        self.id.datetime()
    }
}

#[derive(Clone)]
pub struct Task {
    pub job: Ulid,
    pub seq: i32,
    pub name: String,
    pub script: String,
    pub env_clear: bool,
    pub env: HashMap<String, String>,
    pub user_id: Option<u32>,
    pub group_id: Option<u32>,
    pub workdir: Option<String>,
    pub complete: bool,
    pub failed: bool,
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

pub struct JobEvent {
    pub job: Ulid,
    pub task: Option<i32>,
    pub seq: usize,
    pub stream: String,
    pub time: DateTime<Utc>,
    pub payload: String,
}

pub struct JobOutput {
    pub job: Ulid,
    pub id: Ulid,
    pub path: String,
    pub size: u64,
}

pub struct Worker {
    pub id: Ulid,
    pub bootstrap: String,
    pub token: Option<String>,
    pub instance_id: Option<String>,
    pub deleted: bool,
    pub recycle: bool,
    pub lastping: Option<DateTime<Utc>>,
}

impl Worker {
    #[allow(dead_code)]
    pub fn agent_ok(&self) -> bool {
        /*
         * Until we have a token from the worker, the agent hasn't started up
         * yet.
         */
        self.token.is_some()
    }
}

fn to_ulid(val: String) -> rusqlite::Result<Ulid> {
    Ok(to_ulid_opt(Some(val))?.unwrap())
}

fn to_ulid_opt(val: Option<String>) -> rusqlite::Result<Option<Ulid>> {
    if let Some(val) = val.as_deref() {
        match Ulid::from_str(val) {
            Ok(ulid) => Ok(Some(ulid)),
            Err(e) => Err(rusqlite::Error::FromSqlConversionFailure(
                UNKNOWN_COLUMN,
                Type::Null,
                Box::new(e),
            )),
        }
    } else {
        Ok(None)
    }
}

fn from_hashmap<K, V>(map: &HashMap<K, V>) -> Result<String>
where
    for<'de> K: Serialize + std::hash::Hash + Eq,
    for<'de> V: Serialize,
{
    Ok(serde_json::to_string(&map)?)
}

fn to_hashmap<K, V>(val: String) -> rusqlite::Result<HashMap<K, V>>
where
    for<'de> K: Deserialize<'de> + std::hash::Hash + Eq,
    for<'de> V: Deserialize<'de>,
{
    Ok(to_hashmap_opt(Some(val))?.unwrap())
}

fn to_hashmap_opt<K, V>(
    val: Option<String>,
) -> rusqlite::Result<Option<HashMap<K, V>>>
where
    for<'de> K: Deserialize<'de> + std::hash::Hash + Eq,
    for<'de> V: Deserialize<'de>,
{
    if let Some(val) = val.as_deref() {
        match serde_json::from_str(val) {
            Ok(map) => Ok(Some(map)),
            Err(e) => Err(rusqlite::Error::FromSqlConversionFailure(
                UNKNOWN_COLUMN,
                Type::Null,
                Box::new(e),
            )),
        }
    } else {
        Ok(None)
    }
}

fn row_to_opt_count(row: &Row) -> rusqlite::Result<Option<usize>> {
    let n: Option<i64> = row.get("n")?;
    n.map(|n| {
        if n >= 0 {
            Ok(n as usize)
        } else {
            Err(rusqlite::Error::FromSqlConversionFailure(
                UNKNOWN_COLUMN,
                Type::Null,
                anyhow!("negative count: {}", n).into(),
            ))
        }
    })
    .transpose()
}

fn row_to_count(row: &Row) -> rusqlite::Result<usize> {
    let n: i64 = row.get("n")?;
    if n >= 0 {
        Ok(n as usize)
    } else {
        Err(rusqlite::Error::FromSqlConversionFailure(
            UNKNOWN_COLUMN,
            Type::Null,
            anyhow!("negative count: {}", n).into(),
        ))
    }
}

fn row_to_user(row: &Row) -> rusqlite::Result<User> {
    Ok(User {
        id: to_ulid(row.get("id")?)?,
        name: row.get("name")?,
        token: row.get("token")?,
        time_create: row.get("time_create")?,
    })
}

fn row_to_job(row: &Row) -> rusqlite::Result<Job> {
    Ok(Job {
        id: to_ulid(row.get("id")?)?,
        owner: to_ulid(row.get("owner")?)?,
        name: row.get("name")?,
        target: row.get("target")?,
        complete: row.get("complete")?,
        failed: row.get("failed")?,
        worker: to_ulid_opt(row.get("worker")?)?,
    })
}

fn row_to_task(row: &Row) -> rusqlite::Result<Task> {
    Ok(Task {
        job: to_ulid(row.get("job")?)?,
        seq: row.get("seq")?,
        name: row.get("name")?,
        script: row.get("script")?,
        env_clear: row.get("env_clear")?,
        env: to_hashmap(row.get("env")?)?,
        user_id: row.get("user_id")?,
        group_id: row.get("group_id")?,
        workdir: row.get("workdir")?,
        complete: row.get("complete")?,
        failed: row.get("failed")?,
    })
}

fn row_to_job_event(row: &Row) -> rusqlite::Result<JobEvent> {
    Ok(JobEvent {
        job: to_ulid(row.get("job")?)?,
        task: row.get("task")?,
        seq: row.get::<_, i64>("seq")? as usize,
        stream: row.get("stream")?,
        time: row.get("time")?,
        payload: row.get("payload")?,
    })
}

fn row_to_job_output(row: &Row) -> rusqlite::Result<JobOutput> {
    Ok(JobOutput {
        job: to_ulid(row.get("job")?)?,
        id: to_ulid(row.get("id")?)?,
        path: row.get("path")?,
        size: row.get::<_, i64>("size")? as u64,
    })
}

fn row_to_job_output_rule(row: &Row) -> rusqlite::Result<String> {
    row.get("rule")
}

fn row_to_worker(row: &Row) -> rusqlite::Result<Worker> {
    Ok(Worker {
        id: to_ulid(row.get("id")?)?,
        bootstrap: row.get("bootstrap")?,
        instance_id: row.get("instance_id")?,
        token: row.get("token")?,
        deleted: row.get("deleted")?,
        recycle: row.get("recycle")?,
        lastping: row.get("lastping")?,
    })
}

impl Database {
    pub fn new<P: AsRef<Path>>(log: Logger, path: P) -> Result<Database> {
        let conn = Connection::open(path.as_ref())?;

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS user (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            token TEXT NOT NULL UNIQUE,
            time_create TEXT NOT NULL)",
        )
        .context("CREATE TABLE user")?;

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS job (
            id TEXT PRIMARY KEY,
            owner TEXT NOT NULL,
            name TEXT NOT NULL,
            target TEXT NOT NULL,
            complete INTEGER NOT NULL,
            failed INTEGER NOT NULL,
            worker TEXT)",
        )
        .context("CREATE TABLE job")?;

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS task (
            job TEXT NOT NULL,
            seq INTEGER NOT NULL,
            name TEXT NOT NULL,
            script TEXT NOT NULL,
            env_clear INTEGER NOT NULL,
            env TEXT NOT NULL,
            user_id INTEGER,
            group_id INTEGER,
            workdir TEXT,
            complete INTEGER NOT NULL,
            failed INTEGER NOT NULL,
            PRIMARY KEY (job, seq))",
        )
        .context("CREATE TABLE task")?;

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS job_output_rule (
            job TEXT NOT NULL,
            seq INTEGER NOT NULL,
            rule TEXT NOT NULL,
            PRIMARY KEY (job, seq))",
        )
        .context("CREATE TABLE job_output_rule")?;

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS job_output (
            job TEXT NOT NULL,
            path TEXT NOT NULL,
            size INTEGER NOT NULL,
            id TEXT NOT NULL UNIQUE,
            PRIMARY KEY (job, path))",
        )
        .context("CREATE TABLE job_output")?;

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS job_event (
            job TEXT NOT NULL,
            task INTEGER,
            seq INTEGER NOT NULL,
            stream TEXT NOT NULL,
            time TEXT NOT NULL,
            payload TEXT NOT NULL,
            PRIMARY KEY (job, seq))",
        )
        .context("CREATE TABLE job_event")?;

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS worker (
            id TEXT PRIMARY KEY,
            bootstrap TEXT NOT NULL UNIQUE,
            token TEXT UNIQUE,
            instance_id TEXT,
            deleted INTEGER,
            recycle INTEGER,
            lastping TEXT)",
        )
        .context("CREATE TABLE worker")?;

        Ok(Database(log, Mutex::new(Inner { conn })))
    }

    pub fn workers(&self) -> Result<Vec<Worker>> {
        let c = &mut self.1.lock().unwrap().conn;

        let mut q = c.prepare("SELECT * FROM worker ORDER BY id ASC")?;

        let rows = q.query_map([], row_to_worker)?;
        Ok(rows.into_iter().collect::<std::result::Result<_, _>>()?)
    }

    pub fn worker_jobs(&self, worker: &Ulid) -> Result<Vec<Job>> {
        let c = &mut self.1.lock().unwrap().conn;

        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;

        self.i_jobs_for_worker(&tx, worker)
    }

    pub fn free_workers(&self) -> Result<Vec<Worker>> {
        let c = &mut self.1.lock().unwrap().conn;

        let mut q = c.prepare(
            "SELECT w.* \
            FROM worker w LEFT OUTER JOIN job j ON w.id = j.worker \
            WHERE j.worker IS NULL \
            AND NOT w.deleted \
            AND NOT w.recycle \
            AND w.token IS NOT NULL \
            ORDER BY id ASC",
        )?;

        let rows = q.query_map([], row_to_worker)?;
        Ok(rows.into_iter().collect::<std::result::Result<_, _>>()?)
    }

    pub fn worker_recycle_all(&self) -> Result<usize> {
        let c = &mut self.1.lock().unwrap().conn;

        let mut q = c.prepare(
            "UPDATE worker SET recycle = 1 \
            WHERE deleted = 0",
        )?;

        Ok(q.execute([])?)
    }

    pub fn worker_recycle(&self, id: &Ulid) -> Result<bool> {
        let c = &mut self.1.lock().unwrap().conn;

        let mut q = c.prepare("UPDATE worker SET recycle = 1 WHERE id = ?")?;

        Ok(q.execute(params![&id.to_string()])? > 0)
    }

    pub fn worker_destroy(&self, id: &Ulid) -> Result<bool> {
        let c = &mut self.1.lock().unwrap().conn;

        let mut q = c.prepare("UPDATE worker SET deleted = 1 WHERE id = ?")?;

        Ok(q.execute(params![&id.to_string()])? > 0)
    }

    pub fn worker_ping(&self, id: &Ulid) -> Result<bool> {
        let c = &mut self.1.lock().unwrap().conn;

        let now = Utc::now();
        let mut q = c.prepare("UPDATE worker SET lastping = ? WHERE id = ?")?;

        Ok(q.execute(params![&now, &id.to_string()])? > 0)
    }

    pub fn worker_assign_job(&self, worker: &Ulid, job: &Ulid) -> OResult<()> {
        let c = &mut self.1.lock().unwrap().conn;

        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;

        let w = self.i_worker_get(&tx, worker)?;
        if w.deleted || w.recycle {
            conflict!("worker {} already deleted, cannot assign job", w.id);
        }

        let j = self.i_job_get(&tx, job)?;
        if let Some(jw) = j.worker.as_ref() {
            conflict!("job {} already assigned to worker {}", j.id, jw);
        }

        let c = self.i_worker_job_count(&tx, worker)?;
        if c > 0 {
            conflict!("worker {} already has {} jobs assigned", worker, c);
        }

        let uc = tx
            .prepare("UPDATE job SET worker = ? WHERE id = ?")?
            .execute(params![worker.to_string(), job.to_string(),])?;
        assert_eq!(uc, 1);

        self.i_job_event_insert(
            &tx,
            job,
            None,
            0,
            "control",
            &Utc::now(),
            &format!("job assigned to worker {}", w.id),
        )?;

        tx.commit()?;
        Ok(())
    }

    pub fn worker_bootstrap(
        &self,
        bootstrap: &str,
        token: &str,
    ) -> Result<Option<Worker>> {
        let log = &self.0;
        let c = &mut self.1.lock().unwrap().conn;

        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;

        let w = self.i_worker_from_bootstrap(&tx, bootstrap)?;
        assert_eq!(&w.bootstrap, bootstrap);

        if w.deleted {
            error!(log, "worker {} already deleted, cannot bootstrap", w.id);
            return Ok(None);
        }

        if w.instance_id.is_none() {
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
                    w.id.to_string(),
                    current,
                    token
                );
                return Ok(None);
            }
        } else {
            let mut q = tx.prepare(
                "UPDATE worker SET token = ?, lastping = ? \
                WHERE id = ? AND bootstrap = ? AND token IS NULL",
            )?;

            let count = q.execute(params![
                token,
                &Utc::now(),
                &w.id.to_string(),
                bootstrap,
            ])?;
            assert_eq!(count, 1);
        }

        let w = self.i_worker_get(&tx, &w.id)?;

        tx.commit()?;
        Ok(Some(w))
    }

    pub fn worker_associate(
        &self,
        id: &Ulid,
        instance_id: &str,
    ) -> OResult<()> {
        let c = &mut self.1.lock().unwrap().conn;

        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;

        let w = self.i_worker_get(&tx, id)?;

        if w.deleted {
            conflict!("worker {} already deleted, cannot associate");
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
                    id.to_string(),
                    current,
                    instance_id
                );
            }
        } else {
            /*
             * The worker is not yet associated with an instance ID.
             */
            let mut q = tx.prepare(
                "UPDATE worker SET instance_id = ? \
                WHERE id = ?",
            )?;

            let count = q.execute(params![instance_id, &id.to_string()])?;
            assert_eq!(count, 1);
        }

        Ok(tx.commit()?)
    }

    fn i_worker_get(&self, tx: &Transaction, id: &Ulid) -> Result<Worker> {
        let mut q = tx.prepare("SELECT * FROM worker WHERE id = ?")?;

        Ok(q.query_row(params![&id.to_string()], row_to_worker)?)
    }

    pub fn worker_get(&self, id: &Ulid) -> Result<Worker> {
        let c = &mut self.1.lock().unwrap().conn;
        let tx = c.transaction()?;
        let out = self.i_worker_get(&tx, id)?;
        tx.commit()?;
        Ok(out)
    }

    pub fn worker_auth(&self, token: &str) -> Result<Worker> {
        let c = &mut self.1.lock().unwrap().conn;

        let mut q = c.prepare("SELECT * FROM worker WHERE token = ?")?;

        let mut rows = q
            .query_map(params![token], row_to_worker)
            .context("SELECT worker")?;

        let user = rows.next().transpose()?;
        let extra = rows.next().transpose()?;

        match (user, extra) {
            (None, _) => bail!("auth failure"),
            (Some(u), Some(x)) => bail!("token error ({}, {})", u.id, x.id),
            (Some(u), None) => Ok(u),
        }
    }

    fn i_worker_from_bootstrap(
        &self,
        tx: &Transaction,
        bootstrap: &str,
    ) -> Result<Worker> {
        let mut q = tx.prepare("SELECT * FROM worker WHERE bootstrap = ?")?;

        Ok(q.query_row(params![bootstrap], row_to_worker)?)
    }

    pub fn worker_create(&self) -> Result<Worker> {
        let w = Worker {
            id: Ulid::generate(),
            bootstrap: genkey(64),
            instance_id: None,
            token: None,
            deleted: false,
            recycle: false,
            lastping: None,
        };

        let c = &mut self.1.lock().unwrap().conn;

        let mut q = c.prepare(
            "INSERT INTO worker ( \
            id, bootstrap, deleted, recycle) VALUES ( \
            ?, ?, ?, ?)",
        )?;

        let count = q.execute(params![
            &w.id.to_string(),
            &w.bootstrap,
            &w.deleted,
            &w.recycle,
        ])?;
        assert_eq!(count, 1);

        Ok(w)
    }

    pub fn jobs(&self) -> Result<Vec<Job>> {
        let c = &mut self.1.lock().unwrap().conn;

        let mut q = c.prepare("SELECT * FROM job ORDER BY id ASC")?;

        let rows = q.query_map([], row_to_job)?;
        Ok(rows.into_iter().collect::<std::result::Result<_, _>>()?)
    }

    pub fn job_tasks(&self, job: &Ulid) -> Result<Vec<Task>> {
        let c = &mut self.1.lock().unwrap().conn;

        let mut q = c.prepare(
            "SELECT * FROM task \
            WHERE job = ? ORDER BY seq ASC",
        )?;

        let rows = q.query_map(params![&job.to_string()], row_to_task)?;
        Ok(rows.into_iter().collect::<std::result::Result<_, _>>()?)
    }

    pub fn job_output_rules(&self, job: &Ulid) -> Result<Vec<String>> {
        let c = &mut self.1.lock().unwrap().conn;

        let mut q = c.prepare(
            "SELECT rule FROM job_output_rule \
            WHERE job = ? ORDER BY seq ASC",
        )?;

        let rows =
            q.query_map(params![&job.to_string()], row_to_job_output_rule)?;
        Ok(rows.into_iter().collect::<std::result::Result<_, _>>()?)
    }

    pub fn job_outputs(&self, job: &Ulid) -> Result<Vec<JobOutput>> {
        let c = &mut self.1.lock().unwrap().conn;

        let mut q = c.prepare(
            "SELECT * FROM job_output \
            WHERE job = ? ORDER BY id ASC",
        )?;

        let rows = q.query_map(params![&job.to_string()], row_to_job_output)?;
        Ok(rows.into_iter().collect::<std::result::Result<_, _>>()?)
    }

    pub fn job_events(
        &self,
        job: &Ulid,
        minseq: usize,
    ) -> Result<Vec<JobEvent>> {
        let c = &mut self.1.lock().unwrap().conn;

        let mut q = c.prepare(
            "SELECT * FROM job_event \
            WHERE job = ? AND seq >= ? ORDER BY seq ASC",
        )?;

        let rows = q.query_map(
            params![&job.to_string(), &(minseq as i64)],
            row_to_job_event,
        )?;
        Ok(rows.into_iter().collect::<std::result::Result<_, _>>()?)
    }

    pub fn job_by_str(&self, job: &str) -> Result<Job> {
        let id = Ulid::from_str(job)?;
        self.job_by_id(&id)
    }

    pub fn job_by_id(&self, job: &Ulid) -> Result<Job> {
        let c = &mut self.1.lock().unwrap().conn;
        let mut tx =
            c.transaction_with_behavior(TransactionBehavior::Deferred)?;
        tx.set_drop_behavior(DropBehavior::Commit);
        self.i_job_get(&tx, job)
    }

    fn i_job_get(&self, tx: &Transaction, id: &Ulid) -> Result<Job> {
        let mut q = tx.prepare("SELECT * FROM job WHERE id = ?")?;

        Ok(q.query_row(params![&id.to_string()], row_to_job)?)
    }

    pub fn job_by_id_opt(&self, job: &Ulid) -> Result<Option<Job>> {
        let c = &mut self.1.lock().unwrap().conn;
        let mut tx =
            c.transaction_with_behavior(TransactionBehavior::Deferred)?;
        tx.set_drop_behavior(DropBehavior::Commit);
        self.i_job_get_opt(&tx, job)
    }

    fn i_job_get_opt(
        &self,
        tx: &Transaction,
        id: &Ulid,
    ) -> Result<Option<Job>> {
        let mut q = tx.prepare("SELECT * FROM job WHERE id = ?")?;

        Ok(q.query_row(params![&id.to_string()], row_to_job).optional()?)
    }

    fn i_job_task_get(
        &self,
        tx: &Transaction,
        job: &Ulid,
        seq: i32,
    ) -> Result<Task> {
        let mut q =
            tx.prepare("SELECT * FROM task WHERE job = ? AND seq = ?")?;

        Ok(q.query_row(params![&job.to_string(), seq], row_to_task)?)
    }

    fn i_job_tasks_get(
        &self,
        tx: &Transaction,
        job: &Ulid,
    ) -> Result<Vec<Task>> {
        let mut q =
            tx.prepare("SELECT * FROM task WHERE job = ? ORDER BY seq ASC")?;

        let rows = q.query_map(params![&job.to_string()], row_to_task)?;
        Ok(rows.into_iter().collect::<std::result::Result<_, _>>()?)
    }

    fn i_jobs_for_worker(
        &self,
        tx: &Transaction,
        worker: &Ulid,
    ) -> Result<Vec<Job>> {
        let mut q = tx.prepare("SELECT * FROM job WHERE worker = ?")?;

        let rows = q.query_map(params![&worker.to_string()], row_to_job)?;
        Ok(rows.into_iter().collect::<std::result::Result<_, _>>()?)
    }

    pub fn job_create(
        &self,
        owner: &Ulid,
        name: &str,
        target: &str,
        tasks: Vec<CreateTask>,
        output_rules: &[String],
    ) -> Result<Job> {
        let j = Job {
            id: Ulid::generate(),
            owner: *owner,
            name: name.to_string(),
            target: target.to_string(),
            complete: false,
            failed: false,
            worker: None,
        };

        let c = &mut self.1.lock().unwrap().conn;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;

        {
            let mut q = tx.prepare(
                "INSERT INTO job ( \
                id, owner, name, target, failed, complete) VALUES ( \
                ?, ?, ?, ?, ?, ?)",
            )?;

            let c = q.execute(params![
                &j.id.to_string(),
                &j.owner.to_string(),
                &j.name,
                &j.target,
                false,
                false,
            ])?;
            assert_eq!(c, 1);
        }

        if !tasks.is_empty() {
            let mut q = tx.prepare(
                "INSERT INTO task (\
                job, seq, name, script, env_clear, \
                env, user_id, group_id, workdir, failed, \
                complete) \
                VALUES (\
                    ?, ?, ?, ?, ?, \
                    ?, ?, ?, ?, ?,
                    ?)",
            )?;

            for (i, t) in tasks.iter().enumerate() {
                let c = q.execute(params![
                    &j.id.to_string(),
                    &(i as i64),
                    &t.name,
                    &t.script,
                    &t.env_clear,
                    &from_hashmap(&t.env)?,
                    &t.user_id,
                    &t.group_id,
                    &t.workdir,
                    false,
                    false,
                ])?;
                assert_eq!(c, 1);
            }
        } else {
            bail!("a job must have at least one task");
        }

        if !output_rules.is_empty() {
            let mut q = tx.prepare(
                "INSERT INTO job_output_rule (job, seq, rule) VALUES \
                (?, ?, ?)",
            )?;

            for (i, rule) in output_rules.iter().enumerate() {
                let c =
                    q.execute(params![&j.id.to_string(), &(i as i64), &rule])?;
                assert_eq!(c, 1);
            }
        }

        tx.commit()?;
        Ok(j)
    }

    pub fn job_output_by_str(
        &self,
        job: &str,
        output: &str,
    ) -> Result<JobOutput> {
        let id = Ulid::from_str(job)?;
        let output = Ulid::from_str(output)?;
        let c = &mut self.1.lock().unwrap().conn;
        let mut tx =
            c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.set_drop_behavior(DropBehavior::Commit);
        self.i_job_output_get(&tx, &id, &output)
    }

    fn i_job_output_get(
        &self,
        tx: &Transaction,
        job: &Ulid,
        output: &Ulid,
    ) -> Result<JobOutput> {
        let mut q =
            tx.prepare("SELECT * FROM job_output WHERE job = ? AND id = ?")?;

        Ok(q.query_row(
            params![&job.to_string(), &output.to_string()],
            row_to_job_output,
        )?)
    }

    pub fn job_add_output(
        &self,
        job: &Ulid,
        path: &str,
        id: &Ulid,
        size: u64,
    ) -> OResult<()> {
        let c = &mut self.1.lock().unwrap().conn;

        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;

        let j = self.i_job_get(&tx, job)?;
        if j.complete {
            conflict!("job already complete, cannot add more files");
        }

        self.i_job_output_insert(&tx, job, path, id, size)?;

        tx.commit()?;
        Ok(())
    }

    pub fn job_append_event(
        &self,
        job: &Ulid,
        task: Option<u32>,
        stream: &str,
        time: &DateTime<Utc>,
        payload: &str,
    ) -> OResult<usize> {
        let c = &mut self.1.lock().unwrap().conn;

        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;

        let j = self.i_job_get(&tx, job)?;
        if j.complete {
            conflict!("job already complete, cannot append");
        }

        /*
         * Find the next job event ID and insert the record:
         */
        let seq = self.i_job_next_seq(&tx, job)?;
        self.i_job_event_insert(&tx, job, task, seq, stream, time, payload)?;

        tx.commit()?;
        Ok(seq)
    }

    pub fn job_complete(&self, job: &Ulid, failed: bool) -> Result<bool> {
        let c = &mut self.1.lock().unwrap().conn;

        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;

        let j = self.i_job_get(&tx, job)?;
        if j.complete {
            /*
             * This job is already complete.
             */
            return Ok(false);
        }

        /*
         * Mark any tasks that have not yet completed as failed, as they will
         * not be executed.
         */
        let mut tasks_failed = false;
        for (i, t) in self.i_job_tasks_get(&tx, job)?.iter().enumerate() {
            if t.failed {
                tasks_failed = true;
            }
            if t.failed || t.complete {
                continue;
            }

            let seq = self.i_job_next_seq(&tx, job)?;
            self.i_job_event_insert(
                &tx,
                job,
                None,
                seq,
                "control",
                &Utc::now(),
                &format!("task {} was incomplete, marked failed", i),
            )?;

            let mut q = tx.prepare(
                "UPDATE task SET failed = ?, complete = ? WHERE \
                job = ? AND seq = ? AND NOT complete",
            )?;

            let uc = q.execute(params![
                &true,
                &true,
                &job.to_string(),
                &(i as i64)
            ])?;
            assert_eq!(uc, 1);
        }

        let failed = if failed {
            true
        } else if tasks_failed {
            /*
             * If a task failed, we must report job-level failure even if the
             * job was for some reason not explicitly failed.
             */
            let seq = self.i_job_next_seq(&tx, job)?;
            self.i_job_event_insert(
                &tx,
                job,
                None,
                seq,
                "control",
                &Utc::now(),
                "job failed because at least one task failed",
            )?;
            true
        } else {
            false
        };

        {
            let mut q = tx.prepare(
                "UPDATE job SET complete = ?, failed = ? \
                WHERE id = ? AND NOT complete",
            )?;

            let uc = q.execute(params![&true, &failed, &job.to_string()])?;
            assert_eq!(uc, 1);
        }

        tx.commit()?;
        Ok(true)
    }

    pub fn task_complete(
        &self,
        job: &Ulid,
        seq: u32,
        failed: bool,
    ) -> Result<bool> {
        let c = &mut self.1.lock().unwrap().conn;

        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;

        let t = self.i_job_task_get(&tx, job, seq as i32)?;
        if t.complete {
            return Ok(false);
        }

        {
            let mut q = tx.prepare(
                "UPDATE task SET complete = ?, failed = ? \
                WHERE job = ? AND seq = ? AND NOT complete",
            )?;

            let uc =
                q.execute(params![&true, &failed, &t.job.to_string(), t.seq])?;
            assert_eq!(uc, 1);
        }

        tx.commit()?;
        Ok(true)
    }

    fn i_job_output_insert(
        &self,
        tx: &Transaction,
        job: &Ulid,
        path: &str,
        id: &Ulid,
        size: u64,
    ) -> Result<()> {
        let mut q = tx.prepare(
            "INSERT INTO job_output (job, path, id, size) \
            VALUES (?, ?, ?, ?)",
        )?;

        let ic = q.execute(params![
            &job.to_string(),
            path,
            &id.to_string(),
            &(size as i64),
        ])?;
        assert_eq!(ic, 1);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn i_job_event_insert(
        &self,
        tx: &Transaction,
        job: &Ulid,
        task: Option<u32>,
        seq: usize,
        stream: &str,
        time: &DateTime<Utc>,
        payload: &str,
    ) -> Result<()> {
        let mut q = tx.prepare(
            "INSERT INTO job_event (\
            job, task, seq, stream, time, payload) \
            VALUES (?, ?, ?, ?, ?, ?)",
        )?;

        let ic = q.execute(params![
            &job.to_string(),
            &task.map(|n| n as i32),
            &(seq as i64),
            stream,
            time,
            payload,
        ])?;
        assert_eq!(ic, 1);
        Ok(())
    }

    fn i_job_next_seq(&self, tx: &Transaction, job: &Ulid) -> Result<usize> {
        let mut q = tx.prepare(
            "SELECT MAX(seq) AS n FROM job_event \
            WHERE job = ?",
        )?;

        Ok(q.query_row(params![&job.to_string()], row_to_opt_count)?
            .map(|n| n + 1)
            .unwrap_or(0))
    }

    pub fn user_jobs(&self, owner: &Ulid) -> Result<Vec<Job>> {
        let c = &mut self.1.lock().unwrap().conn;

        let mut q = c.prepare("SELECT * FROM job WHERE owner = ?")?;

        let rows = q.query_map(params![&owner.to_string()], row_to_job)?;

        Ok(rows.into_iter().collect::<std::result::Result<_, _>>()?)
    }

    fn i_worker_job_count(&self, tx: &Transaction, id: &Ulid) -> Result<usize> {
        let mut q = tx.prepare(
            "SELECT COUNT(*) AS n FROM job WHERE \
            worker = ?",
        )?;

        Ok(q.query_row(params![&id.to_string()], row_to_count)?)
    }

    pub fn worker_job(&self, worker: &Ulid) -> Result<Option<Job>> {
        let c = &mut self.1.lock().unwrap().conn;

        let mut q = c.prepare("SELECT * FROM job WHERE worker = ?")?;

        let rows = q.query_map(params![&worker.to_string()], row_to_job)?;

        let t: Vec<Job> =
            rows.into_iter().collect::<std::result::Result<_, _>>()?;
        match t.len() {
            0 => Ok(None),
            1 => Ok(Some(t[0].clone())),
            n => bail!("found {} jobs for worker {}", n, worker.to_string()),
        }
    }

    pub fn users(&self) -> Result<Vec<User>> {
        let c = &mut self.1.lock().unwrap().conn;

        let mut q = c.prepare("SELECT * FROM user ORDER BY id ASC")?;

        let rows = q.query_map([], row_to_user)?;
        Ok(rows.into_iter().collect::<std::result::Result<_, _>>()?)
    }

    pub fn user_create(&self, name: &str) -> Result<User> {
        let u = User {
            id: Ulid::generate(),
            name: name.to_string(),
            token: genkey(48),
            time_create: Utc::now(),
        };

        let c = &mut self.1.lock().unwrap().conn;

        let mut q = c.prepare(
            "INSERT INTO user ( \
            id, name, token, time_create) VALUES ( \
            ?, ?, ?, ?)",
        )?;

        let count = q.execute(params![
            &u.id.to_string(),
            &u.name,
            &u.token,
            &u.time_create,
        ])?;
        assert_eq!(count, 1);

        Ok(u)
    }

    pub fn user_auth(&self, token: &str) -> Result<User> {
        let c = &mut self.1.lock().unwrap().conn;

        let mut q = c.prepare(
            "SELECT id, name, token, time_create FROM \
            user WHERE token = ?",
        )?;

        let mut rows = q
            .query_map(params![token], |row| {
                Ok(User {
                    id: to_ulid(row.get("id")?)?,
                    name: row.get("name")?,
                    token: row.get("token")?,
                    time_create: row.get("time_create")?,
                })
            })
            .context("SELECT user")?;

        let user = rows.next().transpose()?;
        let extra = rows.next().transpose()?;

        match (user, extra) {
            (None, _) => bail!("auth failure"),
            (Some(u), Some(x)) => bail!("token error ({}, {})", u.id, x.id),
            (Some(u), None) => Ok(u),
        }
    }
}
