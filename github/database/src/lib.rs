/*
 * Copyright 2021 Oxide Computer Company
 */

use anyhow::{anyhow, bail, Context, Result};
use buildomat_common::*;
use chrono::prelude::*;
use rusqlite::{
    params, types::Type, OptionalExtension, Row, Transaction,
    TransactionBehavior,
};
use rusty_ulid::Ulid;
use serde::{Deserialize, Serialize};
use serde_with::{DeserializeFromStr, SerializeDisplay};
use slog::Logger;
use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
use std::sync::Mutex;
use thiserror::Error;

/* XXX from rusqlite? */
const UNKNOWN_COLUMN: usize = std::usize::MAX;

#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("conflict: {0}")]
    Conflict(String),
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type DBResult<T> = std::result::Result<T, DatabaseError>;

pub trait ToSQLiteError<T> {
    fn or_conv_fail(self) -> rusqlite::Result<T>;
}

impl<T: std::fmt::Debug> ToSQLiteError<T> for serde_json::Result<T> {
    fn or_conv_fail(self) -> rusqlite::Result<T> {
        self.map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(
                UNKNOWN_COLUMN,
                Type::Null,
                anyhow!("conversion failure: {:?}", e).into(),
            )
        })
    }
}

impl<T: std::fmt::Debug> ToSQLiteError<T> for anyhow::Result<T> {
    fn or_conv_fail(self) -> rusqlite::Result<T> {
        self.map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(
                UNKNOWN_COLUMN,
                Type::Null,
                anyhow!("conversion failure: {:?}", e).into(),
            )
        })
    }
}

trait RowExt {
    fn get_ulid(&self, n: &str) -> rusqlite::Result<Ulid>;
}

impl RowExt for Row<'_> {
    fn get_ulid(&self, n: &str) -> rusqlite::Result<Ulid> {
        Ok(Ulid::from_str(&self.get::<_, String>(n)?).unwrap())
    }
}

macro_rules! conflict {
    ($msg:expr) => {
        return Err(DatabaseError::Conflict($msg.to_string()))
    };
    ($fmt:expr, $($arg:tt)*) => {
        return Err(DatabaseError::Conflict(format!($fmt, $($arg)*)))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobFile {
    pub path: String,
    pub name: String,
    pub variety: CheckRunVariety,
    pub config: serde_json::Value,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Plan {
    pub jobfiles: Vec<JobFile>,
}

#[derive(Debug, Clone)]
pub struct Delivery {
    pub seq: usize,
    pub uuid: String,
    pub event: String,
    pub headers: HashMap<String, String>,
    pub payload: serde_json::Value,
    pub recvtime: DateTime<Utc>,
    pub ack: Option<i64>,
}

pub fn row_to_delivery(row: &Row) -> rusqlite::Result<Delivery> {
    Ok(Delivery {
        seq: row.get::<_, i64>("seq")? as usize,
        uuid: row.get("uuid")?,
        event: row.get("event")?,
        headers: serde_json::from_value(
            row.get::<_, serde_json::Value>("headers")?,
        )
        .or_conv_fail()?,
        payload: row.get("payload")?,
        recvtime: row.get("recvtime")?,
        ack: row.get("ack")?,
    })
}

#[derive(Debug, Clone)]
pub struct Repository {
    pub id: i64,
    pub owner: String,
    pub name: String,
}

pub fn row_to_repository(row: &Row) -> rusqlite::Result<Repository> {
    Ok(Repository {
        id: row.get("id")?,
        name: row.get("name")?,
        owner: row.get("owner")?,
    })
}

#[derive(Debug, Clone, PartialEq, DeserializeFromStr, SerializeDisplay)]
pub enum CheckRunVariety {
    Control,
    AlwaysPass,
    FailFirst,
    Basic,
}

impl CheckRunVariety {
    pub fn is_control(&self) -> bool {
        matches!(self, CheckRunVariety::Control)
    }
}

impl FromStr for CheckRunVariety {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "control" => CheckRunVariety::Control,
            "always_pass" => CheckRunVariety::AlwaysPass,
            "fail_first" => CheckRunVariety::FailFirst,
            "basic" => CheckRunVariety::Basic,
            x => bail!("unknown check run class: {:?}", x),
        })
    }
}

impl std::fmt::Display for CheckRunVariety {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use CheckRunVariety::*;

        write!(
            f,
            "{}",
            match self {
                Control => "control",
                AlwaysPass => "always_pass",
                FailFirst => "fail_first",
                Basic => "basic",
            }
        )
    }
}

#[derive(Debug, Clone)]
pub enum CheckSuiteState {
    Created,
    Parked,
    Planned,
    Running,
    Complete,
    Retired,
}

impl FromStr for CheckSuiteState {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "created" => CheckSuiteState::Created,
            "parked" => CheckSuiteState::Parked,
            "planned" => CheckSuiteState::Planned,
            "running" => CheckSuiteState::Running,
            "complete" => CheckSuiteState::Complete,
            "retired" => CheckSuiteState::Retired,
            x => bail!("unknown check suite state: {:?}", x),
        })
    }
}

impl ToString for CheckSuiteState {
    fn to_string(&self) -> String {
        use CheckSuiteState::*;

        match self {
            Created => "created",
            Parked => "parked",
            Planned => "planned",
            Running => "running",
            Complete => "complete",
            Retired => "retired",
        }
        .to_string()
    }
}

#[derive(Debug, Clone)]
pub struct CheckSuite {
    pub id: Ulid,
    pub repo: i64,
    pub install: i64,
    pub github_id: i64,
    pub head_sha: String,
    pub head_branch: Option<String>,
    pub state: CheckSuiteState,
    pub plan: Option<Plan>,
    pub plan_sha: Option<String>,
    pub url_key: String,
}

pub fn row_to_check_suite(row: &Row) -> rusqlite::Result<CheckSuite> {
    Ok(CheckSuite {
        id: row.get_ulid("id")?,
        repo: row.get("repo")?,
        install: row.get("install")?,
        github_id: row.get("github_id")?,
        head_sha: row.get("head_sha")?,
        head_branch: row.get("head_branch")?,
        state: CheckSuiteState::from_str(&row.get::<_, String>("state")?)
            .or_conv_fail()?,
        plan: row
            .get::<_, Option<String>>("plan")?
            .map(|s| serde_json::from_str(&s))
            .transpose()
            .or_conv_fail()?,
        plan_sha: row.get("plan_sha")?,
        url_key: row.get("url_key")?,
    })
}

#[derive(Debug, Clone)]
pub struct CheckRun {
    pub id: Ulid,
    pub check_suite: Ulid,
    /**
     * User-visible name of this Check Run.
     */
    pub name: String,
    /**
     * Which action do we need to take to perform this check run.
     */
    pub variety: CheckRunVariety,
    /**
     * Input job file content; e.g., a bash script.
     */
    pub content: Option<String>,
    /**
     * Input job file configuration; interpreted by the variety routines.
     */
    pub config: Option<serde_json::Value>,
    /**
     * Per-variety private state tracking data.
     */
    pub private: Option<serde_json::Value>,
    /**
     * Is this the current instance of this Check Run within the containing
     * Check Suite?
     */
    pub active: bool,
    /**
     * Do we believe that GitHub has received the most recent status information
     * and output about this Check Run?
     */
    pub flushed: bool,
    /**
     * What ID has GitHub given us when we created this instance of the Check
     * Run?
     */
    pub github_id: Option<i64>,
}

impl CheckRun {
    pub fn get_config<T>(&self) -> Result<T>
    where
        for<'de> T: Deserialize<'de>,
    {
        let config = if let Some(config) = &self.config {
            config.clone()
        } else {
            serde_json::json!({})
        };
        Ok(serde_json::from_value(config)?)
    }

    pub fn get_private<T>(&self) -> Result<T>
    where
        for<'de> T: Deserialize<'de>,
    {
        let private = if let Some(private) = &self.private {
            private.clone()
        } else {
            serde_json::json!({})
        };
        Ok(serde_json::from_value(private)?)
    }

    pub fn set_private<T: Serialize>(&mut self, private: T) -> Result<()> {
        self.private = Some(serde_json::to_value(private)?);
        Ok(())
    }
}

pub fn row_to_check_run(row: &Row) -> rusqlite::Result<CheckRun> {
    Ok(CheckRun {
        id: row.get_ulid("id")?,
        check_suite: row.get_ulid("check_suite")?,
        name: row.get("name")?,
        variety: CheckRunVariety::from_str(&row.get::<_, String>("variety")?)
            .or_conv_fail()?,
        private: row.get("private")?,
        content: row.get("content")?,
        config: row.get("config")?,
        //private: row
        //    .get::<_, Option<String>>("private")?
        //    .map(|s| serde_json::from_str(&s))
        //    .transpose()
        //    .or_conv_fail()?,
        active: row.get("active")?,
        flushed: row.get("flushed")?,
        github_id: row.get("github_id")?,
    })
}

pub fn row_to_opt_count(row: &Row) -> rusqlite::Result<Option<usize>> {
    row.get::<_, Option<i64>>(0)?
        .map(|n| {
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

pub struct Database(Logger, Mutex<rusqlite::Connection>);

impl Database {
    pub fn new<P: AsRef<Path>>(log: Logger, path: P) -> Result<Database> {
        let p = path.as_ref();
        let conn = rusqlite::Connection::open(p)?;

        conn.pragma_update(None, "journal_mode", &"WAL".to_string())?;
        conn.pragma_update(None, "foreign_keys", &"ON".to_string())?;

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS delivery (
            seq INTEGER PRIMARY KEY,
            uuid TEXT NOT NULL UNIQUE,
            event TEXT NOT NULL,
            headers TEXT NOT NULL,
            payload TEXT NOT NULL,
            recvtime DATETIME NOT NULL,
            ack INTEGER)",
        )
        .context("CREATE TABLE delivery")?;

        /*
         * Repositories may be renamed or change owner in the future, so we
         * refer to them internally by their unique ID number and cache the
         * translation.
         */
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS repository (
            id INTEGER PRIMARY KEY,
            owner TEXT NOT NULL,
            name TEXT NOT NULL)",
        )
        .context("CREATE TABLE repository")?;

        /*
         * We receive notifications for a "check suite", which I think we want
         * to record in the database?  When a check suite is opened, it appears
         * to be for a particular repository and commit.
         *
         * Track our current state in "state": "created", "planned", ...
         *
         * Assemble our expected set of check runs in "plan" somehow.
         */
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS check_suite (
            id TEXT PRIMARY KEY,
            repo INTEGER NOT NULL,
            install INTEGER NOT NULL,
            github_id INTEGER NOT NULL,
            head_sha TEXT NOT NULL,
            head_branch TEXT,
            state TEXT NOT NULL,
            plan TEXT,
            plan_sha TEXT,
            url_key TEXT NOT NULL,
            UNIQUE (repo, github_id))",
        )
        .context("CREATE TABLE check_suite")?;

        /*
         * Then, we have to actually _do_ the check run.
         */
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS check_run (
            id TEXT PRIMARY KEY,
            check_suite TEXT NOT NULL
                REFERENCES check_suite (id)
                ON UPDATE RESTRICT
                ON DELETE RESTRICT,
            name TEXT NOT NULL,
            variety TEXT NOT NULL,
            content TEXT,
            config TEXT,
            private TEXT,
            active INTEGER NOT NULL,
            flushed INTEGER NOT NULL,
            github_id INTEGER)",
        )
        .context("CREATE TABLE check_run")?;

        Ok(Database(log, Mutex::new(conn)))
    }

    /**
     * Get the next sequence number in the webhook delivery table.  Run in a
     * transaction with the insertion.
     */
    fn i_delivery_next_seq(&self, tx: &Transaction) -> Result<usize> {
        let mut q = tx.prepare("SELECT MAX(seq) AS n FROM delivery")?;

        Ok(q.query_row([], row_to_opt_count)?
            .map(|n| n.checked_add(1).unwrap())
            .unwrap_or(0))
    }

    pub fn store_delivery(
        &self,
        uuid: &str,
        event: &str,
        headers: &HashMap<String, String>,
        payload: &serde_json::Value,
        recvtime: DateTime<Utc>,
    ) -> DBResult<usize> {
        let mut c = self.1.lock().unwrap();

        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;

        let seq = self.i_delivery_next_seq(&tx)?;

        {
            let mut q = tx.prepare(
                "INSERT INTO delivery (\
                seq, uuid, event, headers, payload, recvtime) \
                VALUES (?, ?, ?, ?, ?, ?)",
            )?;

            let ic = q.execute(params![
                &(seq as i64),
                uuid,
                event,
                &serde_json::to_value(headers).unwrap(),
                payload,
                &recvtime,
            ])?;
            assert_eq!(ic, 1);
        }

        tx.commit()?;

        Ok(seq)
    }

    pub fn delivery_ack(&self, seq: usize, ack: u64) -> Result<()> {
        let c = &mut self.1.lock().unwrap();

        let mut q = c.prepare("UPDATE delivery SET ack = ? WHERE seq = ?")?;

        let ic = q.execute(params![ack, seq])?;
        assert_eq!(ic, 1);

        Ok(())
    }

    pub fn delivery_unack(&self, seq: usize) -> Result<()> {
        let c = &mut self.1.lock().unwrap();

        let mut q =
            c.prepare("UPDATE delivery SET ack = NULL WHERE seq = ?")?;

        let ic = q.execute(params![seq])?;

        assert!(ic < 2);
        if ic == 0 {
            bail!("delivery {} not found", seq);
        }

        Ok(())
    }

    pub fn list_deliveries_unacked(&self) -> Result<Vec<Delivery>> {
        let c = &mut self.1.lock().unwrap();

        let mut q = c.prepare(
            "SELECT * FROM delivery WHERE ack IS NULL \
            ORDER BY seq ASC",
        )?;

        let rows = q.query_map([], row_to_delivery)?;

        Ok(rows.into_iter().collect::<std::result::Result<_, _>>()?)
    }

    pub fn list_deliveries(&self) -> Result<Vec<Delivery>> {
        let c = &mut self.1.lock().unwrap();

        let mut q = c.prepare("SELECT * FROM delivery ORDER BY seq ASC")?;

        let rows = q.query_map([], row_to_delivery)?;

        Ok(rows.into_iter().collect::<std::result::Result<_, _>>()?)
    }

    pub fn load_check_run(&self, id: &Ulid) -> Result<CheckRun> {
        let c = &mut self.1.lock().unwrap();

        let mut q = c.prepare("SELECT * FROM check_run WHERE id = ?")?;

        Ok(q.query_row(params![&id.to_string()], row_to_check_run)?)
    }

    pub fn load_check_suite(&self, id: &Ulid) -> Result<CheckSuite> {
        let c = &mut self.1.lock().unwrap();

        let mut q = c.prepare("SELECT * FROM check_suite WHERE id = ?")?;

        Ok(q.query_row(params![&id.to_string()], row_to_check_suite)?)
    }

    pub fn load_delivery(&self, seq: usize) -> Result<Delivery> {
        let c = &mut self.1.lock().unwrap();

        let mut q = c.prepare("SELECT * FROM delivery WHERE seq = ?")?;

        Ok(q.query_row(params![seq as i64], row_to_delivery)?)
    }

    pub fn load_repository(&self, id: i64) -> DBResult<Repository> {
        let c = self.1.lock().unwrap();

        let mut q = c.prepare("SELECT * FROM repository WHERE id = ?")?;

        Ok(q.query_row(params![id], row_to_repository)?)
    }

    pub fn store_repository(
        &self,
        id: i64,
        owner: &str,
        name: &str,
    ) -> DBResult<()> {
        let mut c = self.1.lock().unwrap();

        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;

        {
            let mut q = tx.prepare(
                "INSERT INTO repository (\
                id, name, owner) \
                VALUES (?, ?, ?) \
                ON CONFLICT (id) DO UPDATE SET \
                name = excluded.name, \
                owner = excluded.owner",
            )?;

            q.execute(params![id, name, owner,])?;
        }

        tx.commit()?;

        Ok(())
    }

    pub fn list_repositories(&self) -> Result<Vec<Repository>> {
        let c = &mut self.1.lock().unwrap();

        let mut q = c.prepare("SELECT * FROM repository ORDER BY id ASC")?;

        let rows = q.query_map([], row_to_repository)?;

        Ok(rows.into_iter().collect::<std::result::Result<_, _>>()?)
    }

    pub fn list_check_suites(&self) -> Result<Vec<CheckSuite>> {
        let c = &mut self.1.lock().unwrap();

        let mut q = c.prepare("SELECT * FROM check_suite ORDER BY id ASC")?;

        let rows = q.query_map([], row_to_check_suite)?;

        Ok(rows.into_iter().collect::<std::result::Result<_, _>>()?)
    }

    pub fn list_check_suites_active(&self) -> Result<Vec<CheckSuite>> {
        let c = &mut self.1.lock().unwrap();

        let mut q = c.prepare(
            "SELECT * FROM check_suite \
            WHERE state <> 'parked' AND \
            state <> 'complete' AND \
            state <> 'retired' \
            ORDER BY id ASC",
        )?;

        let rows = q.query_map([], row_to_check_suite)?;

        Ok(rows.into_iter().collect::<std::result::Result<_, _>>()?)
    }

    pub fn list_check_runs_for_suite(
        &self,
        check_suite: &Ulid,
    ) -> Result<Vec<CheckRun>> {
        let c = &mut self.1.lock().unwrap();

        let mut q = c.prepare(
            "SELECT * FROM check_run \
            WHERE check_suite = ? ORDER BY id ASC",
        )?;

        let rows = q.query_map([&check_suite.to_string()], row_to_check_run)?;

        Ok(rows.into_iter().collect::<std::result::Result<_, _>>()?)
    }

    fn i_load_check_run(
        &self,
        tx: &Transaction,
        id: &Ulid,
    ) -> DBResult<CheckRun> {
        let mut q = tx.prepare("SELECT * FROM check_run WHERE id = ?")?;

        Ok(q.query_row(params![&id.to_string()], row_to_check_run)?)
    }

    fn i_load_check_suite(
        &self,
        tx: &Transaction,
        id: &Ulid,
    ) -> DBResult<CheckSuite> {
        let mut q = tx.prepare("SELECT * FROM check_suite WHERE id = ?")?;

        Ok(q.query_row(params![&id.to_string()], row_to_check_suite)?)
    }

    pub fn ensure_check_suite(
        &self,
        repo: i64,
        install: i64,
        github_id: i64,
        head_sha: &str,
        head_branch: Option<&str>,
    ) -> DBResult<CheckSuite> {
        let mut c = self.1.lock().unwrap();

        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;

        let res = {
            let mut q0 = tx.prepare(
                "SELECT * FROM check_suite WHERE repo = ? AND github_id = ?",
            )?;

            if let Some(row) = q0
                .query_row(params![repo, github_id], row_to_check_suite)
                .optional()?
            {
                return Ok(row);
            }

            /*
             * Give the check suite a unique ID that we control, and a URL key
             * that will be hard to guess.
             */
            let id = Ulid::generate();
            let url_key = genkey(48);

            let mut qu = tx.prepare(
                "INSERT INTO check_suite (\
                id, repo, install, github_id, head_sha, head_branch, \
                state, url_key) \
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            )?;

            let ic = qu.execute(params![
                &id.to_string(),
                repo,
                install,
                github_id,
                &head_sha,
                &head_branch,
                &CheckSuiteState::Created.to_string(),
                &url_key,
            ])?;

            assert_eq!(ic, 1);

            self.i_load_check_suite(&tx, &id)?
        };

        tx.commit()?;

        Ok(res)
    }

    pub fn update_check_suite(&self, check_suite: &CheckSuite) -> DBResult<()> {
        let mut c = self.1.lock().unwrap();

        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;

        let cur = self.i_load_check_suite(&tx, &check_suite.id)?;

        assert_eq!(cur.id, check_suite.id);
        assert_eq!(cur.repo, check_suite.repo);
        assert_eq!(cur.install, check_suite.install);
        assert_eq!(cur.github_id, check_suite.github_id);
        assert_eq!(cur.head_sha, check_suite.head_sha);
        assert_eq!(cur.head_branch, check_suite.head_branch);
        assert_eq!(cur.url_key, check_suite.url_key);

        {
            let mut qu = tx.prepare(
                "UPDATE check_suite SET \
                state = ?, plan = ?, plan_sha = ? WHERE id = ?",
            )?;

            let uc = qu.execute(params![
                &check_suite.state.to_string(),
                check_suite
                    .plan
                    .as_ref()
                    .map(serde_json::to_value)
                    .transpose()
                    .or_conv_fail()?,
                &check_suite.plan_sha,
                &check_suite.id.to_string(),
            ])?;
            assert_eq!(uc, 1);
        }

        tx.commit()?;

        Ok(())
    }

    pub fn ensure_check_run(
        &self,
        check_suite: &Ulid,
        name: &str,
        variety: &CheckRunVariety,
    ) -> DBResult<CheckRun> {
        let mut c = self.1.lock().unwrap();

        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;

        let res = {
            /*
             * It is possible that we are creating a new check run instance with
             * a variety different from prior check run instances of the same
             * name within this suite; e.g., if the user re-runs suite creation
             * with an updated plan.
             *
             * First, clear out any check runs that match our name but not our
             * variety.
             */
            let mut qc = tx.prepare(
                "UPDATE check_run SET active = 0 WHERE \
                check_suite = ? AND name = ? AND variety <> ? AND active <> 0",
            )?;

            qc.execute(params![
                &check_suite.to_string(),
                name,
                &variety.to_string(),
            ])?;

            /*
             * Then, determine if there is an active check run for this suite
             * with the expected name and variety.
             */
            let mut q0 = tx.prepare(
                "SELECT * FROM check_run WHERE \
                check_suite = ? AND name = ? AND variety = ? AND active = ?",
            )?;

            let rows = q0.query_map(
                params![
                    &check_suite.to_string(),
                    name,
                    &variety.to_string(),
                    true,
                ],
                row_to_check_run,
            )?;
            let rows =
                rows.into_iter().collect::<std::result::Result<Vec<_>, _>>()?;

            match rows.len() {
                0 => (),
                1 => return Ok(rows[0].clone()),
                n => {
                    conflict!(
                        "found {} active check runs for {}/{}",
                        n,
                        check_suite,
                        name
                    );
                }
            }

            /*
             * Give the check run a unique ID that we control, and that we can
             * use for the "external_id" field and in details URLs.
             */
            let id = Ulid::generate();

            let mut qu = tx.prepare(
                "INSERT INTO check_run (\
                id, check_suite, name, variety, active, flushed) \
                VALUES (?, ?, ?, ?, ?, ?)",
            )?;

            let ic = qu.execute(params![
                &id.to_string(),
                &check_suite.to_string(),
                name,
                &variety.to_string(),
                true,
                false,
            ])?;

            assert_eq!(ic, 1);

            self.i_load_check_run(&tx, &id)?
        };

        tx.commit()?;

        Ok(res)
    }

    pub fn update_check_run(&self, check_run: &CheckRun) -> DBResult<()> {
        let mut c = self.1.lock().unwrap();

        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;

        let cur = self.i_load_check_run(&tx, &check_run.id)?;

        assert_eq!(cur.id, check_run.id);
        assert_eq!(cur.check_suite, check_run.check_suite);
        assert_eq!(cur.name, check_run.name);
        assert_eq!(cur.variety, check_run.variety);

        {
            let mut qu = tx.prepare(
                "UPDATE check_run SET \
                active = ?, flushed = ?, github_id = ?, private = ?,
                content = ?, config = ? \
                WHERE id = ?",
            )?;

            let uc = qu.execute(params![
                &check_run.active,
                &check_run.flushed,
                &check_run.github_id,
                &check_run.private,
                &check_run.content,
                &check_run.config,
                &check_run.id.to_string(),
            ])?;
            assert_eq!(uc, 1);
        }

        tx.commit()?;

        Ok(())
    }
}
