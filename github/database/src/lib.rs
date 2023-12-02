/*
 * Copyright 2023 Oxide Computer Company
 */

use anyhow::Result;
use buildomat_common::*;
use buildomat_database::rusqlite;
use chrono::prelude::*;
use rusqlite::Transaction;
use sea_query::{
    DeleteStatement, Expr, InsertStatement, OnConflict, Order, Query,
    SelectStatement, SqliteQueryBuilder, UpdateStatement,
};
use sea_query_rusqlite::{RusqliteBinder, RusqliteValues};
use slog::{debug, Logger};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;
use thiserror::Error;

mod tables;

mod itypes {
    use buildomat_database::{
        rusqlite, sqlite_integer_new_type, sqlite_ulid_new_type,
    };

    sqlite_integer_new_type!(DeliverySeq, usize, BigUnsigned);

    sqlite_ulid_new_type!(CheckSuiteId);
    sqlite_ulid_new_type!(CheckRunId);

    pub use buildomat_database::{Dictionary, IsoDate, JsonValue};
}

pub mod types {
    pub use crate::itypes::*;
    pub use crate::tables::*;
    pub use buildomat_database::{IsoDate, JsonValue};
}

use itypes::*;
use types::*;

#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("conflict: {0}")]
    Conflict(String),
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

impl DatabaseError {
    pub fn is_locked_database(&self) -> bool {
        match self {
            DatabaseError::Sql(e) => {
                e.to_string().contains("database is locked")
            }
            _ => false,
        }
    }
}

pub type DBResult<T> = std::result::Result<T, DatabaseError>;

macro_rules! conflict {
    ($msg:expr) => {
        return Err(DatabaseError::Conflict(format!($msg)))
    };
    ($fmt:expr, $($arg:tt)*) => {
        return Err(DatabaseError::Conflict(format!($fmt, $($arg)*)))
    }
}

struct Inner {
    conn: rusqlite::Connection,
}

pub struct Database(Logger, Mutex<Inner>);

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

    /**
     * Get the next sequence number in the webhook delivery table.  Run in a
     * transaction with the insertion.
     */
    pub fn store_delivery(
        &self,
        uuid: &str,
        event: &str,
        headers: &HashMap<String, String>,
        payload: &serde_json::Value,
        recvtime: DateTime<Utc>,
    ) -> DBResult<(DeliverySeq, bool)> {
        let c = &mut self.1.lock().unwrap().conn;
        let mut tx = c.transaction_with_behavior(
            rusqlite::TransactionBehavior::Immediate,
        )?;

        /*
         * In the event of a delivery failure, an operator may direct GitHub
         * to replay a prior delivery.  Use the GitHub-provided unique UUID
         * to locate our existing record of a replayed delivery if one
         * exists.
         */
        let old: Option<Delivery> = self.tx_get_row_opt(
            &mut tx,
            Query::select()
                .from(DeliveryDef::Table)
                .columns(Delivery::columns())
                .and_where(Expr::col(DeliveryDef::Uuid).eq(uuid))
                .to_owned(),
        )?;

        if let Some(old) = old {
            let Delivery {
                seq: oldseq,
                uuid: olduuid,
                event: oldevent,
                payload: oldpayload,

                /*
                 * Ignore fields that are specific to our internal handling
                 * of the incoming request and any subsequent processing:
                 */
                headers: _,
                recvtime: _,
                ack: _,
            } = old;

            assert_eq!(&olduuid, uuid);
            if event != oldevent || payload != &oldpayload.0 {
                conflict!(
                    "delivery {oldseq} exists for {uuid} with different \
                    payload"
                );
            }

            return Ok((oldseq, false));
        }

        let max: Option<DeliverySeq> = self.tx_get_row(
            &mut tx,
            Query::select()
                .from(DeliveryDef::Table)
                .expr(Expr::col(DeliveryDef::Seq).max())
                .to_owned(),
        )?;

        /*
         * Make the next ID one after the current maximum ID, or start at
         * zero if there are no deliveries thus far.
         */
        let seq =
            max.map(|seq| DeliverySeq(seq.0 + 1)).unwrap_or(DeliverySeq(0));

        let ic = self.tx_exec_insert(
            &mut tx,
            Delivery {
                seq,
                uuid: uuid.to_string(),
                event: event.to_string(),
                headers: Dictionary(headers.clone()),
                payload: JsonValue(payload.clone()),
                recvtime: IsoDate(recvtime),
                ack: None,
            }
            .insert(),
        )?;
        assert_eq!(ic, 1);

        tx.commit()?;
        Ok((seq, true))
    }

    pub fn delivery_load(&self, seq: DeliverySeq) -> DBResult<Delivery> {
        self.get_row(Delivery::find(seq))
    }

    pub fn delivery_ack(&self, seq: DeliverySeq, ack: u64) -> DBResult<()> {
        let ic = self.exec_update(
            Query::update()
                .table(DeliveryDef::Table)
                .and_where(Expr::col(DeliveryDef::Seq).eq(seq))
                .value(DeliveryDef::Ack, ack)
                .to_owned(),
        )?;
        assert!(ic < 2);

        if ic == 0 {
            conflict!("delivery {seq} not found");
        }

        Ok(())
    }

    pub fn delivery_unack(&self, seq: DeliverySeq) -> DBResult<()> {
        let ic = self.exec_update(
            Query::update()
                .table(DeliveryDef::Table)
                .and_where(Expr::col(DeliveryDef::Seq).eq(seq))
                .value(DeliveryDef::Ack, None::<DeliverySeq>)
                .to_owned(),
        )?;
        assert!(ic < 2);

        if ic == 0 {
            conflict!("delivery {seq} not found");
        }

        Ok(())
    }

    pub fn list_deliveries_unacked(&self) -> DBResult<Vec<Delivery>> {
        self.get_rows(
            Query::select()
                .from(DeliveryDef::Table)
                .columns(Delivery::columns())
                .and_where(Expr::col(DeliveryDef::Ack).is_null())
                .order_by(DeliveryDef::Seq, Order::Asc)
                .to_owned(),
        )
    }

    /**
     * Get the delivery with the earliest receive time, if one exists.  This is
     * used for archiving.
     */
    pub fn delivery_earliest(&self) -> DBResult<Option<Delivery>> {
        self.get_row_opt(
            Query::select()
                .from(DeliveryDef::Table)
                .columns(Delivery::columns())
                .order_by(DeliveryDef::Recvtime, Order::Asc)
                .limit(1)
                .to_owned(),
        )
    }

    /**
     * Load all deliveries that occur on the same day as this delivery.
     */
    pub fn same_day_deliveries(&self, d: &Delivery) -> DBResult<Vec<Delivery>> {
        /*
         * IsoDate fields should be in RFC3339 format, but some records written
         * in the past had a slightly different format.  Both formats are
         * prefixed with "%Y-%m-%d", though, so we can produce a LIKE clause
         * that will find all the records on the same day as this one.
         */
        let prefix = d.recvtime.0.format("%Y-%m-%d%%").to_string();

        self.get_rows(
            Query::select()
                .from(DeliveryDef::Table)
                .columns(Delivery::columns())
                .and_where(Expr::col(DeliveryDef::Recvtime).like(prefix))
                .order_by(DeliveryDef::Recvtime, Order::Asc)
                .to_owned(),
        )
    }

    pub fn remove_deliveries(
        &self,
        dels: &[(DeliverySeq, String)],
    ) -> DBResult<()> {
        let c = &mut self.1.lock().unwrap().conn;
        let mut tx = c.transaction_with_behavior(
            rusqlite::TransactionBehavior::Immediate,
        )?;

        for (seq, uuid) in dels.iter() {
            let dc = self.tx_exec_delete(
                &mut tx,
                Query::delete()
                    .from_table(DeliveryDef::Table)
                    .and_where(Expr::col(DeliveryDef::Seq).eq(*seq))
                    .and_where(Expr::col(DeliveryDef::Uuid).eq(uuid))
                    .to_owned(),
            )?;
            if dc != 1 {
                conflict!("failed to delete delivery {}", seq);
            }
        }

        tx.commit()?;
        Ok(())
    }

    pub fn list_deliveries_recent(
        &self,
        n: usize,
    ) -> DBResult<Vec<DeliverySeq>> {
        Ok(self
            .get_rows(
                Query::select()
                    .from(DeliveryDef::Table)
                    .column(DeliveryDef::Seq)
                    .order_by(DeliveryDef::Seq, Order::Desc)
                    .limit(n.try_into().unwrap())
                    .to_owned(),
            )?
            .into_iter()
            .rev()
            .collect())
    }

    pub fn list_deliveries(&self) -> DBResult<Vec<DeliverySeq>> {
        self.get_rows(
            Query::select()
                .from(DeliveryDef::Table)
                .column(DeliveryDef::Seq)
                .order_by(DeliveryDef::Seq, Order::Asc)
                .to_owned(),
        )
    }

    pub fn load_check_run(&self, id: CheckRunId) -> DBResult<CheckRun> {
        self.get_row(CheckRun::find(id))
    }

    pub fn load_check_suite(&self, id: CheckSuiteId) -> DBResult<CheckSuite> {
        self.get_row(CheckSuite::find(id))
    }

    pub fn load_check_suite_by_github_id(
        &self,
        repo: i64,
        github_id: i64,
    ) -> DBResult<CheckSuite> {
        self.get_row(CheckSuite::find_by_github_id(repo, github_id))
    }

    pub fn load_delivery(&self, seq: DeliverySeq) -> DBResult<Delivery> {
        self.get_row(Delivery::find(seq))
    }

    pub fn load_repository(&self, id: i64) -> DBResult<Repository> {
        self.get_row(Repository::find(id))
    }

    pub fn lookup_repository(
        &self,
        owner: &str,
        name: &str,
    ) -> DBResult<Option<Repository>> {
        self.get_row_opt(
            Query::select()
                .from(RepositoryDef::Table)
                .columns(Repository::columns())
                .and_where(Expr::col(RepositoryDef::Owner).eq(owner))
                .and_where(Expr::col(RepositoryDef::Name).eq(name))
                .to_owned(),
        )
    }

    pub fn store_repository(
        &self,
        id: i64,
        owner: &str,
        name: &str,
    ) -> DBResult<()> {
        let r =
            Repository { id, owner: owner.to_string(), name: name.to_string() };

        self.exec_insert(
            r.insert()
                .on_conflict(
                    OnConflict::column(RepositoryDef::Id)
                        .update_column(RepositoryDef::Name)
                        .update_column(RepositoryDef::Owner)
                        .to_owned(),
                )
                .to_owned(),
        )?;

        Ok(())
    }

    pub fn list_repositories(&self) -> DBResult<Vec<Repository>> {
        self.get_rows(
            Query::select()
                .from(RepositoryDef::Table)
                .columns(Repository::columns())
                .order_by(RepositoryDef::Id, Order::Asc)
                .to_owned(),
        )
    }

    pub fn repo_to_install(&self, repo: &Repository) -> DBResult<Install> {
        /*
         * First, locate the user that owns the repository.
         */
        let user: User = self.get_row(
            Query::select()
                .from(UserDef::Table)
                .columns(User::columns())
                .and_where(Expr::col(UserDef::Login).eq(&repo.owner))
                .to_owned(),
        )?;

        /*
         * Check for an installation that belongs to this user.
         */
        self.get_row(
            Query::select()
                .from(InstallDef::Table)
                .columns(Install::columns())
                .and_where(Expr::col(InstallDef::Owner).eq(user.id))
                .to_owned(),
        )
    }

    pub fn load_install(&self, id: i64) -> DBResult<Install> {
        self.get_row(Install::find(id))
    }

    pub fn store_install(&self, id: i64, owner: i64) -> DBResult<()> {
        let i = Install { id, owner };

        self.exec_insert(
            i.insert()
                .on_conflict(
                    OnConflict::column(InstallDef::Id)
                        .update_column(InstallDef::Owner)
                        .to_owned(),
                )
                .to_owned(),
        )?;

        Ok(())
    }

    pub fn load_user(&self, id: i64) -> DBResult<User> {
        self.get_row(User::find(id))
    }

    pub fn store_user(
        &self,
        id: i64,
        login: &str,
        usertype: UserType,
        name: Option<&str>,
        email: Option<&str>,
    ) -> DBResult<()> {
        let u = User {
            id,
            login: login.to_string(),
            usertype,
            name: name.map(|s| s.to_string()),
            email: email.map(|s| s.to_string()),
        };

        self.exec_insert(
            u.insert()
                .on_conflict(
                    OnConflict::column(UserDef::Id)
                        .update_column(UserDef::Login)
                        .update_column(UserDef::Name)
                        .update_column(UserDef::Email)
                        .update_column(UserDef::Usertype)
                        .to_owned(),
                )
                .to_owned(),
        )?;

        Ok(())
    }

    pub fn list_check_suites(&self) -> DBResult<Vec<CheckSuite>> {
        self.get_rows(
            Query::select()
                .from(CheckSuiteDef::Table)
                .columns(CheckSuite::columns())
                .order_by(CheckSuiteDef::Id, Order::Asc)
                .to_owned(),
        )
    }

    pub fn list_check_suite_ids(&self) -> DBResult<Vec<CheckSuiteId>> {
        self.get_rows(
            Query::select()
                .from(CheckSuiteDef::Table)
                .column(CheckSuiteDef::Id)
                .order_by(CheckSuiteDef::Id, Order::Asc)
                .to_owned(),
        )
    }

    pub fn list_check_suites_active(&self) -> DBResult<Vec<CheckSuite>> {
        self.get_rows(
            Query::select()
                .from(CheckSuiteDef::Table)
                .columns(CheckSuite::columns())
                .and_where(
                    Expr::col(CheckSuiteDef::State)
                        .is_not(CheckSuiteState::Parked),
                )
                .and_where(
                    Expr::col(CheckSuiteDef::State)
                        .is_not(CheckSuiteState::Complete),
                )
                .and_where(
                    Expr::col(CheckSuiteDef::State)
                        .is_not(CheckSuiteState::Retired),
                )
                .order_by(CheckSuiteDef::Id, Order::Asc)
                .to_owned(),
        )
    }

    pub fn list_check_runs_for_suite(
        &self,
        check_suite: CheckSuiteId,
    ) -> DBResult<Vec<CheckRun>> {
        self.get_rows(
            Query::select()
                .from(CheckRunDef::Table)
                .columns(CheckRun::columns())
                .and_where(Expr::col(CheckRunDef::CheckSuite).eq(check_suite))
                .order_by(CheckRunDef::Id, Order::Asc)
                .to_owned(),
        )
    }

    pub fn find_check_runs_from_github_id(
        &self,
        repo: i64,
        github_id: i64,
    ) -> DBResult<Vec<(CheckSuite, CheckRun)>> {
        /*
         * First, locate any check runs that have this GitHub ID:
         */
        let runs: Vec<CheckRun> = self.get_rows(
            Query::select()
                .from(CheckRunDef::Table)
                .columns(CheckRun::columns())
                .and_where(Expr::col(CheckRunDef::GithubId).eq(github_id))
                .to_owned(),
        )?;

        Ok(runs
            .into_iter()
            .filter_map(|run| {
                let res = self
                    .get_row::<CheckSuite>(CheckSuite::find(run.check_suite));

                if let Ok(cs) = res {
                    if cs.repo == repo {
                        return Some((cs, run));
                    }
                }

                None
            })
            .collect())
    }

    pub fn load_check_run_for_suite_by_name(
        &self,
        check_suite: CheckSuiteId,
        check_run_name: &str,
    ) -> DBResult<Option<CheckRun>> {
        self.get_row_opt(
            Query::select()
                .from(CheckRunDef::Table)
                .columns(CheckRun::columns())
                .and_where(Expr::col(CheckRunDef::CheckSuite).eq(check_suite))
                .and_where(Expr::col(CheckRunDef::Active).eq(true))
                .and_where(Expr::col(CheckRunDef::Name).eq(check_run_name))
                .to_owned(),
        )
    }

    pub fn ensure_check_suite(
        &self,
        repo: i64,
        install: i64,
        github_id: i64,
        head_sha: &str,
        head_branch: Option<&str>,
    ) -> DBResult<CheckSuite> {
        let c = &mut self.1.lock().unwrap().conn;
        let mut tx = c.transaction_with_behavior(
            rusqlite::TransactionBehavior::Immediate,
        )?;

        /*
         * If the record exists in the database already, just return it:
         */
        if let Some(cs) = self.tx_get_row_opt(
            &mut tx,
            Query::select()
                .from(CheckSuiteDef::Table)
                .columns(CheckSuite::columns())
                .and_where(Expr::col(CheckSuiteDef::Repo).eq(repo))
                .and_where(Expr::col(CheckSuiteDef::GithubId).eq(github_id))
                .to_owned(),
        )? {
            tx.commit()?;
            return Ok(cs);
        }

        /*
         * Give the check suite a unique ID that we control, and a URL key
         * that will be hard to guess.
         */
        let cs = CheckSuite {
            id: CheckSuiteId::generate(),
            url_key: genkey(48),
            repo,
            install,
            github_id,
            head_sha: head_sha.to_string(),
            head_branch: head_branch.map(|s| s.to_string()),
            state: CheckSuiteState::Created,
            plan: None,
            plan_sha: None,
            pr_by: None,
            requested_by: None,
            approved_by: None,
        };

        let ic = self.tx_exec_insert(&mut tx, cs.insert())?;
        assert_eq!(ic, 1);

        tx.commit()?;
        Ok(cs)
    }

    pub fn update_check_suite(&self, check_suite: &CheckSuite) -> DBResult<()> {
        let c = &mut self.1.lock().unwrap().conn;
        let mut tx = c.transaction_with_behavior(
            rusqlite::TransactionBehavior::Immediate,
        )?;

        let cur: CheckSuite =
            self.tx_get_row(&mut tx, CheckSuite::find(check_suite.id))?;

        /*
         * With the current data model, it is an error to modify any of
         * these fields:
         */
        assert_eq!(cur.id, check_suite.id);
        assert_eq!(cur.repo, check_suite.repo);
        assert_eq!(cur.install, check_suite.install);
        assert_eq!(cur.github_id, check_suite.github_id);
        assert_eq!(cur.head_sha, check_suite.head_sha);
        assert_eq!(cur.head_branch, check_suite.head_branch);
        assert_eq!(cur.url_key, check_suite.url_key);

        let uc = self.tx_exec_update(
            &mut tx,
            Query::update()
                .table(CheckSuiteDef::Table)
                .and_where(Expr::col(CheckSuiteDef::Id).eq(check_suite.id))
                .value(CheckSuiteDef::State, check_suite.state)
                .value(CheckSuiteDef::Plan, check_suite.plan.clone())
                .value(CheckSuiteDef::PlanSha, check_suite.plan_sha.clone())
                .value(CheckSuiteDef::PrBy, check_suite.pr_by)
                .value(CheckSuiteDef::RequestedBy, check_suite.requested_by)
                .value(CheckSuiteDef::ApprovedBy, check_suite.approved_by)
                .to_owned(),
        )?;
        assert_eq!(uc, 1);

        tx.commit()?;
        Ok(())
    }

    pub fn ensure_check_run(
        &self,
        check_suite: CheckSuiteId,
        name: &str,
        variety: CheckRunVariety,
        dependencies: &HashMap<String, JobFileDepend>,
    ) -> DBResult<CheckRun> {
        let c = &mut self.1.lock().unwrap().conn;
        let mut tx = c.transaction_with_behavior(
            rusqlite::TransactionBehavior::Immediate,
        )?;

        /*
         * It is possible that we are creating a new check run instance with
         * a variety different from prior check run instances of the same
         * name within this suite; e.g., if the user re-runs suite creation
         * with an updated plan.
         *
         * First, clear out any active check runs that match our name but
         * not our variety.
         */
        self.tx_exec_update(
            &mut tx,
            Query::update()
                .table(CheckRunDef::Table)
                .and_where(Expr::col(CheckRunDef::CheckSuite).eq(check_suite))
                .and_where(Expr::col(CheckRunDef::Name).eq(name))
                .and_where(Expr::col(CheckRunDef::Variety).ne(variety))
                .and_where(Expr::col(CheckRunDef::Active).eq(true))
                .value(CheckRunDef::Active, false)
                .to_owned(),
        )?;

        /*
         * Then, determine if there is an active check run for this suite
         * with the expected name and variety.
         */
        if let Some(existing) = self.tx_get_row_opt(
            &mut tx,
            Query::select()
                .from(CheckRunDef::Table)
                .columns(CheckRun::columns())
                .and_where(Expr::col(CheckRunDef::CheckSuite).eq(check_suite))
                .and_where(Expr::col(CheckRunDef::Name).eq(name))
                .and_where(Expr::col(CheckRunDef::Variety).eq(variety))
                .and_where(Expr::col(CheckRunDef::Active).eq(true))
                .to_owned(),
        )? {
            tx.commit()?;
            return Ok(existing);
        }

        /*
         * Give the check run a unique ID that we control, and that we can
         * use for the "external_id" field and in details URLs.
         */
        let cr = CheckRun {
            id: CheckRunId::generate(),
            check_suite,
            name: name.to_string(),
            variety,
            content: None,
            config: None,
            private: None,
            active: true,
            flushed: false,
            github_id: None,
            dependencies: Some(JsonValue(serde_json::to_value(dependencies)?)),
        };

        let ic = self.tx_exec_insert(&mut tx, cr.insert())?;
        assert_eq!(ic, 1);

        tx.commit()?;
        Ok(cr)
    }

    pub fn update_check_run(&self, check_run: &CheckRun) -> DBResult<()> {
        let c = &mut self.1.lock().unwrap().conn;
        let mut tx = c.transaction_with_behavior(
            rusqlite::TransactionBehavior::Immediate,
        )?;

        let cur: CheckRun =
            self.tx_get_row(&mut tx, CheckRun::find(check_run.id))?;

        /*
         * With the current data model, it is an error to modify any of
         * these fields:
         */
        assert_eq!(cur.id, check_run.id);
        assert_eq!(cur.check_suite, check_run.check_suite);
        assert_eq!(cur.name, check_run.name);
        assert_eq!(cur.variety, check_run.variety);
        assert_eq!(
            cur.dependencies.as_ref().map(|v| &v.0),
            check_run.dependencies.as_ref().map(|v| &v.0)
        );

        let uc = self.tx_exec_update(
            &mut tx,
            Query::update()
                .table(CheckRunDef::Table)
                .and_where(Expr::col(CheckRunDef::Id).eq(check_run.id))
                .value(CheckRunDef::Active, check_run.active)
                .value(CheckRunDef::Flushed, check_run.flushed)
                .value(CheckRunDef::GithubId, check_run.github_id)
                .value(CheckRunDef::Private, check_run.private.clone())
                .value(CheckRunDef::Content, check_run.content.clone())
                .value(CheckRunDef::Config, check_run.config.clone())
                .to_owned(),
        )?;
        assert_eq!(uc, 1);

        tx.commit()?;
        Ok(())
    }

    /*
     * Helper routines for database access:
     */

    fn tx_exec_delete(
        &self,
        tx: &mut Transaction,
        d: DeleteStatement,
    ) -> DBResult<usize> {
        let (q, v) = d.build_rusqlite(SqliteQueryBuilder);
        self.tx_exec(tx, q, v)
    }

    fn tx_exec_update(
        &self,
        tx: &mut Transaction,
        u: UpdateStatement,
    ) -> DBResult<usize> {
        let (q, v) = u.build_rusqlite(SqliteQueryBuilder);
        self.tx_exec(tx, q, v)
    }

    fn tx_exec_insert(
        &self,
        tx: &mut Transaction,
        i: InsertStatement,
    ) -> DBResult<usize> {
        let (q, v) = i.build_rusqlite(SqliteQueryBuilder);
        self.tx_exec(tx, q, v)
    }

    fn tx_exec(
        &self,
        tx: &mut Transaction,
        q: String,
        v: RusqliteValues,
    ) -> DBResult<usize> {
        let mut s = tx.prepare(&q)?;
        let out = s.execute(&*v.as_params())?;

        Ok(out)
    }

    #[allow(unused)]
    fn exec_delete(&self, d: DeleteStatement) -> DBResult<usize> {
        let (q, v) = d.build_rusqlite(SqliteQueryBuilder);
        debug!(self.0, "query: {q}"; "sql" => true);
        self.exec(q, v)
    }

    fn exec_update(&self, u: UpdateStatement) -> DBResult<usize> {
        let (q, v) = u.build_rusqlite(SqliteQueryBuilder);
        debug!(self.0, "query: {q}"; "sql" => true);
        self.exec(q, v)
    }

    fn exec_insert(&self, i: InsertStatement) -> DBResult<usize> {
        let (q, v) = i.build_rusqlite(SqliteQueryBuilder);
        debug!(self.0, "query: {q}"; "sql" => true);
        self.exec(q, v)
    }

    fn exec(&self, q: String, v: RusqliteValues) -> DBResult<usize> {
        let c = &mut self.1.lock().unwrap().conn;

        let out = c.prepare(&q)?.execute(&*v.as_params())?;

        Ok(out)
    }

    #[allow(unused)]
    fn get_strings(&self, s: SelectStatement) -> DBResult<Vec<String>> {
        let (q, v) = s.build_rusqlite(SqliteQueryBuilder);
        debug!(self.0, "query: {q}"; "sql" => true);
        let c = &mut self.1.lock().unwrap().conn;

        let mut s = c.prepare(&q)?;
        let out = s.query_map(&*v.as_params(), |row| row.get(0))?;

        Ok(out.collect::<rusqlite::Result<_>>()?)
    }

    fn get_rows<T: FromRow>(&self, s: SelectStatement) -> DBResult<Vec<T>> {
        let (q, v) = s.build_rusqlite(SqliteQueryBuilder);
        debug!(self.0, "query: {q}"; "sql" => true);
        let c = &mut self.1.lock().unwrap().conn;

        let mut s = c.prepare(&q)?;
        let out = s.query_map(&*v.as_params(), T::from_row)?;

        Ok(out.collect::<rusqlite::Result<_>>()?)
    }

    fn get_row<T: FromRow>(&self, s: SelectStatement) -> DBResult<T> {
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

    fn get_row_opt<T: FromRow>(
        &self,
        s: SelectStatement,
    ) -> DBResult<Option<T>> {
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

    fn tx_get_row_opt<T: FromRow>(
        &self,
        tx: &mut Transaction,
        s: SelectStatement,
    ) -> DBResult<Option<T>> {
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

    #[allow(unused)]
    fn tx_get_strings(
        &self,
        tx: &mut Transaction,
        s: SelectStatement,
    ) -> DBResult<Vec<String>> {
        let (q, v) = s.build_rusqlite(SqliteQueryBuilder);
        debug!(self.0, "query: {q}"; "sql" => true);
        let mut s = tx.prepare(&q)?;
        let out = s.query_map(&*v.as_params(), |row| row.get(0))?;

        Ok(out.collect::<rusqlite::Result<_>>()?)
    }

    #[allow(unused)]
    fn tx_get_rows<T: FromRow>(
        &self,
        tx: &mut Transaction,
        s: SelectStatement,
    ) -> DBResult<Vec<T>> {
        let (q, v) = s.build_rusqlite(SqliteQueryBuilder);
        debug!(self.0, "query: {q}"; "sql" => true);
        let mut s = tx.prepare(&q)?;
        let out = s.query_map(&*v.as_params(), T::from_row)?;

        Ok(out.collect::<rusqlite::Result<_>>()?)
    }

    fn tx_get_row<T: FromRow>(
        &self,
        tx: &mut Transaction,
        s: SelectStatement,
    ) -> DBResult<T> {
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
