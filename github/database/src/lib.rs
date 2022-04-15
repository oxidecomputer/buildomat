/*
 * Copyright 2021 Oxide Computer Company
 */

use anyhow::{bail, Result};
use buildomat_common::db::*;
use buildomat_common::*;
use chrono::prelude::*;
use diesel::prelude::*;
use slog::Logger;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;
use thiserror::Error;
#[macro_use]
extern crate diesel;

mod models;
mod schema;

use models::*;

pub mod types {
    pub use super::models::*;
    pub use buildomat_common::db::{IsoDate, JsonValue};
}

#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("conflict: {0}")]
    Conflict(String),
    #[error(transparent)]
    Sql(#[from] diesel::result::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

pub type DBResult<T> = std::result::Result<T, DatabaseError>;

macro_rules! conflict {
    ($msg:expr) => {
        return Err(DatabaseError::Conflict($msg.to_string()))
    };
    ($fmt:expr, $($arg:tt)*) => {
        return Err(DatabaseError::Conflict(format!($fmt, $($arg)*)))
    }
}

struct Inner {
    conn: diesel::SqliteConnection,
}

pub struct Database(Logger, Mutex<Inner>);

impl Database {
    pub fn new<P: AsRef<Path>>(
        log: Logger,
        path: P,
        cache_kb: Option<u32>,
    ) -> Result<Database> {
        let conn = buildomat_common::db::sqlite_setup(
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
    ) -> DBResult<DeliverySeq> {
        use schema::delivery;

        let c = &mut self.1.lock().unwrap().conn;

        c.immediate_transaction(|tx| {
            let max: Option<DeliverySeq> = delivery::dsl::delivery
                .select(diesel::dsl::max(delivery::dsl::seq))
                .get_result(tx)?;

            /*
             * Make the next ID one after the current maximum ID, or start at
             * zero if there are no deliveries thus far.
             */
            let seq =
                max.map(|seq| DeliverySeq(seq.0 + 1)).unwrap_or(DeliverySeq(0));

            let ic = diesel::insert_into(delivery::dsl::delivery)
                .values(Delivery {
                    seq,
                    uuid: uuid.to_string(),
                    event: event.to_string(),
                    headers: Dictionary(headers.clone()),
                    payload: JsonValue(payload.clone()),
                    recvtime: IsoDate(recvtime),
                    ack: None,
                })
                .execute(tx)?;
            assert_eq!(ic, 1);

            Ok(seq)
        })
    }

    pub fn delivery_load(&self, seq: DeliverySeq) -> Result<Delivery> {
        use schema::delivery;

        let c = &mut self.1.lock().unwrap().conn;

        Ok(delivery::dsl::delivery.find(seq).get_result(c)?)
    }

    pub fn delivery_ack(&self, seq: DeliverySeq, ack: u64) -> Result<()> {
        use schema::delivery;

        let c = &mut self.1.lock().unwrap().conn;

        let ic = diesel::update(delivery::dsl::delivery)
            .filter(delivery::dsl::seq.eq(seq))
            .set(delivery::dsl::ack.eq(Some(ack as i64)))
            .execute(c)?;
        assert!(ic < 2);

        if ic == 0 {
            bail!("delivery {} not found", seq);
        }

        Ok(())
    }

    pub fn delivery_unack(&self, seq: DeliverySeq) -> Result<()> {
        use schema::delivery;

        let c = &mut self.1.lock().unwrap().conn;

        let ic = diesel::update(delivery::dsl::delivery)
            .filter(delivery::dsl::seq.eq(seq))
            .set(delivery::dsl::ack.eq(None::<i64>))
            .execute(c)?;
        assert!(ic < 2);

        if ic == 0 {
            bail!("delivery {} not found", seq);
        }

        Ok(())
    }

    pub fn list_deliveries_unacked(&self) -> Result<Vec<Delivery>> {
        use schema::delivery;

        let c = &mut self.1.lock().unwrap().conn;

        Ok(delivery::dsl::delivery
            .filter(delivery::dsl::ack.is_null())
            .order_by(delivery::dsl::seq.asc())
            .get_results(c)?)
    }

    /**
     * Get the delivery with the earliest receive time, if one exists.  This is
     * used for archiving.
     */
    pub fn delivery_earliest(&self) -> Result<Option<Delivery>> {
        use schema::delivery;

        let c = &mut self.1.lock().unwrap().conn;

        Ok(delivery::dsl::delivery
            .order_by(delivery::dsl::recvtime.asc())
            .limit(1)
            .get_result(c)
            .optional()?)
    }

    /**
     * Load all deliveries that occur on the same day as this delivery.
     */
    pub fn same_day_deliveries(&self, d: &Delivery) -> Result<Vec<Delivery>> {
        use schema::delivery;

        /*
         * IsoDate fields should be in RFC3339 format, but some records written
         * in the past had a slightly different format.  Both formats are
         * prefixed with "%Y-%m-%d", though, so we can produce a LIKE clause
         * that will find all the records on the same day as this one.
         */
        let prefix = d.recvtime.0.format("%Y-%m-%d%%").to_string();

        let c = &mut self.1.lock().unwrap().conn;
        Ok(delivery::dsl::delivery
            .filter(delivery::dsl::recvtime.like(&prefix))
            .order_by(delivery::dsl::recvtime.asc())
            .get_results(c)?)
    }

    pub fn remove_deliveries(&self, dels: &[Delivery]) -> Result<()> {
        use schema::delivery;

        let c = &mut self.1.lock().unwrap().conn;

        c.immediate_transaction(|tx| {
            for del in dels.iter() {
                let dc = diesel::delete(delivery::dsl::delivery)
                    .filter(delivery::dsl::seq.eq(del.seq))
                    .filter(delivery::dsl::uuid.eq(&del.uuid))
                    .execute(tx)?;
                if dc != 1 {
                    bail!("failed to delete delivery {}", del.seq);
                }
            }
            Ok(())
        })
    }

    pub fn list_deliveries_recent(&self, n: usize) -> Result<Vec<DeliverySeq>> {
        use schema::delivery;

        let c = &mut self.1.lock().unwrap().conn;

        let res: Vec<(DeliverySeq,)> = delivery::dsl::delivery
            .select((delivery::dsl::seq,))
            .order_by(delivery::dsl::seq.desc())
            .limit(n.try_into().unwrap())
            .get_results(c)?;

        Ok(res.iter().map(|r| r.0).rev().collect())
    }

    pub fn list_deliveries(&self) -> Result<Vec<DeliverySeq>> {
        use schema::delivery;

        let c = &mut self.1.lock().unwrap().conn;

        let res: Vec<(DeliverySeq,)> = delivery::dsl::delivery
            .select((delivery::dsl::seq,))
            .order_by(delivery::dsl::seq.asc())
            .get_results(c)?;

        Ok(res.iter().map(|r| r.0).collect())
    }

    pub fn load_check_run(&self, id: &CheckRunId) -> Result<CheckRun> {
        use schema::check_run;

        let c = &mut self.1.lock().unwrap().conn;

        Ok(check_run::dsl::check_run.find(id).get_result(c)?)
    }

    pub fn load_check_suite(&self, id: &CheckSuiteId) -> Result<CheckSuite> {
        use schema::check_suite;

        let c = &mut self.1.lock().unwrap().conn;

        Ok(check_suite::dsl::check_suite.find(id).get_result(c)?)
    }

    pub fn load_delivery(&self, seq: DeliverySeq) -> Result<Delivery> {
        use schema::delivery;

        let c = &mut self.1.lock().unwrap().conn;

        Ok(delivery::dsl::delivery.find(seq).get_result(c)?)
    }

    pub fn load_repository(&self, id: i64) -> DBResult<Repository> {
        use schema::repository;

        let c = &mut self.1.lock().unwrap().conn;

        Ok(repository::dsl::repository.find(id).get_result(c)?)
    }

    pub fn lookup_repository(
        &self,
        owner: &str,
        name: &str,
    ) -> DBResult<Option<Repository>> {
        use schema::repository;

        let c = &mut self.1.lock().unwrap().conn;

        Ok(repository::dsl::repository
            .filter(repository::dsl::owner.eq(owner))
            .filter(repository::dsl::name.eq(name))
            .get_result(c)
            .optional()?)
    }

    pub fn store_repository(
        &self,
        id: i64,
        owner: &str,
        name: &str,
    ) -> DBResult<()> {
        use schema::repository;

        let c = &mut self.1.lock().unwrap().conn;

        let r =
            Repository { id, owner: owner.to_string(), name: name.to_string() };

        diesel::insert_into(repository::dsl::repository)
            .values(&r)
            .on_conflict(repository::dsl::id)
            .do_update()
            .set((
                repository::dsl::name.eq(&r.name),
                repository::dsl::owner.eq(&r.owner),
            ))
            .execute(c)?;

        Ok(())
    }

    pub fn list_repositories(&self) -> Result<Vec<Repository>> {
        use schema::repository;

        let c = &mut self.1.lock().unwrap().conn;

        Ok(repository::dsl::repository
            .order_by(repository::dsl::id.asc())
            .get_results(c)?)
    }

    pub fn load_install(&self, id: i64) -> DBResult<Install> {
        use schema::install;

        let c = &mut self.1.lock().unwrap().conn;

        Ok(install::dsl::install.find(id).get_result(c)?)
    }

    pub fn store_install(&self, id: i64, owner: i64) -> DBResult<()> {
        use schema::install;

        let c = &mut self.1.lock().unwrap().conn;

        let i = Install { id, owner };

        diesel::insert_into(install::dsl::install)
            .values(&i)
            .on_conflict(install::dsl::id)
            .do_update()
            .set((install::dsl::owner.eq(i.owner),))
            .execute(c)?;

        Ok(())
    }

    pub fn load_user(&self, id: i64) -> DBResult<User> {
        use schema::user;

        let c = &mut self.1.lock().unwrap().conn;

        Ok(user::dsl::user.find(id).get_result(c)?)
    }

    pub fn store_user(
        &self,
        id: i64,
        login: &str,
        usertype: UserType,
        name: Option<&str>,
        email: Option<&str>,
    ) -> DBResult<()> {
        use schema::user;

        let c = &mut self.1.lock().unwrap().conn;

        let u = User {
            id,
            login: login.to_string(),
            usertype,
            name: name.map(|s| s.to_string()),
            email: email.map(|s| s.to_string()),
        };

        diesel::insert_into(user::dsl::user)
            .values(&u)
            .on_conflict(user::dsl::id)
            .do_update()
            .set((
                user::dsl::login.eq(&u.login),
                user::dsl::name.eq(&u.name),
                user::dsl::email.eq(&u.email),
                user::dsl::usertype.eq(u.usertype),
            ))
            .execute(c)?;

        Ok(())
    }

    pub fn list_check_suites(&self) -> Result<Vec<CheckSuite>> {
        use schema::check_suite;

        let c = &mut self.1.lock().unwrap().conn;

        Ok(check_suite::dsl::check_suite
            .order_by(check_suite::dsl::id.asc())
            .get_results(c)?)
    }

    pub fn list_check_suites_active(&self) -> Result<Vec<CheckSuite>> {
        use schema::check_suite;

        let c = &mut self.1.lock().unwrap().conn;

        Ok(check_suite::dsl::check_suite
            .filter(check_suite::dsl::state.is_not(CheckSuiteState::Parked))
            .filter(check_suite::dsl::state.is_not(CheckSuiteState::Complete))
            .filter(check_suite::dsl::state.is_not(CheckSuiteState::Retired))
            .order_by(check_suite::dsl::id.asc())
            .get_results(c)?)
    }

    pub fn list_check_runs_for_suite(
        &self,
        check_suite: &CheckSuiteId,
    ) -> Result<Vec<CheckRun>> {
        use schema::check_run;

        let c = &mut self.1.lock().unwrap().conn;

        Ok(check_run::dsl::check_run
            .filter(check_run::dsl::check_suite.eq(check_suite))
            .order_by(check_run::dsl::id.asc())
            .get_results(c)?)
    }

    pub fn load_check_run_for_suite_by_name(
        &self,
        check_suite: &CheckSuiteId,
        check_run_name: &str,
    ) -> Result<Option<CheckRun>> {
        use schema::check_run;

        let c = &mut self.1.lock().unwrap().conn;

        Ok(check_run::dsl::check_run
            .filter(check_run::dsl::check_suite.eq(check_suite))
            .filter(check_run::dsl::active.eq(true))
            .filter(check_run::dsl::name.eq(check_run_name))
            .get_result(c)
            .optional()?)
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

        c.immediate_transaction(|tx| {
            use schema::check_suite::dsl;

            /*
             * If the record exists in the database already, just return it:
             */
            if let Some(cs) = dsl::check_suite
                .filter(dsl::repo.eq(repo))
                .filter(dsl::github_id.eq(github_id))
                .get_result(tx)
                .optional()?
            {
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

            let ic = diesel::insert_into(dsl::check_suite)
                .values(&cs)
                .execute(tx)?;
            assert_eq!(ic, 1);

            Ok(cs)
        })
    }

    pub fn update_check_suite(&self, check_suite: &CheckSuite) -> DBResult<()> {
        let c = &mut self.1.lock().unwrap().conn;

        c.immediate_transaction(|tx| {
            use schema::check_suite::dsl;

            let cur: CheckSuite =
                dsl::check_suite.find(&check_suite.id).get_result(tx)?;

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

            let uc = diesel::update(dsl::check_suite)
                .filter(dsl::id.eq(&check_suite.id))
                .set((
                    dsl::state.eq(&check_suite.state),
                    dsl::plan.eq(&check_suite.plan),
                    dsl::plan_sha.eq(&check_suite.plan_sha),
                    dsl::pr_by.eq(&check_suite.pr_by),
                    dsl::requested_by.eq(&check_suite.requested_by),
                    dsl::approved_by.eq(&check_suite.approved_by),
                ))
                .execute(tx)?;
            assert_eq!(uc, 1);

            Ok(())
        })
    }

    pub fn ensure_check_run(
        &self,
        check_suite: &CheckSuiteId,
        name: &str,
        variety: &CheckRunVariety,
        dependencies: &HashMap<String, JobFileDepend>,
    ) -> DBResult<CheckRun> {
        let c = &mut self.1.lock().unwrap().conn;

        c.immediate_transaction(|tx| {
            use schema::check_run::dsl;

            /*
             * It is possible that we are creating a new check run instance with
             * a variety different from prior check run instances of the same
             * name within this suite; e.g., if the user re-runs suite creation
             * with an updated plan.
             *
             * First, clear out any active check runs that match our name but
             * not our variety.
             */
            diesel::update(dsl::check_run)
                .filter(dsl::check_suite.eq(check_suite))
                .filter(dsl::name.eq(name))
                .filter(dsl::variety.ne(variety))
                .filter(dsl::active.eq(true))
                .set(dsl::active.eq(false))
                .execute(tx)?;

            /*
             * Then, determine if there is an active check run for this suite
             * with the expected name and variety.
             */
            let rows = dsl::check_run
                .filter(dsl::check_suite.eq(check_suite))
                .filter(dsl::name.eq(name))
                .filter(dsl::variety.eq(variety))
                .filter(dsl::active.eq(true))
                .get_results::<CheckRun>(tx)?;

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
            let cr = CheckRun {
                id: CheckRunId::generate(),
                check_suite: *check_suite,
                name: name.to_string(),
                variety: *variety,
                content: None,
                config: None,
                private: None,
                active: true,
                flushed: false,
                github_id: None,
                dependencies: Some(JsonValue(serde_json::to_value(
                    dependencies,
                )?)),
            };

            let ic =
                diesel::insert_into(dsl::check_run).values(&cr).execute(tx)?;
            assert_eq!(ic, 1);

            Ok(cr)
        })
    }

    pub fn update_check_run(&self, check_run: &CheckRun) -> DBResult<()> {
        let c = &mut self.1.lock().unwrap().conn;

        c.immediate_transaction(|tx| {
            use schema::check_run::dsl;

            let cur: CheckRun =
                dsl::check_run.find(&check_run.id).get_result(tx)?;

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

            let uc = diesel::update(dsl::check_run)
                .filter(dsl::id.eq(&check_run.id))
                .set((
                    dsl::active.eq(check_run.active),
                    dsl::flushed.eq(check_run.flushed),
                    dsl::github_id.eq(&check_run.github_id),
                    dsl::private.eq(&check_run.private),
                    dsl::content.eq(&check_run.content),
                    dsl::config.eq(&check_run.config),
                ))
                .execute(tx)?;
            assert_eq!(uc, 1);

            Ok(())
        })
    }
}
