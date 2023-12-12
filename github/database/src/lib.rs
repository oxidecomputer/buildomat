/*
 * Copyright 2023 Oxide Computer Company
 */

use anyhow::Result;
use buildomat_common::*;
use buildomat_database::Sqlite;
/*
 * Re-export these so that buildomat-github-* family crates do not also need a
 * buildomat-database dependency:
 */
pub use buildomat_database::{conflict, DBResult, DatabaseError, FromRow};
use chrono::prelude::*;
use sea_query::{Expr, OnConflict, Order, Query};
use slog::Logger;
use std::collections::HashMap;
use std::path::Path;

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

pub struct Database {
    #[allow(unused)]
    log: Logger,
    sql: Sqlite,
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
            include_str!("../schema.sql"),
            cache_kb,
        )?;

        Ok(Database { log, sql })
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
        self.sql.tx_immediate(|h| {
            /*
             * In the event of a delivery failure, an operator may direct GitHub
             * to replay a prior delivery.  Use the GitHub-provided unique UUID
             * to locate our existing record of a replayed delivery if one
             * exists.
             */
            let old: Option<Delivery> = h.get_row_opt(
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

            let max: Option<DeliverySeq> = h.get_row(
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

            let ic = h.exec_insert(
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

            Ok((seq, true))
        })
    }

    pub fn delivery_load(&self, seq: DeliverySeq) -> DBResult<Delivery> {
        self.sql.tx(|h| h.get_row(Delivery::find(seq)))
    }

    pub fn delivery_ack(&self, seq: DeliverySeq, ack: u64) -> DBResult<()> {
        self.sql.tx(|h| {
            let ic = h.exec_update(
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
        })
    }

    pub fn delivery_unack(&self, seq: DeliverySeq) -> DBResult<()> {
        self.sql.tx(|h| {
            let ic = h.exec_update(
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
        })
    }

    pub fn list_deliveries_unacked(&self) -> DBResult<Vec<Delivery>> {
        self.sql.tx(|h| {
            h.get_rows(
                Query::select()
                    .from(DeliveryDef::Table)
                    .columns(Delivery::columns())
                    .and_where(Expr::col(DeliveryDef::Ack).is_null())
                    .order_by(DeliveryDef::Seq, Order::Asc)
                    .to_owned(),
            )
        })
    }

    /**
     * Get the delivery with the earliest receive time, if one exists.  This is
     * used for archiving.
     */
    pub fn delivery_earliest(&self) -> DBResult<Option<Delivery>> {
        self.sql.tx(|h| {
            h.get_row_opt(
                Query::select()
                    .from(DeliveryDef::Table)
                    .columns(Delivery::columns())
                    .order_by(DeliveryDef::Recvtime, Order::Asc)
                    .limit(1)
                    .to_owned(),
            )
        })
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

        self.sql.tx(|h| {
            h.get_rows(
                Query::select()
                    .from(DeliveryDef::Table)
                    .columns(Delivery::columns())
                    .and_where(Expr::col(DeliveryDef::Recvtime).like(prefix))
                    .order_by(DeliveryDef::Recvtime, Order::Asc)
                    .to_owned(),
            )
        })
    }

    pub fn remove_deliveries(
        &self,
        dels: &[(DeliverySeq, String)],
    ) -> DBResult<()> {
        self.sql.tx(|h| {
            for (seq, uuid) in dels.iter() {
                let dc = h.exec_delete(
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

            Ok(())
        })
    }

    pub fn list_deliveries_recent(
        &self,
        n: usize,
    ) -> DBResult<Vec<DeliverySeq>> {
        self.sql.tx(|h| {
            Ok(h.get_rows(
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
        })
    }

    pub fn list_deliveries(&self) -> DBResult<Vec<DeliverySeq>> {
        self.sql.tx(|h| {
            h.get_rows(
                Query::select()
                    .from(DeliveryDef::Table)
                    .column(DeliveryDef::Seq)
                    .order_by(DeliveryDef::Seq, Order::Asc)
                    .to_owned(),
            )
        })
    }

    pub fn load_check_run(&self, id: CheckRunId) -> DBResult<CheckRun> {
        self.sql.tx(|h| h.get_row(CheckRun::find(id)))
    }

    pub fn load_check_suite(&self, id: CheckSuiteId) -> DBResult<CheckSuite> {
        self.sql.tx(|h| h.get_row(CheckSuite::find(id)))
    }

    pub fn load_check_suite_by_github_id(
        &self,
        repo: i64,
        github_id: i64,
    ) -> DBResult<CheckSuite> {
        self.sql
            .tx(|h| h.get_row(CheckSuite::find_by_github_id(repo, github_id)))
    }

    pub fn load_delivery(&self, seq: DeliverySeq) -> DBResult<Delivery> {
        self.sql.tx(|h| h.get_row(Delivery::find(seq)))
    }

    pub fn load_repository(&self, id: i64) -> DBResult<Repository> {
        self.sql.tx(|h| h.get_row(Repository::find(id)))
    }

    pub fn lookup_repository(
        &self,
        owner: &str,
        name: &str,
    ) -> DBResult<Option<Repository>> {
        self.sql.tx(|h| {
            h.get_row_opt(
                Query::select()
                    .from(RepositoryDef::Table)
                    .columns(Repository::columns())
                    .and_where(Expr::col(RepositoryDef::Owner).eq(owner))
                    .and_where(Expr::col(RepositoryDef::Name).eq(name))
                    .to_owned(),
            )
        })
    }

    pub fn store_repository(
        &self,
        id: i64,
        owner: &str,
        name: &str,
    ) -> DBResult<()> {
        let r =
            Repository { id, owner: owner.to_string(), name: name.to_string() };

        self.sql.tx(|h| {
            h.exec_insert(
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
        })
    }

    pub fn list_repositories(&self) -> DBResult<Vec<Repository>> {
        self.sql.tx(|h| {
            h.get_rows(
                Query::select()
                    .from(RepositoryDef::Table)
                    .columns(Repository::columns())
                    .order_by(RepositoryDef::Id, Order::Asc)
                    .to_owned(),
            )
        })
    }

    pub fn repo_to_install(&self, repo: &Repository) -> DBResult<Install> {
        self.sql.tx(|h| {
            /*
             * First, locate the user that owns the repository.
             */
            let user: User = h.get_row(
                Query::select()
                    .from(UserDef::Table)
                    .columns(User::columns())
                    .and_where(Expr::col(UserDef::Login).eq(&repo.owner))
                    .to_owned(),
            )?;

            /*
             * Check for an installation that belongs to this user.
             */
            h.get_row(
                Query::select()
                    .from(InstallDef::Table)
                    .columns(Install::columns())
                    .and_where(Expr::col(InstallDef::Owner).eq(user.id))
                    .to_owned(),
            )
        })
    }

    pub fn load_install(&self, id: i64) -> DBResult<Install> {
        self.sql.tx(|h| h.get_row(Install::find(id)))
    }

    pub fn store_install(&self, id: i64, owner: i64) -> DBResult<()> {
        let i = Install { id, owner };

        self.sql.tx(|h| {
            h.exec_insert(
                i.insert()
                    .on_conflict(
                        OnConflict::column(InstallDef::Id)
                            .update_column(InstallDef::Owner)
                            .to_owned(),
                    )
                    .to_owned(),
            )?;

            Ok(())
        })
    }

    pub fn load_user(&self, id: i64) -> DBResult<User> {
        self.sql.tx(|h| h.get_row(User::find(id)))
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

        self.sql.tx(|h| {
            h.exec_insert(
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
        })
    }

    pub fn list_check_suites(&self) -> DBResult<Vec<CheckSuite>> {
        self.sql.tx(|h| {
            h.get_rows(
                Query::select()
                    .from(CheckSuiteDef::Table)
                    .columns(CheckSuite::columns())
                    .order_by(CheckSuiteDef::Id, Order::Asc)
                    .to_owned(),
            )
        })
    }

    pub fn list_check_suite_ids(&self) -> DBResult<Vec<CheckSuiteId>> {
        self.sql.tx(|h| {
            h.get_rows(
                Query::select()
                    .from(CheckSuiteDef::Table)
                    .column(CheckSuiteDef::Id)
                    .order_by(CheckSuiteDef::Id, Order::Asc)
                    .to_owned(),
            )
        })
    }

    pub fn list_check_suites_active(&self) -> DBResult<Vec<CheckSuite>> {
        self.sql.tx(|h| {
            h.get_rows(
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
        })
    }

    pub fn list_check_runs_for_suite(
        &self,
        check_suite: CheckSuiteId,
    ) -> DBResult<Vec<CheckRun>> {
        self.sql.tx(|h| {
            h.get_rows(
                Query::select()
                    .from(CheckRunDef::Table)
                    .columns(CheckRun::columns())
                    .and_where(
                        Expr::col(CheckRunDef::CheckSuite).eq(check_suite),
                    )
                    .order_by(CheckRunDef::Id, Order::Asc)
                    .to_owned(),
            )
        })
    }

    pub fn find_check_runs_from_github_id(
        &self,
        repo: i64,
        github_id: i64,
    ) -> DBResult<Vec<(CheckSuite, CheckRun)>> {
        self.sql.tx(|h| {
            /*
             * First, locate any check runs that have this GitHub ID:
             */
            let runs: Vec<CheckRun> = h.get_rows(
                Query::select()
                    .from(CheckRunDef::Table)
                    .columns(CheckRun::columns())
                    .and_where(Expr::col(CheckRunDef::GithubId).eq(github_id))
                    .to_owned(),
            )?;

            Ok(runs
                .into_iter()
                .filter_map(|run| {
                    let res = h.get_row::<CheckSuite>(CheckSuite::find(
                        run.check_suite,
                    ));

                    if let Ok(cs) = res {
                        if cs.repo == repo {
                            return Some((cs, run));
                        }
                    }

                    None
                })
                .collect())
        })
    }

    pub fn load_check_run_for_suite_by_name(
        &self,
        check_suite: CheckSuiteId,
        check_run_name: &str,
    ) -> DBResult<Option<CheckRun>> {
        self.sql.tx(|h| {
            h.get_row_opt(
                Query::select()
                    .from(CheckRunDef::Table)
                    .columns(CheckRun::columns())
                    .and_where(
                        Expr::col(CheckRunDef::CheckSuite).eq(check_suite),
                    )
                    .and_where(Expr::col(CheckRunDef::Active).eq(true))
                    .and_where(Expr::col(CheckRunDef::Name).eq(check_run_name))
                    .to_owned(),
            )
        })
    }

    pub fn ensure_check_suite(
        &self,
        repo: i64,
        install: i64,
        github_id: i64,
        head_sha: &str,
        head_branch: Option<&str>,
    ) -> DBResult<CheckSuite> {
        self.sql.tx_immediate(|h| {
            /*
             * If the record exists in the database already, just return it:
             */
            if let Some(cs) = h.get_row_opt(
                Query::select()
                    .from(CheckSuiteDef::Table)
                    .columns(CheckSuite::columns())
                    .and_where(Expr::col(CheckSuiteDef::Repo).eq(repo))
                    .and_where(Expr::col(CheckSuiteDef::GithubId).eq(github_id))
                    .to_owned(),
            )? {
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

            let ic = h.exec_insert(cs.insert())?;
            assert_eq!(ic, 1);

            Ok(cs)
        })
    }

    pub fn update_check_suite(&self, check_suite: &CheckSuite) -> DBResult<()> {
        self.sql.tx_immediate(|h| {
            let cur: CheckSuite =
                h.get_row(CheckSuite::find(check_suite.id))?;

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

            let uc = h.exec_update(
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

            Ok(())
        })
    }

    pub fn ensure_check_run(
        &self,
        check_suite: CheckSuiteId,
        name: &str,
        variety: CheckRunVariety,
        dependencies: &HashMap<String, JobFileDepend>,
    ) -> DBResult<CheckRun> {
        self.sql.tx_immediate(|h| {
            /*
             * It is possible that we are creating a new check run instance with
             * a variety different from prior check run instances of the same
             * name within this suite; e.g., if the user re-runs suite creation
             * with an updated plan.
             *
             * First, clear out any active check runs that match our name but
             * not our variety.
             */
            h.exec_update(
                Query::update()
                    .table(CheckRunDef::Table)
                    .and_where(
                        Expr::col(CheckRunDef::CheckSuite).eq(check_suite),
                    )
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
            if let Some(existing) = h.get_row_opt(
                Query::select()
                    .from(CheckRunDef::Table)
                    .columns(CheckRun::columns())
                    .and_where(
                        Expr::col(CheckRunDef::CheckSuite).eq(check_suite),
                    )
                    .and_where(Expr::col(CheckRunDef::Name).eq(name))
                    .and_where(Expr::col(CheckRunDef::Variety).eq(variety))
                    .and_where(Expr::col(CheckRunDef::Active).eq(true))
                    .to_owned(),
            )? {
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
                dependencies: Some(JsonValue(serde_json::to_value(
                    dependencies,
                )?)),
            };

            let ic = h.exec_insert(cr.insert())?;
            assert_eq!(ic, 1);

            Ok(cr)
        })
    }

    pub fn update_check_run(&self, check_run: &CheckRun) -> DBResult<()> {
        self.sql.tx_immediate(|h| {
            let cur: CheckRun = h.get_row(CheckRun::find(check_run.id))?;

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

            let uc = h.exec_update(
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

            Ok(())
        })
    }
}
