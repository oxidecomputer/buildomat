/*
 * Copyright 2023 Oxide Computer Company
 */

use std::path::Path;
use std::sync::Mutex;

use anyhow::Result;
use buildomat_common::*;
use buildomat_database::sqlite::rusqlite;
use chrono::prelude::*;
use rusqlite::Transaction;
#[allow(unused_imports)]
use rusty_ulid::Ulid;
use sea_query::{
    DeleteStatement, Expr, InsertStatement, Order, Query, SelectStatement,
    SqliteQueryBuilder, UpdateStatement,
};
use sea_query_rusqlite::{RusqliteBinder, RusqliteValues};
#[allow(unused_imports)]
use slog::{debug, error, info, warn, Logger};
use thiserror::Error;

mod tables;

mod types {
    use buildomat_database::sqlite::rusqlite;
    use buildomat_database::sqlite_integer_new_type;

    sqlite_integer_new_type!(InstanceSeq, u64, BigUnsigned);
    sqlite_integer_new_type!(EventSeq, u64, BigUnsigned);

    pub use buildomat_database::sqlite::IsoDate;
}

pub use tables::*;
pub use types::*;

#[derive(Error, Debug)]
pub enum OperationError {
    #[error("conflict: {0}")]
    Conflict(String),
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
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

impl Database {
    pub fn new<P: AsRef<Path>>(
        log: Logger,
        path: P,
        cache_kb: Option<u32>,
    ) -> Result<Database> {
        let conn = buildomat_database::sqlite::sqlite_setup(
            &log,
            path,
            include_str!("../../schema.sql"),
            cache_kb,
        )?;

        Ok(Database(log, Mutex::new(Inner { conn })))
    }

    pub fn i_next_seq_for_host(
        &self,
        nodename: &str,
        tx: &mut Transaction,
    ) -> OResult<InstanceSeq> {
        let max: Option<InstanceSeq> = self.tx_get_row(
            tx,
            Query::select()
                .from(InstanceDef::Table)
                .expr(Expr::col(InstanceDef::Seq).max())
                .and_where(Expr::col(InstanceDef::Nodename).eq(nodename))
                .to_owned(),
        )?;

        Ok(InstanceSeq(max.map(|v| v.0).unwrap_or(0).checked_add(1).unwrap()))
    }

    pub fn i_next_seq_for_instance(
        &self,
        i: &Instance,
        tx: &mut Transaction,
    ) -> OResult<EventSeq> {
        let max: Option<EventSeq> = self.tx_get_row(
            tx,
            Query::select()
                .from(InstanceEventDef::Table)
                .expr(Expr::col(InstanceEventDef::Seq).max())
                .and_where(
                    Expr::col(InstanceEventDef::Nodename).eq(&i.nodename),
                )
                .and_where(Expr::col(InstanceEventDef::Instance).eq(i.seq))
                .to_owned(),
        )?;

        Ok(EventSeq(max.map(|v| v.0).unwrap_or(0).checked_add(1).unwrap()))
    }

    pub fn instance_for_host(
        &self,
        nodename: &str,
    ) -> OResult<Option<Instance>> {
        self.get_row_opt(Instance::find_for_host(nodename))
    }

    pub fn instance_get(
        &self,
        nodename: &str,
        seq: InstanceSeq,
    ) -> OResult<Option<Instance>> {
        self.get_row_opt(Instance::find(nodename, seq))
    }

    /**
     * Create an instance in the prebooted state.
     */
    pub fn instance_create(
        &self,
        nodename: &str,
        target: &str,
        worker: &str,
        bootstrap: &str,
    ) -> OResult<Instance> {
        let key = genkey(32);

        let c = &mut self.1.lock().unwrap().conn;
        let mut tx = c.transaction_with_behavior(
            rusqlite::TransactionBehavior::Immediate,
        )?;

        let existing = self.tx_get_row_opt::<Instance>(
            &mut tx,
            Instance::find_for_host(nodename),
        )?;
        if existing.is_some() {
            conflict!("host {nodename} already has an active instance");
        }

        let i = Instance {
            nodename: nodename.to_string(),
            seq: self.i_next_seq_for_host(nodename, &mut tx)?,
            worker: worker.to_string(),
            target: target.to_string(),
            state: InstanceState::Preboot,
            key,
            bootstrap: bootstrap.to_string(),
            flushed: false,
        };

        let ic = self.tx_exec_insert(&mut tx, i.insert())?;
        assert_eq!(ic, 1);

        tx.commit()?;
        Ok(i)
    }

    pub fn active_instances(&self) -> OResult<Vec<Instance>> {
        self.get_rows(Instance::find_active())
    }

    pub fn instance_destroy(&self, i: &Instance) -> OResult<()> {
        let c = &mut self.1.lock().unwrap().conn;
        let mut tx = c.transaction_with_behavior(
            rusqlite::TransactionBehavior::Immediate,
        )?;

        /*
         * Fetch the current instance state:
         */
        let i: Instance =
            self.tx_get_row(&mut tx, Instance::find(&i.nodename, i.seq))?;

        match i.state {
            InstanceState::Preboot | InstanceState::Booted => {
                let uc = self.tx_exec_update(
                    &mut tx,
                    Query::update()
                        .table(InstanceDef::Table)
                        .and_where(
                            Expr::col(InstanceDef::Nodename).eq(i.nodename),
                        )
                        .and_where(Expr::col(InstanceDef::Seq).eq(i.seq))
                        .value(InstanceDef::State, InstanceState::Destroying)
                        .to_owned(),
                )?;
                assert_eq!(uc, 1);
            }
            InstanceState::Destroying => (),
            InstanceState::Destroyed => {
                conflict!("instance already completely destroyed");
            }
        }

        tx.commit()?;
        Ok(())
    }

    pub fn instance_mark_destroyed(&self, i: &Instance) -> OResult<()> {
        let c = &mut self.1.lock().unwrap().conn;
        let mut tx = c.transaction_with_behavior(
            rusqlite::TransactionBehavior::Immediate,
        )?;

        /*
         * Fetch the current instance state:
         */
        let i: Instance =
            self.tx_get_row(&mut tx, Instance::find(&i.nodename, i.seq))?;

        match i.state {
            InstanceState::Preboot | InstanceState::Booted => {
                conflict!("instance was not already being destroyed");
            }
            InstanceState::Destroying => {
                let uc = self.tx_exec_update(
                    &mut tx,
                    Query::update()
                        .table(InstanceDef::Table)
                        .and_where(
                            Expr::col(InstanceDef::Nodename).eq(i.nodename),
                        )
                        .and_where(Expr::col(InstanceDef::Seq).eq(i.seq))
                        .value(InstanceDef::State, InstanceState::Destroyed)
                        .to_owned(),
                )?;
                assert_eq!(uc, 1);
            }
            InstanceState::Destroyed => (),
        }

        tx.commit()?;
        Ok(())
    }

    pub fn instance_mark_flushed(&self, i: &Instance) -> OResult<()> {
        let uc = self.exec_update(
            Query::update()
                .table(InstanceDef::Table)
                .and_where(Expr::col(InstanceDef::Nodename).eq(&i.nodename))
                .and_where(Expr::col(InstanceDef::Seq).eq(i.seq))
                .value(InstanceDef::Flushed, true)
                .to_owned(),
        )?;
        assert_eq!(uc, 1);

        Ok(())
    }

    pub fn instance_boot(&self, i: &Instance) -> OResult<()> {
        let c = &mut self.1.lock().unwrap().conn;
        let mut tx = c.transaction_with_behavior(
            rusqlite::TransactionBehavior::Immediate,
        )?;

        /*
         * Fetch the current instance state:
         */
        let i: Instance =
            self.tx_get_row(&mut tx, Instance::find(&i.nodename, i.seq))?;

        match i.state {
            InstanceState::Preboot => {
                let uc = self.tx_exec_update(
                    &mut tx,
                    Query::update()
                        .table(InstanceDef::Table)
                        .and_where(
                            Expr::col(InstanceDef::Nodename).eq(i.nodename),
                        )
                        .and_where(Expr::col(InstanceDef::Seq).eq(i.seq))
                        .value(InstanceDef::State, InstanceState::Booted)
                        .to_owned(),
                )?;
                assert_eq!(uc, 1);
            }
            InstanceState::Booted => (),
            InstanceState::Destroying | InstanceState::Destroyed => {
                conflict!("instance already being destroyed");
            }
        }

        tx.commit()?;
        Ok(())
    }

    pub fn instance_append(
        &self,
        i: &Instance,
        stream: &str,
        msg: &str,
        time: DateTime<Utc>,
    ) -> OResult<InstanceEvent> {
        let c = &mut self.1.lock().unwrap().conn;
        let mut tx = c.transaction_with_behavior(
            rusqlite::TransactionBehavior::Immediate,
        )?;

        /*
         * Fetch the current instance state:
         */
        let i: Instance =
            self.tx_get_row(&mut tx, Instance::find(&i.nodename, i.seq))?;

        let ie = match i.state {
            InstanceState::Preboot | InstanceState::Booted => {
                let ie = InstanceEvent {
                    nodename: i.nodename.to_string(),
                    instance: i.seq,
                    seq: self.i_next_seq_for_instance(&i, &mut tx)?,
                    stream: stream.to_string(),
                    payload: msg.to_string(),
                    uploaded: false,
                    time: IsoDate(time),
                };

                let ic = self.tx_exec_insert(&mut tx, ie.insert())?;
                assert_eq!(ic, 1);

                ie
            }
            InstanceState::Destroying | InstanceState::Destroyed => {
                conflict!("instance already being destroyed");
            }
        };

        tx.commit()?;
        Ok(ie)
    }

    pub fn instance_next_event_to_upload(
        &self,
        i: &Instance,
    ) -> OResult<Option<InstanceEvent>> {
        self.get_row_opt(
            Query::select()
                .from(InstanceEventDef::Table)
                .columns(InstanceEvent::columns())
                .and_where(
                    Expr::col(InstanceEventDef::Nodename).eq(&i.nodename),
                )
                .and_where(Expr::col(InstanceEventDef::Instance).eq(i.seq))
                .and_where(Expr::col(InstanceEventDef::Uploaded).eq(false))
                .order_by(InstanceEventDef::Seq, Order::Asc)
                .limit(1)
                .to_owned(),
        )
    }

    pub fn instance_mark_event_uploaded(
        &self,
        i: &Instance,
        ie: &InstanceEvent,
    ) -> OResult<()> {
        let uc = self.exec_update(
            Query::update()
                .table(InstanceEventDef::Table)
                .and_where(
                    Expr::col(InstanceEventDef::Nodename).eq(&i.nodename),
                )
                .and_where(Expr::col(InstanceEventDef::Instance).eq(i.seq))
                .and_where(Expr::col(InstanceEventDef::Seq).eq(ie.seq))
                .and_where(Expr::col(InstanceEventDef::Uploaded).eq(false))
                .value(InstanceEventDef::Uploaded, true)
                .to_owned(),
        )?;
        assert!(uc == 0 || uc == 1);

        Ok(())
    }

    /*
     * Helper routines for database access:
     */

    #[allow(unused)]
    fn tx_exec_delete(
        &self,
        tx: &mut Transaction,
        d: DeleteStatement,
    ) -> OResult<usize> {
        let (q, v) = d.build_rusqlite(SqliteQueryBuilder);
        self.tx_exec(tx, q, v)
    }

    fn tx_exec_update(
        &self,
        tx: &mut Transaction,
        u: UpdateStatement,
    ) -> OResult<usize> {
        let (q, v) = u.build_rusqlite(SqliteQueryBuilder);
        self.tx_exec(tx, q, v)
    }

    fn tx_exec_insert(
        &self,
        tx: &mut Transaction,
        i: InsertStatement,
    ) -> OResult<usize> {
        let (q, v) = i.build_rusqlite(SqliteQueryBuilder);
        self.tx_exec(tx, q, v)
    }

    fn tx_exec(
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
    fn exec_delete(&self, d: DeleteStatement) -> OResult<usize> {
        let (q, v) = d.build_rusqlite(SqliteQueryBuilder);
        debug!(self.0, "query: {q}"; "sql" => true);
        self.exec(q, v)
    }

    fn exec_update(&self, u: UpdateStatement) -> OResult<usize> {
        let (q, v) = u.build_rusqlite(SqliteQueryBuilder);
        debug!(self.0, "query: {q}"; "sql" => true);
        self.exec(q, v)
    }

    fn exec_insert(&self, i: InsertStatement) -> OResult<usize> {
        let (q, v) = i.build_rusqlite(SqliteQueryBuilder);
        debug!(self.0, "query: {q}"; "sql" => true);
        self.exec(q, v)
    }

    fn exec(&self, q: String, v: RusqliteValues) -> OResult<usize> {
        let c = &mut self.1.lock().unwrap().conn;

        let out = c.prepare(&q)?.execute(&*v.as_params())?;

        Ok(out)
    }

    fn get_strings(&self, s: SelectStatement) -> OResult<Vec<String>> {
        let (q, v) = s.build_rusqlite(SqliteQueryBuilder);
        debug!(self.0, "query: {q}"; "sql" => true);
        let c = &mut self.1.lock().unwrap().conn;

        let mut s = c.prepare(&q)?;
        let out = s.query_map(&*v.as_params(), |row| row.get(0))?;

        Ok(out.collect::<rusqlite::Result<_>>()?)
    }

    fn get_rows<T: FromRow>(&self, s: SelectStatement) -> OResult<Vec<T>> {
        let (q, v) = s.build_rusqlite(SqliteQueryBuilder);
        debug!(self.0, "query: {q}"; "sql" => true);
        let c = &mut self.1.lock().unwrap().conn;

        let mut s = c.prepare(&q)?;
        let out = s.query_map(&*v.as_params(), T::from_row)?;

        Ok(out.collect::<rusqlite::Result<_>>()?)
    }

    fn get_row<T: FromRow>(&self, s: SelectStatement) -> OResult<T> {
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

    fn tx_get_row_opt<T: FromRow>(
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

    fn tx_get_strings(
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

    fn tx_get_rows<T: FromRow>(
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

    fn tx_get_row<T: FromRow>(
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
