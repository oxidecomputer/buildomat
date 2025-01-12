/*
 * Copyright 2024 Oxide Computer Company
 */

use std::path::Path;
use std::sync::Condvar;
use std::thread::ThreadId;
use std::{collections::HashMap, sync::Mutex};

use anyhow::Result;
use chrono::prelude::*;
pub use jmclib::sqlite::rusqlite;
use rusqlite::Row;
use sea_query::{
    ColumnRef, DeleteStatement, Iden, InsertStatement, Nullable, SeaRc,
    SelectStatement, SqliteQueryBuilder, UpdateStatement, Value,
};
use sea_query_rusqlite::{RusqliteBinder, RusqliteValues};
use slog::{debug, Logger};
use thiserror::Error;

#[usdt::provider]
mod buildomat__database {
    fn sql__query__start(query: String) {}
    fn sql__query__end() {}
}
use buildomat__database::{sql__query__end, sql__query__start};

pub trait FromRow: Sized {
    fn columns() -> Vec<ColumnRef>;
    fn from_row(row: &Row) -> rusqlite::Result<Self>;

    fn bare_columns() -> Vec<SeaRc<dyn Iden>> {
        Self::columns()
            .into_iter()
            .map(|v| match v {
                ColumnRef::TableColumn(_, c) => c,
                _ => unreachable!(),
            })
            .collect()
    }
}

/*
 * This simple FromRow implementation allows us to automatically lift out single
 * column queries where the output type is a base SQL type, or close equivalent,
 * like a u32 or String.  This reduces boilerplate on simple queries that SELECT
 * MAX() or COUNT() from some table.
 */
impl<T: rusqlite::types::FromSql> FromRow for T {
    fn columns() -> Vec<ColumnRef> {
        unimplemented!()
    }

    fn from_row(row: &Row) -> rusqlite::Result<T> {
        row.get(0)
    }
}

#[macro_export]
macro_rules! sqlite_sql_enum {
    ($name:ident ( $($derives:ident),* ) => { $($arms:tt)* }) => {
        #[derive(
            Debug,
            Clone,
            Copy,
            PartialEq,
            Eq,
            strum::Display,
            strum::EnumString,
            serde::Serialize,
            serde::Deserialize,
            $($derives),*
        )]
        #[serde(rename_all = "snake_case")]
        #[strum(serialize_all = "snake_case")]
        pub enum $name { $($arms)* }

        impl From<$name> for sea_query::Value {
            fn from(value: $name) -> sea_query::Value {
                sea_query::Value::String(Some(Box::new(value.to_string())))
            }
        }

        impl sea_query::Nullable for $name {
            fn null() -> sea_query::Value {
                sea_query::Value::String(None)
            }
        }

        impl rusqlite::types::FromSql for $name {
            fn column_result(
                v: rusqlite::types::ValueRef<'_>,
            ) -> rusqlite::types::FromSqlResult<Self> {
                use std::str::FromStr;

                if let rusqlite::types::ValueRef::Text(t) = v {
                    if let Ok(s) = String::from_utf8(t.to_vec()) {
                        match $name::from_str(&s) {
                            Ok(v) => Ok(v),
                            Err(e) => {
                                Err(rusqlite::types::FromSqlError::Other(
                                    format!("invalid enum: {e}").into(),
                                ))
                            }
                        }
                    } else {
                        Err(rusqlite::types::FromSqlError::Other(
                            "invalid UTF-8".into(),
                        ))
                    }
                } else {
                    Err(rusqlite::types::FromSqlError::InvalidType)
                }
            }
        }
    };

    ($name:ident => { $($arms:tt)* }) => {
        $crate::sqlite_sql_enum!($name () => { $($arms)* });
    };
}

#[macro_export]
macro_rules! sqlite_json_new_type {
    ($name:ident, $mytype:ty) => {
        #[derive(Clone, Debug)]
        pub struct $name(pub $mytype);

        impl From<$name> for sea_query::Value {
            fn from(value: $name) -> sea_query::Value {
                sea_query::Value::String(Some(Box::new(
                    serde_json::to_string(&value.0).unwrap(),
                )))
            }
        }

        impl sea_query::Nullable for $name {
            fn null() -> sea_query::Value {
                sea_query::Value::String(None)
            }
        }

        impl rusqlite::types::FromSql for $name {
            fn column_result(
                v: rusqlite::types::ValueRef<'_>,
            ) -> rusqlite::types::FromSqlResult<Self> {
                if let rusqlite::types::ValueRef::Text(t) = v {
                    match serde_json::from_slice(t) {
                        Ok(o) => Ok(Self(o)),
                        Err(e) => Err(rusqlite::types::FromSqlError::Other(
                            format!("invalid JSON: {e}").into(),
                        )),
                    }
                } else {
                    Err(rusqlite::types::FromSqlError::InvalidType)
                }
            }
        }

        impl From<$name> for $mytype {
            fn from(t: $name) -> Self {
                t.0
            }
        }

        impl From<$mytype> for $name {
            fn from(t: $mytype) -> $name {
                $name(t)
            }
        }

        impl std::ops::Deref for $name {
            type Target = $mytype;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }
    };
}

#[macro_export]
macro_rules! sqlite_integer_new_type {
    ($name:ident, $mytype:ty, $intype:ident) => {
        #[derive(
            Clone,
            Copy,
            Debug,
            PartialEq,
            Hash,
            Eq,
            serde::Serialize,
            serde::Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(pub $mytype);

        impl From<$name> for sea_query::Value {
            fn from(value: $name) -> sea_query::Value {
                sea_query::Value::$intype(Some(value.0.try_into().unwrap()))
            }
        }

        impl sea_query::Nullable for $name {
            fn null() -> sea_query::Value {
                sea_query::Value::$intype(None)
            }
        }

        impl rusqlite::types::FromSql for $name {
            fn column_result(
                v: rusqlite::types::ValueRef<'_>,
            ) -> rusqlite::types::FromSqlResult<Self> {
                if let rusqlite::types::ValueRef::Integer(i) = v {
                    match i.try_into() {
                        Ok(n) => Ok(Self(n)),
                        Err(e) => Err(rusqlite::types::FromSqlError::Other(
                            format!("invalid number: {i}: {e}").into(),
                        )),
                    }
                } else {
                    Err(rusqlite::types::FromSqlError::InvalidType)
                }
            }
        }

        impl std::str::FromStr for $name {
            type Err = std::num::ParseIntError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Ok($name(s.parse()?))
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0.to_string())
            }
        }

        impl From<$name> for $mytype {
            fn from(val: $name) -> Self {
                val.0
            }
        }
    };
}

#[macro_export]
macro_rules! sqlite_ulid_new_type {
    ($name:ident ; $($derives:ident),* ) => {
        #[derive(
            Clone,
            Copy,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            Debug,
            serde::Serialize,
            serde::Deserialize,
            $($derives),*
        )]
        #[serde(transparent)]
        pub struct $name(pub rusty_ulid::Ulid);

        impl From<$name> for sea_query::Value {
            fn from(value: $name) -> sea_query::Value {
                sea_query::Value::String(Some(Box::new(value.0.to_string())))
            }
        }

        impl sea_query::Nullable for $name {
            fn null() -> sea_query::Value {
                sea_query::Value::String(None)
            }
        }

        impl rusqlite::types::FromSql for $name {
            fn column_result(
                v: rusqlite::types::ValueRef<'_>,
            ) -> rusqlite::types::FromSqlResult<Self> {
                use std::str::FromStr;

                if let rusqlite::types::ValueRef::Text(t) = v {
                    if let Ok(s) = String::from_utf8(t.to_vec()) {
                        match rusty_ulid::Ulid::from_str(&s) {
                            Ok(id) => Ok($name(id)),
                            Err(e) => {
                                Err(rusqlite::types::FromSqlError::Other(
                                    format!("invalid ULID: {e}").into(),
                                ))
                            }
                        }
                    } else {
                        Err(rusqlite::types::FromSqlError::Other(
                            "invalid UTF-8".into(),
                        ))
                    }
                } else {
                    Err(rusqlite::types::FromSqlError::InvalidType)
                }
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0.to_string())
            }
        }

        impl std::str::FromStr for $name {
            type Err = rusty_ulid::DecodingError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Ok($name(rusty_ulid::Ulid::from_str(s)?))
            }
        }

        impl $name {
            pub fn generate() -> $name {
                $name(rusty_ulid::Ulid::generate())
            }

            pub fn datetime(&self) -> chrono::DateTime<chrono::Utc> {
                chrono::DateTime::from_timestamp_millis(
                    self.0.timestamp().try_into().unwrap()
                ).unwrap()
            }

            pub fn age(&self) -> std::time::Duration {
                chrono::Utc::now()
                    .signed_duration_since(self.datetime())
                    .to_std()
                    .unwrap_or_else(|_| std::time::Duration::from_secs(0))
            }
        }
    };

    ($name:ident) => {
        $crate::sqlite_ulid_new_type!($name ; );
    }
}

/*
 * IsoDate
 */

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IsoDate(pub DateTime<Utc>);

impl From<IsoDate> for Value {
    fn from(v: IsoDate) -> Self {
        Value::String(Some(Box::new(
            v.0.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        )))
    }
}

impl Nullable for IsoDate {
    fn null() -> Value {
        Value::String(None)
    }
}

impl rusqlite::types::FromSql for IsoDate {
    fn column_result(
        v: rusqlite::types::ValueRef<'_>,
    ) -> rusqlite::types::FromSqlResult<Self> {
        if let rusqlite::types::ValueRef::Text(t) = v {
            if let Ok(s) = String::from_utf8(t.to_vec()) {
                Ok(IsoDate(DateTime::from(
                    match DateTime::parse_from_rfc3339(&s) {
                        Ok(fo) => fo,
                        Err(e1) => {
                            /*
                             * Try an older date format from before we switched
                             * to diesel:
                             */
                            match DateTime::parse_from_str(
                                &s,
                                "%Y-%m-%d %H:%M:%S%.9f%z",
                            ) {
                                Ok(fo) => fo,
                                Err(_) => {
                                    return Err(
                                        rusqlite::types::FromSqlError::Other(
                                            format!("invalid date: {e1}")
                                                .into(),
                                        ),
                                    );
                                }
                            }
                        }
                    },
                )))
            } else {
                Err(rusqlite::types::FromSqlError::Other(
                    "invalid UTF-8".into(),
                ))
            }
        } else {
            Err(rusqlite::types::FromSqlError::InvalidType)
        }
    }
}

impl From<IsoDate> for DateTime<Utc> {
    fn from(val: IsoDate) -> Self {
        val.0
    }
}

impl From<DateTime<Utc>> for IsoDate {
    fn from(dt: DateTime<Utc>) -> Self {
        IsoDate(dt)
    }
}

impl std::ops::Deref for IsoDate {
    type Target = DateTime<Utc>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl IsoDate {
    pub fn age(&self) -> std::time::Duration {
        Utc::now()
            .signed_duration_since(self.0)
            .to_std()
            .unwrap_or_else(|_| std::time::Duration::from_secs(0))
    }

    pub fn now() -> IsoDate {
        IsoDate(Utc::now())
    }
}

sqlite_json_new_type!(Dictionary, HashMap<String, String>);
sqlite_json_new_type!(JsonValue, serde_json::Value);

pub struct Sqlite {
    log: Logger,
    conn: Mutex<rusqlite::Connection>,
    busy: Mutex<Option<ThreadId>>,
    cv: Condvar,
}

impl Sqlite {
    pub fn setup<P: AsRef<Path>, S: AsRef<str>>(
        log: Logger,
        path: P,
        schema: S,
        cache_kb: Option<u32>,
    ) -> Result<Sqlite> {
        let path = path.as_ref();

        let mut setup = jmclib::sqlite::SqliteSetup::new();
        setup.schema(schema.as_ref());
        setup.log(log.clone());

        /*
         * Disable integrity checking for the moment, because it takes _a very
         * long time_ on the truly astronomical (100GB+) buildomat core API
         * database right now.
         */
        setup.check_integrity(false);

        if let Some(kb) = cache_kb {
            /*
             * If requested, set the page cache size to something other than the
             * default value of 2MB.
             */
            setup.cache_kb(kb);
        }

        Ok(Sqlite {
            log,
            conn: Mutex::new(setup.open(path)?),
            busy: Mutex::new(None),
            cv: Default::default(),
        })
    }

    pub fn tx_immediate<T>(
        &self,
        func: impl FnOnce(&mut Handle) -> DBResult<T>,
    ) -> DBResult<T> {
        self.tx_common(rusqlite::TransactionBehavior::Immediate, func)
    }

    pub fn tx<T>(
        &self,
        func: impl FnOnce(&mut Handle) -> DBResult<T>,
    ) -> DBResult<T> {
        self.tx_common(rusqlite::TransactionBehavior::Deferred, func)
    }

    fn tx_common<T>(
        &self,
        txb: rusqlite::TransactionBehavior,
        func: impl FnOnce(&mut Handle) -> DBResult<T>,
    ) -> DBResult<T> {
        let curthread = std::thread::current().id();

        /*
         * Keep track of which thread is currently engaged in a transaction.  If
         * we accidentally attempt to start a second transaction recursively, we
         * want to fail.
         */
        {
            let mut b = self.busy.lock().unwrap();
            while let Some(owner) = *b {
                if owner == curthread {
                    conflict!("nested transactions would cause deadlock");
                }

                b = self.cv.wait(b).unwrap();
            }
            *b = Some(curthread);
        }

        let res = self.tx_common_locked(txb, func);

        {
            let mut b = self.busy.lock().unwrap();

            /*
             * This code is completely synchronous, so we must not be migrated
             * to another LWP prior to the ultimate return to our caller.
             */
            assert_eq!(b.take(), Some(curthread));

            self.cv.notify_all();
        }

        res
    }

    fn tx_common_locked<T>(
        &self,
        txb: rusqlite::TransactionBehavior,
        func: impl FnOnce(&mut Handle) -> DBResult<T>,
    ) -> DBResult<T> {
        let mut c = self.conn.lock().unwrap();

        let mut h = Handle {
            tx: c.transaction_with_behavior(txb)?,
            log: self.log.clone(),
        };

        /*
         * If this routine exits before expected due to an error, make sure we
         * roll back any work that was done.  This is the current default
         * behaviour, but is sufficiently critical that we explicitly request
         * it:
         */
        h.tx.set_drop_behavior(rusqlite::DropBehavior::Rollback);

        match func(&mut h) {
            Ok(res) => {
                h.tx.commit()?;
                Ok(res)
            }
            Err(e) => {
                h.tx.rollback()?;
                Err(e)
            }
        }
    }
}

pub type DBResult<T> = std::result::Result<T, DatabaseError>;

#[derive(Error, Debug)]
pub enum DatabaseError {
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

#[macro_export]
macro_rules! conflict {
    ($msg:expr) => {
        return Err($crate::DatabaseError::Conflict(format!($msg)))
    };
    ($fmt:expr, $($arg:tt)*) => {
        return Err($crate::DatabaseError::Conflict(format!($fmt, $($arg)*)))
    }
}

pub struct Handle<'a> {
    log: Logger,
    tx: rusqlite::Transaction<'a>,
}

impl<'a> Handle<'a> {
    pub fn get_row_opt<T: FromRow>(
        &mut self,
        s: SelectStatement,
    ) -> DBResult<Option<T>> {
        let (q, v) = s.build_rusqlite(SqliteQueryBuilder);
        debug!(self.log, "query: {q}"; "sql" => true);

        sql__query__start!(|| &q);
        let mut s = self.tx.prepare(&q)?;
        let out = s.query_map(&*v.as_params(), T::from_row)?;
        let mut out = out.collect::<rusqlite::Result<Vec<T>>>()?;
        sql__query__end!(|| ());

        match out.len() {
            0 => Ok(None),
            1 => Ok(Some(out.pop().unwrap())),
            n => conflict!("found {n} records when we wanted only 1"),
        }
    }

    pub fn get_strings(&mut self, s: SelectStatement) -> DBResult<Vec<String>> {
        let (q, v) = s.build_rusqlite(SqliteQueryBuilder);
        debug!(self.log, "query: {q}"; "sql" => true);

        sql__query__start!(|| &q);
        let mut s = self.tx.prepare(&q)?;
        let out = s.query_map(&*v.as_params(), |row| row.get(0))?;
        let res = out.collect::<rusqlite::Result<_>>()?;
        sql__query__end!(|| ());

        Ok(res)
    }

    pub fn get_rows<T: FromRow>(
        &mut self,
        s: SelectStatement,
    ) -> DBResult<Vec<T>> {
        let (q, v) = s.build_rusqlite(SqliteQueryBuilder);
        debug!(self.log, "query: {q}"; "sql" => true);

        sql__query__start!(|| &q);
        let mut s = self.tx.prepare(&q)?;
        let out = s.query_map(&*v.as_params(), T::from_row)?;
        let res = out.collect::<rusqlite::Result<_>>()?;
        sql__query__end!(|| ());

        Ok(res)
    }

    pub fn get_row<T: FromRow>(&mut self, s: SelectStatement) -> DBResult<T> {
        let (q, v) = s.build_rusqlite(SqliteQueryBuilder);
        debug!(self.log, "query: {q}"; "sql" => true);

        sql__query__start!(|| &q);
        let mut s = self.tx.prepare(&q)?;
        let out = s.query_map(&*v.as_params(), T::from_row)?;
        let mut out = out.collect::<rusqlite::Result<Vec<T>>>()?;
        sql__query__end!(|| ());

        match out.len() {
            0 => conflict!("record not found"),
            1 => Ok(out.pop().unwrap()),
            n => conflict!("found {n} records when we wanted only 1"),
        }
    }

    pub fn exec_delete(&mut self, d: DeleteStatement) -> DBResult<usize> {
        let (q, v) = d.build_rusqlite(SqliteQueryBuilder);
        debug!(self.log, "query: {q}"; "sql" => true);
        self.exec(q, v)
    }

    pub fn exec_update(&mut self, u: UpdateStatement) -> DBResult<usize> {
        let (q, v) = u.build_rusqlite(SqliteQueryBuilder);
        debug!(self.log, "query: {q}"; "sql" => true);
        self.exec(q, v)
    }

    pub fn exec_insert(&mut self, i: InsertStatement) -> DBResult<usize> {
        let (q, v) = i.build_rusqlite(SqliteQueryBuilder);
        debug!(self.log, "query: {q}"; "sql" => true);
        self.exec(q, v)
    }

    fn exec(&mut self, q: String, v: RusqliteValues) -> DBResult<usize> {
        sql__query__start!(|| &q);
        let mut s = self.tx.prepare(&q)?;
        let out = s.execute(&*v.as_params())?;
        sql__query__end!(|| ());

        Ok(out)
    }
}
