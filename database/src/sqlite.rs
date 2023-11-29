/*
 * Copyright 2023 Oxide Computer Company
 */

use std::collections::HashMap;
use std::path::Path;

use anyhow::Result;
use chrono::prelude::*;
pub use jmclib::sqlite::rusqlite;
use sea_query::{Nullable, Value};
use slog::{info, Logger};

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
            $($derives),*
        )]
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
        #[derive(Clone, Copy, Debug, PartialEq, Hash, Eq)]
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
    ($name:ident) => {
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
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

            pub fn datetime(&self) -> DateTime<Utc> {
                self.0.datetime()
            }

            pub fn age(&self) -> std::time::Duration {
                Utc::now()
                    .signed_duration_since(self.0.datetime())
                    .to_std()
                    .unwrap_or_else(|_| std::time::Duration::from_secs(0))
            }
        }
    };
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

pub fn sqlite_setup<P: AsRef<Path>, S: AsRef<str>>(
    log: &Logger,
    path: P,
    schema: S,
    cache_kb: Option<u32>,
) -> Result<rusqlite::Connection> {
    let path = path.as_ref();

    let mut setup = jmclib::sqlite::SqliteSetup::new();
    setup.schema(schema.as_ref());
    setup.log(log.clone());

    if let Some(kb) = cache_kb {
        /*
         * If requested, set the page cache size to something other than the
         * default value of 2MB.
         */
        setup.cache_kb(kb);
    }

    info!(log, "opening database {path:?}");
    setup.open(path)
}
