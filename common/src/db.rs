use anyhow::{bail, Result};
use chrono::prelude::*;
use diesel::deserialize::FromSql;
use diesel::prelude::*;
use diesel::serialize::ToSql;
use slog::{info, Logger};
use std::collections::HashMap;
use std::path::Path;

#[macro_export]
macro_rules! sql_for_enum {
    ($name:ident) => {
        impl ToSql<diesel::sql_types::Text, diesel::sqlite::Sqlite> for $name
        where
            String: ToSql<diesel::sql_types::Text, diesel::sqlite::Sqlite>,
        {
            fn to_sql(
                &self,
                out: &mut diesel::serialize::Output<diesel::sqlite::Sqlite>,
            ) -> diesel::serialize::Result {
                out.set_value(self.to_string());
                Ok(diesel::serialize::IsNull::No)
            }
        }

        impl<DB> FromSql<diesel::sql_types::Text, DB> for $name
        where
            DB: diesel::backend::Backend,
            String: FromSql<diesel::sql_types::Text, DB>,
        {
            fn from_sql(
                bytes: diesel::backend::RawValue<DB>,
            ) -> diesel::deserialize::Result<Self> {
                Ok($name::from_str(&String::from_sql(bytes)?)?)
            }
        }
    };
}
pub use sql_for_enum;

#[macro_export]
macro_rules! json_new_type {
    ($name:ident, $mytype:ty) => {
        #[derive(
            Clone, Debug, FromSqlRow, diesel::expression::AsExpression,
        )]
        #[diesel(sql_type = diesel::sql_types::Text)]
        pub struct $name(pub $mytype);

        impl ToSql<diesel::sql_types::Text, diesel::sqlite::Sqlite> for $name
        where
            String: ToSql<diesel::sql_types::Text, diesel::sqlite::Sqlite>,
        {
            fn to_sql(
                &self,
                out: &mut diesel::serialize::Output<diesel::sqlite::Sqlite>,
            ) -> diesel::serialize::Result {
                out.set_value(serde_json::to_string(&self.0)?);
                Ok(diesel::serialize::IsNull::No)
            }
        }

        impl<DB> FromSql<diesel::sql_types::Text, DB> for $name
        where
            DB: diesel::backend::Backend,
            String: FromSql<diesel::sql_types::Text, DB>,
        {
            fn from_sql(
                bytes: diesel::backend::RawValue<DB>,
            ) -> diesel::deserialize::Result<Self> {
                Ok($name(serde_json::from_str(&String::from_sql(bytes)?)?))
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
pub use json_new_type;

#[macro_export]
macro_rules! integer_new_type {
    ($name:ident, $mytype:ty, $intype:ty, $sqltype:ident, $sqlts:ty) => {
        #[derive(
            Clone,
            Copy,
            Debug,
            PartialEq,
            Hash,
            Eq,
            FromSqlRow,
            diesel::expression::AsExpression,
        )]
        #[diesel(sql_type = $sqlts)]
        pub struct $name(pub $mytype);

        impl ToSql<diesel::sql_types::$sqltype, diesel::sqlite::Sqlite>
            for $name
        where
            $intype: ToSql<diesel::sql_types::$sqltype, diesel::sqlite::Sqlite>,
        {
            fn to_sql(
                &self,
                out: &mut diesel::serialize::Output<diesel::sqlite::Sqlite>,
            ) -> diesel::serialize::Result {
                assert!(self.0 <= (<$intype>::MAX as $mytype));
                out.set_value((self.0 as $intype));
                Ok(diesel::serialize::IsNull::No)
            }
        }

        impl<DB> FromSql<diesel::sql_types::$sqltype, DB> for $name
        where
            DB: diesel::backend::Backend,
            $intype: FromSql<diesel::sql_types::$sqltype, DB>,
        {
            fn from_sql(
                bytes: diesel::backend::RawValue<DB>,
            ) -> diesel::deserialize::Result<Self> {
                let n = <$intype>::from_sql(bytes)? as $mytype;
                Ok($name(n))
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
pub use integer_new_type;

#[macro_export]
macro_rules! ulid_new_type {
    ($name:ident) => {
        #[derive(
            Clone,
            Copy,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            Debug,
            FromSqlRow,
            diesel::expression::AsExpression,
        )]
        #[diesel(sql_type = diesel::sql_types::Text)]
        pub struct $name(pub rusty_ulid::Ulid);

        impl ToSql<diesel::sql_types::Text, diesel::sqlite::Sqlite> for $name
        where
            String: ToSql<diesel::sql_types::Text, diesel::sqlite::Sqlite>,
        {
            fn to_sql(
                &self,
                out: &mut diesel::serialize::Output<diesel::sqlite::Sqlite>,
            ) -> diesel::serialize::Result {
                out.set_value(self.0.to_string());
                Ok(diesel::serialize::IsNull::No)
            }
        }

        impl<DB> FromSql<diesel::sql_types::Text, DB> for $name
        where
            DB: diesel::backend::Backend,
            String: FromSql<diesel::sql_types::Text, DB>,
        {
            fn from_sql(
                bytes: diesel::backend::RawValue<DB>,
            ) -> diesel::deserialize::Result<Self> {
                let s = String::from_sql(bytes)?;
                Ok($name(rusty_ulid::Ulid::from_str(&s)?))
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
pub use ulid_new_type;

/*
 * IsoDate
 */

#[derive(Clone, Copy, Debug, FromSqlRow, diesel::expression::AsExpression)]
#[diesel(sql_type = diesel::sql_types::Text)]
pub struct IsoDate(pub DateTime<Utc>);

impl ToSql<diesel::sql_types::Text, diesel::sqlite::Sqlite> for IsoDate
where
    String: ToSql<diesel::sql_types::Text, diesel::sqlite::Sqlite>,
{
    fn to_sql(
        &self,
        out: &mut diesel::serialize::Output<diesel::sqlite::Sqlite>,
    ) -> diesel::serialize::Result {
        out.set_value(
            self.0.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        );
        Ok(diesel::serialize::IsNull::No)
    }
}

impl<DB> FromSql<diesel::sql_types::Text, DB> for IsoDate
where
    DB: diesel::backend::Backend,
    String: FromSql<diesel::sql_types::Text, DB>,
{
    fn from_sql(
        bytes: diesel::backend::RawValue<DB>,
    ) -> diesel::deserialize::Result<Self> {
        let s = String::from_sql(bytes)?;
        let fo = match DateTime::parse_from_rfc3339(&s) {
            Ok(fo) => fo,
            Err(e1) => {
                /*
                 * Try an older date format from before we switched to diesel:
                 */
                match DateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S%.9f%z") {
                    Ok(fo) => fo,
                    Err(_) => {
                        return Err(
                            diesel::result::Error::DeserializationError(
                                e1.into(),
                            )
                            .into(),
                        )
                    }
                }
            }
        };
        Ok(IsoDate(DateTime::from(fo)))
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
}

json_new_type!(Dictionary, HashMap<String, String>);
json_new_type!(JsonValue, serde_json::Value);

pub fn sqlite_setup<P: AsRef<Path>, S: AsRef<str>>(
    log: &Logger,
    path: P,
    schema: S,
    cache_kb: Option<u32>,
) -> Result<diesel::SqliteConnection> {
    let url = if let Some(path) = path.as_ref().to_str() {
        format!("sqlite://{}", path)
    } else {
        bail!("path to database must be UTF-8 safe to pass in SQLite URL");
    };

    info!(log, "opening database {:?}", url);
    let mut c = diesel::SqliteConnection::establish(&url)?;

    /*
     * Enable foreign key processing, which is off by default.  Without enabling
     * this, there is no referential integrity check between primary and foreign
     * keys in tables.
     */
    diesel::sql_query("PRAGMA foreign_keys = 'ON'").execute(&mut c)?;

    /*
     * Enable the WAL.
     */
    diesel::sql_query("PRAGMA journal_mode = 'WAL'").execute(&mut c)?;

    if let Some(kb) = cache_kb {
        /*
         * If requested, set the page cache size to something other than the
         * default value of 2MB.
         */
        diesel::sql_query(format!("PRAGMA cache_size = -{}", kb))
            .execute(&mut c)?;
    }

    #[derive(QueryableByName)]
    struct UserVersion {
        #[diesel(sql_type = diesel::sql_types::Integer)]
        user_version: i32,
    }

    /*
     * Take the schema file and split it on the special comments we use to
     * separate statements.
     */
    let mut steps: Vec<(i32, String)> = Vec::new();
    let mut version = None;
    let mut statement = String::new();

    for l in schema.as_ref().lines() {
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
            .get_result::<UserVersion>(&mut c)?
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
         * Perform the version check, statement execution, and version update
         * inside a single transaction.  Not all of the things we could do in a
         * statement are transactional, but if we are doing an INSERT SELECT to
         * copy things from one table to another, we don't want that to conk out
         * half way and run again.
         */
        c.immediate_transaction::<_, anyhow::Error, _>(|tx| {
            /*
             * Determine the current user version.
             */
            let uv = diesel::sql_query("PRAGMA user_version")
                .get_result::<UserVersion>(tx)?
                .user_version;

            if version > uv {
                info!(log, "apply version {}, run {}", version, statement);

                diesel::sql_query(statement).execute(tx)?;

                /*
                 * Update the user version.
                 */
                diesel::sql_query(format!("PRAGMA user_version = {}", version))
                    .execute(tx)?;

                info!(log, "version {} ok", version);
            }

            Ok(())
        })?;
    }

    Ok(c)
}
