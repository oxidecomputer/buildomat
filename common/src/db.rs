use anyhow::{bail, Result};
use chrono::prelude::*;
use diesel::deserialize::FromSql;
use diesel::prelude::*;
use diesel::serialize::ToSql;
use slog::{info, Logger};
use std::collections::HashMap;
use std::path::Path;

#[macro_export]
macro_rules! integer_new_type {
    ($name:ident, $mytype:ty, $intype:ty, $sqltype:ident, $sqlts:literal) => {
        #[derive(
            Clone, Copy, Debug, FromSqlRow, diesel::expression::AsExpression,
        )]
        #[sql_type = $sqlts]
        pub struct $name(pub $mytype);

        impl<DB> ToSql<diesel::sql_types::$sqltype, DB> for $name
        where
            DB: diesel::backend::Backend,
            $intype: ToSql<diesel::sql_types::$sqltype, DB>,
        {
            fn to_sql<W: std::io::Write>(
                &self,
                out: &mut diesel::serialize::Output<W, DB>,
            ) -> diesel::serialize::Result {
                assert!(self.0 <= (<$intype>::MAX as $mytype));
                (self.0 as $intype).to_sql(out)
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

        impl Into<$mytype> for $name {
            fn into(self) -> $mytype {
                self.0
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
            Hash,
            Debug,
            FromSqlRow,
            diesel::expression::AsExpression,
        )]
        #[sql_type = "diesel::sql_types::Text"]
        pub struct $name(pub rusty_ulid::Ulid);

        impl<DB> ToSql<diesel::sql_types::Text, DB> for $name
        where
            DB: diesel::backend::Backend,
            String: ToSql<diesel::sql_types::Text, DB>,
        {
            fn to_sql<W: std::io::Write>(
                &self,
                out: &mut diesel::serialize::Output<W, DB>,
            ) -> diesel::serialize::Result {
                self.0.to_string().to_sql(out)
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

        impl $name {
            pub fn generate() -> $name {
                $name(rusty_ulid::Ulid::generate())
            }

            pub fn datetime(&self) -> DateTime<Utc> {
                self.0.datetime()
            }
        }
    };
}
pub use ulid_new_type;

/*
 * IsoDate
 */

#[derive(Clone, Copy, Debug, FromSqlRow, diesel::expression::AsExpression)]
#[sql_type = "diesel::sql_types::Text"]
pub struct IsoDate(pub DateTime<Utc>);

impl<DB> ToSql<diesel::sql_types::Text, DB> for IsoDate
where
    DB: diesel::backend::Backend,
    String: ToSql<diesel::sql_types::Text, DB>,
{
    fn to_sql<W: std::io::Write>(
        &self,
        out: &mut diesel::serialize::Output<W, DB>,
    ) -> diesel::serialize::Result {
        let s = self.0.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
        s.to_sql(out)
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

impl Into<DateTime<Utc>> for IsoDate {
    fn into(self) -> DateTime<Utc> {
        self.0.clone()
    }
}

impl From<DateTime<Utc>> for IsoDate {
    fn from(dt: DateTime<Utc>) -> Self {
        IsoDate(dt)
    }
}

/*
 * Dictionary
 */

#[derive(Debug, FromSqlRow, diesel::expression::AsExpression)]
#[sql_type = "diesel::sql_types::Text"]
pub struct Dictionary(pub HashMap<String, String>);

impl<DB> ToSql<diesel::sql_types::Text, DB> for Dictionary
where
    DB: diesel::backend::Backend,
    String: ToSql<diesel::sql_types::Text, DB>,
{
    fn to_sql<W: std::io::Write>(
        &self,
        out: &mut diesel::serialize::Output<W, DB>,
    ) -> diesel::serialize::Result {
        serde_json::to_string(&self.0)?.to_sql(out)
    }
}

impl<DB> FromSql<diesel::sql_types::Text, DB> for Dictionary
where
    DB: diesel::backend::Backend,
    String: FromSql<diesel::sql_types::Text, DB>,
{
    fn from_sql(
        bytes: diesel::backend::RawValue<DB>,
    ) -> diesel::deserialize::Result<Self> {
        Ok(Dictionary(serde_json::from_str(&String::from_sql(bytes)?)?))
    }
}

impl std::ops::Deref for Dictionary {
    type Target = HashMap<String, String>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub fn sqlite_setup<P: AsRef<Path>, S: AsRef<str>>(
    log: &Logger,
    path: P,
    schema: S,
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

    #[derive(QueryableByName)]
    struct UserVersion {
        #[sql_type = "diesel::sql_types::Integer"]
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
         * Determine the current user version.
         */
        let uv = diesel::sql_query("PRAGMA user_version")
            .get_result::<UserVersion>(&mut c)?
            .user_version;
        if version > uv {
            info!(log, "apply version {}, run {}", version, statement);
            diesel::sql_query(statement).execute(&mut c)?;
            diesel::sql_query(format!("PRAGMA user_version = {}", version))
                .execute(&mut c)?;
        }
    }

    Ok(c)
}
