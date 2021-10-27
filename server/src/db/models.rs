use super::schema::*;
use chrono::prelude::*;
use diesel::deserialize::FromSql;
use diesel::serialize::ToSql;
use rusty_ulid::Ulid;
use std::collections::HashMap;
use std::str::FromStr;

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

integer_new_type!(UnixUid, u32, i32, Integer, "diesel::sql_types::Integer");
integer_new_type!(UnixGid, u32, i32, Integer, "diesel::sql_types::Integer");
integer_new_type!(DataSize, u64, i64, BigInt, "diesel::sql_types::BigInt");

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
        pub struct $name(pub Ulid);

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
                Ok($name(Ulid::from_str(&s)?))
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0.to_string())
            }
        }

        impl $name {
            pub fn generate() -> $name {
                $name(Ulid::generate())
            }

            pub fn datetime(&self) -> DateTime<Utc> {
                self.0.datetime()
            }
        }
    };
}

ulid_new_type!(UserId);
ulid_new_type!(JobId);
ulid_new_type!(JobOutputId);
ulid_new_type!(TaskId);
ulid_new_type!(WorkerId);

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
        let fo = DateTime::parse_from_rfc3339(&s)?;
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

#[derive(Debug, Queryable, Insertable, Identifiable)]
#[table_name = "user"]
#[primary_key(id)]
pub struct User {
    pub id: UserId,
    pub name: String,
    pub token: String,
    pub time_create: IsoDate,
}

#[derive(Debug, Queryable, Insertable, Identifiable)]
#[table_name = "task"]
#[primary_key(job, seq)]
pub struct Task {
    pub job: JobId,
    pub seq: i32,
    pub name: String,
    pub script: String,
    pub env_clear: bool,
    pub env: Dictionary,
    pub user_id: Option<UnixUid>,
    pub group_id: Option<UnixGid>,
    pub workdir: Option<String>,
    pub complete: bool,
    pub failed: bool,
}

impl Task {
    pub fn from_create(ct: &super::CreateTask, job: JobId, seq: usize) -> Task {
        Task {
            job: job,
            seq: seq as i32,
            name: ct.name.to_string(),
            script: ct.script.to_string(),
            env_clear: ct.env_clear,
            env: Dictionary(ct.env.clone()),
            user_id: ct.user_id.map(|uid| UnixUid(uid)),
            group_id: ct.group_id.map(|gid| UnixGid(gid)),
            workdir: ct.workdir.clone(),
            complete: false,
            failed: false,
        }
    }
}

#[derive(Debug, Clone, Queryable, Insertable, Identifiable)]
#[table_name = "job_event"]
#[primary_key(job, seq)]
pub struct JobEvent {
    pub job: JobId,
    pub task: Option<i32>,
    pub seq: i32,
    pub stream: String,
    /**
     * The time at which the core API server received or generated this event.
     */
    pub time: IsoDate,
    pub payload: String,
    /**
     * If this event was received from a remote system, such as a worker, this
     * time reflects the time on that remote system when the event was
     * generated.  Due to issues with NTP, etc, it might not align exactly with
     * the time field.
     */
    pub time_remote: Option<IsoDate>,
}

impl JobEvent {
    pub fn age(&self) -> std::time::Duration {
        if let Ok(age) = Utc::now().signed_duration_since(self.time.0).to_std()
        {
            age
        } else {
            std::time::Duration::from_secs(0)
        }
    }
}

#[derive(Debug, Queryable, Insertable, Identifiable)]
#[table_name = "job_output"]
#[primary_key(job, path)]
pub struct JobOutput {
    pub job: JobId,
    pub path: String,
    pub size: DataSize,
    pub id: JobOutputId,
}

#[derive(Debug, Clone, Queryable, Insertable, Identifiable)]
#[table_name = "worker"]
#[primary_key(id)]
pub struct Worker {
    pub id: WorkerId,
    pub bootstrap: String,
    pub token: Option<String>,
    pub instance_id: Option<String>,
    pub deleted: bool,
    pub recycle: bool,
    pub lastping: Option<IsoDate>,
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

#[derive(Clone, Debug, Queryable, Insertable, Identifiable)]
#[table_name = "job"]
#[primary_key(id)]
pub struct Job {
    pub id: JobId,
    pub owner: UserId,
    pub name: String,
    pub target: String,
    pub complete: bool,
    pub failed: bool,
    pub worker: Option<WorkerId>,
}

impl Job {
    #[allow(dead_code)]
    pub fn time_submit(&self) -> DateTime<Utc> {
        self.id.datetime()
    }
}
