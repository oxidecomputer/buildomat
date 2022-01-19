use super::schema::*;
use chrono::prelude::*;
use diesel::deserialize::FromSql;
use diesel::serialize::ToSql;
use std::str::FromStr;
use std::time::Duration;

use buildomat_common::db::*;
pub use buildomat_common::db::{Dictionary, IsoDate};

integer_new_type!(UnixUid, u32, i32, Integer, "diesel::sql_types::Integer");
integer_new_type!(UnixGid, u32, i32, Integer, "diesel::sql_types::Integer");
integer_new_type!(DataSize, u64, i64, BigInt, "diesel::sql_types::BigInt");

ulid_new_type!(UserId);
ulid_new_type!(JobId);
ulid_new_type!(JobFileId);
ulid_new_type!(TaskId);
ulid_new_type!(WorkerId);
ulid_new_type!(FactoryId);
ulid_new_type!(TargetId);

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
            job,
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
    pub fn age(&self) -> Duration {
        Utc::now()
            .signed_duration_since(self.time.0)
            .to_std()
            .unwrap_or_else(|_| Duration::from_secs(0))
    }
}

#[derive(Debug, Queryable, Insertable, Identifiable)]
#[table_name = "job_output"]
#[primary_key(job, path)]
pub struct JobOutput {
    pub job: JobId,
    pub path: String,
    pub id: JobFileId,
}

#[derive(Debug, Queryable, Insertable, Identifiable)]
#[table_name = "job_input"]
#[primary_key(job, name)]
pub struct JobInput {
    pub job: JobId,
    pub name: String,
    pub id: Option<JobFileId>,
}

impl JobInput {
    pub fn from_create(name: &str, job: JobId) -> JobInput {
        JobInput { job, name: name.to_string(), id: None }
    }
}

#[derive(Debug, Queryable, Insertable, Identifiable)]
#[table_name = "job_file"]
#[primary_key(job, id)]
pub struct JobFile {
    pub job: JobId,
    pub id: JobFileId,
    pub size: DataSize,
    /**
     * When was this file successfully uploaded to the object store?
     */
    pub time_archived: Option<IsoDate>,
}

#[derive(Debug, Clone, Queryable, Insertable, Identifiable)]
#[table_name = "worker"]
#[primary_key(id)]
pub struct Worker {
    pub id: WorkerId,
    pub bootstrap: String,
    pub token: Option<String>,
    pub factory_private: Option<String>,
    pub deleted: bool,
    pub recycle: bool,
    pub lastping: Option<IsoDate>,
    pub factory: Option<FactoryId>,
    pub target: Option<TargetId>,
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

    pub fn age(&self) -> Duration {
        Utc::now()
            .signed_duration_since(self.id.datetime())
            .to_std()
            .unwrap_or_else(|_| Duration::from_secs(0))
    }

    pub fn factory(&self) -> FactoryId {
        self.factory.unwrap_or_else(|| {
            /*
             * XXX No new workers will be created without a factory, but but old
             * records might not have had one.  This is the ID of a made up
             * factory that does not otherwise exist:
             */
            FactoryId::from_str("00E82MSW0000000000000FF000").unwrap()
        })
    }

    pub fn target(&self) -> TargetId {
        self.target.unwrap_or_else(|| {
            /*
             * XXX No new records should be created without a resolved target
             * ID, but old records might not have had one.  This is the ID of
             * the canned "default" target:
             */
            TargetId::from_str("00E82MSW0000000000000TT000").unwrap()
        })
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
    pub waiting: bool,
    pub target_id: Option<TargetId>,
}

impl Job {
    #[allow(dead_code)]
    pub fn time_submit(&self) -> DateTime<Utc> {
        self.id.datetime()
    }

    pub fn target(&self) -> TargetId {
        self.target_id.unwrap_or_else(|| {
            /*
             * XXX No new records should be created without a resolved target
             * ID, but old records might not have had one.  This is the ID of
             * the canned "default" target:
             */
            TargetId::from_str("00E82MSW0000000000000TT000").unwrap()
        })
    }
}

#[derive(Debug, Clone, Queryable, Insertable, Identifiable)]
#[table_name = "factory"]
#[primary_key(id)]
pub struct Factory {
    pub id: FactoryId,
    pub name: String,
    pub token: String,
    pub lastping: Option<IsoDate>,
}

#[derive(Debug, Clone, Queryable, Insertable, Identifiable)]
#[table_name = "target"]
#[primary_key(id)]
pub struct Target {
    pub id: TargetId,
    pub name: String,
    pub desc: String,
    pub redirect: Option<TargetId>,
}
