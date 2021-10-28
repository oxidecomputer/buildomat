use super::schema::*;
use chrono::prelude::*;
use diesel::deserialize::FromSql;
use diesel::serialize::ToSql;
use std::str::FromStr;

use buildomat_common::db::*;
pub use buildomat_common::db::{Dictionary, IsoDate};

integer_new_type!(UnixUid, u32, i32, Integer, "diesel::sql_types::Integer");
integer_new_type!(UnixGid, u32, i32, Integer, "diesel::sql_types::Integer");
integer_new_type!(DataSize, u64, i64, BigInt, "diesel::sql_types::BigInt");

ulid_new_type!(UserId);
ulid_new_type!(JobId);
ulid_new_type!(JobOutputId);
ulid_new_type!(TaskId);
ulid_new_type!(WorkerId);

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
