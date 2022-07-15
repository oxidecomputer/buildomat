/*
 * Copyright 2022 Oxide Computer Company
 */

use super::schema::*;
use chrono::prelude::*;
use diesel::deserialize::FromSql;
use diesel::serialize::ToSql;
use std::str::FromStr;
use std::time::Duration;

use buildomat_common::db::*;
pub use buildomat_common::db::{Dictionary, IsoDate};

integer_new_type!(UnixUid, u32, i32, Integer, diesel::sql_types::Integer);
integer_new_type!(UnixGid, u32, i32, Integer, diesel::sql_types::Integer);
integer_new_type!(DataSize, u64, i64, BigInt, diesel::sql_types::BigInt);

ulid_new_type!(UserId);
ulid_new_type!(JobId);
ulid_new_type!(JobFileId);
ulid_new_type!(TaskId);
ulid_new_type!(WorkerId);
ulid_new_type!(FactoryId);
ulid_new_type!(TargetId);

#[derive(Debug, Queryable, Insertable, Identifiable)]
#[diesel(table_name = user)]
#[diesel(primary_key(id))]
pub struct User {
    pub id: UserId,
    pub name: String,
    pub token: String,
    pub time_create: IsoDate,
}

#[derive(Debug, Queryable, Insertable, Identifiable)]
#[diesel(table_name = user_privilege)]
#[diesel(primary_key(user, privilege))]
pub struct Privilege {
    pub user: UserId,
    pub privilege: String,
}

#[derive(Debug)]
pub struct AuthUser {
    pub user: User,
    pub privileges: Vec<String>,
}

impl AuthUser {
    pub fn has_privilege(&self, privilege: &str) -> bool {
        self.privileges.iter().any(|s| privilege == s)
    }
}

impl std::ops::Deref for AuthUser {
    type Target = User;

    fn deref(&self) -> &Self::Target {
        &self.user
    }
}

#[derive(Debug, Queryable, Insertable, Identifiable)]
#[diesel(table_name = task)]
#[diesel(primary_key(job, seq))]
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
            user_id: ct.user_id.map(UnixUid),
            group_id: ct.group_id.map(UnixGid),
            workdir: ct.workdir.clone(),
            complete: false,
            failed: false,
        }
    }
}

#[derive(Debug, Clone, Queryable, Insertable, Identifiable)]
#[diesel(table_name = job_event)]
#[diesel(primary_key(job, seq))]
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
        self.time.age()
    }
}

#[derive(Debug, Queryable, Insertable, Identifiable)]
#[diesel(table_name = job_output_rule)]
#[diesel(primary_key(job, seq))]
pub struct JobOutputRule {
    pub job: JobId,
    pub seq: i32,
    pub rule: String,
    pub ignore: bool,
    pub size_change_ok: bool,
    pub require_match: bool,
}

impl JobOutputRule {
    pub fn from_create(
        cd: &super::CreateOutputRule,
        job: JobId,
        seq: usize,
    ) -> JobOutputRule {
        JobOutputRule {
            job,
            seq: seq.try_into().unwrap(),
            rule: cd.rule.to_string(),
            ignore: cd.ignore,
            size_change_ok: cd.size_change_ok,
            require_match: cd.require_match,
        }
    }
}

#[derive(Debug, Queryable, Insertable, Identifiable)]
#[diesel(table_name = job_output)]
#[diesel(primary_key(job, path))]
pub struct JobOutput {
    pub job: JobId,
    pub path: String,
    pub id: JobFileId,
}

#[derive(Debug, Queryable, Insertable, Identifiable)]
#[diesel(table_name = job_input)]
#[diesel(primary_key(job, name))]
pub struct JobInput {
    pub job: JobId,
    pub name: String,
    pub id: Option<JobFileId>,
    pub other_job: Option<JobId>,
}

impl JobInput {
    pub fn from_create(name: &str, job: JobId) -> JobInput {
        JobInput { job, name: name.to_string(), id: None, other_job: None }
    }
}

#[derive(Debug, Queryable, Insertable, Identifiable)]
#[diesel(table_name = job_file)]
#[diesel(primary_key(job, id))]
pub struct JobFile {
    pub job: JobId,
    pub id: JobFileId,
    pub size: DataSize,
    /**
     * When was this file successfully uploaded to the object store?
     */
    pub time_archived: Option<IsoDate>,
}

#[derive(Debug, Queryable, Insertable, Identifiable)]
#[diesel(table_name = published_file)]
#[diesel(primary_key(owner, series, version, name))]
pub struct PublishedFile {
    pub owner: UserId,
    pub series: String,
    pub version: String,
    pub name: String,
    pub job: JobId,
    pub file: JobFileId,
}

#[derive(Debug, Clone, Queryable, Insertable, Identifiable)]
#[diesel(table_name = worker)]
#[diesel(primary_key(id))]
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
    pub wait_for_flush: bool,
}

impl Worker {
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
#[diesel(table_name = job)]
#[diesel(primary_key(id))]
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
    pub cancelled: bool,
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
#[diesel(table_name = factory)]
#[diesel(primary_key(id))]
pub struct Factory {
    pub id: FactoryId,
    pub name: String,
    pub token: String,
    pub lastping: Option<IsoDate>,
}

#[derive(Debug, Clone, Queryable, Insertable, Identifiable)]
#[diesel(table_name = target)]
#[diesel(primary_key(id))]
pub struct Target {
    pub id: TargetId,
    pub name: String,
    pub desc: String,
    pub redirect: Option<TargetId>,
    pub privilege: Option<String>,
}

#[derive(Debug, Clone, Queryable, Insertable, Identifiable)]
#[diesel(table_name = job_depend)]
#[diesel(primary_key(job, name))]
pub struct JobDepend {
    pub job: JobId,
    pub name: String,
    pub prior_job: JobId,
    pub copy_outputs: bool,
    pub on_failed: bool,
    pub on_completed: bool,
    pub satisfied: bool,
}

impl JobDepend {
    pub fn from_create(cd: &super::CreateDepend, job: JobId) -> JobDepend {
        JobDepend {
            job,
            name: cd.name.to_string(),
            prior_job: cd.prior_job,
            copy_outputs: cd.copy_outputs,
            on_failed: cd.on_failed,
            on_completed: cd.on_completed,
            satisfied: false,
        }
    }
}

#[derive(Debug, Clone, Queryable, Insertable, Identifiable)]
#[diesel(table_name = job_time)]
#[diesel(primary_key(job, name))]
pub struct JobTime {
    pub job: JobId,
    pub name: String,
    pub time: IsoDate,
}
