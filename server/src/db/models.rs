/*
 * Copyright 2023 Oxide Computer Company
 */

use anyhow::Result;
use buildomat_types::metadata;
use chrono::prelude::*;
use rusqlite::Row;
use sea_query::{enum_def, ColumnRef, Iden, IdenStatic, SeaRc};
use std::str::FromStr;
use std::time::Duration;

use buildomat_database::sqlite::rusqlite;
pub use buildomat_database::sqlite::{Dictionary, IsoDate, JsonValue};
use buildomat_database::{
    sqlite_integer_new_type, sqlite_json_new_type, sqlite_ulid_new_type,
};

sqlite_integer_new_type!(UnixUid, u32, i32);
sqlite_integer_new_type!(UnixGid, u32, i32);
sqlite_integer_new_type!(DataSize, u64, i64);

sqlite_ulid_new_type!(UserId);
sqlite_ulid_new_type!(JobId);
sqlite_ulid_new_type!(JobFileId);
sqlite_ulid_new_type!(TaskId);
sqlite_ulid_new_type!(WorkerId);
sqlite_ulid_new_type!(FactoryId);
sqlite_ulid_new_type!(TargetId);

use std::convert::TryFrom;

pub trait FromRow: Sized {
    fn columns() -> Vec<ColumnRef>;
    fn from_row(row: &Row) -> rusqlite::Result<Self>;
}

#[derive(Debug)]
//#[diesel(table_name = user)]
//#[diesel(primary_key(id))]
pub struct User {
    pub id: UserId,
    pub name: String,
    pub token: String,
    pub time_create: IsoDate,
}

#[derive(Debug)]
//#[diesel(table_name = user_privilege)]
//#[diesel(primary_key(user, privilege))]
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

#[derive(Iden)]
pub enum JobTagDef {
    Table,
    Job,
    Name,
    Value,
}

#[derive(Debug)]
//#[diesel(table_name = task)]
//#[diesel(primary_key(job, seq))]
#[enum_def(prefix = "", suffix = "Def")]
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

impl FromRow for Task {
    fn columns() -> Vec<ColumnRef> {
        [
            TaskDef::Job,
            TaskDef::Seq,
            TaskDef::Name,
            TaskDef::Script,
            TaskDef::EnvClear,
            TaskDef::Env,
            TaskDef::UserId,
            TaskDef::GroupId,
            TaskDef::Workdir,
            TaskDef::Complete,
            TaskDef::Failed,
        ]
        .into_iter()
        .map(|col| {
            ColumnRef::TableColumn(SeaRc::new(TaskDef::Table), SeaRc::new(col))
        })
        .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<Task> {
        todo!()
        //let s = TaskDef::Token.as_str();
    }
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

#[derive(Debug, Clone)]
//#[diesel(table_name = job_event)]
//#[diesel(primary_key(job, seq))]
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

#[derive(Debug)]
//#[diesel(table_name = job_output_rule)]
//#[diesel(primary_key(job, seq))]
#[enum_def(prefix = "", suffix = "Def")]
pub struct JobOutputRule {
    pub job: JobId,
    pub seq: i32,
    pub rule: String,
    pub ignore: bool,
    pub size_change_ok: bool,
    pub require_match: bool,
}

impl FromRow for JobOutputRule {
    fn columns() -> Vec<ColumnRef> {
        [
            JobOutputRuleDef::Job,
            JobOutputRuleDef::Seq,
            JobOutputRuleDef::Rule,
            JobOutputRuleDef::Ignore,
            JobOutputRuleDef::SizeChangeOk,
            JobOutputRuleDef::RequireMatch,
        ]
        .into_iter()
        .map(|col| {
            ColumnRef::TableColumn(
                SeaRc::new(JobOutputRuleDef::Table),
                SeaRc::new(col),
            )
        })
        .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<JobOutputRule> {
        todo!()
        //let s = JobOutputRuleDef::Token.as_str();
    }
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

#[derive(Debug)]
//#[diesel(table_name = job_output)]
//#[diesel(primary_key(job, path))]
pub struct JobOutput {
    pub job: JobId,
    pub path: String,
    pub id: JobFileId,
}

#[derive(Debug)]
//#[diesel(table_name = job_input)]
//#[diesel(primary_key(job, name))]
pub struct JobInput {
    pub job: JobId,
    pub name: String,
    pub id: Option<JobFileId>,
    /**
     * Files are identified by a (job ID, file ID) tuple.  In the case of an
     * output that is copied to another job as an input, this field contains the
     * job ID of the job which actually holds the file.  If this is not set, the
     * file is an input that was uploaded directly by the user creating the job
     * and is stored with the job that owns the input record.
     */
    pub other_job: Option<JobId>,
}

impl JobInput {
    pub fn from_create(name: &str, job: JobId) -> JobInput {
        JobInput { job, name: name.to_string(), id: None, other_job: None }
    }
}

#[derive(Debug)]
//#[diesel(table_name = job_file)]
//#[diesel(primary_key(job, id))]
pub struct JobFile {
    pub job: JobId,
    pub id: JobFileId,
    pub size: DataSize,
    /**
     * When was this file successfully uploaded to the object store?
     */
    pub time_archived: Option<IsoDate>,
}

#[derive(Debug)]
//#[diesel(table_name = published_file)]
//#[diesel(primary_key(owner, series, version, name))]
pub struct PublishedFile {
    pub owner: UserId,
    pub series: String,
    pub version: String,
    pub name: String,
    pub job: JobId,
    pub file: JobFileId,
}

#[derive(Debug, Clone)]
//#[diesel(table_name = worker)]
//#[diesel(primary_key(id))]
#[enum_def(prefix = "", suffix = "Def")]
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
    pub factory_metadata: Option<JsonValue>,
}

impl FromRow for Worker {
    fn columns() -> Vec<ColumnRef> {
        [
            WorkerDef::Id,
            WorkerDef::Bootstrap,
            WorkerDef::Token,
            WorkerDef::FactoryPrivate,
            WorkerDef::Deleted,
            WorkerDef::Recycle,
            WorkerDef::Lastping,
            WorkerDef::Factory,
            WorkerDef::Target,
            WorkerDef::WaitForFlush,
            WorkerDef::FactoryMetadata,
        ]
        .into_iter()
        .map(|col| {
            ColumnRef::TableColumn(
                SeaRc::new(WorkerDef::Table),
                SeaRc::new(col),
            )
        })
        .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<Worker> {
        todo!()
        //let s = WorkerDef::Token.as_str();
    }
}

impl Worker {
    pub fn legacy_default_factory_id() -> FactoryId {
        /*
         * XXX No new workers will be created without a factory, but old records
         * might not have had one.  This is the ID of a made up factory that
         * does not otherwise exist:
         */
        FactoryId::from_str("00E82MSW0000000000000FF000").unwrap()
    }

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
        self.factory.unwrap_or_else(Worker::legacy_default_factory_id)
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

    pub fn factory_metadata(
        &self,
    ) -> Result<Option<metadata::FactoryMetadata>> {
        self.factory_metadata
            .as_ref()
            .map(|j| Ok(serde_json::from_value(j.0.clone())?))
            .transpose()
    }
}

#[derive(Clone, Debug)]
//#[diesel(table_name = job)]
//#[diesel(primary_key(id))]
#[enum_def(prefix = "", suffix = "Def")]
pub struct Job {
    pub id: JobId,
    pub owner: UserId,
    pub name: String,
    /**
     * The original target name specified by the user, prior to resolution.
     */
    pub target: String,
    pub complete: bool,
    pub failed: bool,
    pub worker: Option<WorkerId>,
    pub waiting: bool,
    /**
     * The resolved target ID.  This field should be accessed through the
     * target() method to account for old records where this ID was optional.
     */
    pub target_id: Option<TargetId>,
    pub cancelled: bool,
    /**
     * When was this job successfully uploaded to the object store?
     */
    pub time_archived: Option<IsoDate>,
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

    pub fn is_archived(&self) -> bool {
        self.time_archived.is_some()
    }
}

impl FromRow for Job {
    fn columns() -> Vec<ColumnRef> {
        [
            JobDef::Id,
            JobDef::Owner,
            JobDef::Name,
            JobDef::Target,
            JobDef::Complete,
            JobDef::Failed,
            JobDef::Worker,
            JobDef::Waiting,
            JobDef::TargetId,
            JobDef::Cancelled,
            JobDef::TimeArchived,
        ]
        .into_iter()
        .map(|col| {
            ColumnRef::TableColumn(SeaRc::new(JobDef::Table), SeaRc::new(col))
        })
        .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<Job> {
        todo!()
        //let s = WorkerDef::Token.as_str();
    }
}

#[derive(Debug, Clone)]
//#[diesel(table_name = factory)]
//#[diesel(primary_key(id))]
pub struct Factory {
    pub id: FactoryId,
    pub name: String,
    pub token: String,
    pub lastping: Option<IsoDate>,
}

#[derive(Debug, Clone)]
//#[diesel(table_name = target)]
//#[diesel(primary_key(id))]
pub struct Target {
    pub id: TargetId,
    pub name: String,
    pub desc: String,
    pub redirect: Option<TargetId>,
    pub privilege: Option<String>,
}

#[derive(Debug, Clone)]
//#[diesel(table_name = job_depend)]
//#[diesel(primary_key(job, name))]
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

#[derive(Debug, Clone)]
//#[diesel(table_name = job_time)]
//#[diesel(primary_key(job, name))]
pub struct JobTime {
    pub job: JobId,
    pub name: String,
    pub time: IsoDate,
}

#[derive(Debug, Clone)]
//#[diesel(table_name = job_store)]
//#[diesel(primary_key(job, name))]
pub struct JobStore {
    pub job: JobId,
    pub name: String,
    pub value: String,
    pub secret: bool,
    pub source: String,
    pub time_update: IsoDate,
}
