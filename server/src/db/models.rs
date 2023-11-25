/*
 * Copyright 2023 Oxide Computer Company
 */

use anyhow::Result;
use buildomat_types::metadata;
use chrono::prelude::*;
use rusqlite::Row;
use sea_query::{
    enum_def, ColumnRef, Expr, Iden, IdenStatic, InsertStatement, OnConflict,
    Query, SeaRc, SelectStatement, SimpleExpr, Value,
};
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

#[derive(Debug)]
//#[diesel(table_name = user)]
//#[diesel(primary_key(id))]
#[enum_def(prefix = "", suffix = "Def")]
pub struct User {
    pub id: UserId,
    pub name: String,
    pub token: String,
    pub time_create: IsoDate,
}

impl FromRow for User {
    fn columns() -> Vec<ColumnRef> {
        [UserDef::Id, UserDef::Name, UserDef::Token, UserDef::TimeCreate]
            .into_iter()
            .map(|col| {
                ColumnRef::TableColumn(
                    SeaRc::new(UserDef::Table),
                    SeaRc::new(col),
                )
            })
            .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<User> {
        todo!()
        //let s = UserDef::Token.as_str();
    }
}

impl User {
    pub fn find(id: UserId) -> SelectStatement {
        Query::select()
            .from(UserDef::Table)
            .columns(User::columns())
            .and_where(Expr::col(UserDef::Id).eq(id))
            .to_owned()
    }

    pub fn insert(&self) -> InsertStatement {
        Query::insert()
            .into_table(JobDef::Table)
            .columns(Self::bare_columns())
            .values_panic([
                self.id.into(),
                self.name.clone().into(),
                self.token.clone().into(),
                self.time_create.into(),
            ])
            .to_owned()
    }
}

#[derive(Debug)]
//#[diesel(table_name = user_privilege)]
//#[diesel(primary_key(user, privilege))]
#[enum_def(prefix = "", suffix = "Def")]
pub struct UserPrivilege {
    pub user: UserId,
    pub privilege: String,
}

impl UserPrivilege {
    pub fn upsert(&self) -> InsertStatement {
        Query::insert()
            .into_table(UserPrivilegeDef::Table)
            .columns([UserPrivilegeDef::User, UserPrivilegeDef::Privilege])
            .values_panic([self.user.into(), self.privilege.clone().into()])
            .on_conflict(OnConflict::new().do_nothing().to_owned())
            .to_owned()
    }
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

/*
 * This table doesn't have its own model struct:
 */
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

    pub fn find(job: JobId, seq: u32) -> SelectStatement {
        Query::select()
            .from(TaskDef::Table)
            .columns(Task::columns())
            .and_where(Expr::col(TaskDef::Job).eq(job))
            .and_where(Expr::col(TaskDef::Seq).eq(seq))
            .to_owned()
    }

    pub fn insert(&self) -> InsertStatement {
        Query::insert()
            .into_table(TaskDef::Table)
            .columns(Self::bare_columns())
            .values_panic([
                self.job.into(),
                self.seq.into(),
                self.name.clone().into(),
                self.script.clone().into(),
                self.env_clear.into(),
                self.env.clone().into(),
                self.user_id.into(),
                self.group_id.into(),
                self.workdir.clone().into(),
                self.complete.into(),
                self.failed.into(),
            ])
            .to_owned()
    }
}

#[derive(Debug, Clone)]
//#[diesel(table_name = job_event)]
//#[diesel(primary_key(job, seq))]
#[enum_def(prefix = "", suffix = "Def")]
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

impl FromRow for JobEvent {
    fn columns() -> Vec<ColumnRef> {
        [
            JobEventDef::Job,
            JobEventDef::Task,
            JobEventDef::Seq,
            JobEventDef::Stream,
            JobEventDef::Time,
            JobEventDef::Payload,
            JobEventDef::TimeRemote,
        ]
        .into_iter()
        .map(|col| {
            ColumnRef::TableColumn(
                SeaRc::new(JobEventDef::Table),
                SeaRc::new(col),
            )
        })
        .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<JobEvent> {
        todo!()
        //let s = TaskDef::Token.as_str();
    }
}

impl JobEvent {
    pub fn insert(&self) -> InsertStatement {
        Query::insert()
            .into_table(JobEventDef::Table)
            .columns(Self::bare_columns())
            .values_panic([
                self.job.into(),
                self.task.into(),
                self.seq.into(),
                self.stream.clone().into(),
                self.time.into(),
                self.payload.clone().into(),
                self.time_remote.into(),
            ])
            .to_owned()
    }

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

    pub fn insert(&self) -> InsertStatement {
        Query::insert()
            .into_table(JobOutputRuleDef::Table)
            .columns(Self::bare_columns())
            .values_panic([
                self.job.into(),
                self.seq.into(),
                self.rule.clone().into(),
                self.ignore.into(),
                self.size_change_ok.into(),
                self.require_match.into(),
            ])
            .to_owned()
    }
}

#[derive(Debug)]
//#[diesel(table_name = job_output)]
//#[diesel(primary_key(job, path))]
#[enum_def(prefix = "", suffix = "Def")]
pub struct JobOutput {
    pub job: JobId,
    pub path: String,
    pub id: JobFileId,
}

impl FromRow for JobOutput {
    fn columns() -> Vec<ColumnRef> {
        [JobOutputDef::Job, JobOutputDef::Path, JobOutputDef::Id]
            .into_iter()
            .map(|col| {
                ColumnRef::TableColumn(
                    SeaRc::new(JobOutputDef::Table),
                    SeaRc::new(col),
                )
            })
            .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<JobOutput> {
        todo!()
        //let s = JobOutputDef::Token.as_str();
    }
}

impl JobOutput {
    pub fn find(job: JobId, file: JobFileId) -> SelectStatement {
        Query::select()
            .from(JobOutputDef::Table)
            .columns(JobOutput::columns())
            .and_where(Expr::col(JobOutputDef::Job).eq(job))
            .and_where(Expr::col(JobOutputDef::Id).eq(file))
            .to_owned()
    }

    pub fn insert(&self) -> InsertStatement {
        Query::insert()
            .into_table(JobOutputDef::Table)
            .columns(Self::bare_columns())
            .values_panic([
                self.job.into(),
                self.path.clone().into(),
                self.id.into(),
            ])
            .to_owned()
    }
}

#[derive(Debug)]
//#[diesel(table_name = job_input)]
//#[diesel(primary_key(job, name))]
#[enum_def(prefix = "", suffix = "Def")]
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

impl FromRow for JobInput {
    fn columns() -> Vec<ColumnRef> {
        [
            JobInputDef::Job,
            JobInputDef::Name,
            JobInputDef::Id,
            JobInputDef::OtherJob,
        ]
        .into_iter()
        .map(|col| {
            ColumnRef::TableColumn(
                SeaRc::new(JobInputDef::Table),
                SeaRc::new(col),
            )
        })
        .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<JobInput> {
        todo!()
        //let s = JobInputDef::Token.as_str();
    }
}

impl JobInput {
    pub fn insert(&self) -> InsertStatement {
        Query::insert()
            .into_table(JobInputDef::Table)
            .columns(Self::bare_columns())
            .values_panic([
                self.job.into(),
                self.name.clone().into(),
                self.id.into(),
                self.other_job.into(),
            ])
            .to_owned()
    }

    pub fn upsert(&self) -> InsertStatement {
        self.insert()
            .on_conflict(OnConflict::new().do_nothing().to_owned())
            .to_owned()
    }

    pub fn from_create(name: &str, job: JobId) -> JobInput {
        JobInput { job, name: name.to_string(), id: None, other_job: None }
    }
}

#[derive(Debug)]
//#[diesel(table_name = job_file)]
//#[diesel(primary_key(job, id))]
#[enum_def(prefix = "", suffix = "Def")]
pub struct JobFile {
    pub job: JobId,
    pub id: JobFileId,
    pub size: DataSize,
    /**
     * When was this file successfully uploaded to the object store?
     */
    pub time_archived: Option<IsoDate>,
}

impl FromRow for JobFile {
    fn columns() -> Vec<ColumnRef> {
        [
            JobFileDef::Job,
            JobFileDef::Id,
            JobFileDef::Size,
            JobFileDef::TimeArchived,
        ]
        .into_iter()
        .map(|col| {
            ColumnRef::TableColumn(
                SeaRc::new(JobFileDef::Table),
                SeaRc::new(col),
            )
        })
        .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<JobFile> {
        todo!()
        //let s = JobFileDef::Token.as_str();
    }
}

impl JobFile {
    pub fn find(job: JobId, file: JobFileId) -> SelectStatement {
        Query::select()
            .from(JobFileDef::Table)
            .columns(JobFile::columns())
            .and_where(Expr::col(JobFileDef::Job).eq(job))
            .and_where(Expr::col(JobFileDef::Id).eq(file))
            .to_owned()
    }

    pub fn insert(&self) -> InsertStatement {
        Query::insert()
            .into_table(JobFileDef::Table)
            .columns(Self::bare_columns())
            .values_panic([
                self.job.into(),
                self.id.into(),
                self.size.into(),
                self.time_archived.into(),
            ])
            .to_owned()
    }
}

#[derive(Debug)]
//#[diesel(table_name = published_file)]
//#[diesel(primary_key(owner, series, version, name))]
#[enum_def(prefix = "", suffix = "Def")]
pub struct PublishedFile {
    pub owner: UserId,
    pub series: String,
    pub version: String,
    pub name: String,
    pub job: JobId,
    pub file: JobFileId,
}

impl FromRow for PublishedFile {
    fn columns() -> Vec<ColumnRef> {
        [
            PublishedFileDef::Owner,
            PublishedFileDef::Series,
            PublishedFileDef::Version,
            PublishedFileDef::Name,
            PublishedFileDef::Job,
            PublishedFileDef::File,
        ]
        .into_iter()
        .map(|col| {
            ColumnRef::TableColumn(
                SeaRc::new(PublishedFileDef::Table),
                SeaRc::new(col),
            )
        })
        .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<PublishedFile> {
        todo!()
        //let s = PublishedFileDef::Token.as_str();
    }
}

impl PublishedFile {
    pub fn find(
        owner: UserId,
        series: &str,
        version: &str,
        name: &str,
    ) -> SelectStatement {
        Query::select()
            .from(PublishedFileDef::Table)
            .columns(PublishedFile::columns())
            .and_where(Expr::col(PublishedFileDef::Owner).eq(owner))
            .and_where(Expr::col(PublishedFileDef::Series).eq(series))
            .and_where(Expr::col(PublishedFileDef::Version).eq(version))
            .and_where(Expr::col(PublishedFileDef::Name).eq(name))
            .to_owned()
    }

    pub fn insert(&self) -> InsertStatement {
        Query::insert()
            .into_table(PublishedFileDef::Table)
            .columns(Self::bare_columns())
            .values_panic([
                self.owner.into(),
                self.series.clone().into(),
                self.version.clone().into(),
                self.name.clone().into(),
                self.job.into(),
                self.file.into(),
            ])
            .to_owned()
    }
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
    pub fn insert(&self) -> InsertStatement {
        Query::insert()
            .into_table(WorkerDef::Table)
            .columns(Self::bare_columns())
            .values_panic([
                self.id.into(),
                self.bootstrap.clone().into(),
                self.token.clone().into(),
                self.factory_private.clone().into(),
                self.deleted.into(),
                self.recycle.into(),
                self.lastping.into(),
                self.factory.into(),
                self.wait_for_flush.into(),
                self.factory_metadata.clone().into(),
            ])
            .to_owned()
    }

    pub fn find(id: WorkerId) -> SelectStatement {
        Query::select()
            .from(WorkerDef::Table)
            .columns(Worker::columns())
            .and_where(Expr::col(WorkerDef::Id).eq(id))
            .to_owned()
    }

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

    pub fn find(id: JobId) -> SelectStatement {
        Query::select()
            .from(JobDef::Table)
            .columns(Job::columns())
            .and_where(Expr::col(JobDef::Id).eq(id))
            .to_owned()
    }

    pub fn insert(&self) -> InsertStatement {
        Query::insert()
            .into_table(JobDef::Table)
            .columns(Self::bare_columns())
            .values_panic([
                self.id.into(),
                self.owner.into(),
                self.name.clone().into(),
                self.target.clone().into(),
                self.complete.into(),
                self.failed.into(),
                self.worker.into(),
                self.waiting.into(),
                self.target_id.into(),
                self.cancelled.into(),
                self.time_archived.into(),
            ])
            .to_owned()
    }
}

#[derive(Debug, Clone)]
//#[diesel(table_name = factory)]
//#[diesel(primary_key(id))]
#[enum_def(prefix = "", suffix = "Def")]
pub struct Factory {
    pub id: FactoryId,
    pub name: String,
    pub token: String,
    pub lastping: Option<IsoDate>,
}

impl FromRow for Factory {
    fn columns() -> Vec<ColumnRef> {
        [
            FactoryDef::Id,
            FactoryDef::Name,
            FactoryDef::Token,
            FactoryDef::Lastping,
        ]
        .into_iter()
        .map(|col| {
            ColumnRef::TableColumn(
                SeaRc::new(FactoryDef::Table),
                SeaRc::new(col),
            )
        })
        .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<Factory> {
        todo!()
        //let s = WorkerDef::Token.as_str();
    }
}

impl Factory {
    pub fn find(id: FactoryId) -> SelectStatement {
        Query::select()
            .from(FactoryDef::Table)
            .columns(Factory::columns())
            .and_where(Expr::col(FactoryDef::Id).eq(id))
            .to_owned()
    }

    pub fn insert(&self) -> InsertStatement {
        Query::insert()
            .into_table(FactoryDef::Table)
            .columns(Self::bare_columns())
            .values_panic([
                self.id.into(),
                self.name.clone().into(),
                self.token.clone().into(),
                self.lastping.into(),
            ])
            .to_owned()
    }
}

#[derive(Debug, Clone)]
//#[diesel(table_name = target)]
//#[diesel(primary_key(id))]
#[enum_def(prefix = "", suffix = "Def")]
pub struct Target {
    pub id: TargetId,
    pub name: String,
    pub desc: String,
    pub redirect: Option<TargetId>,
    pub privilege: Option<String>,
}

impl FromRow for Target {
    fn columns() -> Vec<ColumnRef> {
        [
            TargetDef::Id,
            TargetDef::Name,
            TargetDef::Desc,
            TargetDef::Redirect,
            TargetDef::Privilege,
        ]
        .into_iter()
        .map(|col| {
            ColumnRef::TableColumn(
                SeaRc::new(TargetDef::Table),
                SeaRc::new(col),
            )
        })
        .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<Target> {
        todo!()
        //let s = WorkerDef::Token.as_str();
    }
}

impl Target {
    pub fn find(id: TargetId) -> SelectStatement {
        Query::select()
            .from(TargetDef::Table)
            .columns(Target::columns())
            .and_where(Expr::col(TargetDef::Id).eq(id))
            .to_owned()
    }

    pub fn find_by_name(name: &str) -> SelectStatement {
        Query::select()
            .from(TargetDef::Table)
            .columns(Target::columns())
            .and_where(Expr::col(TargetDef::Name).eq(name))
            .to_owned()
    }

    pub fn insert(&self) -> InsertStatement {
        Query::insert()
            .into_table(TargetDef::Table)
            .columns(Self::bare_columns())
            .values_panic([
                self.id.into(),
                self.name.clone().into(),
                self.desc.clone().into(),
                self.redirect.into(),
                self.privilege.clone().into(),
            ])
            .to_owned()
    }
}

#[derive(Debug, Clone)]
//#[diesel(table_name = job_depend)]
//#[diesel(primary_key(job, name))]
#[enum_def(prefix = "", suffix = "Def")]
pub struct JobDepend {
    pub job: JobId,
    pub name: String,
    pub prior_job: JobId,
    pub copy_outputs: bool,
    pub on_failed: bool,
    pub on_completed: bool,
    pub satisfied: bool,
}

impl FromRow for JobDepend {
    fn columns() -> Vec<ColumnRef> {
        [
            JobDependDef::Job,
            JobDependDef::Name,
            JobDependDef::PriorJob,
            JobDependDef::CopyOutputs,
            JobDependDef::OnFailed,
            JobDependDef::OnCompleted,
            JobDependDef::Satisfied,
        ]
        .into_iter()
        .map(|col| {
            ColumnRef::TableColumn(
                SeaRc::new(JobDependDef::Table),
                SeaRc::new(col),
            )
        })
        .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<JobDepend> {
        todo!()
        //let s = WorkerDef::Token.as_str();
    }
}

impl JobDepend {
    pub fn find(job: JobId, name: &str) -> SelectStatement {
        Query::select()
            .from(JobDependDef::Table)
            .columns(JobDepend::columns())
            .and_where(Expr::col(JobDependDef::Job).eq(job))
            .and_where(Expr::col(JobDependDef::Name).eq(name))
            .to_owned()
    }

    pub fn insert(&self) -> InsertStatement {
        Query::insert()
            .into_table(JobDependDef::Table)
            .columns(Self::bare_columns())
            .values_panic([
                self.job.into(),
                self.name.clone().into(),
                self.prior_job.into(),
                self.copy_outputs.into(),
                self.on_failed.into(),
                self.on_completed.into(),
                self.satisfied.into(),
            ])
            .to_owned()
    }

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
#[enum_def(prefix = "", suffix = "Def")]
pub struct JobTime {
    pub job: JobId,
    pub name: String,
    pub time: IsoDate,
}

impl FromRow for JobTime {
    fn columns() -> Vec<ColumnRef> {
        [JobTimeDef::Job, JobTimeDef::Name, JobTimeDef::Time]
            .into_iter()
            .map(|col| {
                ColumnRef::TableColumn(
                    SeaRc::new(JobTimeDef::Table),
                    SeaRc::new(col),
                )
            })
            .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<JobTime> {
        todo!()
        //let s = WorkerDef::Token.as_str();
    }
}

impl JobTime {
    pub fn upsert(&self) -> InsertStatement {
        Query::insert()
            .into_table(JobTimeDef::Table)
            .columns(Self::bare_columns())
            .values_panic([
                self.job.into(),
                self.name.clone().into(),
                self.time.into(),
            ])
            .on_conflict(OnConflict::new().do_nothing().to_owned())
            .to_owned()
    }

    pub fn find(job: JobId, name: &str) -> SelectStatement {
        Query::select()
            .from(JobTimeDef::Table)
            .columns(JobTime::columns())
            .and_where(Expr::col(JobTimeDef::Job).eq(job))
            .and_where(Expr::col(JobTimeDef::Name).eq(name))
            .to_owned()
    }
}

#[derive(Debug, Clone)]
//#[diesel(table_name = job_store)]
//#[diesel(primary_key(job, name))]
#[enum_def(prefix = "", suffix = "Def")]
pub struct JobStore {
    pub job: JobId,
    pub name: String,
    pub value: String,
    pub secret: bool,
    pub source: String,
    pub time_update: IsoDate,
}

impl FromRow for JobStore {
    fn columns() -> Vec<ColumnRef> {
        [
            JobStoreDef::Job,
            JobStoreDef::Name,
            JobStoreDef::Value,
            JobStoreDef::Secret,
            JobStoreDef::Source,
            JobStoreDef::TimeUpdate,
        ]
        .into_iter()
        .map(|col| {
            ColumnRef::TableColumn(
                SeaRc::new(JobStoreDef::Table),
                SeaRc::new(col),
            )
        })
        .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<JobStore> {
        todo!()
        //let s = WorkerDef::Token.as_str();
    }
}

impl JobStore {
    pub fn find(job: JobId, name: &str) -> SelectStatement {
        Query::select()
            .from(JobStoreDef::Table)
            .columns(JobStore::columns())
            .and_where(Expr::col(JobStoreDef::Job).eq(job))
            .and_where(Expr::col(JobStoreDef::Name).eq(name))
            .to_owned()
    }

    pub fn insert(&self) -> InsertStatement {
        Query::insert()
            .into_table(JobStoreDef::Table)
            .columns(Self::bare_columns())
            .values_panic([
                self.job.into(),
                self.name.clone().into(),
                self.value.clone().into(),
                self.secret.into(),
                self.source.clone().into(),
                self.time_update.into(),
            ])
            .to_owned()
    }
}

/*
 * This implementation allows us to use the existing tx_get_row() routine to
 * fish out a MAX() value for the task "seq" column.
 */
impl FromRow for Option<i32> {
    fn columns() -> Vec<ColumnRef> {
        Default::default()
    }

    fn from_row(row: &Row) -> rusqlite::Result<Option<i32>> {
        Ok(row.get(0)?)
    }
}

/*
 * Joins are a bit of a mess, so produced some helper implementations for pairs
 * of objects we need to return:
 */
impl FromRow for (JobInput, Option<JobFile>) {
    fn columns() -> Vec<ColumnRef> {
        JobInput::columns()
            .into_iter()
            .chain(JobFile::columns().into_iter())
            .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<Self> {
        todo!()
    }
}

impl FromRow for (JobOutput, JobFile) {
    fn columns() -> Vec<ColumnRef> {
        JobOutput::columns()
            .into_iter()
            .chain(JobFile::columns().into_iter())
            .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<Self> {
        todo!()
    }
}
