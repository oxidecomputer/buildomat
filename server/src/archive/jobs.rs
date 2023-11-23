/*
 * Copyright 2023 Oxide Computer Company
 */

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};
use chrono::prelude::*;
use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use slog::{debug, error, info, warn, Logger};

use crate::{db, Central};

trait FromArchiveDate {
    fn restore_from_archive(&self) -> Result<db::IsoDate>;
}

impl FromArchiveDate for String {
    fn restore_from_archive(&self) -> Result<db::IsoDate> {
        Ok(db::IsoDate(DateTime::from(DateTime::parse_from_rfc3339(self)?)))
    }
}

trait ToArchiveDate {
    fn to_archive(&self) -> String;
}

impl ToArchiveDate for db::IsoDate {
    fn to_archive(&self) -> String {
        self.0.to_archive()
    }
}

impl ToArchiveDate for DateTime<Utc> {
    fn to_archive(&self) -> String {
        self.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ArchivedTask {
    pub seq: u32,
    pub name: String,
    pub script: String,
    pub env_clear: bool,
    pub env: HashMap<String, String>,
    pub user_id: Option<u32>,
    pub group_id: Option<u32>,
    pub workdir: Option<String>,
    pub complete: bool,
    pub failed: bool,
}

impl From<db::Task> for ArchivedTask {
    fn from(input: db::Task) -> Self {
        let db::Task {
            job: _,
            /*
             * For most records with a seq column the actual value is not
             * relevant, merely the sorted order of those records.  In the case
             * of tasks, seq is used in the task ID field of job events that
             * were emitted by a particular task.  We want to make sure they
             * always line up.
             */
            seq,
            name,
            script,
            env_clear,
            env,
            user_id,
            group_id,
            workdir,
            complete,
            failed,
        } = input;

        ArchivedTask {
            seq: seq.try_into().unwrap(),
            name,
            script,
            env_clear,
            env: env.0,
            user_id: user_id.map(|i| i.0),
            group_id: group_id.map(|i| i.0),
            workdir,
            complete,
            failed,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ArchivedEvent {
    pub task: Option<u32>,
    pub stream: String,
    /**
     * The time at which the core API server received or generated this event.
     */
    pub time: String,
    /**
     * If this event was received from a remote system, such as a worker, this
     * time reflects the time on that remote system when the event was
     * generated.  Due to issues with NTP, etc, it might not align exactly with
     * the time field.
     */
    pub time_remote: Option<String>,
    pub payload: String,
}

impl From<db::JobEvent> for ArchivedEvent {
    fn from(input: db::JobEvent) -> Self {
        let db::JobEvent {
            job: _,
            task,
            /*
             * Job events in the database have a "seq" column that reflects the
             * order that they were recorded by the system.  job_events()
             * returns them in ascending order.  We store the records in that
             * same order in the archived file, but elide the "seq" column.
             */
            seq: _,
            stream,
            time,
            payload,
            time_remote,
        } = input;

        ArchivedEvent {
            task: task.map(|i| i.try_into().unwrap()),
            stream,
            time: time.to_archive(),
            time_remote: time_remote.map(|t| t.to_archive()),
            payload,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ArchivedOutput {
    pub path: String,
    pub file: ArchivedFile,
}

impl TryFrom<(db::JobOutput, db::JobFile)> for ArchivedOutput {
    type Error = anyhow::Error;

    fn try_from(input: (db::JobOutput, db::JobFile)) -> Result<Self> {
        let db::JobOutput { job: _, id: _, path } = input.0;

        Ok(ArchivedOutput { path, file: input.1.try_into()? })
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ArchivedInput {
    pub name: String,
    pub file: Option<ArchivedFile>,
    pub other_job_id: Option<String>,
}

impl TryFrom<(db::JobInput, Option<db::JobFile>)> for ArchivedInput {
    type Error = anyhow::Error;

    fn try_from(input: (db::JobInput, Option<db::JobFile>)) -> Result<Self> {
        let db::JobInput { job: _, id: _, name, other_job } = input.0;

        Ok(ArchivedInput {
            name,
            file: input.1.map(ArchivedFile::try_from).transpose()?,
            other_job_id: other_job.map(|i| i.to_string()),
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ArchivedFile {
    job_id: String,
    id: String,
    size: u64,
    time_archived: String,
}

impl ArchivedFile {
    fn job(&self) -> Result<db::JobId> {
        Ok(self.job_id.parse()?)
    }

    fn id(&self) -> Result<db::JobFileId> {
        Ok(self.id.parse()?)
    }

    fn time_archived(&self) -> Result<db::IsoDate> {
        self.time_archived.restore_from_archive()
    }
}

impl TryFrom<db::JobFile> for ArchivedFile {
    type Error = anyhow::Error;

    fn try_from(input: db::JobFile) -> Result<Self> {
        let db::JobFile { job, id, size, time_archived } = input;

        let Some(time_archived) = time_archived else {
            bail!("job file not yet archived");
        };

        Ok(ArchivedFile {
            job_id: job.to_string(),
            id: id.to_string(),
            size: size.0,
            time_archived: time_archived.to_archive(),
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ArchivedOutputRule {
    pub rule: String,
    pub ignore: bool,
    pub size_change_ok: bool,
    pub require_match: bool,
}

impl From<db::JobOutputRule> for ArchivedOutputRule {
    fn from(input: db::JobOutputRule) -> Self {
        let db::JobOutputRule {
            job: _,
            /*
             * Output rules in the database have a "seq" column that reflects
             * the order that they were specified in the job specification, and
             * job_output_rules() returns them in ascending order.  We store the
             * records in that same order in the archived file, but elide the
             * "seq" column.
             */
            seq: _,
            rule,
            ignore,
            size_change_ok,
            require_match,
        } = input;

        ArchivedOutputRule { rule, ignore, size_change_ok, require_match }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ArchivedStoreEntry {
    name: String,
    value: Option<String>,
    secret: bool,
    source: String,
    time_update: String,
}

impl ArchivedStoreEntry {
    pub fn value(&self) -> Option<&str> {
        self.value.as_deref()
    }

    pub fn secret(&self) -> bool {
        self.secret
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    pub fn time_update(&self) -> Result<db::IsoDate> {
        self.time_update.restore_from_archive()
    }
}

impl From<db::JobStore> for ArchivedStoreEntry {
    fn from(input: db::JobStore) -> Self {
        let db::JobStore { job: _, name, value, secret, source, time_update } =
            input;

        ArchivedStoreEntry {
            name,
            /*
             * Elide secret values from the archive:
             */
            value: if secret { Some(value) } else { None },
            secret,
            source,
            time_update: time_update.to_archive(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ArchivedDepend {
    prior_job: String,
    copy_outputs: bool,
    on_failed: bool,
    on_completed: bool,
    satisfied: bool,
}

impl From<db::JobDepend> for ArchivedDepend {
    fn from(input: db::JobDepend) -> Self {
        let db::JobDepend {
            job: _,
            name: _,
            prior_job,
            copy_outputs,
            on_failed,
            on_completed,
            satisfied,
        } = input;

        ArchivedDepend {
            prior_job: prior_job.to_string(),
            copy_outputs,
            on_failed,
            on_completed,
            satisfied,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ArchivedFactoryInfo {
    id: String,
    name: String,
}

impl From<db::Factory> for ArchivedFactoryInfo {
    fn from(input: db::Factory) -> Self {
        let db::Factory { id, name, token: _, lastping: _ } = input;

        ArchivedFactoryInfo { id: id.to_string(), name }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ArchivedWorkerInfo {
    id: String,
    target: String,
    factory: ArchivedFactoryInfo,
    factory_private: Option<String>,
    factory_metadata: Option<serde_json::Value>,
}

impl From<(db::Worker, db::Factory)> for ArchivedWorkerInfo {
    fn from(input: (db::Worker, db::Factory)) -> Self {
        let target = input.0.target().to_string();
        let db::Worker {
            id,
            factory_private,
            factory_metadata,

            target: _,
            bootstrap: _,
            token: _,
            deleted: _,
            recycle: _,
            lastping: _,
            factory: _,
            wait_for_flush: _,
        } = input.0;
        let factory = ArchivedFactoryInfo::from(input.1);

        ArchivedWorkerInfo {
            id: id.to_string(),
            target,
            factory,
            factory_private,
            factory_metadata: factory_metadata.map(|v| v.0),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ArchivedJob {
    v: String,

    id: String,
    name: String,

    /*
     * We store the failed and cancelled bits here, but not the completed or
     * waiting bits; a job can only be archived once it is complete, and when it
     * is complete it can no longer be waiting.
     */
    failed: bool,
    cancelled: bool,

    /*
     * Store both the user ID and the login name for the user at the time the
     * job was archived.  If the user has been deleted since the job was
     * archived, we can at least have some idea of who they were.  The
     * owner_name field is only for diagnostic purposes.
     */
    owner_id: String,
    owner_name: String,

    /*
     * Store both the original input target name (from the user) and the
     * ultimately resolved target ID.  We also store, for diagnostic purposes,
     * the final resolved target name at the time the job was archived.
     */
    target_name: String,
    target_id: String,
    target_resolved_name: String,
    target_resolved_desc: String,

    /*
     * Store both the worker ID and some purely diagnostic information about the
     * worker and factory at the point of job archival.
     */
    worker_id: Option<String>,
    worker_info: Option<ArchivedWorkerInfo>,

    tasks: Vec<ArchivedTask>,
    output_rules: Vec<ArchivedOutputRule>,
    tags: HashMap<String, String>,
    inputs: Vec<ArchivedInput>,
    outputs: Vec<ArchivedOutput>,
    times: HashMap<String, String>,
    events: Vec<ArchivedEvent>,
    store: HashMap<String, ArchivedStoreEntry>,
    depends: HashMap<String, ArchivedDepend>,
}

impl ArchivedJob {
    pub fn is_valid(&self) -> bool {
        self.v == "1"
    }

    pub fn version(&self) -> &str {
        &self.v
    }

    pub fn job_events(&self, minseq: usize) -> Result<Vec<db::JobEvent>> {
        let job: db::JobId = self.id.parse()?;

        self.events
            .iter()
            .enumerate()
            .filter(|(seq, _)| *seq >= minseq)
            .map(|(seq, ev)| {
                Ok(db::JobEvent {
                    job,
                    task: ev.task.map(|t| t.try_into().unwrap()),
                    seq: seq.try_into().unwrap(),
                    stream: ev.stream.clone(),
                    time: ev.time.restore_from_archive()?,
                    payload: ev.payload.clone(),
                    time_remote: ev
                        .time_remote
                        .as_ref()
                        .map(|t| t.restore_from_archive())
                        .transpose()?,
                })
            })
            .collect::<Result<Vec<_>>>()
    }

    pub fn job_outputs(&self) -> Result<Vec<(db::JobOutput, db::JobFile)>> {
        let job: db::JobId = self.id.parse()?;

        self.outputs
            .iter()
            .map(|f| {
                let output = db::JobOutput {
                    job,
                    path: f.path.clone(),
                    id: f.file.id()?,
                };

                let file = db::JobFile {
                    job: f.file.job()?,
                    id: f.file.id()?,
                    size: db::DataSize(f.file.size),
                    time_archived: Some(f.file.time_archived()?),
                };

                Ok((output, file))
            })
            .collect::<Result<Vec<_>>>()
    }

    pub fn job_output(&self, id: db::JobFileId) -> Result<db::JobOutput> {
        let job: db::JobId = self.id.parse()?;

        self.outputs
            .iter()
            .find(|f| f.file.id().ok() == Some(id))
            .map(|f| {
                Ok(db::JobOutput {
                    job,
                    path: f.path.clone(),
                    id: f.file.id()?,
                })
            })
            .ok_or_else(|| anyhow!("file {id} for job {job} not in archive"))?
    }

    pub fn times(&self) -> Result<HashMap<String, DateTime<Utc>>> {
        self.times
            .iter()
            .map(|(name, time)| {
                Ok((name.clone(), time.restore_from_archive()?.0))
            })
            .collect::<Result<HashMap<_, _>>>()
    }

    pub fn tags(&self) -> Result<HashMap<String, String>> {
        Ok(self.tags.clone())
    }

    pub fn output_rules(&self) -> Result<Vec<db::JobOutputRule>> {
        let job: db::JobId = self.id.parse()?;

        self.output_rules
            .iter()
            .enumerate()
            .map(|(seq, r)| {
                let ArchivedOutputRule {
                    rule,
                    ignore,
                    size_change_ok,
                    require_match,
                } = r;

                Ok(db::JobOutputRule {
                    job,
                    seq: seq.try_into().unwrap(),
                    rule: rule.clone(),
                    ignore: *ignore,
                    size_change_ok: *size_change_ok,
                    require_match: *require_match,
                })
            })
            .collect::<Result<Vec<_>>>()
    }

    pub fn tasks(&self) -> Result<Vec<db::Task>> {
        let job: db::JobId = self.id.parse()?;

        self.tasks
            .iter()
            .map(|t| {
                let ArchivedTask {
                    seq,
                    name,
                    script,
                    env_clear,
                    env,
                    user_id,
                    group_id,
                    workdir,
                    complete,
                    failed,
                } = t;

                Ok(db::Task {
                    job,
                    seq: (*seq).try_into().unwrap(),
                    name: name.clone(),
                    script: script.clone(),
                    env_clear: *env_clear,
                    env: db::Dictionary(env.clone()),
                    user_id: user_id.map(db::UnixUid),
                    group_id: group_id.map(db::UnixGid),
                    workdir: workdir.clone(),
                    failed: *failed,
                    complete: *complete,
                })
            })
            .collect::<Result<Vec<_>>>()
    }

    pub fn store(&self) -> &HashMap<String, ArchivedStoreEntry> {
        &self.store
    }
}

async fn archive_jobs_one(log: &Logger, c: &Central) -> Result<bool> {
    let start = Instant::now();

    let (reason, job) = if let Some(job) =
        c.inner.lock().unwrap().archive_queue.pop_front()
    {
        /*
         * Service explicit requests from the operator to archive a job
         * first.
         */
        let job = c.db.job(job)?;
        if !job.complete {
            warn!(log, "job {} not complete; cannot archive yet", job.id);
            return Ok(false);
        }
        if job.is_archived() {
            warn!(log, "job {} was already archived; ignoring request", job.id);
            return Ok(false);
        }
        ("operator request", job)
    } else if c.config.job.auto_archive {
        /*
         * Otherwise, if auto-archiving is enabled, archive the next as-yet
         * unarchived job.
         */
        if let Some(job) = c.db.job_next_unarchived()? {
            ("automatic", job)
        } else {
            return Ok(false);
        }
    } else {
        return Ok(false);
    };

    assert!(job.complete);
    assert!(job.time_archived.is_none());

    info!(log, "archiving job {} [{reason}]...", job.id);

    /*
     * We need to collect a variety of materials together in order to create the
     * archive of the job.
     */
    let events =
        c.db.job_events(job.id, 0)?
            .into_iter()
            .map(ArchivedEvent::from)
            .collect::<Vec<_>>();
    let tasks =
        c.db.job_tasks(job.id)?
            .into_iter()
            .map(ArchivedTask::from)
            .collect::<Vec<_>>();
    let output_rules =
        c.db.job_output_rules(job.id)?
            .into_iter()
            .map(ArchivedOutputRule::from)
            .collect::<Vec<_>>();
    let tags = c.db.job_tags(job.id)?;
    let inputs =
        c.db.job_inputs(job.id)?
            .into_iter()
            .map(|i| i.try_into())
            .collect::<Result<Vec<_>>>()?;
    let outputs =
        c.db.job_outputs(job.id)?
            .into_iter()
            .map(|o| o.try_into())
            .collect::<Result<Vec<_>>>()?;
    let times =
        c.db.job_times(job.id)?
            .into_iter()
            .map(|(name, time)| (name, time.to_archive()))
            .collect::<HashMap<String, String>>();
    let target_id = job.target().to_string();
    let (target_resolved_name, target_resolved_desc) = {
        let t = c.db.target_get(job.target())?;
        (t.name, t.desc)
    };
    let store =
        c.db.job_store(job.id)?
            .into_iter()
            .map(|(k, v)| (k, ArchivedStoreEntry::from(v)))
            .collect();
    let depends =
        c.db.job_depends(job.id)?
            .into_iter()
            .map(|d| (d.name.to_string(), ArchivedDepend::from(d)))
            .collect();
    let worker_info = job
        .worker
        .map(|id| {
            let w = c.db.worker_get(id)?;
            let f = c.db.factory_get(w.factory())?;
            Ok::<_, anyhow::Error>(ArchivedWorkerInfo::from((w, f)))
        })
        .transpose()?;

    let db::Job {
        id,
        owner,
        name,
        target,
        failed,
        worker,
        cancelled,
        complete: _,
        waiting: _,
        time_archived: _,

        /*
         * We use the target_id value we already fetched above, so ignore it
         * here:
         */
        target_id: _,
    } = job;

    let Some(owner) = c.db.user(owner)? else {
        bail!("could not locate user {owner}");
    };

    let aj = ArchivedJob {
        v: "1".into(),
        id: id.to_string(),
        name,
        failed,
        cancelled,

        owner_id: owner.id.to_string(),
        owner_name: owner.name.to_string(),

        target_name: target,
        target_id,
        target_resolved_name,
        target_resolved_desc,

        worker_id: worker.map(|i| i.to_string()),
        worker_info,

        events,
        tasks,
        output_rules,
        tags,
        inputs,
        outputs,
        times,
        store,
        depends,
    };

    c.archive_store(log, id, aj).await?;

    c.db.job_mark_archived(id, Utc::now())?;

    let dur = Instant::now().saturating_duration_since(start);
    info!(log, "job {id} archived"; "duration_ms" => dur.as_millis());

    Ok(true)
}

pub(crate) async fn archive_jobs(log: Logger, c: Arc<Central>) -> Result<()> {
    let delay = Duration::from_secs(1);

    info!(log, "start job archive task");

    loop {
        match archive_jobs_one(&log, &c).await {
            Ok(true) => continue,
            Ok(false) => (),
            Err(e) => error!(log, "job archive task error: {:?}", e),
        }

        tokio::time::sleep(delay).await;
    }
}
