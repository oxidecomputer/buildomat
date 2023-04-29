/*
 * Copyright 2023 Oxide Computer Company
 */

use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};
use chrono::prelude::*;
use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use slog::{debug, error, info, warn, Logger};

use crate::{db, Central};

trait FromArchiveDate {
    fn from_archive(&self) -> Result<db::IsoDate>;
}

impl FromArchiveDate for String {
    fn from_archive(&self) -> Result<db::IsoDate> {
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
    pub time: String,
    pub time_remote: Option<String>,
    pub payload: String,
}

impl From<db::JobEvent> for ArchivedEvent {
    fn from(input: db::JobEvent) -> Self {
        let db::JobEvent {
            job: _,
            task,
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
        Ok(self.time_archived.from_archive()?)
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
struct ArchivedPublishedFile {
    pub series: String,
    pub version: String,
    pub name: String,
    pub job_id: String,
    pub file_id: String,
}

impl From<db::PublishedFile> for ArchivedPublishedFile {
    fn from(input: db::PublishedFile) -> Self {
        let db::PublishedFile { owner: _, series, version, name, job, file } =
            input;

        ArchivedPublishedFile {
            series,
            version,
            name,
            job_id: job.to_string(),
            file_id: file.to_string(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ArchivedJob {
    v: String,

    id: String,
    name: String,
    failed: bool,
    cancelled: bool,

    owner_id: String,
    owner_name: String,

    target_name: String,
    target_id: String,

    worker_id: Option<String>,

    tasks: Vec<ArchivedTask>,
    output_rules: Vec<ArchivedOutputRule>,
    tags: HashMap<String, String>,
    inputs: Vec<ArchivedInput>,
    outputs: Vec<ArchivedOutput>,
    times: HashMap<String, String>,
    published_files: Vec<ArchivedPublishedFile>,
    events: Vec<ArchivedEvent>,
}

impl ArchivedJob {
    pub fn is_valid(&self) -> bool {
        self.v == "1"
    }

    pub fn job_events(&self, minseq: usize) -> Result<Vec<db::JobEvent>> {
        let job: db::JobId = self.id.parse()?;

        Ok(self
            .events
            .iter()
            .enumerate()
            .filter(|(seq, _)| *seq >= minseq)
            .map(|(seq, ev)| {
                Ok(db::JobEvent {
                    job,
                    task: ev.task.map(|t| t.try_into().unwrap()),
                    seq: seq.try_into().unwrap(),
                    stream: ev.stream.clone(),
                    time: ev.time.from_archive()?,
                    payload: ev.payload.clone(),
                    time_remote: ev
                        .time_remote
                        .as_ref()
                        .map(|t| t.from_archive())
                        .transpose()?,
                })
            })
            .collect::<Result<Vec<_>>>()?)
    }

    pub fn job_outputs(&self) -> Result<Vec<(db::JobOutput, db::JobFile)>> {
        let job: db::JobId = self.id.parse()?;

        Ok(self
            .outputs
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
            .collect::<Result<Vec<_>>>()?)
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
        Ok(self
            .times
            .iter()
            .map(|(name, time)| Ok((name.clone(), time.from_archive()?.0)))
            .collect::<Result<HashMap<_, _>>>()?)
    }

    pub fn tags(&self) -> Result<HashMap<String, String>> {
        Ok(self.tags.clone())
    }

    pub fn output_rules(&self) -> Result<Vec<db::JobOutputRule>> {
        let job: db::JobId = self.id.parse()?;

        Ok(self
            .output_rules
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
            .collect::<Result<Vec<_>>>()?)
    }

    pub fn tasks(&self) -> Result<Vec<db::Task>> {
        let job: db::JobId = self.id.parse()?;

        Ok(self
            .tasks
            .iter()
            .enumerate()
            .map(|(seq, t)| {
                let ArchivedTask {
                    seq: _,
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
                    seq: seq.try_into().unwrap(),
                    name: name.clone(),
                    script: script.clone(),
                    env_clear: *env_clear,
                    env: db::Dictionary(env.clone()),
                    user_id: user_id.map(|n| db::UnixUid(n)),
                    group_id: group_id.map(|n| db::UnixGid(n)),
                    workdir: workdir.clone(),
                    failed: *failed,
                    complete: *complete,
                })
            })
            .collect::<Result<Vec<_>>>()?)
    }
}

async fn archive_jobs_one(log: &Logger, c: &Central) -> Result<()> {
    let start = Instant::now();
    let Some(job) = c.db.job_next_unarchived()? else {
        return Ok(());
    };
    assert!(job.complete);
    assert!(job.time_archived.is_none());

    info!(log, "archiving job {}...", job.id);

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
    let published_files =
        c.db.job_published_files(job.id)?
            .into_iter()
            .map(ArchivedPublishedFile::from)
            .collect::<Vec<_>>();
    let target_id = job.target().to_string();

    let db::Job {
        id,
        owner,
        name,
        target,
        failed,
        worker,
        cancelled,
        target_id: _,
        complete: _,
        waiting: _,
        time_archived: _,
    } = job;

    let Some(owner) = c.db.user_get_by_id(owner)? else {
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

        worker_id: worker.map(|i| i.to_string()),

        events,
        tasks,
        output_rules,
        tags,
        inputs,
        outputs,
        times,
        published_files,
    };

    let apath = c.archive_path(id)?;
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&apath)?;

    serde_json::to_writer_pretty(&mut f, &aj)?;
    f.flush()?;

    c.db.job_mark_archived(id, Utc::now())?;

    let dur = Instant::now().saturating_duration_since(start);
    info!(log, "job {id} archived"; "duration_ms" => dur.as_millis());

    Ok(())
}

pub(crate) async fn archive_jobs(log: Logger, c: Arc<Central>) -> Result<()> {
    let delay = Duration::from_secs(1);

    info!(log, "start job archive task");

    loop {
        if let Err(e) = archive_jobs_one(&log, &c).await {
            error!(log, "job archive task error: {:?}", e);
        }

        tokio::time::sleep(delay).await;
    }
}
