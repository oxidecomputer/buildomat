/*
 * Copyright 2025 Oxide Computer Company
 */

use core::mem::size_of;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Seek, Write};
use std::os::unix::prelude::FileExt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};
use chrono::prelude::*;
use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use slog::{debug, error, info, warn, Logger};
use tlvc_text::{Piece, Tag};

use crate::{db, Central};

pub const JOB_ARCHIVE_VERSION: u32 = 2;

struct PreparedArchive {
    id: db::JobId,
    file: File,
    file_len: usize,
}

#[derive(Clone)]
struct FileReader {
    f: Arc<std::fs::File>,
    offset: u64,
}

impl tlvc::TlvcRead for FileReader {
    type Error = std::io::Error;

    fn extent(
        &self,
    ) -> std::result::Result<u64, tlvc::TlvcReadError<Self::Error>> {
        /*
         * We will offset all of our reads in the file, so we need to adjust the
         * file size we're returning to the consumer to ignore the portion of
         * the file we're skipping.  If the offset is past the end of the file,
         * just return 0.
         */
        Ok(self
            .f
            .metadata()
            .map_err(tlvc::TlvcReadError::User)?
            .len()
            .saturating_sub(self.offset))
    }

    fn read_exact(
        &self,
        offset: u64,
        dest: &mut [u8],
    ) -> std::result::Result<(), tlvc::TlvcReadError<Self::Error>> {
        let Some(offset) = offset.checked_add(self.offset) else {
            /*
             * Simulate the error that we expect from read_exact_at().
             */
            return Err(tlvc::TlvcReadError::User(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "failed to fill whole buffer",
            )));
        };

        self.f.read_exact_at(dest, offset).map_err(tlvc::TlvcReadError::User)
    }
}

trait WrappedTlvcError<T> {
    fn wrap(self) -> Result<T>;
}

impl<T> WrappedTlvcError<T> for Result<T, tlvc::TlvcReadError<std::io::Error>> {
    fn wrap(self) -> Result<T> {
        self.map_err(|e| anyhow!("tlvc read error: {e:?}"))
    }
}

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
            seq,
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
            task,
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

impl TryFrom<db::JobOutputAndFile> for ArchivedOutput {
    type Error = anyhow::Error;

    fn try_from(input: db::JobOutputAndFile) -> Result<Self> {
        let db::JobOutput { job: _, id: _, path } = input.output;

        Ok(ArchivedOutput { path, file: input.file.try_into()? })
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ArchivedInput {
    pub name: String,
    pub file: Option<ArchivedFile>,
    pub other_job_id: Option<String>,
}

impl TryFrom<db::JobInputAndFile> for ArchivedInput {
    type Error = anyhow::Error;

    fn try_from(input: db::JobInputAndFile) -> Result<Self> {
        let db::JobInput { job: _, id: _, name, other_job } = input.input;

        Ok(ArchivedInput {
            name,
            file: input.file.map(ArchivedFile::try_from).transpose()?,
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
        let db::Factory {
            id,
            name,
            token: _,
            lastping: _,
            enable: _,
            hold_workers: _,
        } = input;

        ArchivedFactoryInfo { id: id.to_string(), name }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ArchivedWorkerInfo {
    id: String,
    target: String,
    factory: ArchivedFactoryInfo,
    factory_private: Option<String>,
    factory_ip: Option<String>,
    factory_metadata: Option<serde_json::Value>,
}

impl From<(db::Worker, db::Factory)> for ArchivedWorkerInfo {
    fn from(input: (db::Worker, db::Factory)) -> Self {
        let target = input.0.target().to_string();
        let db::Worker {
            id,
            factory_private,
            factory_ip,
            factory_metadata,

            target: _,
            bootstrap: _,
            token: _,
            deleted: _,
            recycle: _,
            lastping: _,
            factory: _,
            wait_for_flush: _,
            hold_time: _,
            hold_reason: _,
            diagnostics: _,
        } = input.0;
        let factory = ArchivedFactoryInfo::from(input.1);

        ArchivedWorkerInfo {
            id: id.to_string(),
            target,
            factory,
            factory_private,
            factory_ip,
            factory_metadata: factory_metadata.map(|v| v.0),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ArchivedJob {
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
    store: HashMap<String, ArchivedStoreEntry>,
    depends: HashMap<String, ArchivedDepend>,
}

pub struct LoadedArchivedJob {
    id: db::JobId,
    f: File,
    job: ArchivedJob,
    offset_index: u64,
    offset_events: u64,
}

impl LoadedArchivedJob {
    pub fn job_events(
        &self,
        minseq: usize,
        limit: u64,
    ) -> Result<Vec<db::JobEvent>> {
        /*
         * Locate the INDX record in the file so that we can read from it.
         */
        let mut tw = TlvcWrapper::new(&self.f, self.offset_index)?;
        let idx = tw.next_with_tag(*b"INDX")?;

        /*
         * Event sequence numbers start at 1, but we may end up with a zero
         * value for minseq due to the way this function gets called.
         */
        let minseq = minseq.max(1);

        let idxlen: usize = idx.header().len.get().try_into().unwrap();
        let count = idxlen / size_of::<u64>();

        if minseq > count {
            /*
             * Attemping to read beyond the stream end results in an empty
             * result set.
             */
            return Ok(Default::default());
        }

        let mut buf = [0u8; size_of::<u64>()];
        idx.read_exact(
            minseq
                /*
                 * The first index slot is for the event with a sequence number
                 * of 1:
                 */
                .checked_sub(1)
                .unwrap()
                /*
                 * Each index entry is a single u64 with the byte offset of the
                 * associated event data:
                 */
                .checked_mul(size_of::<u64>())
                .unwrap()
                .try_into()
                .unwrap(),
            &mut buf,
        )
        .wrap()?;
        let offset = u64::from_be_bytes(buf);

        /*
         * Locate the EVNT record using the offset we found in the index.
         */
        let mut tw = TlvcWrapper::new(
            &self.f,
            self.offset_events.checked_add(offset).unwrap(),
        )?;
        let mut out: Vec<ArchivedEvent> = Default::default();
        while out.len() < limit.try_into().unwrap() {
            let Some(evnt) = &tw.next_with_tag_as_string_opt(*b"EVNT")? else {
                break;
            };

            out.push(serde_json::from_str(evnt)?);
        }

        out.into_iter()
            .enumerate()
            .map(|(seq, ev)| {
                Ok(db::JobEvent {
                    job: self.id,
                    task: ev.task,
                    seq: minseq.checked_add(seq).unwrap().try_into().unwrap(),
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

    pub fn job_outputs(&self) -> Result<Vec<db::JobOutputAndFile>> {
        self.job
            .outputs
            .iter()
            .map(|f| {
                let output = db::JobOutput {
                    job: self.id,
                    path: f.path.clone(),
                    id: f.file.id()?,
                };

                let file = db::JobFile {
                    job: f.file.job()?,
                    id: f.file.id()?,
                    size: db::DataSize(f.file.size),
                    time_archived: Some(f.file.time_archived()?),
                };

                Ok(db::JobOutputAndFile { output, file })
            })
            .collect::<Result<Vec<_>>>()
    }

    pub fn job_output(&self, id: db::JobFileId) -> Result<db::JobOutput> {
        let job = self.id;

        self.job
            .outputs
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
        self.job
            .times
            .iter()
            .map(|(name, time)| {
                Ok((name.clone(), time.restore_from_archive()?.0))
            })
            .collect::<Result<HashMap<_, _>>>()
    }

    pub fn tags(&self) -> Result<HashMap<String, String>> {
        Ok(self.job.tags.clone())
    }

    pub fn output_rules(&self) -> Result<Vec<db::JobOutputRule>> {
        self.job
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
                    job: self.id,
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
        self.job
            .tasks
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
                    job: self.id,
                    seq: *seq,
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
        &self.job.store
    }

    pub fn load_from_file(f: File) -> Result<LoadedArchivedJob> {
        /*
         * To begin with, we need to read from the start of the file to get the
         * BMAT header.
         */
        let mut tw = TlvcWrapper::new(&f, 0)?;
        {
            let mut hdr = tw.next_with_tag_as_chunks(*b"BMAT")?;

            let vers = hdr.next_with_tag_as_u32(*b"VERS")?;
            if vers != 1 {
                bail!("unsupported BMAT container version {vers}");
            }

            let ctype = hdr.next_with_tag_as_string(*b"TYPE")?;
            if ctype != "archive" {
                bail!("unsupported BMAT.TYPE {ctype}");
            }

            {
                let mut arch = hdr.next_with_tag_as_chunks(*b"ARCH")?;
                let avers = arch.next_with_tag_as_u32(*b"VERS")?;
                if avers != JOB_ARCHIVE_VERSION {
                    bail!(
                        "unsupported BMAT.ARCH.VERS: got {avers}, \
                want {JOB_ARCHIVE_VERSION}"
                    );
                }

                let atype = arch.next_with_tag_as_string(*b"TYPE")?;
                if atype != "job" {
                    bail!("unsupported BMAT.ARCH.TYPE {atype}");
                }
            }
        }

        /*
         * Load the job information from the file, which comes after
         * the entire BMAT chunk.
         */
        let job: ArchivedJob =
            serde_json::from_str(&tw.next_with_tag_as_string(*b"JOB\0")?)?;

        /*
         * The index comes right after the job information.  Save the offset so
         * that we can look at it later.
         */
        let offset_index = tw.next_header_offset;
        tw.next_with_tag(*b"INDX")?;

        let offset_events = tw.next_header_offset;

        Ok(LoadedArchivedJob {
            id: job.id.parse()?,
            f,
            job,
            offset_index,
            offset_events,
        })
    }
}

struct TlvcWrapper {
    reader: tlvc::TlvcReader<FileReader>,
    next_header_offset: u64,
}

impl TlvcWrapper {
    fn new(file: &std::fs::File, offset: u64) -> Result<TlvcWrapper> {
        let reader = tlvc::TlvcReader::begin(FileReader {
            f: Arc::new(file.try_clone()?),
            offset,
        })
        .wrap()?;

        Ok(TlvcWrapper { reader, next_header_offset: 0 })
    }

    fn next_with_tag_as_chunks(&mut self, tag: [u8; 4]) -> Result<TlvcWrapper> {
        let ch = self.next_with_tag(tag)?;
        Ok(TlvcWrapper { reader: ch.read_as_chunks(), next_header_offset: 0 })
    }

    fn next_with_tag_as_u32(&mut self, tag: [u8; 4]) -> Result<u32> {
        let ch = self.next_with_tag(tag)?;
        let body_len: usize = ch.header().len.get().try_into().unwrap();
        if body_len != size_of::<u32>() {
            bail!("tag {tag:?} has unexpected body length {body_len}");
        }

        let mut data = [0u8; size_of::<u32>()];
        ch.read_exact(0, &mut data).wrap()?;

        Ok(u32::from_be_bytes(data))
    }

    fn next_with_tag_as_string(&mut self, tag: [u8; 4]) -> Result<String> {
        self.next_with_tag_as_string_opt(tag)?
            .ok_or_else(|| anyhow!("expected tag {tag:?}, got EOF"))
    }

    fn next_with_tag_as_string_opt(
        &mut self,
        tag: [u8; 4],
    ) -> Result<Option<String>> {
        let Some(ch) = self.next_with_tag_opt(tag)? else {
            return Ok(None);
        };

        let mut data = vec![0u8; ch.header().len.get().try_into().unwrap()];
        ch.read_exact(0, &mut data).wrap()?;

        Ok(Some(String::from_utf8(data)?))
    }

    fn next_with_tag_opt(
        &mut self,
        tag: [u8; 4],
    ) -> Result<Option<tlvc::ChunkHandle<FileReader>>> {
        self.reader
            .next()
            .wrap()?
            .map(|ch| {
                let actual = ch.header().tag;
                if tag != actual {
                    bail!("expected tag {tag:?}, got tag {actual:?}");
                }

                let mut trash = [0u8; 512];
                ch.check_body_checksum(&mut trash).wrap()?;

                self.next_header_offset = self
                    .next_header_offset
                    .checked_add(
                        ch.header().total_len_in_bytes().try_into().unwrap(),
                    )
                    .unwrap();

                Ok(ch)
            })
            .transpose()
    }

    fn next_with_tag(
        &mut self,
        tag: [u8; 4],
    ) -> Result<tlvc::ChunkHandle<FileReader>> {
        self.next_with_tag_opt(tag)?
            .ok_or_else(|| anyhow!("expected tag {tag:?}, got EOF"))
    }
}

fn archive_jobs_sync_work(
    log: &Logger,
    c: &Central,
) -> Result<Option<PreparedArchive>> {
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
            return Ok(None);
        }
        if job.is_archived() {
            warn!(log, "job {} was already archived; ignoring request", job.id);
            return Ok(None);
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
            return Ok(None);
        }
    } else {
        return Ok(None);
    };

    assert!(job.complete);
    assert!(job.time_archived.is_none());

    info!(log, "archiving job {} [{reason}]...", job.id);

    /*
     * We need to collect a variety of materials together in order to create the
     * archive of the job.
     */
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
        let t = c.db.target(job.target())?;
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
            let w = c.db.worker(id)?;
            let f = c.db.factory(w.factory())?;
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

        /*
         * Jobs cannot be archived until they are completed, and if they are
         * completed they cannot still be waiting.  These are dynamic flags that
         * we don't need to include here.
         */
        complete: _,
        waiting: _,

        /*
         * These fields track when the job was successfully archived or purged,
         * and thus cannot appear in the archive itself.
         */
        time_archived: _,
        time_purged: _,

        /*
         * We use the target_id value we already fetched above, so ignore it
         * here:
         */
        target_id: _,
    } = job;

    let Some(owner) = c.db.user(owner)? else {
        bail!("could not locate user {owner}");
    };

    /*
     * This structure should store things that don't grow substantially for
     * longer-running jobs that produce more output.  In particular, we store
     * the job event stream in a different way.  This is important because we
     * will need to deserialise this object (at a cost of CPU and memory) in
     * order to answer questions about the job: heavier jobs should not use more
     * resources than lighter jobs.
     */
    let aj = ArchivedJob {
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

        tasks,
        output_rules,
        tags,
        inputs,
        outputs,
        times,
        store,
        depends,
    };

    /*
     * Load job events from the database in chunks.  Serialize those chunks into
     * a temporary file (the "back" file), and build an index of their positions
     * within that temporary file.
     */
    let mut tf = tempfile::tempfile()?;
    let mut minseq = 1;
    let mut index: Vec<u64> = Default::default();
    let mut pos = 0;
    loop {
        let events =
            c.db.job_events(job.id, minseq, 100)?
                .into_iter()
                .map(ArchivedEvent::from)
                .collect::<Vec<_>>();

        if events.is_empty() {
            break;
        }
        minseq += events.len();

        for e in events {
            let buf = tlvc_text::pack(&vec![Piece::Chunk(
                Tag::new(*b"EVNT"),
                vec![Piece::Bytes(serde_json::to_vec(&e)?)],
            )]);

            index.push(pos);
            pos = pos.checked_add(buf.len().try_into().unwrap()).unwrap();

            tf.write_all(&buf)?;
        }
    }
    tf.flush()?;
    let efl = tf.metadata()?.len();
    if efl != pos {
        bail!("event file length {efl} != pos {pos}");
    }

    /*
     * Create another temporary file where we will assemble the file header, the
     * top-level job information, and the event index.
     */
    let mut ftf = tempfile::tempfile()?;
    ftf.write_all(&tlvc_text::pack(&vec![
        /*
         * The file starts with a BMAT chunk that identifies this as a job
         * archive file of a particular version.
         */
        Piece::Chunk(
            Tag::new(*b"BMAT"),
            vec![
                Piece::Chunk(
                    Tag::new(*b"VERS"),
                    /*
                     * This version field is the version of the overall
                     * container file format; in particular, of this BMAT file
                     * header record.  This version does not need to change for
                     * changes to job archiving data.
                     */
                    vec![Piece::Bytes(1u32.to_be_bytes().into())],
                ),
                Piece::Chunk(
                    Tag::new(*b"TYPE"),
                    vec![Piece::String("archive".into())],
                ),
                Piece::Chunk(
                    Tag::new(*b"ARCH"),
                    vec![
                        Piece::Chunk(
                            Tag::new(*b"VERS"),
                            vec![Piece::Bytes(
                                JOB_ARCHIVE_VERSION.to_be_bytes().into(),
                            )],
                        ),
                        Piece::Chunk(
                            Tag::new(*b"TYPE"),
                            vec![Piece::String("job".into())],
                        ),
                    ],
                ),
            ],
        ),
        /*
         * The next chunk in a (TYPE=archive, ARCH.TYPE=job) file is a JOB chunk
         * that contains the serialised job-level data:
         */
        Piece::Chunk(
            Tag::new(*b"JOB\0"),
            vec![Piece::Bytes(serde_json::to_vec(&aj)?)],
        ),
        /*
         * Next is the INDX chunk, which contains a list of offsets in the file
         * for each job event record.  The offsets are relative to the end of
         * the INDX record; the EVNT records begin immediately after EVNT.  The
         * index allows random access into the event stream without the need to
         * seek through the entire file.
         */
        Piece::Chunk(
            Tag::new(*b"INDX"),
            vec![Piece::Bytes({
                let mut v = Vec::with_capacity(index.len() * 8);
                for i in index {
                    v.extend(i.to_be_bytes());
                }
                v
            })],
        ),
    ]))?;
    ftf.flush()?;

    /*
     * XXX Copy the data from the back file onto the end of the file with the
     * headers.  Ideally we should just be able to pass two files in order to
     * the object store and have them be concatenated...
     */
    tf.rewind()?;
    std::io::copy(&mut tf, &mut ftf)?;
    drop(tf);

    ftf.flush()?;
    ftf.rewind()?;
    let file_len = ftf.metadata()?.len().try_into().unwrap();

    Ok(Some(PreparedArchive { id, file: ftf, file_len }))
}

async fn archive_jobs_one(log: &Logger, c: &Arc<Central>) -> Result<bool> {
    let start = Instant::now();

    /*
     * The archiving work is synchronous and may take several seconds for larger
     * jobs.  Avoid holding up other async tasks while we wait:
     */
    let Some(pa) =
        tokio::task::block_in_place(|| archive_jobs_sync_work(log, c))?
    else {
        return Ok(false);
    };
    let id = pa.id;

    c.archive_store(log, id, JOB_ARCHIVE_VERSION, pa.file, pa.file_len).await?;

    c.db.job_mark_archived(id, Utc::now())?;

    let dur = Instant::now().saturating_duration_since(start);
    info!(log, "job {id} archived"; "duration_msec" => dur.as_millis());

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

fn purge_jobs_sync_work(
    log: &Logger,
    c: &Central,
) -> Result<Option<db::JobId>> {
    let (reason, job) = if let Some(job) =
        c.inner.lock().unwrap().purge_queue.pop_front()
    {
        /*
         * Service explicit requests from the operator to purge a job first.
         */
        let job = c.db.job(job)?;
        if !job.is_archived() {
            warn!(log, "job {} not archived; cannot purge yet", job.id);
            return Ok(None);
        }
        if job.is_purged() {
            warn!(log, "job {} was already purged; ignoring request", job.id);
            return Ok(None);
        }
        ("operator request", job)
    } else if c.config.job.auto_purge {
        /*
         * Otherwise, if auto-purging is enabled, purge the next as-yet
         * unpurged job.
         */
        if let Some(job) = c.db.job_next_unpurged()? {
            if let Some(time) = job.time_archived {
                if time.age().as_secs() < 14 * 86400 {
                    /*
                     * Only purge once the job has been archived for at least a
                     * fortnight.
                     */
                    return Ok(None);
                }
            }

            ("automatic", job)
        } else {
            return Ok(None);
        }
    } else {
        return Ok(None);
    };

    assert!(job.complete);
    assert!(job.time_archived.is_some());
    assert!(job.time_purged.is_none());

    info!(log, "purging job {} [{reason}]...", job.id);

    /*
     * Purging a job from the database involves removing the live records. This
     * should have no impact on access to details about the job, as we will
     * fetch those details from the archive file once the job has been archived.
     */
    c.db.job_purge(job.id)?;

    Ok(Some(job.id))
}

async fn purge_jobs_one(log: &Logger, c: &Arc<Central>) -> Result<bool> {
    let start = Instant::now();

    /*
     * The work to purge a job is synchronous and may take several seconds for
     * larger jobs.  Avoid holding up other async tasks while we wait:
     */
    let id = tokio::task::block_in_place(|| purge_jobs_sync_work(log, c))?;

    if let Some(id) = id {
        let dur = Instant::now().saturating_duration_since(start);
        info!(log, "job {id} purged"; "duration_msec" => dur.as_millis());
    }

    Ok(id.is_some())
}

pub(crate) async fn purge_jobs(log: Logger, c: Arc<Central>) -> Result<()> {
    let delay = Duration::from_secs(1);

    info!(log, "start job purge task");

    loop {
        match purge_jobs_one(&log, &c).await {
            Ok(true) => continue,
            Ok(false) => (),
            Err(e) => error!(log, "job purge task error: {:?}", e),
        }

        tokio::time::sleep(delay).await;
    }
}
