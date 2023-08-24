use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};

use anyhow::{bail, Result};
use rusty_ulid::Ulid;
use slog::{error, info, o, Logger};

use crate::{db::JobId, Central};

/**
 * Track work required to commit chunks into files.  For large files this can
 * take some time, so we need to make it an asynchronous request.  Background
 * work is identified by the combination of the job ID and a ULID chosen by the
 * agent to allow for easy idempotency.
 */
pub struct Files {
    log: Logger,
    inner: Arc<Mutex<Inner>>,
    cv: Arc<Condvar>,
}

#[derive(Default)]
struct Inner {
    commits: HashMap<BackgroundId, Arc<FileCommit>>,
    queue: VecDeque<BackgroundId>,
    completed_jobs: HashSet<JobId>,
}

struct FileCommit {
    expected_size: u64,
    chunks: Vec<Ulid>,
    kind: FileKind,
    state: Mutex<State>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileKind {
    Input { name: String },
    Output { path: String },
}

#[derive(Debug)]
enum State {
    Queued,
    Active(Instant),
    Complete(Duration, Option<String>),
}

impl FileCommit {
    fn observe(&self) -> Option<std::result::Result<(), String>> {
        let fcs = self.state.lock().unwrap();

        match &*fcs {
            State::Queued | State::Active(..) => None,
            State::Complete(dur, res) => Some(match res {
                None => Ok(()),
                Some(e) => Err(format!("{e} (after {}ms)", dur.as_millis())),
            }),
        }
    }

    fn pending(&self) -> bool {
        let fcs = self.state.lock().unwrap();

        !matches!(&*fcs, State::Complete(..))
    }

    fn mark_active(&self) {
        let mut fcs = self.state.lock().unwrap();
        assert!(matches!(&*fcs, State::Queued));
        *fcs = State::Active(Instant::now());
    }

    fn mark_complete_common(&self, msg: Option<String>) {
        let mut fcs = self.state.lock().unwrap();
        match &*fcs {
            State::Queued | State::Complete(..) => {
                panic!("unexpected state {:?}", *fcs)
            }
            State::Active(start) => {
                *fcs = State::Complete(
                    Instant::now().saturating_duration_since(*start),
                    msg,
                );
            }
        }
    }

    fn mark_ok(&self) {
        self.mark_complete_common(None)
    }

    fn mark_failed(&self, msg: String) {
        self.mark_complete_common(Some(msg))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct BackgroundId(JobId, Ulid);

fn thread_file_commit(
    c: Arc<Central>,
    log: Logger,
    cv: Arc<Condvar>,
    inner: Arc<Mutex<Inner>>,
) {
    loop {
        let (bgid, fc) = {
            let mut g = inner.lock().unwrap();
            loop {
                /*
                 * Attempt to pull work off the queue:
                 */
                let Some(bgid) = g.queue.pop_front() else {
                    g = cv.wait(g).unwrap();
                    continue;
                };

                /*
                 * Mark the commit we pulled off the queue as active before we
                 * drop the global lock:
                 */
                let fc = Arc::clone(&g.commits.get(&bgid).unwrap());
                fc.mark_active();

                break (bgid, fc);
            }
        };

        let start = Instant::now();
        info!(log, "starting work on job {} commit {}", bgid.0, bgid.1);

        let fid = match c.commit_file(bgid.0, &fc.chunks, fc.expected_size) {
            Ok(fid) => fid,
            Err(e) => {
                error!(log, "job {} commit {} failed: {e}", bgid.0, bgid.1);

                fc.mark_failed(e.to_string());
                continue;
            }
        };

        /*
         * The file ID of the fully assembled file now needs to be listed in the
         * database as either an input or an output:
         */
        let res = match &fc.kind {
            FileKind::Input { name } => {
                c.db.job_add_input(bgid.0, &name, fid, fc.expected_size)
            }
            FileKind::Output { path } => {
                c.db.job_add_output(bgid.0, &path, fid, fc.expected_size)
            }
        };

        let dur = Instant::now().saturating_duration_since(start).as_millis();
        if let Err(e) = res {
            error!(
                log,
                "job {} commit {} failed: database: {e}", bgid.0, bgid.1;
                "duration_msec" => dur,
            );

            fc.mark_failed(e.to_string());
        } else {
            info!(
                log,
                "finished work on job {} commit {}", bgid.0, bgid.1;
                "duration_msec" => dur
            );

            fc.mark_ok();
        }
    }
}

impl Files {
    pub fn new(log: Logger) -> Files {
        Files {
            log,
            cv: Arc::new(Condvar::new()),
            inner: Arc::new(Mutex::new(Inner::default())),
        }
    }

    pub(crate) fn start(&self, c: &Arc<Central>, threads: usize) {
        /*
         * Create a thread pool for file commit I/O.
         */
        for n in 0..threads {
            let log = self.log.new(o!("file-commit" => n));
            let inner = Arc::clone(&self.inner);
            let cv = Arc::clone(&self.cv);
            let c = Arc::clone(c);
            std::thread::Builder::new()
                .name(format!("file-commit-{n:02}"))
                .spawn(move || thread_file_commit(c, log, cv, inner))
                .unwrap();
        }
    }

    /**
     * Enqueue a file commit job.  This routine is intended to be idempotent.
     * If called with the same arguments over and over, it will eventually
     * return Some(Ok) or Some(Err).  An error is terminal: the client should
     * stop retrying and make other arrangements.
     */
    pub fn commit_file(
        &self,
        job: JobId,
        commit_id: Ulid,
        kind: FileKind,
        expected_size: u64,
        chunks: Vec<Ulid>,
    ) -> Result<Option<std::result::Result<(), String>>> {
        let mut fi = self.inner.lock().unwrap();
        let id = BackgroundId(job, commit_id);

        if let Some(fc) = fi.commits.get(&id) {
            /*
             * First confirm that the input request matches what we were told
             * last time.
             */
            if fc.kind != kind
                || fc.expected_size != expected_size
                || fc.chunks != chunks
            {
                bail!(
                    "job {job} reused commit {commit_id} with \
                    different arguments"
                );
            }

            Ok(fc.observe())
        } else {
            if fi.completed_jobs.contains(&job) {
                bail!("job {job} is already complete; cannot enqueue jobs");
            }

            /*
             * Enqueue the commit request!
             */
            let fc = Arc::new(FileCommit {
                expected_size,
                chunks,
                kind,
                state: Mutex::new(State::Queued),
            });

            fi.commits.insert(id, fc);
            fi.queue.push_back(id);
            self.cv.notify_all();

            Ok(None)
        }
    }

    /**
     * Check to see if a job has been submitted with this commit ID for this job
     * already.
     */
    pub fn commit_file_exists(&self, job: JobId, commit_id: Ulid) -> bool {
        let fi = self.inner.lock().unwrap();

        fi.commits.contains_key(&BackgroundId(job, commit_id))
    }

    /**
     * Once a job is confirmed as completed in the database, we can safely ditch
     * all of our in-memory tracking for that job.
     */
    pub fn forget_job(&self, job: JobId) {
        let mut fi = self.inner.lock().unwrap();

        assert!(fi.completed_jobs.contains(&job));

        /*
         * Make sure nothing has slipped back in...
         */
        assert!(!fi.queue.iter().any(|id| id.0 == job));
        assert!(!fi.commits.iter().any(|(id, fc)| id.0 == job && fc.pending()));

        let remove = fi
            .commits
            .keys()
            .filter(|id| id.0 == job)
            .cloned()
            .collect::<Vec<_>>();
        for bgid in remove {
            fi.commits.remove(&bgid);
        }
    }

    /**
     * Make sure there are no queued or active commits left for a particular job
     * then lock out further enqueued tasks.
     */
    pub fn mark_job_completed(&self, job: JobId) -> Result<()> {
        let mut fi = self.inner.lock().unwrap();

        if fi.commits.iter().any(|(id, fc)| id.0 == job && fc.pending()) {
            bail!(
                "cannot complete job while file commit tasks are still \
                in progress"
            );
        }
        assert!(!fi.queue.iter().any(|id| id.0 == job));

        fi.completed_jobs.insert(job);
        Ok(())
    }
}
