/*
 * Copyright 2026 Oxide Computer Company
 */

use crate::db::types::*;
use crate::Central;
use anyhow::Result;
use slog::{error, info, o, Logger};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

pub(crate) async fn coordinator_loop(c: Arc<Central>) {
    let log = c.log.new(o!("component" => "coordinator"));

    let mut tasks = HashMap::new();
    loop {
        if let Err(e) = coordinator_iteration(&log, &c, &mut tasks).await {
            error!(log, "coordinator error: {e:?}");
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn coordinator_iteration(
    log: &Logger,
    c: &Arc<Central>,
    tasks: &mut HashMap<WorkerId, Task>,
) -> Result<()> {
    let mut to_spawn = Vec::new();
    let mut to_stop = Vec::new();

    /*
     * Tasks that manage a worker tracked in the database.
     */
    let workers = c.db.workers()?;
    for worker in &workers {
        if let WorkerState::Destroyed = worker.state {
            to_stop.push(worker.id);
        }
        if let Some(task) = tasks.get(&worker.id) {
            if task.join_handle.is_finished() {
                /*
                 * If the task exited but the worker is not marked as destroyed,
                 * start the task again.  This can only happen with panics.
                 */
                to_spawn.push(worker.id);
            }
        } else {
            to_spawn.push(worker.id);
        }
    }

    /*
     * Tasks that manage a worker *not* tracked in the database.
     */
    let worker_ids = workers.iter().map(|w| w.id).collect::<HashSet<_>>();
    for id in tasks.keys() {
        if !worker_ids.contains(id) {
            to_stop.push(*id);
        }
    }

    for worker_id in to_stop {
        let Some(task) = tasks.get(&worker_id) else { continue };

        if task.join_handle.is_finished() {
            tasks.remove(&worker_id);
        } else if !task.stop.swap(true, Ordering::Relaxed) {
            info!(log, "stopping task for worker {worker_id}");
        }
    }

    for worker_id in to_spawn {
        let stop = Arc::new(AtomicBool::new(false));

        let join_handle = tokio::spawn(worker_loop(
            Arc::clone(c),
            worker_id,
            Arc::clone(&stop),
        ));

        info!(log, "spawned task for worker {worker_id}");
        tasks.insert(worker_id, Task { stop, join_handle });
    }

    Ok(())
}

async fn worker_loop(c: Arc<Central>, id: WorkerId, stop: Arc<AtomicBool>) {
    let log = c.log.new(o!(
        "component" => "worker",
        "worker" => id.to_string(),
    ));

    while !stop.load(Ordering::Relaxed) {
        match crate::illumos::worker_iteration(&log, &c, id).await {
            Ok(DoNext::Immediate) => {}
            Ok(DoNext::Sleep) => {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(err) => {
                error!(log, "worker {id} iteration failed: {err}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

pub(crate) enum DoNext {
    Sleep,
    Immediate,
}

struct Task {
    join_handle: JoinHandle<()>,
    stop: Arc<AtomicBool>,
}
