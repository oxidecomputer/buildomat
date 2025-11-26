/*
 * Copyright 2025 Oxide Computer Company
 */

//! Factory Worker
//!
//! The main polling loop that:
//! 1. Validates existing instances against buildomat-server workers
//! 2. Cleans up orphaned workers
//! 3. Acquires new jobs when testbeds are available
//! 4. Creates instances and associates them with workers
//! 5. Starts agent processes for new instances

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use slog::{Logger, debug, error, info, o, trace, warn};
use tokio::sync::Mutex;

use crate::Central;
use crate::db::InstanceSeq;
use crate::executor::{AgentHandle, Executor, ExecutorConfig};

/// Parse instance ID from worker private field.
///
/// Format: "testbed_name/seq" (e.g., "grapefruit-7f495641/5")
fn parse_instance_id(private: &str) -> Result<(String, InstanceSeq)> {
    let t = private.splitn(2, '/').collect::<Vec<_>>();
    if t.len() != 2 {
        bail!("invalid instance id");
    }

    if t[0].trim().is_empty() {
        bail!("invalid testbed name");
    }
    let seq = InstanceSeq::from_str(t[1]).context("invalid sequence number")?;

    Ok((t[0].to_string(), seq))
}

/// Tracks running agents across factory iterations.
pub struct AgentTracker {
    /// Map from instance ID to agent handle
    agents: Mutex<HashMap<String, AgentHandle>>,
}

impl AgentTracker {
    pub fn new() -> Self {
        AgentTracker { agents: Mutex::new(HashMap::new()) }
    }

    /// Add a running agent.
    pub async fn add(&self, handle: AgentHandle) {
        let mut agents = self.agents.lock().await;
        agents.insert(handle.instance_id.clone(), handle);
    }

    /// Check if we have an agent for this instance.
    pub async fn has(&self, instance_id: &str) -> bool {
        let agents = self.agents.lock().await;
        agents.contains_key(instance_id)
    }

    /// Get list of instance IDs with running agents.
    #[allow(dead_code)]
    pub async fn instance_ids(&self) -> Vec<String> {
        let agents = self.agents.lock().await;
        agents.keys().cloned().collect()
    }

    /// Remove and return an agent handle.
    pub async fn remove(&self, instance_id: &str) -> Option<AgentHandle> {
        let mut agents = self.agents.lock().await;
        agents.remove(instance_id)
    }

    /// Check all agents and return IDs of those that have exited.
    pub async fn check_exited(&self) -> Vec<String> {
        let agents = self.agents.lock().await;
        let mut exited = Vec::new();

        for (id, handle) in agents.iter() {
            if !handle.is_running().await {
                exited.push(id.clone());
            }
        }

        exited
    }
}

/// Single iteration of the factory worker loop.
async fn factory_worker_one(
    log: &Logger,
    c: &Central,
    executor: &Executor,
    tracker: &AgentTracker,
) -> Result<()> {
    //
    // Phase 0: Check for completed agents
    //
    for instance_id in tracker.check_exited().await {
        info!(log, "agent exited for instance {}", instance_id);

        if let Some(handle) = tracker.remove(&instance_id).await {
            // Clean up the agent
            executor.cleanup(&handle).await?;

            // Parse instance ID to mark as destroying
            if let Ok((testbed_name, seq)) = parse_instance_id(&instance_id) {
                if let Some(i) = c.db.instance_get(&testbed_name, seq)? {
                    info!(
                        log,
                        "marking instance {} for destruction", instance_id
                    );
                    c.db.instance_destroy(&i)?;
                }
            }
        }
    }

    //
    // Phase 1: Validate existing instances against buildomat-server
    //
    for i in c.db.active_instances()? {
        if i.should_teardown() {
            // Instance is being torn down - make sure agent is stopped
            if tracker.has(&i.id()).await {
                if let Some(handle) = tracker.remove(&i.id()).await {
                    info!(log, "stopping agent for teardown"; "instance" => i.id());
                    executor.cleanup(&handle).await?;
                }
            }
            // Mark as fully destroyed
            c.db.instance_mark_destroyed(&i)?;
            continue;
        }

        // Fetch worker state from core server
        let w = if let Some(w) = c
            .client
            .factory_worker_get()
            .worker(&i.worker)
            .send()
            .await?
            .into_inner()
            .worker
        {
            debug!(log, "instance {} is for worker {}", i.id(), w.id);
            w
        } else {
            warn!(
                log,
                "instance {} is for worker {} which no longer exists",
                i.id(),
                i.worker,
            );
            c.db.instance_destroy(&i)?;
            continue;
        };

        // Validate private data consistency
        if let Some(expected) = w.private.as_deref() {
            if expected != i.id() {
                error!(
                    log,
                    "instance {} for worker {} does not match expected \
                    instance {} from worker private field",
                    i.id(),
                    w.id,
                    expected
                );
                continue;
            }
        } else {
            error!(
                log,
                "instance {} for worker {} has no private data?",
                i.id(),
                w.id
            );
            continue;
        }

        // Check if worker was recycled
        if w.recycle {
            info!(log, "worker {} recycled, destroy instance {}", w.id, i.id());
            c.db.instance_destroy(&i)?;
            continue;
        }

        // Check if agent is online and we need to flush
        if !i.flushed && w.online {
            c.client.factory_worker_flush().worker(&i.worker).send().await?;
            c.db.instance_mark_flushed(&i)?;
            info!(log, "flushed worker {} instance {}", w.id, i.id());
        }
    }

    //
    // Phase 2: Clean up orphaned workers
    //
    for w in c.client.factory_workers().send().await?.into_inner() {
        let rm = if let Some(p) = w.private.as_deref() {
            if let Ok(ii) = parse_instance_id(p) {
                if let Some(i) = c.db.instance_get(&ii.0, ii.1)? {
                    i.destroyed()
                } else {
                    // Instance doesn't exist
                    true
                }
            } else {
                // Invalid instance ID
                true
            }
        } else {
            // Worker was never associated
            true
        };

        if rm {
            info!(log, "destroying orphaned worker {}", w.id);
            c.client.factory_worker_destroy().worker(&w.id).send().await?;
        }
    }

    //
    // Phase 3: Find available testbeds and request work
    //

    // Get list of testbeds currently in use
    let in_use: Vec<String> =
        c.db.active_instances()?.into_iter().map(|i| i.testbed_name).collect();

    // Get targets we can serve with available testbeds
    let mut available_targets = Vec::new();
    for name in c.testbeds.names() {
        if !in_use.contains(&name.to_string()) {
            if let Some(info) = c.testbeds.get(name) {
                if info.enabled {
                    for target in &info.targets {
                        if !available_targets.contains(target) {
                            available_targets.push(target.clone());
                        }
                    }
                }
            }
        }
    }

    if available_targets.is_empty() {
        // No available testbeds, just ping server to stay alive
        c.client.factory_ping().send().await?;
    } else {
        // Request a lease for any of our available targets
        debug!(log, "requesting lease"; "targets" => ?available_targets);

        if let Some(lease) = c
            .client
            .factory_lease()
            .body_map(|body| body.supported_targets(available_targets.clone()))
            .send()
            .await?
            .into_inner()
            .lease
        {
            info!(log, "received lease for target {}", lease.target);

            // Find an available testbed for this target
            if let Some(testbed) =
                c.testbeds.find_available_for_target(&lease.target, &in_use)
            {
                // Create worker
                let w = c
                    .client
                    .factory_worker_create()
                    .body_map(|body| {
                        body.target(&lease.target).wait_for_flush(false)
                    })
                    .send()
                    .await?;
                info!(
                    log,
                    "created worker {} of target {} for testbed {}",
                    w.id,
                    lease.target,
                    testbed.name
                );

                // Create instance
                let i = c.db.instance_create(
                    &testbed.name,
                    &lease.target,
                    &w.id,
                    &w.bootstrap,
                )?;
                info!(log, "created instance {} for worker {}", i.id(), w.id);

                // Associate worker with instance
                c.client
                    .factory_worker_associate()
                    .worker(&w.id)
                    .body_map(|body| body.private(i.id()))
                    .send()
                    .await?;
                info!(
                    log,
                    "associated instance {} with worker {}",
                    i.id(),
                    w.id
                );

                // Start agent for this instance
                let start_result = if testbed.is_local() {
                    info!(log, "starting local agent"; "instance" => i.id());
                    executor.start_local(&i, testbed).await
                } else {
                    info!(log, "starting SSH agent"; "instance" => i.id(), "host" => &testbed.host);
                    executor.start_ssh(&i, testbed).await
                };

                match start_result {
                    Ok(handle) => {
                        info!(log, "agent started for instance {}", i.id());
                        tracker.add(handle).await;
                        c.db.instance_mark_running(&i)?;
                    }
                    Err(e) => {
                        error!(
                            log,
                            "failed to start agent for instance {}: {:?}",
                            i.id(),
                            e
                        );
                        // Mark instance for destruction
                        c.db.instance_destroy(&i)?;
                    }
                }
            } else {
                warn!(
                    log,
                    "server asked for target {} but no testbed available",
                    lease.target
                );
            }
        }
    }

    trace!(log, "factory worker pass complete");
    Ok(())
}

/// Main factory worker loop.
pub async fn factory_worker(c: Arc<Central>) -> Result<()> {
    let log = c.log.new(o!("component" => "factory"));

    let delay = Duration::from_secs(7);

    // Create executor and tracker
    let executor_config = ExecutorConfig::from_central(&c);
    let executor = Executor::new(executor_config, log.clone());
    let tracker = AgentTracker::new();

    info!(log, "start factory worker task");

    loop {
        if let Err(e) = factory_worker_one(&log, &c, &executor, &tracker).await
        {
            error!(log, "factory worker error: {:?}", e);
        }

        tokio::time::sleep(delay).await;
    }
}
