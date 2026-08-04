/*
 * Copyright 2026 Oxide Computer Company
 */

mod chroot;
mod service;
mod unix;
mod user;

use crate::bootstrap_agent::BootstrapContext;
use crate::coordinator::DoNext;
use crate::db::types::*;
use crate::illumos::service::{Destroy, InstState};
use crate::Central;
use anyhow::{bail, Result};
use buildomat_common::unix::Passwd;
use slog::{debug, error, info, warn, Logger};
use smf::State as SmfState;
use std::time::{Duration, Instant};

pub(crate) async fn worker_iteration(
    log: &Logger,
    c: &Central,
    id: WorkerId,
) -> Result<DoNext> {
    let worker = c.db.worker_get(id)?;
    let Some(worker) = worker else {
        bail!("no worker {id} in the database?");
    };

    match worker.state {
        WorkerState::Unconfigured => {
            /*
             * There is a small chance the slot might not exist, if it was
             * removed from the configuration file after the worker was created
             * but before this function was called.
             */
            let Some(slot) = c.config.slots.get(&worker.slot) else {
                error!(log, "worker {id} uses unknown slot {:?}", worker.slot);
                c.db.worker_new_state(id, WorkerState::Destroying)?;
                return Ok(DoNext::Immediate);
            };

            let (uid, gid) = user::create(id, slot)?;
            let chroot = chroot::prepare(c, id, uid)?;

            let ctx = BootstrapContext {
                baseurl: c.config.general.baseurl.clone(),
                bootstrap_token: worker.bootstrap,
                env: slot.env.clone(),
                chroot,
            };

            service::start(id, uid, gid, &ctx)?;

            c.db.worker_new_state(id, WorkerState::Configured)?;
            info!(log, "configured worker {id}");

            Ok(DoNext::Sleep)
        }
        WorkerState::Configured => {
            let InstState::Present { current, target } = service::state(id)?
            else {
                warn!(
                    log,
                    "SMF instance for worker {id} disappeared, \
                     destroying the worker"
                );
                c.db.worker_new_state(id, WorkerState::Destroying)?;
                return Ok(DoNext::Immediate);
            };

            /*
             * We don't care about the current state when the service is
             * transitioning to a new state.
             */
            if target.is_some() {
                return Ok(DoNext::Sleep);
            }

            match current {
                Some(SmfState::Online) => Ok(DoNext::Sleep),
                Some(SmfState::Uninitialized) => {
                    /*
                     * This occurs briefly prior to svc.startd(8) picking up the
                     * new instance.
                     */
                    Ok(DoNext::Sleep)
                }
                Some(SmfState::Maintenance) => {
                    /*
                     * This is a terminal failure state that means the worker is
                     * not running anymore.
                     */
                    warn!(
                        log,
                        "SMF instance in maintenance, \
                         marking worker {id} as broken"
                    );

                    /*
                     * Mark the worker as failed on the core server.  This will
                     * mark the job running on the worker (if any) as failed,
                     * and hold the worker so that an operator can look at it.
                     */
                    c.client
                        .factory_worker_fail()
                        .worker(id.to_string())
                        .body_map(|b| b.reason("SMF service in maintenance"))
                        .send()
                        .await?;

                    c.db.worker_new_state(id, WorkerState::Broken)?;
                    Ok(DoNext::Immediate)
                }
                state => {
                    warn!(log, "SMF instance reached unknown state {state:?}");

                    /*
                     * Mark the worker as failed on the core server.  This will
                     * mark the job running on the worker (if any) as failed,
                     * and hold the worker so that an operator can look at it.
                     */
                    c.client
                        .factory_worker_fail()
                        .worker(id.to_string())
                        .body_map(|b| {
                            b.reason(format!(
                                "SMF instance reached unknown state {state:?}"
                            ))
                        })
                        .send()
                        .await?;
                    c.db.worker_new_state(id, WorkerState::Broken)?;

                    Ok(DoNext::Immediate)
                }
            }
        }
        WorkerState::Destroying => {
            match service::destroy(log, id)? {
                Destroy::Destroying => return Ok(DoNext::Sleep),
                Destroy::Destroyed => {}
            }

            /*
             * While we have disabled the service, it is possible that some
             * processes could have been created outside the contract.  Ensure
             * all processes for the build user are terminated and then remove
             * any files left behind.
             */
            if let Some(user) = Passwd::by_name(&user::name(id))? {
                kill_all(log, &user).await?;
            }

            chroot::cleanup(c, id)?;
            user::remove(id)?;

            info!(log, "worker {id} successfully destroyed");
            c.db.worker_new_state(id, WorkerState::Destroyed)?;
            Ok(DoNext::Immediate)
        }
        WorkerState::Broken => {
            /*
             * We will remain in the broken state until the core server decides
             * to recycle the worker.
             */
            Ok(DoNext::Sleep)
        }
        WorkerState::Destroyed => {
            /*
             * The coordinator will take care of stopping the task instead of
             * sleeping when it's time for it to
             */
            Ok(DoNext::Sleep)
        }
    }
}

async fn kill_all(log: &Logger, u: &Passwd) -> Result<()> {
    let user = if let Some(name) = &u.name {
        format!("user {name} (uid {})", u.uid.0)
    } else {
        format!("uid {}", u.uid.0)
    };

    let id = unix::SigSendId::UserId(u.uid.0);
    if unix::sigsend_maybe(id, libc::SIGKILL)? {
        debug!(log, "killed leftover processes for {user}");

        /*
         * There are a number of reasons that processes might linger for a
         * little while in the table after we terminate them.  Wait for a bit to
         * make sure they're all gone:
         */
        let start = Instant::now();
        while Instant::now().saturating_duration_since(start).as_secs() < 10 {
            /*
             * Try with the 0 argument, which can be used to confirm that no
             * processes exist.
             */
            if !unix::sigsend_maybe(id, 0)? {
                return Ok(());
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        bail!("processes for {user} still exist?");
    }

    Ok(())
}
