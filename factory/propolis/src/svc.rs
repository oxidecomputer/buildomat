use std::{
    collections::HashSet,
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};

use anyhow::{anyhow, bail, Result};
use slog::{info, warn, Logger};
use smf::ScfError;

#[derive(Clone)]
pub struct Monitor(Arc<Inner>);

pub struct Inner {
    log: Logger,
    zonename: String,
    watch_fmris: Vec<String>,
    state: Mutex<State>,
    cv: Condvar,
}

pub struct State {
    shutdown: bool,
    running: bool,
    problems: Vec<String>,
    multiuser: bool,
}

#[allow(unused)]
impl Monitor {
    pub fn new(
        log: Logger,
        zonename: String,
        watch_fmris: Vec<String>,
    ) -> Result<Monitor> {
        let m = Monitor(Arc::new(Inner {
            log,
            zonename,
            watch_fmris,
            state: Mutex::new(State {
                shutdown: false,
                running: false,
                problems: Vec::new(),
                multiuser: false,
            }),
            cv: Condvar::new(),
        }));

        let mi = Arc::clone(&m.0);
        std::thread::spawn(move || {
            monitor_thread(mi);
        });

        Ok(m)
    }

    pub fn has_problems(&self) -> bool {
        let i = self.0.state.lock().unwrap();

        !i.problems.is_empty()
    }

    pub fn with_problems(&self, walker: impl FnOnce(&Vec<String>)) {
        let i = self.0.state.lock().unwrap();

        walker(&i.problems);
    }

    pub fn shutdown(&self) {
        let mut i = self.0.state.lock().unwrap();
        while i.running {
            i.shutdown = true;
            self.0.cv.notify_all();

            i = self.0.cv.wait(i).unwrap();
        }
    }
}

fn monitor_thread(mi: Arc<Inner>) {
    let log = &mi.log;

    /*
     * Signal that we are running!
     */
    {
        let mut i = mi.state.lock().unwrap();
        if i.shutdown {
            mi.cv.notify_all();
            return;
        }
        i.running = true;
    }

    let mut scf = None;

    /*
     * Schedule next polling operation:
     */
    let interval = Duration::from_millis(250);
    let mut next = Instant::now().checked_add(interval).unwrap();

    loop {
        /*
         * Check if we should shut down the thread:
         */
        {
            let mut i = mi.state.lock().unwrap();
            loop {
                if i.shutdown {
                    i.running = false;
                    mi.cv.notify_all();
                    return;
                }

                let rem = next.saturating_duration_since(Instant::now());
                if rem.is_zero() {
                    /*
                     * Don't wait any longer!  Find the next even multiple of
                     * the polling interval that has not yet gone by:
                     */
                    while next < Instant::now() {
                        next = next.checked_add(interval).unwrap();
                    }
                    break;
                }

                (i, _) = mi.cv.wait_timeout(i, rem).unwrap();
            }
        }

        /*
         * Poll service status!
         */
        if scf.is_none() {
            match smf::Scf::new_for_zone(&mi.zonename) {
                Ok(new) => scf = Some(new),
                Err(ScfError::NotFound) | Err(ScfError::NoServer) => {
                    /*
                     * This can happen if we try to poll too early, before the
                     * zone is ready to go.
                     */
                    continue;
                }
                Err(e) => {
                    let mut i = mi.state.lock().unwrap();

                    i.problems = vec![format!("scf error: {e}")];
                    continue;
                }
            }
        }

        /*
         * Before we start looking for problems, we want to make sure we have
         * arrived at the multi-user milestone at least once.
         */
        let multiuser = mi.state.lock().unwrap().multiuser;
        if !multiuser {
            let scf = scf.as_mut().unwrap();

            let Ok(local) = scf.scope_local() else {
                continue;
            };
            match local.get_service("milestone/multi-user-server") {
                Ok(Some(mst)) => match mst.instances() {
                    Ok(mut insts) => {
                        if insts.any(|inst| {
                            inst.ok()
                                .map(|i| {
                                    i.states()
                                        .ok()
                                        .map(|(cur, next)| {
                                            matches!(
                                                cur,
                                                Some(smf::State::Online)
                                            ) && matches!(next, None)
                                        })
                                        .unwrap_or(false)
                                })
                                .unwrap_or(false)
                        }) {
                            info!(log, "reached multi-user milestone!");
                            mi.state.lock().unwrap().multiuser = true;
                        }
                    }
                    Err(_) => (),
                },
                Ok(None) => {
                    warn!(log, "no multiuser milestone service?!");
                }
                Err(e) => {
                    warn!(log, "pre-multiuser scf error: {e}");
                }
            };
            continue;
        }

        match poll_services(scf.as_mut().unwrap(), &mi.watch_fmris) {
            Ok(problems) => {
                let mut i = mi.state.lock().unwrap();

                i.problems = problems;
            }
            Err(e) => {
                let mut i = mi.state.lock().unwrap();

                i.problems = vec![format!("scf error: {e}")];

                /*
                 * If we close the handle now, we can try to open a new one on
                 * the next turn.  This might be necessary if our connection to
                 * the repository was somehow interrupted.
                 */
                scf = None;
            }
        }
    }
}

trait Because<T> {
    fn because(self, msg: &str) -> Result<T>;
}

impl<T> Because<T> for std::result::Result<T, ScfError> {
    fn because(self, msg: &str) -> Result<T> {
        self.map_err(|e| anyhow!("{msg}: {e}"))
    }
}

fn poll_services(
    scf: &mut smf::Scf,
    watch_fmris: &[String],
) -> Result<Vec<String>> {
    let mut problems = Vec::new();
    let mut need_to_see: HashSet<_> = watch_fmris.iter().collect();

    let local = scf.scope_local().because("scope local")?;
    let mut services = local.services().because("enumerate services")?;
    while let Some(service) =
        services.next().transpose().because("next service")?
    {
        let mut instances =
            service.instances().because("enumerate instances")?;
        while let Some(inst) =
            instances.next().transpose().because("next instance")?
        {
            let fmri = inst.fmri().because("fmri")?;

            need_to_see.remove(&fmri);

            let (cur, next) = match inst.states() {
                Ok((cur, next)) => (cur, next),
                Err(ScfError::NotFound) => {
                    /*
                     * This can happen early in boot, or after a new service
                     * instance is created but before the restarter has had a
                     * chance to create the dynamic properties that reflect
                     * state.
                     */
                    continue;
                }
                Err(e) => bail!("getting states for {fmri}: {e}"),
            };

            if let Some(cur) = cur {
                match cur {
                    smf::State::LegacyRun | smf::State::Uninitialized => {
                        /*
                         * We can ignore services in these states.
                         */
                    }
                    smf::State::Disabled => {
                        /*
                         * We only care about disabled services if they are on
                         * our watch list.
                         */
                        if watch_fmris.contains(&fmri) {
                            problems.push(format!("{fmri} is disabled"));
                        }
                    }
                    smf::State::Unknown(wtf) => {
                        problems.push(format!("{fmri} unknown state {wtf:?}"));
                    }
                    smf::State::Offline => {
                        /*
                         * Check to see if this service is in the process of
                         * transitioning to a new state.
                         */
                        if next.is_none() {
                            /*
                             * This service appears to be stuck in the offline
                             * state, which may be due to a dependency that is
                             * itself not online for some reason.
                             */
                            problems.push(format!("{fmri} is offline"));
                        }
                    }
                    smf::State::Degraded => {
                        problems.push(format!("{fmri} is degraded"));
                    }
                    smf::State::Maintenance => {
                        problems.push(format!("{fmri} is in maintenance"));
                    }
                    smf::State::Online => (),
                }
            }

            //if let Some(next) = next {
            //    problems.push(format!("{fmri} is in transition -> {next:?}"));
            //}
        }
    }

    if !need_to_see.is_empty() {
        for fmri in &need_to_see {
            problems.push(format!("service instance {fmri} not found in zone"));
        }
    }

    Ok(problems)
}
