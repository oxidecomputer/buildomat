/*
 * Copyright 2021 Oxide Computer Company
 */

use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
use std::sync::Mutex;

use anyhow::{anyhow, bail, Result};
use buildomat_common::*;
use chrono::prelude::*;
use diesel::prelude::*;
#[allow(unused_imports)]
use rusty_ulid::Ulid;
#[allow(unused_imports)]
use slog::{error, info, warn, Logger};
use thiserror::Error;

mod models;
mod schema;

pub use models::*;

#[derive(Error, Debug)]
pub enum OperationError {
    #[error("conflict: {0}")]
    Conflict(String),
    #[error(transparent)]
    Sql(#[from] diesel::result::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type OResult<T> = std::result::Result<T, OperationError>;

macro_rules! conflict {
    ($msg:expr) => {
        return Err(OperationError::Conflict($msg.to_string()))
    };
    ($fmt:expr, $($arg:tt)*) => {
        return Err(OperationError::Conflict(format!($fmt, $($arg)*)))
    }
}

struct Inner {
    conn: diesel::sqlite::SqliteConnection,
}

pub struct Database(Logger, Mutex<Inner>);

impl Database {
    pub fn new<P: AsRef<Path>>(
        log: Logger,
        path: P,
        cache_kb: Option<u32>,
    ) -> Result<Database> {
        let conn = buildomat_common::db::sqlite_setup(
            &log,
            path,
            include_str!("../../schema.sql"),
            cache_kb,
        )?;

        Ok(Database(log, Mutex::new(Inner { conn })))
    }

    pub fn i_instance_for_host(
        &self,
        nodename: &str,
        tx: &mut SqliteConnection,
    ) -> OResult<Option<Instance>> {
        use schema::instance::dsl;

        let t: Vec<Instance> = dsl::instance
            .filter(dsl::nodename.eq(nodename))
            .filter(dsl::state.ne(InstanceState::Destroyed))
            .get_results(tx)?;

        match t.len() {
            0 => Ok(None),
            1 => Ok(Some(t[0].clone())),
            n => {
                conflict!("found {} active instances for host {}", n, nodename)
            }
        }
    }

    pub fn i_next_seq_for_host(
        &self,
        nodename: &str,
        tx: &mut SqliteConnection,
    ) -> OResult<InstanceSeq> {
        use schema::instance::dsl;

        let max: Option<i64> = dsl::instance
            .select(diesel::dsl::max(dsl::seq))
            .filter(dsl::nodename.eq(nodename))
            .get_result(tx)?;

        let next = max.unwrap_or(0).checked_add(1).unwrap();

        Ok(InstanceSeq(next.try_into().unwrap()))
    }

    pub fn i_next_seq_for_instance(
        &self,
        i: &Instance,
        tx: &mut SqliteConnection,
    ) -> OResult<EventSeq> {
        use schema::instance_event::dsl;

        let max: Option<i64> = dsl::instance_event
            .select(diesel::dsl::max(dsl::seq))
            .filter(dsl::nodename.eq(&i.nodename))
            .filter(dsl::instance.eq(i.seq))
            .get_result(tx)?;

        let next = max.unwrap_or(0).checked_add(1).unwrap();

        Ok(EventSeq(next.try_into().unwrap()))
    }

    pub fn instance_for_host(
        &self,
        nodename: &str,
    ) -> OResult<Option<Instance>> {
        let c = &mut self.1.lock().unwrap().conn;

        c.immediate_transaction(|tx| self.i_instance_for_host(nodename, tx))
    }

    pub fn instance_get(
        &self,
        nodename: &str,
        seq: InstanceSeq,
    ) -> OResult<Option<Instance>> {
        use schema::instance::dsl;

        let c = &mut self.1.lock().unwrap().conn;

        Ok(dsl::instance.find((&nodename, seq)).get_result(c).optional()?)
    }

    /**
     * Create an instance in the prebooted state.
     */
    pub fn instance_create(
        &self,
        nodename: &str,
        target: &str,
        worker: &str,
        bootstrap: &str,
    ) -> OResult<Instance> {
        let key = genkey(32);
        let c = &mut self.1.lock().unwrap().conn;

        c.immediate_transaction(|tx| {
            use schema::instance::dsl;

            let existing = self.i_instance_for_host(nodename, tx)?;
            if existing.is_some() {
                conflict!("host {} already has an active instance", nodename);
            }

            let i = Instance {
                nodename: nodename.to_string(),
                seq: self.i_next_seq_for_host(nodename, tx)?,
                worker: worker.to_string(),
                target: target.to_string(),
                state: InstanceState::Preboot,
                key,
                bootstrap: bootstrap.to_string(),
                flushed: false,
            };

            let ic =
                diesel::insert_into(dsl::instance).values(&i).execute(tx)?;
            assert_eq!(ic, 1);

            Ok(i)
        })
    }

    pub fn active_instances(&self) -> OResult<Vec<Instance>> {
        let c = &mut self.1.lock().unwrap().conn;

        use schema::instance::dsl;

        Ok(dsl::instance
            .filter(dsl::state.ne(InstanceState::Destroyed))
            .order_by(dsl::nodename.asc())
            .get_results(c)?)
    }

    pub fn instance_destroy(&self, i: &Instance) -> OResult<()> {
        let c = &mut self.1.lock().unwrap().conn;

        c.immediate_transaction(|tx| {
            use schema::instance::dsl;

            /*
             * Fetch the current instance state:
             */
            let i: Instance =
                dsl::instance.find((&i.nodename, i.seq)).get_result(tx)?;

            match i.state {
                InstanceState::Preboot | InstanceState::Booted => {
                    let uc = diesel::update(dsl::instance)
                        .filter(dsl::nodename.eq(i.nodename))
                        .filter(dsl::seq.eq(i.seq))
                        .set(dsl::state.eq(InstanceState::Destroying))
                        .execute(tx)?;
                    assert_eq!(uc, 1);
                    Ok(())
                }
                InstanceState::Destroying => Ok(()),
                InstanceState::Destroyed => {
                    conflict!("instance already completely destroyed");
                }
            }
        })
    }

    pub fn instance_mark_destroyed(&self, i: &Instance) -> OResult<()> {
        let c = &mut self.1.lock().unwrap().conn;

        c.immediate_transaction(|tx| {
            use schema::instance::dsl;

            /*
             * Fetch the current instance state:
             */
            let i: Instance =
                dsl::instance.find((&i.nodename, i.seq)).get_result(tx)?;

            match i.state {
                InstanceState::Preboot | InstanceState::Booted => {
                    conflict!("instance was not already being destroyed");
                }
                InstanceState::Destroying => {
                    let uc = diesel::update(dsl::instance)
                        .filter(dsl::nodename.eq(i.nodename))
                        .filter(dsl::seq.eq(i.seq))
                        .set(dsl::state.eq(InstanceState::Destroyed))
                        .execute(tx)?;
                    assert_eq!(uc, 1);
                    Ok(())
                }
                InstanceState::Destroyed => Ok(()),
            }
        })
    }

    pub fn instance_mark_flushed(&self, i: &Instance) -> OResult<()> {
        let c = &mut self.1.lock().unwrap().conn;

        c.immediate_transaction(|tx| {
            use schema::instance::dsl;

            let uc = diesel::update(dsl::instance)
                .filter(dsl::nodename.eq(&i.nodename))
                .filter(dsl::seq.eq(i.seq))
                .set(dsl::flushed.eq(true))
                .execute(tx)?;
            assert_eq!(uc, 1);

            Ok(())
        })
    }

    pub fn instance_boot(&self, i: &Instance) -> OResult<()> {
        let c = &mut self.1.lock().unwrap().conn;

        c.immediate_transaction(|tx| {
            use schema::instance::dsl;

            /*
             * Fetch the current instance state:
             */
            let i: Instance =
                dsl::instance.find((&i.nodename, i.seq)).get_result(tx)?;

            match i.state {
                InstanceState::Preboot => {
                    let uc = diesel::update(dsl::instance)
                        .filter(dsl::nodename.eq(i.nodename))
                        .filter(dsl::seq.eq(i.seq))
                        .set(dsl::state.eq(InstanceState::Booted))
                        .execute(tx)?;
                    assert_eq!(uc, 1);
                    Ok(())
                }
                InstanceState::Booted => Ok(()),
                InstanceState::Destroying | InstanceState::Destroyed => {
                    conflict!("instance already being destroyed");
                }
            }
        })
    }

    pub fn instance_append(
        &self,
        i: &Instance,
        stream: &str,
        msg: &str,
        time: DateTime<Utc>,
    ) -> OResult<InstanceEvent> {
        let c = &mut self.1.lock().unwrap().conn;

        c.immediate_transaction(|tx| {
            use schema::{instance, instance_event};

            /*
             * Fetch the current instance state:
             */
            let i: Instance = instance::dsl::instance
                .find((&i.nodename, i.seq))
                .get_result(tx)?;

            match i.state {
                InstanceState::Preboot | InstanceState::Booted => {
                    let ie = InstanceEvent {
                        nodename: i.nodename.to_string(),
                        instance: i.seq,
                        seq: self.i_next_seq_for_instance(&i, tx)?,
                        stream: stream.to_string(),
                        payload: msg.to_string(),
                        uploaded: false,
                        time: IsoDate(time),
                    };

                    diesel::insert_into(instance_event::dsl::instance_event)
                        .values(&ie)
                        .execute(tx)?;

                    Ok(ie)
                }
                InstanceState::Destroying | InstanceState::Destroyed => {
                    conflict!("instance already being destroyed");
                }
            }
        })
    }

    pub fn instance_next_event_to_upload(
        &self,
        i: &Instance,
    ) -> OResult<Option<InstanceEvent>> {
        use schema::instance_event::dsl;

        let c = &mut self.1.lock().unwrap().conn;

        Ok(dsl::instance_event
            .filter(dsl::nodename.eq(&i.nodename))
            .filter(dsl::instance.eq(i.seq))
            .filter(dsl::uploaded.eq(false))
            .order_by(dsl::seq.asc())
            .limit(1)
            .get_result(c)
            .optional()?)
    }

    pub fn instance_mark_event_uploaded(
        &self,
        i: &Instance,
        ie: &InstanceEvent,
    ) -> OResult<()> {
        use schema::instance_event::dsl;

        let c = &mut self.1.lock().unwrap().conn;

        let uc = diesel::update(dsl::instance_event)
            .set((dsl::uploaded.eq(true),))
            .filter(dsl::nodename.eq(&i.nodename))
            .filter(dsl::instance.eq(i.seq))
            .filter(dsl::seq.eq(ie.seq))
            .filter(dsl::uploaded.eq(false))
            .execute(c)?;
        assert!(uc == 0 || uc == 1);

        Ok(())
    }
}
