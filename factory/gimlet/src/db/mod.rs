/*
 * Copyright 2026 Oxide Computer Company
 */

use std::{collections::HashSet, path::Path};

use anyhow::Result;
use buildomat_database::{
    conflict, DBResult, FromRow, Handle, IsoDate, Sqlite,
};
use chrono::prelude::*;
use sea_query::{Expr, Order, Query};
#[allow(unused_imports)]
use slog::{debug, error, info, warn, Logger};

mod tables;

pub mod types {
    use buildomat_database::{rusqlite, sqlite_integer_new_type};

    sqlite_integer_new_type!(InstanceSeq, u64, BigUnsigned);
    sqlite_integer_new_type!(EventSeq, u64, BigUnsigned);

    pub use super::tables::{InstanceId, InstanceState};
}

pub use tables::*;
use types::{EventSeq, InstanceSeq};

pub struct Database {
    #[allow(unused)]
    log: Logger,
    sql: Sqlite,
}

pub struct CreateInstance {
    pub worker: String,
    pub lease: String,
    pub target: String,
    pub bootstrap: String,
}

impl Database {
    pub fn new<P: AsRef<Path>>(
        log: Logger,
        path: P,
        cache_kb: Option<u32>,
    ) -> Result<Database> {
        let sql = Sqlite::setup(
            log.clone(),
            path,
            include_str!("../../schema.sql"),
            cache_kb,
        )?;

        Ok(Database { log, sql })
    }

    pub fn instance_get(&self, id: &InstanceId) -> DBResult<Option<Instance>> {
        self.sql.tx(|h| h.get_row_opt(Instance::find(id)))
    }

    pub fn instance_new_state(
        &self,
        id: &InstanceId,
        state: InstanceState,
    ) -> DBResult<()> {
        self.sql.tx_immediate(|h| {
            /*
             * Get the existing state of this instance:
             */
            let i: Instance = h.get_row(Instance::find(id))?;

            if i.state == state {
                return Ok(());
            }

            let valid_source_states: &[InstanceState] = match state {
                InstanceState::Preinstall => &[],
                InstanceState::Installing => &[InstanceState::Preinstall],
                InstanceState::Installed => &[InstanceState::Installing],
                InstanceState::Destroying => &[
                    InstanceState::Preinstall,
                    InstanceState::Installing,
                    InstanceState::Installed,
                ],
                InstanceState::Destroyed => &[InstanceState::Destroying],
            };

            if !valid_source_states.contains(&i.state) {
                conflict!(
                    "instance {id} cannot move from state {} to {state}",
                    i.state,
                );
            }

            let uc = h.exec_update(
                Query::update()
                    .table(InstanceDef::Table)
                    .and_where(Expr::col(InstanceDef::Model).eq(id.model()))
                    .and_where(Expr::col(InstanceDef::Serial).eq(id.serial()))
                    .and_where(Expr::col(InstanceDef::Seq).eq(id.seq()))
                    .value(InstanceDef::State, state)
                    .to_owned(),
            )?;
            assert_eq!(uc, 1);
            Ok(())
        })
    }

    pub fn i_next_seq_for_instance(
        &self,
        i: &Instance,
        h: &mut Handle,
    ) -> DBResult<EventSeq> {
        let max: Option<EventSeq> = h.get_row(
            Query::select()
                .from(InstanceEventDef::Table)
                .expr(Expr::col(InstanceEventDef::Seq).max())
                .and_where(Expr::col(InstanceEventDef::Model).eq(&i.model))
                .and_where(Expr::col(InstanceEventDef::Serial).eq(&i.serial))
                .and_where(Expr::col(InstanceEventDef::Instance).eq(i.seq))
                .to_owned(),
        )?;

        Ok(EventSeq(max.map(|v| v.0).unwrap_or(0).checked_add(1).unwrap()))
    }

    pub fn instance_append(
        &self,
        id: &InstanceId,
        stream: &str,
        msg: &str,
        time: DateTime<Utc>,
    ) -> DBResult<()> {
        self.sql.tx_immediate(|h| {
            /*
             * Fetch the current instance state:
             */
            let i: Instance = h.get_row(Instance::find(&id))?;

            match i.state {
                InstanceState::Preinstall
                | InstanceState::Installing
                | InstanceState::Installed => {
                    let ie = InstanceEvent {
                        model: i.model.clone(),
                        serial: i.serial.clone(),
                        instance: i.seq,

                        seq: self.i_next_seq_for_instance(&i, h)?,
                        stream: stream.to_string(),
                        payload: msg.to_string(),
                        uploaded: false,
                        time: IsoDate(time),
                    };

                    let ic = h.exec_insert(ie.insert())?;
                    assert_eq!(ic, 1);
                }
                InstanceState::Destroying | InstanceState::Destroyed => {
                    conflict!("instance already being destroyed");
                }
            }

            Ok(())
        })
    }

    pub fn instances_active(&self) -> DBResult<Vec<Instance>> {
        self.sql.tx(|h| {
            h.get_rows(
                Query::select()
                    .from(InstanceDef::Table)
                    .columns(Instance::columns())
                    .and_where(
                        Expr::col(InstanceDef::State)
                            .ne(InstanceState::Destroyed),
                    )
                    .order_by(InstanceDef::Model, Order::Asc)
                    .order_by(InstanceDef::Serial, Order::Asc)
                    .order_by(InstanceDef::Seq, Order::Asc)
                    .to_owned(),
            )
        })
    }

    pub fn instance_create(
        &self,
        host: &crate::HostId,
        create: CreateInstance,
    ) -> DBResult<InstanceId> {
        self.sql.tx_immediate(|h| {
            /*
             * Find the next available instance sequence number, starting at 1
             * if there have not been any instances before.
             */
            let seq: Option<types::InstanceSeq> = h.get_row(
                Query::select()
                    .from(InstanceDef::Table)
                    .expr(Expr::col(InstanceDef::Seq).max())
                    .and_where(Expr::col(InstanceDef::Model).eq(&host.model))
                    .and_where(Expr::col(InstanceDef::Serial).eq(&host.serial))
                    .to_owned(),
            )?;
            let seq = seq.unwrap_or(InstanceSeq(0)).0.checked_add(1).unwrap();

            let i = Instance::new(host, seq, create);

            let ic = h.exec_insert(i.insert())?;
            assert_eq!(ic, 1);

            Ok(i.id())
        })
    }

    pub fn instance_next_event_to_upload(
        &self,
        i: &Instance,
    ) -> DBResult<Option<InstanceEvent>> {
        self.sql.tx(|h| {
            h.get_row_opt(
                Query::select()
                    .from(InstanceEventDef::Table)
                    .columns(InstanceEvent::columns())
                    .and_where(Expr::col(InstanceEventDef::Model).eq(&i.model))
                    .and_where(
                        Expr::col(InstanceEventDef::Serial).eq(&i.serial),
                    )
                    .and_where(Expr::col(InstanceEventDef::Instance).eq(i.seq))
                    .and_where(Expr::col(InstanceEventDef::Uploaded).eq(false))
                    .order_by(InstanceEventDef::Seq, Order::Asc)
                    .limit(1)
                    .to_owned(),
            )
        })
    }

    pub fn instance_mark_event_uploaded(
        &self,
        i: &Instance,
        ie: &InstanceEvent,
    ) -> DBResult<()> {
        self.sql.tx(|h| {
            let uc = h.exec_update(
                Query::update()
                    .table(InstanceEventDef::Table)
                    .and_where(Expr::col(InstanceEventDef::Model).eq(&i.model))
                    .and_where(
                        Expr::col(InstanceEventDef::Serial).eq(&i.serial),
                    )
                    .and_where(Expr::col(InstanceEventDef::Instance).eq(i.seq))
                    .and_where(Expr::col(InstanceEventDef::Seq).eq(ie.seq))
                    .and_where(Expr::col(InstanceEventDef::Uploaded).eq(false))
                    .value(InstanceEventDef::Uploaded, true)
                    .to_owned(),
            )?;
            assert!(uc == 0 || uc == 1);

            Ok(())
        })
    }
}
