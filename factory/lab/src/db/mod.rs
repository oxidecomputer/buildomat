/*
 * Copyright 2023 Oxide Computer Company
 */

use std::path::Path;

use anyhow::Result;
use buildomat_common::*;
use buildomat_database::{DBResult, FromRow, Handle, Sqlite, conflict};
use chrono::prelude::*;
use sea_query::{Expr, Order, Query};
#[allow(unused_imports)]
use slog::{Logger, debug, error, info, warn};

mod tables;

mod types {
    use buildomat_database::{rusqlite, sqlite_integer_new_type};

    sqlite_integer_new_type!(InstanceSeq, u64, BigUnsigned);
    sqlite_integer_new_type!(EventSeq, u64, BigUnsigned);

    pub use buildomat_database::IsoDate;
}

pub use tables::*;
pub use types::*;

pub struct Database {
    #[allow(unused)]
    log: Logger,
    sql: Sqlite,
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

    pub fn i_next_seq_for_host(
        &self,
        nodename: &str,
        h: &mut Handle,
    ) -> DBResult<InstanceSeq> {
        let max: Option<InstanceSeq> = h.get_row(
            Query::select()
                .from(InstanceDef::Table)
                .expr(Expr::col(InstanceDef::Seq).max())
                .and_where(Expr::col(InstanceDef::Nodename).eq(nodename))
                .to_owned(),
        )?;

        Ok(InstanceSeq(max.map(|v| v.0).unwrap_or(0).checked_add(1).unwrap()))
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
                .and_where(
                    Expr::col(InstanceEventDef::Nodename).eq(&i.nodename),
                )
                .and_where(Expr::col(InstanceEventDef::Instance).eq(i.seq))
                .to_owned(),
        )?;

        Ok(EventSeq(max.map(|v| v.0).unwrap_or(0).checked_add(1).unwrap()))
    }

    pub fn instance_for_host(
        &self,
        nodename: &str,
    ) -> DBResult<Option<Instance>> {
        self.sql.tx(|h| h.get_row_opt(Instance::find_for_host(nodename)))
    }

    pub fn instance_get(
        &self,
        nodename: &str,
        seq: InstanceSeq,
    ) -> DBResult<Option<Instance>> {
        self.sql.tx(|h| h.get_row_opt(Instance::find(nodename, seq)))
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
    ) -> DBResult<Instance> {
        let key = genkey(32);

        self.sql.tx_immediate(|h| {
            let existing =
                h.get_row_opt::<Instance>(Instance::find_for_host(nodename))?;
            if existing.is_some() {
                conflict!("host {nodename} already has an active instance");
            }

            let i = Instance {
                nodename: nodename.to_string(),
                seq: self.i_next_seq_for_host(nodename, h)?,
                worker: worker.to_string(),
                target: target.to_string(),
                state: InstanceState::Preboot,
                key,
                bootstrap: bootstrap.to_string(),
                flushed: false,
            };

            let ic = h.exec_insert(i.insert())?;
            assert_eq!(ic, 1);

            Ok(i)
        })
    }

    pub fn active_instances(&self) -> DBResult<Vec<Instance>> {
        self.sql.tx(|h| h.get_rows(Instance::find_active()))
    }

    pub fn instance_destroy(&self, i: &Instance) -> DBResult<()> {
        self.sql.tx_immediate(|h| {
            /*
             * Fetch the current instance state:
             */
            let i: Instance = h.get_row(Instance::find(&i.nodename, i.seq))?;

            match i.state {
                InstanceState::Preboot | InstanceState::Booted => {
                    let uc = h.exec_update(
                        Query::update()
                            .table(InstanceDef::Table)
                            .and_where(
                                Expr::col(InstanceDef::Nodename).eq(i.nodename),
                            )
                            .and_where(Expr::col(InstanceDef::Seq).eq(i.seq))
                            .value(
                                InstanceDef::State,
                                InstanceState::Destroying,
                            )
                            .to_owned(),
                    )?;
                    assert_eq!(uc, 1);
                }
                InstanceState::Destroying => (),
                InstanceState::Destroyed => {
                    conflict!("instance already completely destroyed");
                }
            }

            Ok(())
        })
    }

    pub fn instance_mark_destroyed(&self, i: &Instance) -> DBResult<()> {
        self.sql.tx_immediate(|h| {
            /*
             * Fetch the current instance state:
             */
            let i: Instance = h.get_row(Instance::find(&i.nodename, i.seq))?;

            match i.state {
                InstanceState::Preboot | InstanceState::Booted => {
                    conflict!("instance was not already being destroyed");
                }
                InstanceState::Destroying => {
                    let uc = h.exec_update(
                        Query::update()
                            .table(InstanceDef::Table)
                            .and_where(
                                Expr::col(InstanceDef::Nodename).eq(i.nodename),
                            )
                            .and_where(Expr::col(InstanceDef::Seq).eq(i.seq))
                            .value(InstanceDef::State, InstanceState::Destroyed)
                            .to_owned(),
                    )?;
                    assert_eq!(uc, 1);
                }
                InstanceState::Destroyed => (),
            }

            Ok(())
        })
    }

    pub fn instance_mark_flushed(&self, i: &Instance) -> DBResult<()> {
        self.sql.tx(|h| {
            let uc = h.exec_update(
                Query::update()
                    .table(InstanceDef::Table)
                    .and_where(Expr::col(InstanceDef::Nodename).eq(&i.nodename))
                    .and_where(Expr::col(InstanceDef::Seq).eq(i.seq))
                    .value(InstanceDef::Flushed, true)
                    .to_owned(),
            )?;
            assert_eq!(uc, 1);

            Ok(())
        })
    }

    pub fn instance_boot(&self, i: &Instance) -> DBResult<()> {
        self.sql.tx_immediate(|h| {
            /*
             * Fetch the current instance state:
             */
            let i: Instance = h.get_row(Instance::find(&i.nodename, i.seq))?;

            match i.state {
                InstanceState::Preboot => {
                    let uc = h.exec_update(
                        Query::update()
                            .table(InstanceDef::Table)
                            .and_where(
                                Expr::col(InstanceDef::Nodename).eq(i.nodename),
                            )
                            .and_where(Expr::col(InstanceDef::Seq).eq(i.seq))
                            .value(InstanceDef::State, InstanceState::Booted)
                            .to_owned(),
                    )?;
                    assert_eq!(uc, 1);
                }
                InstanceState::Booted => (),
                InstanceState::Destroying | InstanceState::Destroyed => {
                    conflict!("instance already being destroyed");
                }
            }

            Ok(())
        })
    }

    pub fn instance_append(
        &self,
        i: &Instance,
        stream: &str,
        msg: &str,
        time: DateTime<Utc>,
    ) -> DBResult<InstanceEvent> {
        self.sql.tx_immediate(|h| {
            /*
             * Fetch the current instance state:
             */
            let i: Instance = h.get_row(Instance::find(&i.nodename, i.seq))?;

            let ie = match i.state {
                InstanceState::Preboot | InstanceState::Booted => {
                    let ie = InstanceEvent {
                        nodename: i.nodename.to_string(),
                        instance: i.seq,
                        seq: self.i_next_seq_for_instance(&i, h)?,
                        stream: stream.to_string(),
                        payload: msg.to_string(),
                        uploaded: false,
                        time: IsoDate(time),
                    };

                    let ic = h.exec_insert(ie.insert())?;
                    assert_eq!(ic, 1);

                    ie
                }
                InstanceState::Destroying | InstanceState::Destroyed => {
                    conflict!("instance already being destroyed");
                }
            };

            Ok(ie)
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
                    .and_where(
                        Expr::col(InstanceEventDef::Nodename).eq(&i.nodename),
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
                    .and_where(
                        Expr::col(InstanceEventDef::Nodename).eq(&i.nodename),
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
