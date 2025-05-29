/*
 * Copyright 2025 Oxide Computer Company
 */

use std::{collections::HashSet, path::Path};

use anyhow::Result;
use buildomat_database::{conflict, DBResult, FromRow, Sqlite};
use sea_query::{Expr, Order, Query};
#[allow(unused_imports)]
use slog::{debug, error, info, warn, Logger};

mod tables;

pub mod types {
    use buildomat_database::{rusqlite, sqlite_integer_new_type};

    sqlite_integer_new_type!(InstanceSeq, u64, BigUnsigned);

    pub use super::tables::{InstanceId, InstanceState};
}

pub use tables::*;
use types::InstanceSeq;

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
                InstanceState::Installed => &[InstanceState::Preinstall],
                InstanceState::Destroying => &[
                    InstanceState::Preinstall,
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
                    .and_where(
                        Expr::col(InstanceDef::Model).eq(id.model()),
                    )
                    .and_where(
                        Expr::col(InstanceDef::Serial).eq(id.serial()),
                    )
                    .and_where(Expr::col(InstanceDef::Seq).eq(id.seq()))
                    .value(InstanceDef::State, state)
                    .to_owned(),
            )?;
            assert_eq!(uc, 1);
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
}
