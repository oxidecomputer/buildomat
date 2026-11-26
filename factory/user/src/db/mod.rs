/*
 * Copyright 2026 Oxide Computer Company
 */

use crate::db::types::WorkerId;
use anyhow::Result;
use buildomat_database::{conflict, DBResult, FromRow, Sqlite};
use sea_query::{Expr, Order, Query};
use slog::Logger;
use std::path::Path;

mod tables;

pub mod types {
    use buildomat_database::{
        rusqlite, sqlite_integer_new_type, sqlite_ulid_new_type,
    };

    sqlite_ulid_new_type!(JobId);
    sqlite_ulid_new_type!(TargetId);
    sqlite_ulid_new_type!(WorkerId);
    sqlite_integer_new_type!(InstanceSeq, u64, BigUnsigned);

    pub use super::tables::WorkerState;
}

pub use tables::*;

pub struct Database {
    sql: Sqlite,
}

impl Database {
    pub fn new<P: AsRef<Path>>(
        log: Logger,
        path: P,
        cache_kb: Option<u32>,
    ) -> Result<Database> {
        let sql = Sqlite::setup(
            log,
            path,
            include_str!("../../schema.sql"),
            cache_kb,
        )?;

        Ok(Database { sql })
    }

    pub fn workers(&self) -> DBResult<Vec<Worker>> {
        self.sql.tx(|tx| {
            tx.get_rows(
                Query::select()
                    .from(WorkerDef::Table)
                    .columns(Worker::columns())
                    .order_by(WorkerDef::Id, Order::Asc)
                    .to_owned(),
            )
        })
    }

    pub fn worker_get(&self, id: WorkerId) -> DBResult<Option<Worker>> {
        self.sql.tx(|tx| tx.get_row_opt(Worker::find(id)))
    }

    pub fn worker_create(&self, create: Worker) -> DBResult<WorkerId> {
        self.sql.tx_immediate(|h| {
            let count = h.exec_insert(create.insert())?;
            assert_eq!(count, 1);
            Ok(create.id)
        })
    }

    pub fn worker_delete(&self, id: WorkerId) -> DBResult<()> {
        self.sql.tx_immediate(|h| {
            let count = h.exec_delete(
                Query::delete()
                    .from_table(WorkerDef::Table)
                    .and_where(Expr::col(WorkerDef::Id).eq(id))
                    .to_owned(),
            )?;
            assert_eq!(count, 1);
            Ok(())
        })
    }

    pub fn worker_new_state(
        &self,
        id: WorkerId,
        state: WorkerState,
    ) -> DBResult<()> {
        self.sql.tx_immediate(|tx| {
            /*
             * Get the existing state of this worker:
             */
            let worker: Worker = tx.get_row(Worker::find(id))?;

            if worker.state == state {
                return Ok(());
            }

            let valid_source_states: &[WorkerState] = match state {
                WorkerState::Unconfigured => &[],
                WorkerState::Configured => &[WorkerState::Unconfigured],
                WorkerState::Broken => {
                    &[WorkerState::Unconfigured, WorkerState::Configured]
                }
                WorkerState::Destroying => &[
                    WorkerState::Broken,
                    WorkerState::Unconfigured,
                    WorkerState::Configured,
                ],
                WorkerState::Destroyed => &[WorkerState::Destroying],
            };

            if !valid_source_states.contains(&worker.state) {
                conflict!(
                    "worker {id} cannot move from state {} to {state}",
                    worker.state,
                );
            }

            let count = tx.exec_update(
                Query::update()
                    .table(WorkerDef::Table)
                    .and_where(Expr::col(WorkerDef::Id).eq(id))
                    .value(WorkerDef::State, state)
                    .to_owned(),
            )?;
            assert_eq!(count, 1);
            Ok(())
        })
    }
}
