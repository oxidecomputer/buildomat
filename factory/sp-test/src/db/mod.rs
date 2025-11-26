/*
 * Copyright 2025 Oxide Computer Company
 */

//! Database module for SP-Test Factory
//!
//! Tracks instance state for testbed assignments.

use std::path::Path;

use anyhow::Result;
use buildomat_database::{DBResult, Handle, Sqlite, conflict};
use sea_query::{Expr, Query};
#[allow(unused_imports)]
use slog::{Logger, debug, error, info, warn};

mod tables;

mod types {
    use buildomat_database::{rusqlite, sqlite_integer_new_type};

    sqlite_integer_new_type!(InstanceSeq, u64, BigUnsigned);
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

    fn next_seq_for_testbed(
        &self,
        testbed_name: &str,
        h: &mut Handle,
    ) -> DBResult<InstanceSeq> {
        let max: Option<InstanceSeq> = h.get_row(
            Query::select()
                .from(InstanceDef::Table)
                .expr(Expr::col(InstanceDef::Seq).max())
                .and_where(Expr::col(InstanceDef::TestbedName).eq(testbed_name))
                .to_owned(),
        )?;

        Ok(InstanceSeq(max.map(|v| v.0).unwrap_or(0).checked_add(1).unwrap()))
    }

    /// Get the active instance for a testbed, if any.
    pub fn instance_for_testbed(
        &self,
        testbed_name: &str,
    ) -> DBResult<Option<Instance>> {
        self.sql.tx(|h| h.get_row_opt(Instance::find_for_testbed(testbed_name)))
    }

    /// Get a specific instance by testbed and sequence.
    pub fn instance_get(
        &self,
        testbed_name: &str,
        seq: InstanceSeq,
    ) -> DBResult<Option<Instance>> {
        self.sql.tx(|h| h.get_row_opt(Instance::find(testbed_name, seq)))
    }

    /// Create a new instance for a testbed.
    pub fn instance_create(
        &self,
        testbed_name: &str,
        target: &str,
        worker: &str,
        bootstrap: &str,
    ) -> DBResult<Instance> {
        self.sql.tx_immediate(|h| {
            let existing = h.get_row_opt::<Instance>(
                Instance::find_for_testbed(testbed_name),
            )?;
            if existing.is_some() {
                conflict!(
                    "testbed {testbed_name} already has an active instance"
                );
            }

            let i = Instance {
                testbed_name: testbed_name.to_string(),
                seq: self.next_seq_for_testbed(testbed_name, h)?,
                worker: worker.to_string(),
                target: target.to_string(),
                state: InstanceState::Created,
                bootstrap: bootstrap.to_string(),
                flushed: false,
            };

            let ic = h.exec_insert(i.insert())?;
            assert_eq!(ic, 1);

            Ok(i)
        })
    }

    /// Get all active instances.
    pub fn active_instances(&self) -> DBResult<Vec<Instance>> {
        self.sql.tx(|h| h.get_rows(Instance::find_active()))
    }

    /// Mark an instance for destruction.
    pub fn instance_destroy(&self, i: &Instance) -> DBResult<()> {
        self.sql.tx_immediate(|h| {
            let i: Instance =
                h.get_row(Instance::find(&i.testbed_name, i.seq))?;

            match i.state {
                InstanceState::Created | InstanceState::Running => {
                    let uc = h.exec_update(
                        Query::update()
                            .table(InstanceDef::Table)
                            .and_where(
                                Expr::col(InstanceDef::TestbedName)
                                    .eq(i.testbed_name),
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

    /// Mark an instance as fully destroyed.
    pub fn instance_mark_destroyed(&self, i: &Instance) -> DBResult<()> {
        self.sql.tx_immediate(|h| {
            let i: Instance =
                h.get_row(Instance::find(&i.testbed_name, i.seq))?;

            match i.state {
                InstanceState::Created | InstanceState::Running => {
                    conflict!("instance was not already being destroyed");
                }
                InstanceState::Destroying => {
                    let uc = h.exec_update(
                        Query::update()
                            .table(InstanceDef::Table)
                            .and_where(
                                Expr::col(InstanceDef::TestbedName)
                                    .eq(i.testbed_name),
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

    /// Mark an instance as running (agent is online).
    pub fn instance_mark_running(&self, i: &Instance) -> DBResult<()> {
        self.sql.tx_immediate(|h| {
            let i: Instance =
                h.get_row(Instance::find(&i.testbed_name, i.seq))?;

            match i.state {
                InstanceState::Created => {
                    let uc = h.exec_update(
                        Query::update()
                            .table(InstanceDef::Table)
                            .and_where(
                                Expr::col(InstanceDef::TestbedName)
                                    .eq(i.testbed_name),
                            )
                            .and_where(Expr::col(InstanceDef::Seq).eq(i.seq))
                            .value(InstanceDef::State, InstanceState::Running)
                            .to_owned(),
                    )?;
                    assert_eq!(uc, 1);
                }
                InstanceState::Running => (),
                InstanceState::Destroying | InstanceState::Destroyed => {
                    conflict!("instance already being destroyed");
                }
            }

            Ok(())
        })
    }

    /// Mark an instance as flushed (ready for job).
    pub fn instance_mark_flushed(&self, i: &Instance) -> DBResult<()> {
        self.sql.tx(|h| {
            let uc = h.exec_update(
                Query::update()
                    .table(InstanceDef::Table)
                    .and_where(
                        Expr::col(InstanceDef::TestbedName).eq(&i.testbed_name),
                    )
                    .and_where(Expr::col(InstanceDef::Seq).eq(i.seq))
                    .value(InstanceDef::Flushed, true)
                    .to_owned(),
            )?;
            assert_eq!(uc, 1);

            Ok(())
        })
    }
}
