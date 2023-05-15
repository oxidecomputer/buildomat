/*
 * Copyright 2023 Oxide Computer Company
 */

use std::path::Path;

use anyhow::{bail, Result};
use jmclib::sqlite::rusqlite;
use rusqlite::{
    params, Connection, OptionalExtension, Row, Transaction,
    TransactionBehavior,
};
use slog::Logger;
use tokio::sync::{Mutex, MutexGuard};

pub struct Database {
    log: Logger,
    conn: Mutex<Connection>,
}

impl Database {
    pub fn open<P: AsRef<Path>>(log: Logger, dbfile: P) -> Result<Self> {
        let p = dbfile.as_ref();

        let conn = jmclib::sqlite::SqliteSetup::new()
            .create(true)
            .check_integrity(true)
            .schema(include_str!("../schema.sql"))
            .log(log.clone())
            .open(p)?;

        Ok(Database { log, conn: Mutex::new(conn) })
    }

    pub async fn connect(&self) -> Result<Handle> {
        let guard = self.conn.lock().await;

        Ok(Handle { guard })
    }
}

pub struct Handle<'a> {
    guard: MutexGuard<'a, Connection>,
}

impl Handle<'_> {
    fn i_instance_get(
        tx: &Transaction,
        id: InstanceId,
    ) -> Result<Option<Instance>> {
        let value = tx.query_row(
            &format!(
                "SELECT {cols} FROM instance WHERE instance = ?",
                cols = Instance::ALL,
            ),
            params![id],
            Instance::from_row,
        );
        match value {
            Ok(value) => Ok(Some(value)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn instance_get(&mut self, id: InstanceId) -> Result<Option<Instance>> {
        let tx = self.guard.transaction()?;

        Self::i_instance_get(&tx, id)
    }

    pub fn instance_new_state(
        &mut self,
        id: InstanceId,
        state: InstanceState,
    ) -> Result<()> {
        let tx = self
            .guard
            .transaction_with_behavior(TransactionBehavior::Immediate)?;

        /*
         * Get the existing state of this instance:
         */
        let i = match Self::i_instance_get(&tx, id) {
            Ok(Some(i)) => i,
            Ok(None) => {
                bail!("instance {id} not found");
            }
            Err(e) => {
                bail!("fetching instance {id}: {e}");
            }
        };

        if i.state == state {
            return Ok(());
        }

        tx.prepare("UPDATE instance SET state = ? WHERE id = ?")?
            .execute(params![state, id])?;

        Ok(())
    }

    pub fn instances_active(&self) -> Result<Vec<Instance>> {
        let mut q = self.guard.prepare(&format!(
            "SELECT {cols} FROM instance WHERE state <> ? ORDER BY id ASC",
            cols = Instance::ALL
        ))?;

        let rows = q
            .query(params![InstanceState::Destroyed])?
            .mapped(Instance::from_row)
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    pub fn instance_create(
        &self,
        worker: &str,
        target: &str,
        bootstrap: &str,
    ) -> Result<InstanceId> {
        let instance_id = InstanceId::from(Uuid::new_v4());

        let mut q = self.guard.prepare(
            "INSERT INTO instance
            (id, worker, target, state, bootstrap, flushed)
            VALUES (?, ?, ?, ?, ?, ?)",
        )?;

        q.execute(params![
            instance_id,
            worker,
            target,
            InstanceState::Unconfigured,
            bootstrap,
            true,
        ])?;

        Ok(instance_id)
    }
}

pub mod types {
    use anyhow::{bail, Result};
    use chrono::prelude::*;
    use jmclib::sqlite::rusqlite;
    use rusqlite::{
        types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput},
        Row,
    };
    use std::{fmt::Display, str::FromStr};
    use uuid::Uuid;

    #[derive(Clone, Copy)]
    pub struct InstanceId(Uuid);

    impl Display for InstanceId {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    impl Into<Uuid> for InstanceId {
        fn into(self) -> Uuid {
            self.0
        }
    }

    impl From<Uuid> for InstanceId {
        fn from(value: Uuid) -> Self {
            InstanceId(value)
        }
    }

    impl FromSql for InstanceId {
        fn column_result(
            value: rusqlite::types::ValueRef<'_>,
        ) -> FromSqlResult<Self> {
            Ok(InstanceId(
                Uuid::from_str(value.as_str()?)
                    .map_err(|e| FromSqlError::Other(e.into()))?,
            ))
        }
    }

    impl ToSql for InstanceId {
        fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
            Ok(self.0.to_string().into())
        }
    }

    pub struct Instance {
        pub id: InstanceId,
        pub worker: String,
        pub target: String,
        pub state: InstanceState,
        pub bootstrap: String,
        pub flushed: bool,
    }

    impl Instance {
        pub const ALL: &str = "id, worker, target, state, bootstrap, flushed";

        pub fn from_row(row: &Row) -> rusqlite::Result<Self> {
            Ok(Instance {
                id: row.get("id")?,
                worker: row.get("worker")?,
                target: row.get("target")?,
                state: row.get("state")?,
                bootstrap: row.get("bootstrap")?,
                flushed: row.get("flushed")?,
            })
        }

        pub fn zonename(&self) -> String {
            format!("bmat-{}", self.id.0)
        }
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    pub enum InstanceState {
        Unconfigured,
        Configured,
        Installed,
        ZoneOnline,
        InstanceCreated,
        InstanceRunning,
        Destroying,
        Destroyed,
    }

    impl FromStr for InstanceState {
        type Err = anyhow::Error;

        fn from_str(s: &str) -> Result<Self> {
            use InstanceState::*;

            Ok(match s {
                "unconfigured" => Unconfigured,
                "configured" => Configured,
                "installed" => Installed,
                "zone_online" => ZoneOnline,
                "instance_created" => InstanceCreated,
                "instance_running" => InstanceRunning,
                "destroying" => Destroying,
                "destroyed" => Destroyed,
                x => bail!("unknown instance state: {x:?}"),
            })
        }
    }

    impl std::fmt::Display for InstanceState {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            use InstanceState::*;

            write!(
                f,
                "{}",
                match self {
                    Unconfigured => "unconfigured",
                    Configured => "configured",
                    Installed => "installed",
                    ZoneOnline => "zone_online",
                    InstanceCreated => "instance_created",
                    InstanceRunning => "instance_running",
                    Destroying => "destroying",
                    Destroyed => "destroyed",
                }
            )
        }
    }

    impl ToSql for InstanceState {
        fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
            Ok(self.to_string().into())
        }
    }

    impl FromSql for InstanceState {
        fn column_result(
            value: rusqlite::types::ValueRef<'_>,
        ) -> FromSqlResult<Self> {
            let s = value.as_str()?;
            Ok(InstanceState::from_str(s)
                .map_err(|e| FromSqlError::Other(e.into()))?)
        }
    }

    pub struct EventSeq(u32);

    pub struct InstanceEvent {
        pub id: Uuid,
        pub seq: EventSeq,
        pub stream: String,
        pub payload: String,
        pub uploaded: bool,
        pub time: DateTime<Utc>,
    }
}
use types::*;
use uuid::Uuid;
