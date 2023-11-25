/*
 * Copyright 2023 Oxide Computer Company
 */

use std::{collections::HashSet, path::Path};

use anyhow::{bail, Result};
use jmclib::sqlite::rusqlite;
use rusqlite::{params, Connection, Transaction, TransactionBehavior};
use slog::Logger;
use tokio::sync::{Mutex, MutexGuard};
use types::*;

pub struct Database {
    #[allow(unused)]
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
        id: &InstanceId,
    ) -> Result<Option<Instance>> {
        let value = tx.query_row(
            &format!(
                "SELECT {cols} FROM instance WHERE nodename = ? AND seq = ?",
                cols = Instance::ALL,
            ),
            params![id.nodename(), id.seq()],
            Instance::from_row,
        );
        match value {
            Ok(value) => Ok(Some(value)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn instance_get(
        &mut self,
        id: &InstanceId,
    ) -> Result<Option<Instance>> {
        let tx = self.guard.transaction()?;

        Self::i_instance_get(&tx, id)
    }

    pub fn instance_new_state(
        &mut self,
        id: &InstanceId,
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
            Ok(None) => bail!("instance {id} not found"),
            Err(e) => bail!("fetching instance {id}: {e}"),
        };

        if i.state == state {
            return Ok(());
        }

        let valid_source_states: &[InstanceState] = match state {
            InstanceState::Unconfigured => &[],
            InstanceState::Configured => &[InstanceState::Unconfigured],
            InstanceState::Installed => &[InstanceState::Configured],
            InstanceState::ZoneOnline => &[InstanceState::Installed],
            InstanceState::Destroying => &[
                InstanceState::Unconfigured,
                InstanceState::Configured,
                InstanceState::Installed,
                InstanceState::ZoneOnline,
            ],
            InstanceState::Destroyed => &[InstanceState::Destroying],
        };

        if !valid_source_states.contains(&i.state) {
            bail!(
                "instance {id} cannot move from state {} to {state}",
                i.state,
            );
        }

        tx.prepare(
            "UPDATE instance SET state = ? \
            WHERE nodename = ? AND seq = ?",
        )?
        .execute(params![state, id.nodename(), id.seq()])?;

        tx.commit()?;

        Ok(())
    }

    pub fn instances_active(&self) -> Result<Vec<Instance>> {
        let mut q = self.guard.prepare(&format!(
            "SELECT {cols} FROM instance WHERE state <> ? \
            ORDER BY nodename ASC, seq ASC",
            cols = Instance::ALL
        ))?;

        let rows = q
            .query(params![InstanceState::Destroyed])?
            .mapped(Instance::from_row)
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    pub fn instance_create(
        &mut self,
        nodename: &str,
        worker: &str,
        lease: &str,
        target: &str,
        bootstrap: &str,
        slot: u32,
    ) -> Result<InstanceId> {
        let tx = self
            .guard
            .transaction_with_behavior(TransactionBehavior::Immediate)?;

        /*
         * Find the next available instance sequence number, starting at 1 if
         * there have not been any instances before.
         */
        let seq = {
            let mut q =
                tx.prepare("SELECT MAX(seq) FROM instance WHERE nodename = ?")?;

            q.query_row(params![nodename], |row| {
                Ok(row.get::<_, Option<i64>>(0)?.map(|n| n as u64))
            })?
            .unwrap_or(0)
            .checked_add(1)
            .unwrap()
        };

        let instance_id = {
            let instance_id = InstanceId::new(nodename, seq);

            let mut q = tx.prepare(
                "INSERT INTO instance
                (nodename, seq, worker, lease, target, state, bootstrap, slot)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            )?;

            q.execute(params![
                instance_id.nodename(),
                instance_id.seq(),
                worker,
                lease,
                target,
                InstanceState::Unconfigured,
                bootstrap,
                slot,
            ])?;

            instance_id
        };

        tx.commit()?;

        Ok(instance_id)
    }

    pub fn slots_active(&self) -> Result<HashSet<u32>> {
        let mut q =
            self.guard.prepare("SELECT slot FROM instance WHERE state <> ?")?;

        let res = q
            .query(params![InstanceState::Destroyed])?
            .mapped(|row| Ok(row.get::<_, i32>(0)?.try_into().unwrap()))
            .collect::<rusqlite::Result<HashSet<_>>>()?;

        Ok(res)
    }
}

pub mod types {
    use anyhow::{anyhow, bail, Result};
    use jmclib::sqlite::rusqlite;
    use rusqlite::{
        types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput},
        Row,
    };
    use std::{fmt::Display, str::FromStr};
    use strum::{Display, EnumString};

    #[derive(Clone, PartialEq, Eq, Hash)]
    pub struct InstanceId(String, u64);

    impl FromStr for InstanceId {
        type Err = anyhow::Error;

        fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
            if let Some((node, seq)) = s.split_once('/') {
                let seq: u64 = seq
                    .parse()
                    .map_err(|_| anyhow!("invalid instance ID {s:?}"))?;

                Ok(InstanceId(node.into(), seq))
            } else {
                bail!("invalid instance ID {s:?}");
            }
        }
    }

    impl Display for InstanceId {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}/{}", self.0, self.1)
        }
    }

    impl InstanceId {
        pub fn new(nodename: &str, seq: u64) -> InstanceId {
            assert!(seq > 0);
            InstanceId(nodename.into(), seq)
        }
        pub fn nodename(&self) -> &str {
            &self.0
        }

        pub fn seq(&self) -> u64 {
            self.1
        }

        pub fn zonename(&self) -> String {
            format!("bmat-{:08x}", self.1)
        }

        pub fn local_hostname(&self) -> String {
            format!("bmat-{}", self.flat_id())
        }

        pub fn flat_id(&self) -> String {
            format!("{}-{:08x}", self.0, self.1)
        }

        pub fn from_zonename(
            nodename: &str,
            zonename: &str,
        ) -> Result<InstanceId> {
            let Some(seq) = zonename.strip_prefix("bmat-") else {
                bail!("invalid zonename {zonename:?}");
            };

            match u64::from_str_radix(seq, 16) {
                Ok(seq) => Ok(InstanceId(nodename.into(), seq)),
                Err(e) => bail!("invalid zonename: {zonename:?}: {e}"),
            }
        }
    }

    pub struct Instance {
        pub id: InstanceId,
        pub worker: String,
        pub lease: String,
        pub target: String,
        pub state: InstanceState,
        pub bootstrap: String,
        pub slot: u32,
    }

    impl Instance {
        pub const ALL: &str =
            "nodename, seq, worker, lease, target, state, bootstrap, slot";

        pub fn from_row(row: &Row) -> rusqlite::Result<Self> {
            let nodename: String = row.get("nodename")?;

            Ok(Instance {
                id: InstanceId::new(&nodename, row.get("seq")?),
                worker: row.get("worker")?,
                lease: row.get("lease")?,
                target: row.get("target")?,
                state: row.get("state")?,
                bootstrap: row.get("bootstrap")?,
                slot: row.get("slot")?,
            })
        }
    }

    #[derive(Clone, Copy, PartialEq, Eq, EnumString, Display)]
    #[strum(serialize_all = "snake_case")]
    pub enum InstanceState {
        Unconfigured,
        Configured,
        Installed,
        ZoneOnline,
        Destroying,
        Destroyed,
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
            InstanceState::from_str(s)
                .map_err(|e| FromSqlError::Other(e.into()))
        }
    }
}
