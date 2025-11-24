/*
 * Copyright 2023 Oxide Computer Company
 */

use super::sublude::*;
use anyhow::{Result, anyhow, bail};

sqlite_sql_enum!(InstanceState => {
    Unconfigured,
    Configured,
    Installed,
    ZoneOnline,
    Destroying,
    Destroyed,
});

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

impl std::fmt::Display for InstanceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.0, self.1)
    }
}

impl InstanceId {
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

    pub fn from_zonename(nodename: &str, zonename: &str) -> Result<InstanceId> {
        let Some(seq) = zonename.strip_prefix("bmat-") else {
            bail!("invalid zonename {zonename:?}");
        };

        match u64::from_str_radix(seq, 16) {
            Ok(seq) => Ok(InstanceId(nodename.into(), seq)),
            Err(e) => bail!("invalid zonename: {zonename:?}: {e}"),
        }
    }
}

#[derive(Clone, Debug)]
#[enum_def(prefix = "", suffix = "Def")]
pub struct Instance {
    pub nodename: String,
    pub seq: InstanceSeq,
    pub worker: String,
    pub lease: String,
    pub target: String,
    pub state: InstanceState,
    pub bootstrap: String,
    pub slot: u32,
}

impl FromRow for Instance {
    fn columns() -> Vec<ColumnRef> {
        [
            InstanceDef::Nodename,
            InstanceDef::Seq,
            InstanceDef::Worker,
            InstanceDef::Lease,
            InstanceDef::Target,
            InstanceDef::State,
            InstanceDef::Bootstrap,
            InstanceDef::Slot,
        ]
        .into_iter()
        .map(|col| {
            ColumnRef::TableColumn(
                SeaRc::new(InstanceDef::Table),
                SeaRc::new(col),
            )
        })
        .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<Self> {
        Ok(Instance {
            nodename: row.get(0)?,
            seq: row.get(1)?,
            worker: row.get(2)?,
            lease: row.get(3)?,
            target: row.get(4)?,
            state: row.get(5)?,
            bootstrap: row.get(6)?,
            slot: row.get(7)?,
        })
    }
}

impl Instance {
    pub fn find(id: &InstanceId) -> SelectStatement {
        Query::select()
            .from(InstanceDef::Table)
            .columns(Instance::columns())
            .and_where(Expr::col(InstanceDef::Nodename).eq(&id.0))
            .and_where(Expr::col(InstanceDef::Seq).eq(id.1))
            .to_owned()
    }

    pub fn insert(&self) -> InsertStatement {
        Query::insert()
            .into_table(InstanceDef::Table)
            .columns(Self::bare_columns())
            .values_panic([
                self.nodename.clone().into(),
                self.seq.into(),
                self.worker.clone().into(),
                self.lease.clone().into(),
                self.target.clone().into(),
                self.state.into(),
                self.bootstrap.clone().into(),
                self.slot.into(),
            ])
            .to_owned()
    }

    pub fn id(&self) -> InstanceId {
        InstanceId(self.nodename.clone(), self.seq.0)
    }

    pub fn new(
        nodename: &str,
        seq: u64,
        ci: crate::db::CreateInstance,
    ) -> Instance {
        assert!(seq > 0);

        Instance {
            nodename: nodename.to_string(),
            seq: InstanceSeq(seq),
            state: InstanceState::Unconfigured,
            worker: ci.worker,
            lease: ci.lease,
            target: ci.target,
            bootstrap: ci.bootstrap,
            slot: ci.slot,
        }
    }
}
