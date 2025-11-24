/*
 * Copyright 2025 Oxide Computer Company
 */

use super::sublude::*;
use anyhow::{Result, anyhow, bail};

sqlite_sql_enum!(InstanceState => {
    /**
     * Take control of the machine, which may be in an unknown state.
     */
    Preinstall,

    /**
     * The machine has been returned to a known state, so we can begin installng
     * the agent.
     */
    Installing,

    /**
     * Once the system has been installed and provided with the token, it
     * remains in the installed state until we are told to tear it down.
     */
    Installed,

    /**
     * Destruction of the instance has been requested (for whatever reason) and
     * we need to reset the system and perform any housekeeping required to
     * clean out detritus from this instance.
     */
    Destroying,

    /**
     * This instance has come to rest.
     */
    Destroyed,
});

/*
 * Instances are named for the host on which they run, and a sequence number
 * incremented for each new instance run on that host.
 */
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct InstanceId {
    model: String,
    serial: String,
    seq: u64,
}

impl FromStr for InstanceId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /*
         * Instance IDs should have three non-empty components separated by
         * slashes; e.g., "gimlet/BRM42220010/123982".
         */
        let t = s.split('/').collect::<Vec<_>>();
        if t.len() != 3
            || t.iter().any(|s| {
                s.trim().is_empty()
                    || s.trim() != *s
                    || s.chars().any(|c| !c.is_ascii_alphanumeric())
            })
        {
            bail!("invalid instance ID {s:?}");
        }

        let seq: u64 =
            t[2].parse().map_err(|_| anyhow!("invalid instance ID {s:?}"))?;

        Ok(InstanceId {
            model: t[0].to_string(),
            serial: t[1].to_string(),
            seq,
        })
    }
}

impl std::fmt::Display for InstanceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let InstanceId { model, serial, seq } = self;

        write!(f, "{model}/{serial}/{seq}")
    }
}

impl InstanceId {
    pub fn model(&self) -> &str {
        &self.model
    }

    pub fn serial(&self) -> &str {
        &self.serial
    }

    pub fn seq(&self) -> u64 {
        self.seq
    }

    pub fn host(&self) -> crate::HostId {
        crate::HostId { model: self.model.clone(), serial: self.serial.clone() }
    }
}

#[derive(Clone, Debug)]
#[enum_def(prefix = "", suffix = "Def")]
pub struct Instance {
    pub model: String,
    pub serial: String,
    pub seq: InstanceSeq,

    pub worker: String,
    pub lease: String,
    pub target: String,
    pub state: InstanceState,
    pub bootstrap: String,
}

impl FromRow for Instance {
    fn columns() -> Vec<ColumnRef> {
        [
            InstanceDef::Model,
            InstanceDef::Serial,
            InstanceDef::Seq,
            InstanceDef::Worker,
            InstanceDef::Lease,
            InstanceDef::Target,
            InstanceDef::State,
            InstanceDef::Bootstrap,
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
            model: row.get(0)?,
            serial: row.get(1)?,
            seq: row.get(2)?,

            worker: row.get(3)?,
            lease: row.get(4)?,
            target: row.get(5)?,
            state: row.get(6)?,
            bootstrap: row.get(7)?,
        })
    }
}

impl Instance {
    pub fn find(id: &InstanceId) -> SelectStatement {
        Query::select()
            .from(InstanceDef::Table)
            .columns(Instance::columns())
            .and_where(Expr::col(InstanceDef::Model).eq(&id.model))
            .and_where(Expr::col(InstanceDef::Serial).eq(&id.serial))
            .and_where(Expr::col(InstanceDef::Seq).eq(id.seq))
            .to_owned()
    }

    pub fn insert(&self) -> InsertStatement {
        Query::insert()
            .into_table(InstanceDef::Table)
            .columns(Self::bare_columns())
            .values_panic([
                self.model.clone().into(),
                self.serial.clone().into(),
                self.seq.into(),
                self.worker.clone().into(),
                self.lease.clone().into(),
                self.target.clone().into(),
                self.state.into(),
                self.bootstrap.clone().into(),
            ])
            .to_owned()
    }

    pub fn id(&self) -> InstanceId {
        InstanceId {
            model: self.model.clone(),
            serial: self.serial.clone(),
            seq: self.seq.into(),
        }
    }

    pub fn new(
        host: &crate::HostId,
        seq: u64,
        ci: crate::db::CreateInstance,
    ) -> Instance {
        assert!(seq > 0);

        Instance {
            model: host.model.to_string(),
            serial: host.serial.to_string(),
            seq: InstanceSeq(seq),

            state: InstanceState::Preinstall,
            worker: ci.worker,
            lease: ci.lease,
            target: ci.target,
            bootstrap: ci.bootstrap,
        }
    }
}
