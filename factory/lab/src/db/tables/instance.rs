/*
 * Copyright 2023 Oxide Computer Company
 */

use super::sublude::*;

sqlite_sql_enum!(InstanceState => {
    /**
     * The instance has been created.  We will instruct iPXE to download the
     * boot media (kernel and ramdisk) as well as our postboot script.  The
     * postboot script will signal to the factory once the OS has started up,
     * moving us to the "Booted" state.
     */
    Preboot,

    /**
     * The OS has booted to the point where our postboot script signalled to us.
     * If we get another request for the iPXE scripts, we assume some
     * catastrophe has befallen the host and begin destroying the instance.
     */
    Booted,

    /**
     * Destruction of the instance has been requested for whatever reason.  We
     * need to reset the machine, and upload any final status records to the
     * central server.
     */
    Destroying,

    /**
     * This instance has come to rest.
     */
    Destroyed,
});

#[derive(Clone, Debug)]
#[enum_def(prefix = "", suffix = "Def")]
pub struct Instance {
    pub nodename: String,
    pub seq: InstanceSeq,
    pub worker: String,
    pub target: String,
    pub state: InstanceState,
    pub key: String,
    pub bootstrap: String,
    pub flushed: bool,
}

impl FromRow for Instance {
    fn columns() -> Vec<ColumnRef> {
        [
            InstanceDef::Nodename,
            InstanceDef::Seq,
            InstanceDef::Worker,
            InstanceDef::Target,
            InstanceDef::State,
            InstanceDef::Key,
            InstanceDef::Bootstrap,
            InstanceDef::Flushed,
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
            target: row.get(3)?,
            state: row.get(4)?,
            key: row.get(5)?,
            bootstrap: row.get(6)?,
            flushed: row.get(7)?,
        })
    }
}

impl Instance {
    pub fn find(nodename: &str, seq: InstanceSeq) -> SelectStatement {
        Query::select()
            .from(InstanceDef::Table)
            .columns(Instance::columns())
            .and_where(Expr::col(InstanceDef::Nodename).eq(nodename))
            .and_where(Expr::col(InstanceDef::Seq).eq(seq))
            .to_owned()
    }

    pub fn find_for_host(nodename: &str) -> SelectStatement {
        Query::select()
            .from(InstanceDef::Table)
            .columns(Instance::columns())
            .and_where(Expr::col(InstanceDef::Nodename).eq(nodename))
            /*
             * There may be many instances for a host, but at most one should be
             * in a state other than "destroyed" at any time.
             */
            .and_where(
                Expr::col(InstanceDef::State).ne(InstanceState::Destroyed),
            )
            .to_owned()
    }

    pub fn find_active() -> SelectStatement {
        Query::select()
            .from(InstanceDef::Table)
            .columns(Instance::columns())
            .and_where(
                Expr::col(InstanceDef::State).ne(InstanceState::Destroyed),
            )
            .order_by(InstanceDef::Nodename, Order::Asc)
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
                self.target.clone().into(),
                self.state.into(),
                self.key.clone().into(),
                self.bootstrap.clone().into(),
                self.flushed.into(),
            ])
            .to_owned()
    }

    pub fn id(&self) -> String {
        format!("{}/{}", self.nodename, self.seq.0)
    }

    /**
     * This instance has been completely destroyed.  The host has been reset and
     * is holding at iPXE again.  All remaining state has been forwarded to the
     * core server.
     */
    pub fn destroyed(&self) -> bool {
        matches!(self.state, InstanceState::Destroyed)
    }

    pub fn target(&self) -> &str {
        &self.target
    }

    /**
     * This instance has been created but the host has not yet booted.
     */
    pub fn is_preboot(&self) -> bool {
        matches!(self.state, InstanceState::Preboot)
    }

    /**
     * This instance has been marked for destruction but has not yet been
     * destroyed.  Work to finalise instance state is ongoing.
     */
    pub fn should_teardown(&self) -> bool {
        matches!(
            self.state,
            InstanceState::Destroying | InstanceState::Destroyed,
        )
    }
}

/*
 * These tests attempt to ensure that the concrete representation of the enum
 * does not change, as that would make the database unuseable.
 */
#[cfg(test)]
mod test {
    use super::InstanceState;
    use std::str::FromStr;

    const INSTANCE_STATES: &'static [(&'static str, InstanceState)] = &[
        ("preboot", InstanceState::Preboot),
        ("booted", InstanceState::Booted),
        ("destroying", InstanceState::Destroying),
        ("destroyed", InstanceState::Destroyed),
    ];

    #[test]
    fn instance_state_forward() {
        for (s, e) in INSTANCE_STATES {
            assert_eq!(*s, e.to_string());
        }
    }

    #[test]
    fn instance_state_backward() {
        for (s, e) in INSTANCE_STATES {
            assert_eq!(InstanceState::from_str(s).unwrap(), *e);
        }
    }
}
