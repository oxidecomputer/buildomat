/*
 * Copyright 2025 Oxide Computer Company
 */

use super::sublude::*;

sqlite_sql_enum!(InstanceState => {
    /// Instance has been created but agent not yet running.
    Created,

    /// Agent is running and executing the job.
    Running,

    /// Instance is being torn down.
    Destroying,

    /// Instance has been fully destroyed.
    Destroyed,
});

#[derive(Clone, Debug)]
#[enum_def(prefix = "", suffix = "Def")]
pub struct Instance {
    pub testbed_name: String,
    pub seq: InstanceSeq,
    pub worker: String,
    pub target: String,
    pub state: InstanceState,
    pub bootstrap: String,
    pub flushed: bool,
}

impl FromRow for Instance {
    fn columns() -> Vec<ColumnRef> {
        [
            InstanceDef::TestbedName,
            InstanceDef::Seq,
            InstanceDef::Worker,
            InstanceDef::Target,
            InstanceDef::State,
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
            testbed_name: row.get(0)?,
            seq: row.get(1)?,
            worker: row.get(2)?,
            target: row.get(3)?,
            state: row.get(4)?,
            bootstrap: row.get(5)?,
            flushed: row.get(6)?,
        })
    }
}

impl Instance {
    pub fn bare_columns() -> Vec<InstanceDef> {
        vec![
            InstanceDef::TestbedName,
            InstanceDef::Seq,
            InstanceDef::Worker,
            InstanceDef::Target,
            InstanceDef::State,
            InstanceDef::Bootstrap,
            InstanceDef::Flushed,
        ]
    }

    pub fn find(testbed_name: &str, seq: InstanceSeq) -> SelectStatement {
        Query::select()
            .from(InstanceDef::Table)
            .columns(Instance::columns())
            .and_where(Expr::col(InstanceDef::TestbedName).eq(testbed_name))
            .and_where(Expr::col(InstanceDef::Seq).eq(seq))
            .to_owned()
    }

    pub fn find_for_testbed(testbed_name: &str) -> SelectStatement {
        Query::select()
            .from(InstanceDef::Table)
            .columns(Instance::columns())
            .and_where(Expr::col(InstanceDef::TestbedName).eq(testbed_name))
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
            .order_by(InstanceDef::TestbedName, Order::Asc)
            .to_owned()
    }

    pub fn insert(&self) -> InsertStatement {
        Query::insert()
            .into_table(InstanceDef::Table)
            .columns(Self::bare_columns())
            .values_panic([
                self.testbed_name.clone().into(),
                self.seq.into(),
                self.worker.clone().into(),
                self.target.clone().into(),
                self.state.into(),
                self.bootstrap.clone().into(),
                self.flushed.into(),
            ])
            .to_owned()
    }

    /// Instance ID in format "testbed_name/seq"
    pub fn id(&self) -> String {
        format!("{}/{}", self.testbed_name, self.seq.0)
    }

    pub fn destroyed(&self) -> bool {
        matches!(self.state, InstanceState::Destroyed)
    }

    pub fn should_teardown(&self) -> bool {
        matches!(
            self.state,
            InstanceState::Destroying | InstanceState::Destroyed,
        )
    }
}

#[cfg(test)]
mod test {
    use super::InstanceState;
    use std::str::FromStr;

    const INSTANCE_STATES: &[(&str, InstanceState)] = &[
        ("created", InstanceState::Created),
        ("running", InstanceState::Running),
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
