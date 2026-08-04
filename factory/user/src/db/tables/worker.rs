/*
 * Copyright 2026 Oxide Computer Company
 */

use super::sublude::*;

sqlite_sql_enum!(WorkerState => {
    Unconfigured,
    Configured,
    Broken,
    Destroying,
    Destroyed,
});

#[derive(Clone, Debug)]
#[enum_def(prefix = "", suffix = "Def")]
pub(crate) struct Worker {
    pub(crate) id: WorkerId,
    pub(crate) slot: String,
    pub(crate) state: WorkerState,
    pub(crate) leased_job: JobId,
    pub(crate) bootstrap: String,
}

impl FromRow for Worker {
    fn columns() -> Vec<ColumnRef> {
        [
            WorkerDef::Id,
            WorkerDef::Slot,
            WorkerDef::State,
            WorkerDef::LeasedJob,
            WorkerDef::Bootstrap,
        ]
        .into_iter()
        .map(|col| {
            ColumnRef::TableColumn(
                SeaRc::new(WorkerDef::Table),
                SeaRc::new(col),
            )
        })
        .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<Self> {
        Ok(Worker {
            id: row.get(0)?,
            slot: row.get(1)?,
            state: row.get(2)?,
            leased_job: row.get(3)?,
            bootstrap: row.get(4)?,
        })
    }
}

impl Worker {
    pub(crate) fn find(id: WorkerId) -> SelectStatement {
        Query::select()
            .from(WorkerDef::Table)
            .columns(Worker::columns())
            .and_where(Expr::col(WorkerDef::Id).eq(id))
            .to_owned()
    }

    pub(crate) fn insert(&self) -> InsertStatement {
        Query::insert()
            .into_table(WorkerDef::Table)
            .columns(Self::bare_columns())
            .values_panic([
                self.id.into(),
                self.slot.clone().into(),
                self.state.into(),
                self.leased_job.into(),
                self.bootstrap.clone().into(),
            ])
            .to_owned()
    }
}
