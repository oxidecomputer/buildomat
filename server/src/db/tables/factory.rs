/*
 * Copyright 2026 Oxide Computer Company
 */

use super::sublude::*;

#[derive(Debug, Clone)]
#[enum_def(prefix = "", suffix = "Def")]
pub struct Factory {
    pub id: FactoryId,
    pub name: String,
    pub token: String,
    pub lastping: Option<IsoDate>,
    pub enable: bool,

    /**
     * For debugging purposes, it can be helpful to configure a particular
     * factory so that each worker it creates is marked held.  Worker creation
     * will proceed as it normally would, but a job will not be assigned until
     * the hold is explicitly released by the operator, allowing the operator to
     * inspect or modify the worker environment prior to the start of job
     * execution.
     */
    pub hold_workers: bool,

    /**
     * When workers are marked as held, they are kept for an indefinite period
     * of time.  Some factories have limited available slots, or expensive
     * time-based billing.  In those cases, we may only want to hold workers
     * for a limited period of time before recycling them automatically.
     */
    pub max_hold_age: Option<Seconds>,
}

impl FromRow for Factory {
    fn columns() -> Vec<ColumnRef> {
        [
            FactoryDef::Id,
            FactoryDef::Name,
            FactoryDef::Token,
            FactoryDef::Lastping,
            FactoryDef::Enable,
            FactoryDef::HoldWorkers,
            FactoryDef::MaxHoldAge,
        ]
        .into_iter()
        .map(|col| {
            ColumnRef::TableColumn(
                SeaRc::new(FactoryDef::Table),
                SeaRc::new(col),
            )
        })
        .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<Factory> {
        Ok(Factory {
            id: row.get(0)?,
            name: row.get(1)?,
            token: row.get(2)?,
            lastping: row.get(3)?,
            enable: row.get(4)?,
            hold_workers: row.get(5)?,
            max_hold_age: row.get(6)?,
        })
    }
}

impl Factory {
    pub fn find(id: FactoryId) -> SelectStatement {
        Query::select()
            .from(FactoryDef::Table)
            .columns(Factory::columns())
            .and_where(Expr::col(FactoryDef::Id).eq(id))
            .to_owned()
    }

    pub fn insert(&self) -> InsertStatement {
        Query::insert()
            .into_table(FactoryDef::Table)
            .columns(Self::bare_columns())
            .values_panic([
                self.id.into(),
                self.name.clone().into(),
                self.token.clone().into(),
                self.lastping.into(),
                self.enable.into(),
                self.hold_workers.into(),
                self.max_hold_age.into(),
            ])
            .to_owned()
    }

    pub fn max_hold_age(&self) -> Option<Duration> {
        self.max_hold_age.map(|s| Duration::from_secs(s.0))
    }
}
