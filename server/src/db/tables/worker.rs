/*
 * Copyright 2024 Oxide Computer Company
 */

use super::sublude::*;

#[derive(Debug, Clone)]
#[enum_def(prefix = "", suffix = "Def")]
pub struct Worker {
    pub id: WorkerId,
    pub bootstrap: String,
    pub token: Option<String>,
    pub factory_private: Option<String>,
    pub deleted: bool,
    pub recycle: bool,
    pub lastping: Option<IsoDate>,
    pub factory: Option<FactoryId>,
    pub target: Option<TargetId>,
    pub wait_for_flush: bool,
    pub factory_metadata: Option<JsonValue>,
    pub hold_time: Option<IsoDate>,
    pub hold_reason: Option<String>,
}

impl FromRow for Worker {
    fn columns() -> Vec<ColumnRef> {
        [
            WorkerDef::Id,
            WorkerDef::Bootstrap,
            WorkerDef::Token,
            WorkerDef::FactoryPrivate,
            WorkerDef::Deleted,
            WorkerDef::Recycle,
            WorkerDef::Lastping,
            WorkerDef::Factory,
            WorkerDef::Target,
            WorkerDef::WaitForFlush,
            WorkerDef::FactoryMetadata,
            WorkerDef::HoldTime,
            WorkerDef::HoldReason,
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

    fn from_row(row: &Row) -> rusqlite::Result<Worker> {
        Ok(Worker {
            id: row.get(0)?,
            bootstrap: row.get(1)?,
            token: row.get(2)?,
            factory_private: row.get(3)?,
            deleted: row.get(4)?,
            recycle: row.get(5)?,
            lastping: row.get(6)?,
            factory: row.get(7)?,
            target: row.get(8)?,
            wait_for_flush: row.get(9)?,
            factory_metadata: row.get(10)?,
            hold_time: row.get(11)?,
            hold_reason: row.get(12)?,
        })
    }
}

impl Worker {
    pub fn find(id: WorkerId) -> SelectStatement {
        Query::select()
            .from(WorkerDef::Table)
            .columns(Worker::columns())
            .and_where(Expr::col(WorkerDef::Id).eq(id))
            .to_owned()
    }

    pub fn insert(&self) -> InsertStatement {
        Query::insert()
            .into_table(WorkerDef::Table)
            .columns(Self::bare_columns())
            .values_panic([
                self.id.into(),
                self.bootstrap.clone().into(),
                self.token.clone().into(),
                self.factory_private.clone().into(),
                self.deleted.into(),
                self.recycle.into(),
                self.lastping.into(),
                self.factory.into(),
                self.target.into(),
                self.wait_for_flush.into(),
                self.factory_metadata.clone().into(),
                self.hold_time.into(),
                self.hold_reason.clone().into(),
            ])
            .to_owned()
    }

    pub fn legacy_default_factory_id() -> FactoryId {
        /*
         * XXX No new workers will be created without a factory, but old records
         * might not have had one.  This is the ID of a made up factory that
         * does not otherwise exist:
         */
        FactoryId::from_str("00E82MSW0000000000000FF000").unwrap()
    }

    pub fn agent_ok(&self) -> bool {
        /*
         * Until we have a token from the worker, the agent hasn't started up
         * yet.
         */
        self.token.is_some()
    }

    pub fn age(&self) -> Duration {
        Utc::now()
            .signed_duration_since(self.id.datetime())
            .to_std()
            .unwrap_or_else(|_| Duration::from_secs(0))
    }

    pub fn factory(&self) -> FactoryId {
        self.factory.unwrap_or_else(Worker::legacy_default_factory_id)
    }

    pub fn target(&self) -> TargetId {
        self.target.unwrap_or_else(|| {
            /*
             * XXX No new records should be created without a resolved target
             * ID, but old records might not have had one.  This is the ID of
             * the canned "default" target:
             */
            TargetId::from_str("00E82MSW0000000000000TT000").unwrap()
        })
    }

    pub fn factory_metadata(
        &self,
    ) -> Result<Option<metadata::FactoryMetadata>> {
        self.factory_metadata
            .as_ref()
            .map(|j| Ok(serde_json::from_value(j.0.clone())?))
            .transpose()
    }

    pub fn is_held(&self) -> bool {
        assert_eq!(self.hold_time.is_some(), self.hold_reason.is_some());

        self.hold_time.is_some()
    }

    pub fn hold(&self) -> Option<Hold> {
        if let Some(IsoDate(time)) = self.hold_time {
            let reason = self.hold_reason.as_ref().unwrap().to_string();

            Some(Hold { time, reason })
        } else {
            assert!(self.hold_reason.is_none());

            None
        }
    }
}

#[derive(Debug)]
pub struct Hold {
    pub time: DateTime<Utc>,
    pub reason: String,
}
