/*
 * Copyright 2024 Oxide Computer Company
 */

use super::sublude::*;

#[derive(Debug, Clone)]
#[enum_def(prefix = "", suffix = "Def")]
pub struct WorkerEvent {
    pub worker: WorkerId,
    pub seq: u32,
    pub stream: String,
    /**
     * The time at which the core API server received or generated this event.
     */
    pub time: IsoDate,
    /**
     * This field reflects the time on the remote system when the event was
     * generated.  Due to issues with NTP, etc, it might not align exactly with
     * the time field.
     */
    pub time_remote: Option<IsoDate>,
    pub payload: String,
}

impl FromRow for WorkerEvent {
    fn columns() -> Vec<ColumnRef> {
        [
            WorkerEventDef::Worker,
            WorkerEventDef::Seq,
            WorkerEventDef::Stream,
            WorkerEventDef::Time,
            WorkerEventDef::TimeRemote,
            WorkerEventDef::Payload,
        ]
        .into_iter()
        .map(|col| {
            ColumnRef::TableColumn(
                SeaRc::new(WorkerEventDef::Table),
                SeaRc::new(col),
            )
        })
        .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<WorkerEvent> {
        Ok(WorkerEvent {
            worker: row.get(0)?,
            seq: row.get(1)?,
            stream: row.get(2)?,
            time: row.get(3)?,
            time_remote: row.get(4)?,
            payload: row.get(5)?,
        })
    }
}

impl WorkerEvent {
    pub fn insert(&self) -> InsertStatement {
        Query::insert()
            .into_table(WorkerEventDef::Table)
            .columns(Self::bare_columns())
            .values_panic([
                self.worker.into(),
                self.seq.into(),
                self.stream.clone().into(),
                self.time.into(),
                self.time_remote.into(),
                self.payload.clone().into(),
            ])
            .to_owned()
    }

    #[allow(unused)]
    pub fn age(&self) -> Duration {
        self.time.age()
    }
}
