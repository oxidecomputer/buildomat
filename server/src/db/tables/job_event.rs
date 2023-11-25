/*
 * Copyright 2023 Oxide Computer Company
 */

use super::sublude::*;

#[derive(Debug, Clone)]
#[enum_def(prefix = "", suffix = "Def")]
pub struct JobEvent {
    pub job: JobId,
    pub task: Option<u32>,
    pub seq: u32,
    pub stream: String,
    /**
     * The time at which the core API server received or generated this event.
     */
    pub time: IsoDate,
    pub payload: String,
    /**
     * If this event was received from a remote system, such as a worker, this
     * time reflects the time on that remote system when the event was
     * generated.  Due to issues with NTP, etc, it might not align exactly with
     * the time field.
     */
    pub time_remote: Option<IsoDate>,
}

impl FromRow for JobEvent {
    fn columns() -> Vec<ColumnRef> {
        [
            JobEventDef::Job,
            JobEventDef::Task,
            JobEventDef::Seq,
            JobEventDef::Stream,
            JobEventDef::Time,
            JobEventDef::Payload,
            JobEventDef::TimeRemote,
        ]
        .into_iter()
        .map(|col| {
            ColumnRef::TableColumn(
                SeaRc::new(JobEventDef::Table),
                SeaRc::new(col),
            )
        })
        .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<JobEvent> {
        Ok(JobEvent {
            job: row.get(0)?,
            task: row.get(1)?,
            seq: row.get(2)?,
            stream: row.get(3)?,
            time: row.get(4)?,
            payload: row.get(5)?,
            time_remote: row.get(6)?,
        })
    }
}

impl JobEvent {
    pub fn insert(&self) -> InsertStatement {
        Query::insert()
            .into_table(JobEventDef::Table)
            .columns(Self::bare_columns())
            .values_panic([
                self.job.into(),
                self.task.into(),
                self.seq.into(),
                self.stream.clone().into(),
                self.time.into(),
                self.payload.clone().into(),
                self.time_remote.into(),
            ])
            .to_owned()
    }

    pub fn age(&self) -> Duration {
        self.time.age()
    }
}
