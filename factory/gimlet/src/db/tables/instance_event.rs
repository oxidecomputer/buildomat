/*
 * Copyright 2026 Oxide Computer Company
 */

use super::sublude::*;
use anyhow::{anyhow, bail, Result};

#[derive(Clone, Debug)]
#[enum_def(prefix = "", suffix = "Def")]
pub struct InstanceEvent {
    pub model: String,
    pub serial: String,
    pub instance: InstanceSeq,

    pub seq: EventSeq,
    pub stream: String,
    pub payload: String,
    pub uploaded: bool,
    pub time: IsoDate,
}

impl FromRow for InstanceEvent {
    fn columns() -> Vec<ColumnRef> {
        [
            InstanceEventDef::Model,
            InstanceEventDef::Serial,
            InstanceEventDef::Instance,
            InstanceEventDef::Seq,
            InstanceEventDef::Stream,
            InstanceEventDef::Payload,
            InstanceEventDef::Uploaded,
            InstanceEventDef::Time,
        ]
        .into_iter()
        .map(|col| {
            ColumnRef::TableColumn(
                SeaRc::new(InstanceEventDef::Table),
                SeaRc::new(col),
            )
        })
        .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<Self> {
        Ok(InstanceEvent {
            model: row.get(0)?,
            serial: row.get(1)?,
            instance: row.get(2)?,
            seq: row.get(3)?,
            stream: row.get(4)?,
            payload: row.get(5)?,
            uploaded: row.get(6)?,
            time: row.get(7)?,
        })
    }
}

impl InstanceEvent {
    pub fn insert(&self) -> InsertStatement {
        Query::insert()
            .into_table(InstanceEventDef::Table)
            .columns(Self::bare_columns())
            .values_panic([
                self.model.clone().into(),
                self.serial.clone().into(),
                self.instance.into(),
                self.seq.into(),
                self.stream.clone().into(),
                self.payload.clone().into(),
                self.uploaded.into(),
                self.time.into(),
            ])
            .to_owned()
    }
}
