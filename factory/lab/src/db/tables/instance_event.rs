/*
 * Copyright 2023 Oxide Computer Company
 */

use super::sublude::*;

#[derive(Debug)]
#[enum_def(prefix = "", suffix = "Def")]
pub struct InstanceEvent {
    pub nodename: String,
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
            InstanceEventDef::Nodename,
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
            nodename: row.get(0)?,
            instance: row.get(1)?,
            seq: row.get(2)?,
            stream: row.get(3)?,
            payload: row.get(4)?,
            uploaded: row.get(5)?,
            time: row.get(6)?,
        })
    }
}

impl InstanceEvent {
    pub fn insert(&self) -> InsertStatement {
        Query::insert()
            .into_table(InstanceEventDef::Table)
            .columns(Self::bare_columns())
            .values_panic([
                self.nodename.clone().into(),
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
