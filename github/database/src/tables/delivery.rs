/*
 * Copyright 2023 Oxide Computer Company
 */

use super::sublude::*;

#[derive(Debug, Clone)]
#[enum_def(prefix = "", suffix = "Def")]
pub struct Delivery {
    pub seq: DeliverySeq,
    pub uuid: String,
    pub event: String,
    pub headers: Dictionary,
    pub payload: JsonValue,
    pub recvtime: IsoDate,
    pub ack: Option<i64>,
}

impl FromRow for Delivery {
    fn columns() -> Vec<ColumnRef> {
        [
            DeliveryDef::Seq,
            DeliveryDef::Uuid,
            DeliveryDef::Event,
            DeliveryDef::Headers,
            DeliveryDef::Payload,
            DeliveryDef::Recvtime,
            DeliveryDef::Ack,
        ]
        .into_iter()
        .map(|col| {
            ColumnRef::TableColumn(
                SeaRc::new(DeliveryDef::Table),
                SeaRc::new(col),
            )
        })
        .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<Self> {
        Ok(Delivery {
            seq: row.get(0)?,
            uuid: row.get(1)?,
            event: row.get(2)?,
            headers: row.get(3)?,
            payload: row.get(4)?,
            recvtime: row.get(5)?,
            ack: row.get(6)?,
        })
    }
}

impl Delivery {
    pub fn recvtime_day_prefix(&self) -> String {
        self.recvtime.0.format("%Y-%m-%d").to_string()
    }
}
