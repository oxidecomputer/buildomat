/*
 * Copyright 2026 Oxide Computer Company
 */

use super::sublude::*;

#[derive(Debug, Clone)]
#[enum_def(prefix = "", suffix = "Def")]
pub struct CacheFile {
    #[expect(unused)]
    pub owner: UserId,
    #[expect(unused)]
    pub name: String,
    #[expect(unused)]
    pub size_bytes: DataSize,
    #[expect(unused)]
    pub time_last_use: IsoDate,
}

impl FromRow for CacheFile {
    fn columns() -> Vec<ColumnRef> {
        [
            CacheFileDef::Owner,
            CacheFileDef::Name,
            CacheFileDef::SizeBytes,
            CacheFileDef::TimeLastUse,
        ]
        .into_iter()
        .map(|col| {
            ColumnRef::TableColumn(
                SeaRc::new(CacheFileDef::Table),
                SeaRc::new(col),
            )
        })
        .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<Self> {
        Ok(CacheFile {
            owner: row.get(0)?,
            name: row.get(1)?,
            size_bytes: row.get(2)?,
            time_last_use: row.get(3)?,
        })
    }
}
