/*
 * Copyright 2026 Oxide Computer Company
 */

use super::sublude::*;

#[derive(Debug, Clone)]
#[enum_def(prefix = "", suffix = "Def")]
pub struct CacheFile {
    pub id: CacheFileId,
    pub owner: UserId,
    pub name: String,
    pub size_bytes: DataSize,
    pub time_upload: IsoDate,
    pub time_last_use: Option<IsoDate>,
}

impl FromRow for CacheFile {
    fn columns() -> Vec<ColumnRef> {
        [
            CacheFileDef::Id,
            CacheFileDef::Owner,
            CacheFileDef::Name,
            CacheFileDef::SizeBytes,
            CacheFileDef::TimeUpload,
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
            id: row.get(0)?,
            owner: row.get(1)?,
            name: row.get(2)?,
            size_bytes: row.get(3)?,
            time_upload: row.get(4)?,
            time_last_use: row.get(5)?,
        })
    }
}

impl CacheFile {
    pub fn insert(&self) -> InsertStatement {
        Query::insert()
            .into_table(CacheFileDef::Table)
            .columns(Self::bare_columns())
            .values_panic([
                self.id.into(),
                self.owner.into(),
                self.name.clone().into(),
                self.size_bytes.into(),
                self.time_upload.into(),
                self.time_last_use.into(),
            ])
            .to_owned()
    }
}
