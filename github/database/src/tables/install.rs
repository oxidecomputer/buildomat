/*
 * Copyright 2023 Oxide Computer Company
 */

use super::sublude::*;

#[derive(Debug, Clone)]
#[enum_def(prefix = "", suffix = "Def")]
pub struct Install {
    pub id: i64,
    pub owner: i64,
}

impl FromRow for Install {
    fn columns() -> Vec<ColumnRef> {
        [
            InstallDef::Id,
            InstallDef::Owner,
        ]
        .into_iter()
        .map(|col| {
            ColumnRef::TableColumn(
                SeaRc::new(InstallDef::Table),
                SeaRc::new(col),
            )
        })
        .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<Self> {
        Ok(Install {
            id: row.get(0)?,
            owner: row.get(1)?,
        })
    }
}
