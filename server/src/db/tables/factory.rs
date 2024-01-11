/*
 * Copyright 2023 Oxide Computer Company
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
}

impl FromRow for Factory {
    fn columns() -> Vec<ColumnRef> {
        [
            FactoryDef::Id,
            FactoryDef::Name,
            FactoryDef::Token,
            FactoryDef::Lastping,
            FactoryDef::Enable,
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
            ])
            .to_owned()
    }
}
