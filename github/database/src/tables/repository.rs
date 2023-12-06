/*
 * Copyright 2023 Oxide Computer Company
 */

use super::sublude::*;

#[derive(Debug, Clone)]
#[enum_def(prefix = "", suffix = "Def")]
pub struct Repository {
    pub id: i64,
    pub owner: String,
    pub name: String,
}

impl FromRow for Repository {
    fn columns() -> Vec<ColumnRef> {
        [RepositoryDef::Id, RepositoryDef::Owner, RepositoryDef::Name]
            .into_iter()
            .map(|col| {
                ColumnRef::TableColumn(
                    SeaRc::new(RepositoryDef::Table),
                    SeaRc::new(col),
                )
            })
            .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<Self> {
        Ok(Repository {
            id: row.get(0)?,
            owner: row.get(1)?,
            name: row.get(2)?,
        })
    }
}

impl Repository {
    pub fn find(id: i64) -> SelectStatement {
        Query::select()
            .from(RepositoryDef::Table)
            .columns(Repository::columns())
            .and_where(Expr::col(RepositoryDef::Id).eq(id))
            .to_owned()
    }

    pub fn insert(&self) -> InsertStatement {
        Query::insert()
            .into_table(RepositoryDef::Table)
            .columns(Self::bare_columns())
            .values_panic([
                self.id.into(),
                self.owner.clone().into(),
                self.name.clone().into(),
            ])
            .to_owned()
    }
}
