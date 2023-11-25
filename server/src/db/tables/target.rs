/*
 * Copyright 2023 Oxide Computer Company
 */

use super::sublude::*;

#[derive(Debug, Clone)]
#[enum_def(prefix = "", suffix = "Def")]
pub struct Target {
    pub id: TargetId,
    pub name: String,
    pub desc: String,
    pub redirect: Option<TargetId>,
    pub privilege: Option<String>,
}

impl FromRow for Target {
    fn columns() -> Vec<ColumnRef> {
        [
            TargetDef::Id,
            TargetDef::Name,
            TargetDef::Desc,
            TargetDef::Redirect,
            TargetDef::Privilege,
        ]
        .into_iter()
        .map(|col| {
            ColumnRef::TableColumn(
                SeaRc::new(TargetDef::Table),
                SeaRc::new(col),
            )
        })
        .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<Target> {
        Ok(Target {
            id: row.get(0)?,
            name: row.get(1)?,
            desc: row.get(2)?,
            redirect: row.get(3)?,
            privilege: row.get(4)?,
        })
    }
}

impl Target {
    pub fn find(id: TargetId) -> SelectStatement {
        Query::select()
            .from(TargetDef::Table)
            .columns(Target::columns())
            .and_where(Expr::col(TargetDef::Id).eq(id))
            .to_owned()
    }

    pub fn find_by_name(name: &str) -> SelectStatement {
        Query::select()
            .from(TargetDef::Table)
            .columns(Target::columns())
            .and_where(Expr::col(TargetDef::Name).eq(name))
            .to_owned()
    }

    pub fn insert(&self) -> InsertStatement {
        Query::insert()
            .into_table(TargetDef::Table)
            .columns(Self::bare_columns())
            .values_panic([
                self.id.into(),
                self.name.clone().into(),
                self.desc.clone().into(),
                self.redirect.into(),
                self.privilege.clone().into(),
            ])
            .to_owned()
    }
}
