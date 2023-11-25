/*
 * Copyright 2023 Oxide Computer Company
 */

use super::sublude::*;

#[derive(Debug)]
#[enum_def(prefix = "", suffix = "Def")]
pub struct User {
    pub id: UserId,
    pub name: String,
    pub token: String,
    pub time_create: IsoDate,
}

impl FromRow for User {
    fn columns() -> Vec<ColumnRef> {
        [UserDef::Id, UserDef::Name, UserDef::Token, UserDef::TimeCreate]
            .into_iter()
            .map(|col| {
                ColumnRef::TableColumn(
                    SeaRc::new(UserDef::Table),
                    SeaRc::new(col),
                )
            })
            .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<User> {
        Ok(User {
            id: row.get(0)?,
            name: row.get(1)?,
            token: row.get(2)?,
            time_create: row.get(3)?,
        })
    }
}

impl User {
    pub fn find(id: UserId) -> SelectStatement {
        Query::select()
            .from(UserDef::Table)
            .columns(User::columns())
            .and_where(Expr::col(UserDef::Id).eq(id))
            .to_owned()
    }

    pub fn insert(&self) -> InsertStatement {
        Query::insert()
            .into_table(UserDef::Table)
            .columns(Self::bare_columns())
            .values_panic([
                self.id.into(),
                self.name.clone().into(),
                self.token.clone().into(),
                self.time_create.into(),
            ])
            .to_owned()
    }
}
