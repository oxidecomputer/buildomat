/*
 * Copyright 2023 Oxide Computer Company
 */

use super::sublude::*;

sqlite_sql_enum!(UserType => {
    User,
    Bot,
    #[strum(serialize = "org")]
    Organisation,
});

impl UserType {
    pub fn is_org(&self) -> bool {
        matches!(self, UserType::Organisation)
    }

    pub fn from_github_str(ut: &str) -> Result<Self> {
        Ok(match ut {
            "User" => UserType::User,
            "Bot" => UserType::Bot,
            "Organization" => UserType::Organisation,
            x => bail!("invalid user type from GitHub: {:?}", x),
        })
    }

    pub fn from_github(ut: hooktypes::UserType) -> Self {
        match ut {
            hooktypes::UserType::User => UserType::User,
            hooktypes::UserType::Bot => UserType::Bot,
            hooktypes::UserType::Organization => UserType::Organisation,
        }
    }
}

#[derive(Debug, Clone)]
#[enum_def(prefix = "", suffix = "Def")]
pub struct User {
    pub id: i64,
    pub login: String,
    pub usertype: UserType,
    pub name: Option<String>,
    pub email: Option<String>,
}

impl FromRow for User {
    fn columns() -> Vec<ColumnRef> {
        [
            UserDef::Id,
            UserDef::Login,
            UserDef::Usertype,
            UserDef::Name,
            UserDef::Email,
        ]
        .into_iter()
        .map(|col| {
            ColumnRef::TableColumn(SeaRc::new(UserDef::Table), SeaRc::new(col))
        })
        .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<Self> {
        Ok(User {
            id: row.get(0)?,
            login: row.get(1)?,
            usertype: row.get(2)?,
            name: row.get(3)?,
            email: row.get(4)?,
        })
    }
}

impl User {
    pub fn find(id: i64) -> SelectStatement {
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
                self.login.clone().into(),
                self.usertype.into(),
                self.name.clone().into(),
                self.email.clone().into(),
            ])
            .to_owned()
    }
}

/*
 * These tests attempt to ensure that the concrete representation of the enum
 * does not change, as that would make the database unuseable.
 */
#[cfg(test)]
mod test {
    use super::UserType;
    use std::str::FromStr;

    const USER_TYPES: &'static [(&'static str, UserType)] = &[
        ("user", UserType::User),
        ("bot", UserType::Bot),
        ("org", UserType::Organisation),
    ];

    #[test]
    fn user_type_forward() {
        for (s, e) in USER_TYPES {
            assert_eq!(*s, e.to_string());
        }
    }

    #[test]
    fn user_type_backward() {
        for (s, e) in USER_TYPES {
            assert_eq!(UserType::from_str(s).unwrap(), *e);
        }
    }
}
