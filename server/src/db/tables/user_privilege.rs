/*
 * Copyright 2023 Oxide Computer Company
 */

use super::sublude::*;

#[derive(Debug)]
#[enum_def(prefix = "", suffix = "Def")]
pub struct UserPrivilege {
    pub user: UserId,
    pub privilege: String,
}

impl UserPrivilege {
    pub fn upsert(&self) -> InsertStatement {
        Query::insert()
            .into_table(UserPrivilegeDef::Table)
            .columns([UserPrivilegeDef::User, UserPrivilegeDef::Privilege])
            .values_panic([self.user.into(), self.privilege.clone().into()])
            .on_conflict(OnConflict::new().do_nothing().to_owned())
            .to_owned()
    }
}
