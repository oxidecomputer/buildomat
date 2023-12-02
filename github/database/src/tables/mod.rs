/*
 * Copyright 2023 Oxide Computer Company
 */

use rusqlite::Row;

use crate::itypes::*;
use buildomat_database::sqlite::rusqlite;
use sea_query::{ColumnRef, Iden, SeaRc};

pub trait FromRow: Sized {
    fn columns() -> Vec<ColumnRef>;
    fn from_row(row: &Row) -> rusqlite::Result<Self>;

    fn bare_columns() -> Vec<SeaRc<dyn Iden>> {
        Self::columns()
            .into_iter()
            .map(|v| match v {
                ColumnRef::TableColumn(_, c) => c,
                _ => unreachable!(),
            })
            .collect()
    }
}

mod sublude {
    pub use std::collections::HashMap;
    pub use std::str::FromStr;
    pub use std::time::Duration;

    pub use super::FromRow;
    pub use crate::itypes::*;
    pub use anyhow::{bail, Result};
    pub use buildomat_database::sqlite::rusqlite;
    pub use buildomat_database::{sqlite_json_new_type, sqlite_sql_enum};
    pub use buildomat_github_common::hooktypes;
    pub use chrono::prelude::*;
    pub use rusqlite::Row;
    pub use sea_query::{
        enum_def, ColumnRef, Expr, Iden, IdenStatic, InsertStatement,
        OnConflict, Order, Query, SeaRc, SelectStatement, SimpleExpr, Value,
    };
    pub use serde::{Deserialize, Serialize};
}

mod check_run;
mod check_suite;
mod delivery;
mod install;
mod repository;
mod user;

pub use check_run::*;
pub use check_suite::*;
pub use delivery::*;
pub use install::*;
pub use repository::*;
pub use user::*;

impl<T: FromRow + rusqlite::types::FromSql> FromRow for Option<T> {
    fn columns() -> Vec<ColumnRef> {
        unimplemented!()
    }

    fn from_row(row: &Row) -> rusqlite::Result<Option<T>> {
        Ok(row.get(0)?)
    }
}

/*
 * This implementation allows us to use the existing tx_get_rows() routine to
 * fish out a list of delivery "seq" values.
 */
impl FromRow for DeliverySeq {
    fn columns() -> Vec<ColumnRef> {
        unimplemented!()
    }

    fn from_row(row: &Row) -> rusqlite::Result<DeliverySeq> {
        Ok(row.get(0)?)
    }
}

/*
 * This implementation allows us to use the existing tx_get_rows() routine to
 * fish out a list of check suite ID values.
 */
impl FromRow for CheckSuiteId {
    fn columns() -> Vec<ColumnRef> {
        unimplemented!()
    }

    fn from_row(row: &Row) -> rusqlite::Result<CheckSuiteId> {
        Ok(row.get(0)?)
    }
}
