/*
 * Copyright 2023 Oxide Computer Company
 */

use rusqlite::Row;
use sea_query::{ColumnRef, Iden, SeaRc};

use crate::db::types::*;
use buildomat_database::sqlite::rusqlite;

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
    pub use std::str::FromStr;
    pub use std::time::Duration;

    pub use super::FromRow;
    pub use crate::db::types::*;
    pub use anyhow::Result;
    pub use buildomat_database::sqlite::rusqlite;
    pub use buildomat_database::sqlite_sql_enum;
    pub use buildomat_types::metadata;
    pub use chrono::prelude::*;
    pub use rusqlite::Row;
    pub use sea_query::{
        enum_def, ColumnRef, Expr, Iden, IdenStatic, InsertStatement,
        OnConflict, Order, Query, SeaRc, SelectStatement, SimpleExpr, Value,
    };
}

mod instance;
mod instance_event;

pub use instance::*;
pub use instance_event::*;

/*
 * This implementation allows us to use the existing tx_get_row() routine to
 * fish out a MAX() value for the instance "seq" column.
 */
impl FromRow for Option<InstanceSeq> {
    fn columns() -> Vec<ColumnRef> {
        unimplemented!()
    }

    fn from_row(row: &Row) -> rusqlite::Result<Option<InstanceSeq>> {
        Ok(row.get(0)?)
    }
}

/*
 * This implementation allows us to use the existing tx_get_row() routine to
 * fish out a MAX() value for the instance event "seq" column.
 */
impl FromRow for Option<EventSeq> {
    fn columns() -> Vec<ColumnRef> {
        unimplemented!()
    }

    fn from_row(row: &Row) -> rusqlite::Result<Option<EventSeq>> {
        Ok(row.get(0)?)
    }
}

/*
 * This implementation allows us to use the existing tx_get_row() routine to
 * fish out a COUNT() value.
 */
impl FromRow for usize {
    fn columns() -> Vec<ColumnRef> {
        unimplemented!()
    }

    fn from_row(row: &Row) -> rusqlite::Result<usize> {
        Ok(row.get::<_, i64>(0)?.try_into().unwrap())
    }
}
