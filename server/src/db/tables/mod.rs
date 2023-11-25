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
    pub use crate::db::{CreateDepend, CreateOutputRule, CreateTask};
    pub use anyhow::Result;
    pub use buildomat_database::sqlite::rusqlite;
    pub use buildomat_types::metadata;
    pub use chrono::prelude::*;
    pub use rusqlite::Row;
    pub use sea_query::{
        enum_def, ColumnRef, Expr, Iden, IdenStatic, InsertStatement,
        OnConflict, Query, SeaRc, SelectStatement, SimpleExpr, Value,
    };
}

mod factory;
mod job;
mod job_depend;
mod job_event;
mod job_file;
mod job_input;
mod job_output;
mod job_output_rule;
mod job_store;
mod job_tag;
mod job_time;
mod published_file;
mod target;
mod task;
mod user;
mod user_privilege;
mod worker;

pub use factory::*;
pub use job::*;
pub use job_depend::*;
pub use job_event::*;
pub use job_file::*;
pub use job_input::*;
pub use job_output::*;
pub use job_output_rule::*;
pub use job_store::*;
pub use job_tag::*;
pub use job_time::*;
pub use published_file::*;
pub use target::*;
pub use task::*;
pub use user::*;
pub use user_privilege::*;
pub use worker::*;

#[derive(Debug)]
pub struct AuthUser {
    pub user: User,
    pub privileges: Vec<String>,
}

impl AuthUser {
    pub fn has_privilege(&self, privilege: &str) -> bool {
        self.privileges.iter().any(|s| privilege == s)
    }
}

impl std::ops::Deref for AuthUser {
    type Target = User;

    fn deref(&self) -> &Self::Target {
        &self.user
    }
}

/*
 * This implementation allows us to use the existing tx_get_row() routine to
 * fish out a MAX() value for the task "seq" column.
 */
impl FromRow for Option<u32> {
    fn columns() -> Vec<ColumnRef> {
        unimplemented!()
    }

    fn from_row(row: &Row) -> rusqlite::Result<Option<u32>> {
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

/*
 * Joins are a bit of a mess, so produce some helper implementations for pairs
 * of objects we need to return:
 */
impl FromRow for (JobInput, Option<JobFile>) {
    fn columns() -> Vec<ColumnRef> {
        JobInput::columns()
            .into_iter()
            .chain(JobFile::columns().into_iter())
            .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<Self> {
        let ji = JobInput {
            job: row.get(0)?,
            name: row.get(1)?,
            id: row.get(2)?,
            other_job: row.get(3)?,
        };

        /*
         * The first column of job_file is the job ID, which will be NULL if
         * this record did not appear in the LEFT OUTER JOIN.
         */
        let jf = if let Some(job) = row.get::<_, Option<JobId>>(4)? {
            Some(JobFile {
                job,
                id: row.get(5)?,
                size: row.get(6)?,
                time_archived: row.get(7)?,
            })
        } else {
            None
        };

        Ok((ji, jf))
    }
}

impl FromRow for (JobOutput, JobFile) {
    fn columns() -> Vec<ColumnRef> {
        JobOutput::columns()
            .into_iter()
            .chain(JobFile::columns().into_iter())
            .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<Self> {
        Ok((
            JobOutput { job: row.get(0)?, path: row.get(1)?, id: row.get(2)? },
            JobFile {
                job: row.get(3)?,
                id: row.get(4)?,
                size: row.get(5)?,
                time_archived: row.get(6)?,
            },
        ))
    }
}
