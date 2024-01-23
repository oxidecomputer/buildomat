/*
 * Copyright 2023 Oxide Computer Company
 */

use rusqlite::Row;
use sea_query::{ColumnRef, Cond, Expr, Query, SelectStatement};

use crate::db::types::*;
use buildomat_database::{rusqlite, FromRow};

mod sublude {
    pub use std::str::FromStr;
    pub use std::time::Duration;

    pub use crate::db::types::*;
    pub use crate::db::{CreateDepend, CreateOutputRule, CreateTask};
    pub use anyhow::Result;
    pub use buildomat_database::{rusqlite, FromRow};
    pub use buildomat_types::metadata;
    pub use chrono::prelude::*;
    pub use rusqlite::Row;
    pub use sea_query::{
        enum_def, ColumnRef, Expr, Iden, InsertStatement, OnConflict, Query,
        SeaRc, SelectStatement,
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

/**
 * This synthetic model object represents the left outer join of Inputs and
 * Files.  When an input is created, it may not yet have an associated File, at
 * which point "file" will be None.
 */
pub struct JobInputAndFile {
    pub input: JobInput,
    pub file: Option<JobFile>,
}

impl FromRow for JobInputAndFile {
    fn columns() -> Vec<ColumnRef> {
        JobInput::columns().into_iter().chain(JobFile::columns()).collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<Self> {
        let input = JobInput {
            job: row.get(0)?,
            name: row.get(1)?,
            id: row.get(2)?,
            other_job: row.get(3)?,
        };

        /*
         * The first column of job_file is the job ID, which will be NULL if
         * this record did not appear in the LEFT OUTER JOIN.
         */
        let file = if let Some(job) = row.get::<_, Option<JobId>>(4)? {
            Some(JobFile {
                job,
                id: row.get(5)?,
                size: row.get(6)?,
                time_archived: row.get(7)?,
            })
        } else {
            None
        };

        Ok(JobInputAndFile { input, file })
    }
}

impl JobInputAndFile {
    pub fn base_query() -> SelectStatement {
        Query::select()
            .columns(JobInputAndFile::columns())
            .from(JobInputDef::Table)
            .left_join(
                JobFileDef::Table,
                Cond::all()
                    /*
                     * The file ID column must always match:
                     */
                    .add(
                        Expr::col((JobFileDef::Table, JobFileDef::Id)).eq(
                            Expr::col((JobInputDef::Table, JobInputDef::Id)),
                        ),
                    )
                    .add(
                        Cond::any()
                            /*
                             * Either the other_job field is null, and the input
                             * job ID matches the file job ID directly...
                             */
                            .add(
                                Expr::col((
                                    JobInputDef::Table,
                                    JobInputDef::OtherJob,
                                ))
                                .is_null()
                                .and(
                                    Expr::col((
                                        JobFileDef::Table,
                                        JobFileDef::Job,
                                    ))
                                    .eq(
                                        Expr::col((
                                            JobInputDef::Table,
                                            JobInputDef::Job,
                                        )),
                                    ),
                                ),
                            )
                            /*
                             * ... or the other_job field is populated and it
                             * matches the file job ID instead:
                             */
                            .add(
                                Expr::col((
                                    JobInputDef::Table,
                                    JobInputDef::OtherJob,
                                ))
                                .is_not_null()
                                .and(
                                    Expr::col((
                                        JobFileDef::Table,
                                        JobFileDef::Job,
                                    ))
                                    .eq(
                                        Expr::col((
                                            JobInputDef::Table,
                                            JobInputDef::OtherJob,
                                        )),
                                    ),
                                ),
                            ),
                    ),
            )
            .to_owned()
    }
}

/**
 * This synthetic model object represents the inner join of Outputs and Files.
 * Outputs and their underlying File are created in one transaction, so they
 * always exist together.
 */
pub struct JobOutputAndFile {
    pub output: JobOutput,
    pub file: JobFile,
}

impl FromRow for JobOutputAndFile {
    fn columns() -> Vec<ColumnRef> {
        JobOutput::columns().into_iter().chain(JobFile::columns()).collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<Self> {
        Ok(JobOutputAndFile {
            output: JobOutput {
                job: row.get(0)?,
                path: row.get(1)?,
                id: row.get(2)?,
            },
            file: JobFile {
                job: row.get(3)?,
                id: row.get(4)?,
                size: row.get(5)?,
                time_archived: row.get(6)?,
            },
        })
    }
}

impl JobOutputAndFile {
    pub fn base_query() -> SelectStatement {
        Query::select()
            .columns(JobOutputAndFile::columns())
            .from(JobOutputDef::Table)
            .inner_join(
                JobFileDef::Table,
                /*
                 * The job in both tables must match:
                 */
                Expr::col((JobFileDef::Table, JobFileDef::Job))
                    .eq(Expr::col((JobOutputDef::Table, JobOutputDef::Job)))
                    /*
                     * The file ID must also match:
                     */
                    .and(Expr::col((JobFileDef::Table, JobFileDef::Id)).eq(
                        Expr::col((JobOutputDef::Table, JobOutputDef::Id)),
                    )),
            )
            .to_owned()
    }
}
