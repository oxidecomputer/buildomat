/*
 * Copyright 2023 Oxide Computer Company
 */

use super::sublude::*;

#[derive(Debug, Clone)]
#[enum_def(prefix = "", suffix = "Def")]
#[allow(unused)]
pub struct JobTag {
    pub job: JobId,
    pub name: String,
    pub value: String,
}

impl FromRow for JobTag {
    fn columns() -> Vec<ColumnRef> {
        [JobTagDef::Job, JobTagDef::Name, JobTagDef::Value]
            .into_iter()
            .map(|col| {
                ColumnRef::TableColumn(
                    SeaRc::new(JobTagDef::Table),
                    SeaRc::new(col),
                )
            })
            .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<JobTag> {
        Ok(JobTag { job: row.get(0)?, name: row.get(1)?, value: row.get(2)? })
    }
}
