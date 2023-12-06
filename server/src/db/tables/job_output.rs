/*
 * Copyright 2023 Oxide Computer Company
 */

use super::sublude::*;

#[derive(Debug)]
#[enum_def(prefix = "", suffix = "Def")]
pub struct JobOutput {
    pub job: JobId,
    pub path: String,
    pub id: JobFileId,
}

impl FromRow for JobOutput {
    fn columns() -> Vec<ColumnRef> {
        [JobOutputDef::Job, JobOutputDef::Path, JobOutputDef::Id]
            .into_iter()
            .map(|col| {
                ColumnRef::TableColumn(
                    SeaRc::new(JobOutputDef::Table),
                    SeaRc::new(col),
                )
            })
            .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<JobOutput> {
        Ok(JobOutput { job: row.get(0)?, path: row.get(1)?, id: row.get(2)? })
    }
}

impl JobOutput {
    pub fn find(job: JobId, file: JobFileId) -> SelectStatement {
        Query::select()
            .from(JobOutputDef::Table)
            .columns(JobOutput::columns())
            .and_where(Expr::col(JobOutputDef::Job).eq(job))
            .and_where(Expr::col(JobOutputDef::Id).eq(file))
            .to_owned()
    }

    pub fn insert(&self) -> InsertStatement {
        Query::insert()
            .into_table(JobOutputDef::Table)
            .columns(Self::bare_columns())
            .values_panic([
                self.job.into(),
                self.path.clone().into(),
                self.id.into(),
            ])
            .to_owned()
    }
}
