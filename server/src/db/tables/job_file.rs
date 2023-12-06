/*
 * Copyright 2023 Oxide Computer Company
 */

use super::sublude::*;

#[derive(Debug)]
#[enum_def(prefix = "", suffix = "Def")]
pub struct JobFile {
    pub job: JobId,
    pub id: JobFileId,
    pub size: DataSize,
    /**
     * When was this file successfully uploaded to the object store?
     */
    pub time_archived: Option<IsoDate>,
}

impl FromRow for JobFile {
    fn columns() -> Vec<ColumnRef> {
        [
            JobFileDef::Job,
            JobFileDef::Id,
            JobFileDef::Size,
            JobFileDef::TimeArchived,
        ]
        .into_iter()
        .map(|col| {
            ColumnRef::TableColumn(
                SeaRc::new(JobFileDef::Table),
                SeaRc::new(col),
            )
        })
        .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<JobFile> {
        Ok(JobFile {
            job: row.get(0)?,
            id: row.get(1)?,
            size: row.get(2)?,
            time_archived: row.get(3)?,
        })
    }
}

impl JobFile {
    pub fn find(job: JobId, file: JobFileId) -> SelectStatement {
        Query::select()
            .from(JobFileDef::Table)
            .columns(JobFile::columns())
            .and_where(Expr::col(JobFileDef::Job).eq(job))
            .and_where(Expr::col(JobFileDef::Id).eq(file))
            .to_owned()
    }

    pub fn insert(&self) -> InsertStatement {
        Query::insert()
            .into_table(JobFileDef::Table)
            .columns(Self::bare_columns())
            .values_panic([
                self.job.into(),
                self.id.into(),
                self.size.into(),
                self.time_archived.into(),
            ])
            .to_owned()
    }
}
