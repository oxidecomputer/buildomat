/*
 * Copyright 2023 Oxide Computer Company
 */

use super::sublude::*;

#[derive(Debug, Clone)]
#[enum_def(prefix = "", suffix = "Def")]
pub struct JobStore {
    pub job: JobId,
    pub name: String,
    pub value: String,
    pub secret: bool,
    pub source: String,
    pub time_update: IsoDate,
}

impl FromRow for JobStore {
    fn columns() -> Vec<ColumnRef> {
        [
            JobStoreDef::Job,
            JobStoreDef::Name,
            JobStoreDef::Value,
            JobStoreDef::Secret,
            JobStoreDef::Source,
            JobStoreDef::TimeUpdate,
        ]
        .into_iter()
        .map(|col| {
            ColumnRef::TableColumn(
                SeaRc::new(JobStoreDef::Table),
                SeaRc::new(col),
            )
        })
        .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<JobStore> {
        Ok(JobStore {
            job: row.get(0)?,
            name: row.get(1)?,
            value: row.get(2)?,
            secret: row.get(3)?,
            source: row.get(4)?,
            time_update: row.get(5)?,
        })
    }
}

impl JobStore {
    pub fn find(job: JobId, name: &str) -> SelectStatement {
        Query::select()
            .from(JobStoreDef::Table)
            .columns(JobStore::columns())
            .and_where(Expr::col(JobStoreDef::Job).eq(job))
            .and_where(Expr::col(JobStoreDef::Name).eq(name))
            .to_owned()
    }

    pub fn insert(&self) -> InsertStatement {
        Query::insert()
            .into_table(JobStoreDef::Table)
            .columns(Self::bare_columns())
            .values_panic([
                self.job.into(),
                self.name.clone().into(),
                self.value.clone().into(),
                self.secret.into(),
                self.source.clone().into(),
                self.time_update.into(),
            ])
            .to_owned()
    }
}
