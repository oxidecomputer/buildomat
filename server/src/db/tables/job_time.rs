/*
 * Copyright 2023 Oxide Computer Company
 */

use super::sublude::*;

#[derive(Debug, Clone)]
#[enum_def(prefix = "", suffix = "Def")]
pub struct JobTime {
    pub job: JobId,
    pub name: String,
    pub time: IsoDate,
}

impl FromRow for JobTime {
    fn columns() -> Vec<ColumnRef> {
        [JobTimeDef::Job, JobTimeDef::Name, JobTimeDef::Time]
            .into_iter()
            .map(|col| {
                ColumnRef::TableColumn(
                    SeaRc::new(JobTimeDef::Table),
                    SeaRc::new(col),
                )
            })
            .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<JobTime> {
        Ok(JobTime { job: row.get(0)?, name: row.get(1)?, time: row.get(2)? })
    }
}

impl JobTime {
    pub fn upsert(&self) -> InsertStatement {
        Query::insert()
            .into_table(JobTimeDef::Table)
            .columns(Self::bare_columns())
            .values_panic([
                self.job.into(),
                self.name.clone().into(),
                self.time.into(),
            ])
            .on_conflict(OnConflict::new().do_nothing().to_owned())
            .to_owned()
    }

    pub fn find(job: JobId, name: &str) -> SelectStatement {
        Query::select()
            .from(JobTimeDef::Table)
            .columns(JobTime::columns())
            .and_where(Expr::col(JobTimeDef::Job).eq(job))
            .and_where(Expr::col(JobTimeDef::Name).eq(name))
            .to_owned()
    }
}
