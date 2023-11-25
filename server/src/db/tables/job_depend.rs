/*
 * Copyright 2023 Oxide Computer Company
 */

use super::sublude::*;

#[derive(Debug, Clone)]
#[enum_def(prefix = "", suffix = "Def")]
pub struct JobDepend {
    pub job: JobId,
    pub name: String,
    pub prior_job: JobId,
    pub copy_outputs: bool,
    pub on_failed: bool,
    pub on_completed: bool,
    pub satisfied: bool,
}

impl FromRow for JobDepend {
    fn columns() -> Vec<ColumnRef> {
        [
            JobDependDef::Job,
            JobDependDef::Name,
            JobDependDef::PriorJob,
            JobDependDef::CopyOutputs,
            JobDependDef::OnFailed,
            JobDependDef::OnCompleted,
            JobDependDef::Satisfied,
        ]
        .into_iter()
        .map(|col| {
            ColumnRef::TableColumn(
                SeaRc::new(JobDependDef::Table),
                SeaRc::new(col),
            )
        })
        .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<JobDepend> {
        Ok(JobDepend {
            job: row.get(0)?,
            name: row.get(1)?,
            prior_job: row.get(2)?,
            copy_outputs: row.get(3)?,
            on_failed: row.get(4)?,
            on_completed: row.get(5)?,
            satisfied: row.get(6)?,
        })
    }
}

impl JobDepend {
    pub fn find(job: JobId, name: &str) -> SelectStatement {
        Query::select()
            .from(JobDependDef::Table)
            .columns(JobDepend::columns())
            .and_where(Expr::col(JobDependDef::Job).eq(job))
            .and_where(Expr::col(JobDependDef::Name).eq(name))
            .to_owned()
    }

    pub fn insert(&self) -> InsertStatement {
        Query::insert()
            .into_table(JobDependDef::Table)
            .columns(Self::bare_columns())
            .values_panic([
                self.job.into(),
                self.name.clone().into(),
                self.prior_job.into(),
                self.copy_outputs.into(),
                self.on_failed.into(),
                self.on_completed.into(),
                self.satisfied.into(),
            ])
            .to_owned()
    }

    pub fn from_create(cd: &CreateDepend, job: JobId) -> JobDepend {
        JobDepend {
            job,
            name: cd.name.to_string(),
            prior_job: cd.prior_job,
            copy_outputs: cd.copy_outputs,
            on_failed: cd.on_failed,
            on_completed: cd.on_completed,
            satisfied: false,
        }
    }
}
