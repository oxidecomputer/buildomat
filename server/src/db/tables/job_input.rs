/*
 * Copyright 2023 Oxide Computer Company
 */

use super::sublude::*;

#[derive(Debug)]
#[enum_def(prefix = "", suffix = "Def")]
pub struct JobInput {
    pub job: JobId,
    pub name: String,
    pub id: Option<JobFileId>,
    /**
     * Files are identified by a (job ID, file ID) tuple.  In the case of an
     * output that is copied to another job as an input, this field contains the
     * job ID of the job which actually holds the file.  If this is not set, the
     * file is an input that was uploaded directly by the user creating the job
     * and is stored with the job that owns the input record.
     */
    pub other_job: Option<JobId>,
}

impl FromRow for JobInput {
    fn columns() -> Vec<ColumnRef> {
        [
            JobInputDef::Job,
            JobInputDef::Name,
            JobInputDef::Id,
            JobInputDef::OtherJob,
        ]
        .into_iter()
        .map(|col| {
            ColumnRef::TableColumn(
                SeaRc::new(JobInputDef::Table),
                SeaRc::new(col),
            )
        })
        .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<JobInput> {
        Ok(JobInput {
            job: row.get(0)?,
            name: row.get(1)?,
            id: row.get(2)?,
            other_job: row.get(3)?,
        })
    }
}

impl JobInput {
    pub fn insert(&self) -> InsertStatement {
        Query::insert()
            .into_table(JobInputDef::Table)
            .columns(Self::bare_columns())
            .values_panic([
                self.job.into(),
                self.name.clone().into(),
                self.id.into(),
                self.other_job.into(),
            ])
            .to_owned()
    }

    pub fn upsert(&self) -> InsertStatement {
        self.insert()
            .on_conflict(OnConflict::new().do_nothing().to_owned())
            .to_owned()
    }

    pub fn from_create(name: &str, job: JobId) -> JobInput {
        JobInput { job, name: name.to_string(), id: None, other_job: None }
    }
}
