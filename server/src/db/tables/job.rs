/*
 * Copyright 2023 Oxide Computer Company
 */

use super::sublude::*;

#[derive(Clone, Debug)]
#[enum_def(prefix = "", suffix = "Def")]
pub struct Job {
    pub id: JobId,
    pub owner: UserId,
    pub name: String,
    /**
     * The original target name specified by the user, prior to resolution.
     */
    pub target: String,
    pub complete: bool,
    pub failed: bool,
    pub worker: Option<WorkerId>,
    pub waiting: bool,
    /**
     * The resolved target ID.  This field should be accessed through the
     * target() method to account for old records where this ID was optional.
     */
    pub target_id: Option<TargetId>,
    pub cancelled: bool,
    /**
     * When was this job successfully uploaded to the object store?
     */
    pub time_archived: Option<IsoDate>,
}

impl FromRow for Job {
    fn columns() -> Vec<ColumnRef> {
        [
            JobDef::Id,
            JobDef::Owner,
            JobDef::Name,
            JobDef::Target,
            JobDef::Complete,
            JobDef::Failed,
            JobDef::Worker,
            JobDef::Waiting,
            JobDef::TargetId,
            JobDef::Cancelled,
            JobDef::TimeArchived,
        ]
        .into_iter()
        .map(|col| {
            ColumnRef::TableColumn(SeaRc::new(JobDef::Table), SeaRc::new(col))
        })
        .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<Job> {
        Ok(Job {
            id: row.get(0)?,
            owner: row.get(1)?,
            name: row.get(2)?,
            target: row.get(3)?,
            complete: row.get(4)?,
            failed: row.get(5)?,
            worker: row.get(6)?,
            waiting: row.get(7)?,
            target_id: row.get(8)?,
            cancelled: row.get(9)?,
            time_archived: row.get(10)?,
        })
    }
}

impl Job {
    #[allow(dead_code)]
    pub fn time_submit(&self) -> DateTime<Utc> {
        self.id.datetime()
    }

    pub fn target(&self) -> TargetId {
        self.target_id.unwrap_or_else(|| {
            /*
             * XXX No new records should be created without a resolved target
             * ID, but old records might not have had one.  This is the ID of
             * the canned "default" target:
             */
            TargetId::from_str("00E82MSW0000000000000TT000").unwrap()
        })
    }

    pub fn is_archived(&self) -> bool {
        self.time_archived.is_some()
    }

    pub fn find(id: JobId) -> SelectStatement {
        Query::select()
            .from(JobDef::Table)
            .columns(Job::columns())
            .and_where(Expr::col(JobDef::Id).eq(id))
            .to_owned()
    }

    pub fn insert(&self) -> InsertStatement {
        Query::insert()
            .into_table(JobDef::Table)
            .columns(Self::bare_columns())
            .values_panic([
                self.id.into(),
                self.owner.into(),
                self.name.clone().into(),
                self.target.clone().into(),
                self.complete.into(),
                self.failed.into(),
                self.worker.into(),
                self.waiting.into(),
                self.target_id.into(),
                self.cancelled.into(),
                self.time_archived.into(),
            ])
            .to_owned()
    }
}
