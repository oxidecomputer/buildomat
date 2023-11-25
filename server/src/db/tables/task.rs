/*
 * Copyright 2023 Oxide Computer Company
 */

use super::sublude::*;

#[derive(Debug)]
#[enum_def(prefix = "", suffix = "Def")]
pub struct Task {
    pub job: JobId,
    pub seq: u32,
    pub name: String,
    pub script: String,
    pub env_clear: bool,
    pub env: Dictionary,
    pub user_id: Option<UnixUid>,
    pub group_id: Option<UnixGid>,
    pub workdir: Option<String>,
    pub complete: bool,
    pub failed: bool,
}

impl FromRow for Task {
    fn columns() -> Vec<ColumnRef> {
        [
            TaskDef::Job,
            TaskDef::Seq,
            TaskDef::Name,
            TaskDef::Script,
            TaskDef::EnvClear,
            TaskDef::Env,
            TaskDef::UserId,
            TaskDef::GroupId,
            TaskDef::Workdir,
            TaskDef::Complete,
            TaskDef::Failed,
        ]
        .into_iter()
        .map(|col| {
            ColumnRef::TableColumn(SeaRc::new(TaskDef::Table), SeaRc::new(col))
        })
        .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<Task> {
        Ok(Task {
            job: row.get(0)?,
            seq: row.get(1)?,
            name: row.get(2)?,
            script: row.get(3)?,
            env_clear: row.get(4)?,
            env: row.get(5)?,
            user_id: row.get(6)?,
            group_id: row.get(7)?,
            workdir: row.get(8)?,
            complete: row.get(9)?,
            failed: row.get(10)?,
        })
    }
}

impl Task {
    pub fn from_create(ct: &CreateTask, job: JobId, seq: usize) -> Task {
        Task {
            job,
            seq: seq.try_into().unwrap(),
            name: ct.name.to_string(),
            script: ct.script.to_string(),
            env_clear: ct.env_clear,
            env: Dictionary(ct.env.clone()),
            user_id: ct.user_id.map(UnixUid),
            group_id: ct.group_id.map(UnixGid),
            workdir: ct.workdir.clone(),
            complete: false,
            failed: false,
        }
    }

    pub fn find(job: JobId, seq: u32) -> SelectStatement {
        Query::select()
            .from(TaskDef::Table)
            .columns(Task::columns())
            .and_where(Expr::col(TaskDef::Job).eq(job))
            .and_where(Expr::col(TaskDef::Seq).eq(seq))
            .to_owned()
    }

    pub fn insert(&self) -> InsertStatement {
        Query::insert()
            .into_table(TaskDef::Table)
            .columns(Self::bare_columns())
            .values_panic([
                self.job.into(),
                self.seq.into(),
                self.name.clone().into(),
                self.script.clone().into(),
                self.env_clear.into(),
                self.env.clone().into(),
                self.user_id.into(),
                self.group_id.into(),
                self.workdir.clone().into(),
                self.complete.into(),
                self.failed.into(),
            ])
            .to_owned()
    }
}
