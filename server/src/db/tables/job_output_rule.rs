/*
 * Copyright 2023 Oxide Computer Company
 */

use super::sublude::*;

#[derive(Debug)]
#[enum_def(prefix = "", suffix = "Def")]
pub struct JobOutputRule {
    pub job: JobId,
    pub seq: u32,
    pub rule: String,
    pub ignore: bool,
    pub size_change_ok: bool,
    pub require_match: bool,
}

impl FromRow for JobOutputRule {
    fn columns() -> Vec<ColumnRef> {
        [
            JobOutputRuleDef::Job,
            JobOutputRuleDef::Seq,
            JobOutputRuleDef::Rule,
            JobOutputRuleDef::Ignore,
            JobOutputRuleDef::SizeChangeOk,
            JobOutputRuleDef::RequireMatch,
        ]
        .into_iter()
        .map(|col| {
            ColumnRef::TableColumn(
                SeaRc::new(JobOutputRuleDef::Table),
                SeaRc::new(col),
            )
        })
        .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<JobOutputRule> {
        Ok(JobOutputRule {
            job: row.get(0)?,
            seq: row.get(1)?,
            rule: row.get(2)?,
            ignore: row.get(3)?,
            size_change_ok: row.get(4)?,
            require_match: row.get(5)?,
        })
    }
}

impl JobOutputRule {
    pub fn insert(&self) -> InsertStatement {
        Query::insert()
            .into_table(JobOutputRuleDef::Table)
            .columns(Self::bare_columns())
            .values_panic([
                self.job.into(),
                self.seq.into(),
                self.rule.clone().into(),
                self.ignore.into(),
                self.size_change_ok.into(),
                self.require_match.into(),
            ])
            .to_owned()
    }

    pub fn from_create(
        cd: &CreateOutputRule,
        job: JobId,
        seq: usize,
    ) -> JobOutputRule {
        JobOutputRule {
            job,
            seq: seq.try_into().unwrap(),
            rule: cd.rule.to_string(),
            ignore: cd.ignore,
            size_change_ok: cd.size_change_ok,
            require_match: cd.require_match,
        }
    }
}
