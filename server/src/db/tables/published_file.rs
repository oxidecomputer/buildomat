/*
 * Copyright 2023 Oxide Computer Company
 */

use super::sublude::*;

#[derive(Debug)]
#[enum_def(prefix = "", suffix = "Def")]
pub struct PublishedFile {
    pub owner: UserId,
    pub series: String,
    pub version: String,
    pub name: String,
    pub job: JobId,
    pub file: JobFileId,
}

impl FromRow for PublishedFile {
    fn columns() -> Vec<ColumnRef> {
        [
            PublishedFileDef::Owner,
            PublishedFileDef::Series,
            PublishedFileDef::Version,
            PublishedFileDef::Name,
            PublishedFileDef::Job,
            PublishedFileDef::File,
        ]
        .into_iter()
        .map(|col| {
            ColumnRef::TableColumn(
                SeaRc::new(PublishedFileDef::Table),
                SeaRc::new(col),
            )
        })
        .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<PublishedFile> {
        Ok(PublishedFile {
            owner: row.get(0)?,
            series: row.get(1)?,
            version: row.get(2)?,
            name: row.get(3)?,
            job: row.get(4)?,
            file: row.get(5)?,
        })
    }
}

impl PublishedFile {
    pub fn find(
        owner: UserId,
        series: &str,
        version: &str,
        name: &str,
    ) -> SelectStatement {
        Query::select()
            .from(PublishedFileDef::Table)
            .columns(PublishedFile::columns())
            .and_where(Expr::col(PublishedFileDef::Owner).eq(owner))
            .and_where(Expr::col(PublishedFileDef::Series).eq(series))
            .and_where(Expr::col(PublishedFileDef::Version).eq(version))
            .and_where(Expr::col(PublishedFileDef::Name).eq(name))
            .to_owned()
    }

    pub fn insert(&self) -> InsertStatement {
        Query::insert()
            .into_table(PublishedFileDef::Table)
            .columns(Self::bare_columns())
            .values_panic([
                self.owner.into(),
                self.series.clone().into(),
                self.version.clone().into(),
                self.name.clone().into(),
                self.job.into(),
                self.file.into(),
            ])
            .to_owned()
    }
}
