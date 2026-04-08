/*
 * Copyright 2026 Oxide Computer Company
 */

use super::sublude::*;

#[derive(Debug, Clone)]
#[enum_def(prefix = "", suffix = "Def")]
pub struct CachePendingUpload {
    pub id: CacheFileId,
    /**
     * Opaque ID given to us by S3's multipart upload API.
     */
    pub s3_upload_id: String,
    pub owner: UserId,
    pub name: String,
    pub worker: WorkerId,
    pub size_bytes: DataSize,
    pub chunks: u32,
    pub etags: Option<Vec<String>>,
    pub time_begin: IsoDate,
    pub time_finish: Option<IsoDate>,
}

impl FromRow for CachePendingUpload {
    fn columns() -> Vec<ColumnRef> {
        [
            CachePendingUploadDef::Id,
            CachePendingUploadDef::S3UploadId,
            CachePendingUploadDef::Owner,
            CachePendingUploadDef::Name,
            CachePendingUploadDef::Worker,
            CachePendingUploadDef::SizeBytes,
            CachePendingUploadDef::Chunks,
            CachePendingUploadDef::Etags,
            CachePendingUploadDef::TimeBegin,
            CachePendingUploadDef::TimeFinish,
        ]
        .into_iter()
        .map(|col| {
            ColumnRef::TableColumn(
                SeaRc::new(CachePendingUploadDef::Table),
                SeaRc::new(col),
            )
        })
        .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<CachePendingUpload> {
        Ok(CachePendingUpload {
            id: row.get(0)?,
            s3_upload_id: row.get(1)?,
            owner: row.get(2)?,
            name: row.get(3)?,
            worker: row.get(4)?,
            size_bytes: row.get(5)?,
            chunks: row.get(6)?,
            etags: row.get::<_, Option<String>>(7)?.map(|string| {
                string.split(',').map(|s| s.to_string()).collect()
            }),
            time_begin: row.get(8)?,
            time_finish: row.get(9)?,
        })
    }
}

impl CachePendingUpload {
    pub fn insert(&self) -> InsertStatement {
        Query::insert()
            .into_table(CachePendingUploadDef::Table)
            .columns(Self::bare_columns())
            .values_panic([
                self.id.into(),
                self.s3_upload_id.clone().into(),
                self.owner.into(),
                self.name.clone().into(),
                self.worker.into(),
                self.size_bytes.into(),
                self.chunks.into(),
                self.etags
                    .clone()
                    .map(|etags| etags.join(","))
                    .into(),
                self.time_begin.into(),
                self.time_finish.into(),
            ])
            .to_owned()
    }
}
