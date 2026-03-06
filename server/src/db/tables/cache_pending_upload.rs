/*
 * Copyright 2026 Oxide Computer Company
 */

use super::sublude::*;

#[derive(Debug, Clone)]
#[enum_def(prefix = "", suffix = "Def")]
pub struct CachePendingUpload {
    pub id: CachePendingUploadId,
    /**
     * Opaque ID given to us by S3's multipart upload API.
     */
    pub s3_upload_id: String,
    pub owner: UserId,
    pub name: String,
    pub worker: WorkerId,
    pub size_bytes: DataSize,
    pub chunks: u32,
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
        })
    }
}
