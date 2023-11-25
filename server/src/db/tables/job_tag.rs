/*
 * Copyright 2023 Oxide Computer Company
 */

use super::sublude::*;

/*
 * This table doesn't have its own model struct:
 */
#[derive(Iden)]
#[iden(rename = "job_tag")]
pub enum JobTagDef {
    Table,
    Job,
    Name,
    Value,
}
