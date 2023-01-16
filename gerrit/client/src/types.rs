use std::collections::BTreeMap;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct RevisionCommit {
    pub subject: String,
    pub message: String,
}

#[derive(Debug, Deserialize)]
pub struct Revision {
    pub kind: String,
    pub _number: i32,
    pub commit: RevisionCommit,
}

#[derive(Debug, Deserialize)]
pub struct Change {
    pub project: String,
    pub change_id: String,
    pub _number: i32,
    pub subject: String,
    pub(crate) current_revision: Option<String>,
    pub(crate) revisions: Option<BTreeMap<String, Revision>>,
}

impl Change {
    pub fn current(&self) -> Option<&Revision> {
        if let Some(cur) = &self.current_revision {
            if let Some(revs) = &self.revisions {
                return revs.get(cur);
            }
        }

        None
    }
}
