use std::{collections::BTreeMap, ops::Deref};

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct RevisionCommit {
    pub subject: String,
    pub message: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Revision {
    pub kind: String,
    pub _number: u32,
    pub commit: RevisionCommit,
    #[serde(rename = "ref")]
    pub ref_: String,
}

#[derive(Debug, Clone)]
pub struct RevisionAndCommit<'a>(String, &'a Revision);

impl RevisionAndCommit<'_> {
    pub fn commit_id(&self) -> &str {
        &self.0
    }
}

impl Deref for RevisionAndCommit<'_> {
    type Target = Revision;

    fn deref(&self) -> &Self::Target {
        self.1
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Change {
    pub project: String,
    pub change_id: String,
    pub _number: u32,
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

    pub fn revisions(&self) -> Vec<RevisionAndCommit> {
        if let Some(revs) = &self.revisions {
            let mut revs = revs
                .iter()
                .map(|(cid, r)| RevisionAndCommit(cid.to_string(), r))
                .collect::<Vec<_>>();
            revs.sort_by(|a, b| a.1._number.cmp(&b.1._number));
            revs
        } else {
            Vec::new()
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Project {
    pub id: String,
    pub description: Option<String>,
}
