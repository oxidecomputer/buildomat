/*
 * Copyright 2024 Oxide Computer Company
 */

use buildomat_jobsh::jobfile;

use super::check_run::{CheckRunVariety, JobFileDepend};
use super::sublude::*;

sqlite_sql_enum!(CheckSuiteState => {
    Created,
    Parked,
    Planned,
    Running,
    Complete,
    Retired,
});

impl CheckSuiteState {
    pub fn is_parked(&self) -> bool {
        matches!(self, CheckSuiteState::Parked)
    }
}

sqlite_json_new_type!(JsonPlan, Plan);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Plan {
    pub jobfiles: Vec<JobFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobFile {
    pub path: String,
    pub name: String,
    pub variety: CheckRunVariety,
    pub config: serde_json::Value,
    pub content: String,
    #[serde(default)]
    pub dependencies: HashMap<String, JobFileDepend>,
}

impl From<jobfile::JobFile> for JobFile {
    fn from(value: jobfile::JobFile) -> Self {
        let jobfile::JobFile {
            path,
            name,
            variety,
            config,
            content,
            dependencies,
        } = value;

        Self {
            path,
            name,
            variety: match variety {
                jobfile::Variety::Basic => CheckRunVariety::Basic,
            },
            config,
            content,
            dependencies: dependencies
                .into_iter()
                .map(|(a, b)| (a.into(), b.into()))
                .collect(),
        }
    }
}

#[derive(Debug, Clone)]
#[enum_def(prefix = "", suffix = "Def")]
pub struct CheckSuite {
    pub id: CheckSuiteId,
    pub repo: i64,
    pub install: i64,
    pub github_id: i64,
    pub head_sha: String,
    pub head_branch: Option<String>,
    pub state: CheckSuiteState,
    pub plan: Option<JsonPlan>,
    pub plan_sha: Option<String>,
    pub url_key: String,
    pub pr_by: Option<i64>,
    pub requested_by: Option<i64>,
    pub approved_by: Option<i64>,
}

impl FromRow for CheckSuite {
    fn columns() -> Vec<ColumnRef> {
        [
            CheckSuiteDef::Id,
            CheckSuiteDef::Repo,
            CheckSuiteDef::Install,
            CheckSuiteDef::GithubId,
            CheckSuiteDef::HeadSha,
            CheckSuiteDef::HeadBranch,
            CheckSuiteDef::State,
            CheckSuiteDef::Plan,
            CheckSuiteDef::PlanSha,
            CheckSuiteDef::UrlKey,
            CheckSuiteDef::PrBy,
            CheckSuiteDef::RequestedBy,
            CheckSuiteDef::ApprovedBy,
        ]
        .into_iter()
        .map(|col| {
            ColumnRef::TableColumn(
                SeaRc::new(CheckSuiteDef::Table),
                SeaRc::new(col),
            )
        })
        .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<Self> {
        Ok(CheckSuite {
            id: row.get(0)?,
            repo: row.get(1)?,
            install: row.get(2)?,
            github_id: row.get(3)?,
            head_sha: row.get(4)?,
            head_branch: row.get(5)?,
            state: row.get(6)?,
            plan: row.get(7)?,
            plan_sha: row.get(8)?,
            url_key: row.get(9)?,
            pr_by: row.get(10)?,
            requested_by: row.get(11)?,
            approved_by: row.get(12)?,
        })
    }
}

impl CheckSuite {
    pub fn find(id: CheckSuiteId) -> SelectStatement {
        Query::select()
            .from(CheckSuiteDef::Table)
            .columns(CheckSuite::columns())
            .and_where(Expr::col(CheckSuiteDef::Id).eq(id))
            .to_owned()
    }

    pub fn find_by_github_id(repo: i64, github_id: i64) -> SelectStatement {
        Query::select()
            .from(CheckSuiteDef::Table)
            .columns(CheckSuite::columns())
            .and_where(Expr::col(CheckSuiteDef::Repo).eq(repo))
            .and_where(Expr::col(CheckSuiteDef::GithubId).eq(github_id))
            .to_owned()
    }

    pub fn insert(&self) -> InsertStatement {
        Query::insert()
            .into_table(CheckSuiteDef::Table)
            .columns(Self::bare_columns())
            .values_panic([
                self.id.into(),
                self.repo.into(),
                self.install.into(),
                self.github_id.into(),
                self.head_sha.clone().into(),
                self.head_branch.clone().into(),
                self.state.into(),
                self.plan.clone().into(),
                self.plan_sha.clone().into(),
                self.url_key.clone().into(),
                self.pr_by.into(),
                self.requested_by.into(),
                self.approved_by.into(),
            ])
            .to_owned()
    }
}
