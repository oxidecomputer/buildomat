use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct Payload {
    #[serde(default)]
    pub action: String,
    pub sender: User,
    pub repository: Option<Repository>,
    pub installation: Option<Installation>,
    pub check_suite: Option<CheckSuite>,
    pub check_run: Option<CheckRun>,
    pub pull_request: Option<PullRequest>,
}

#[derive(Deserialize, Debug)]
pub struct User {
    pub login: String,
    pub id: i64,
    pub node_id: String,
    pub name: Option<String>,
    pub email: Option<String>,
    #[serde(rename = "type")]
    pub type_: UserType,
    pub site_admin: bool,
}

#[derive(Deserialize, Debug)]
pub enum UserType {
    Bot,
    User,
    Organization,
}

#[derive(Deserialize, Debug)]
pub struct Repository {
    pub id: i64,
    pub node_id: String,
    pub name: String,
    pub owner: Owner,
}

#[derive(Deserialize, Debug)]
pub struct CheckSuite {
    pub id: i64,
    pub node_id: String,
    pub head_branch: Option<String>,
    pub head_sha: String,
    pub status: CheckSuiteStatus,
}

#[derive(Deserialize, Debug)]
pub struct CheckRun {
    pub id: i64,
    pub node_id: String,
    pub head_sha: String,
    pub external_id: String,
    pub status: CheckRunStatus,
    pub conclusion: Option<CheckRunConclusion>,
}

#[derive(Deserialize, Debug)]
pub struct PullRequest {
    pub id: i64,
    pub number: i64,
    pub title: String,
    pub node_id: String,
    pub state: PullRequestState,
    pub head: PullRequestCommit,
    pub base: PullRequestCommit,
}

#[derive(Deserialize, Debug)]
pub struct PullRequestCommit {
    pub label: String,
    #[serde(rename = "ref")]
    pub ref_: String,
    pub sha: String,
    pub user: User,
    pub repo: Repository,
}

impl PullRequest {
    pub fn is_open(&self) -> bool {
        matches!(self.state, PullRequestState::Open)
    }
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum PullRequestState {
    Open,
    Closed,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum CheckSuiteStatus {
    Requested,
    InProgress,
    Completed,
    Queued,
    /*
     * This status is not documented, and does not appear in the schema,
     * but GitHub Actions (of course) seems to generate it sometimes:
     */
    Pending,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum CheckRunStatus {
    Queued,
    InProgress,
    Completed,
    /*
     * This status is not documented, and does not appear in the schema,
     * but GitHub Actions (of course) seems to generate it sometimes:
     */
    Pending,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum CheckRunConclusion {
    Success,
    Failure,
    Neutral,
    Cancelled,
    TimedOut,
    ActionRequired,
    Stale,
    Skipped,
}

#[derive(Deserialize, Debug)]
pub struct Installation {
    pub id: i64,
    pub node_id: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct Owner {
    pub id: i64,
    pub node_id: String,
    pub login: String,
}
