/*
 * Copyright 2025 Oxide Computer Company
 */

use anyhow::{Result, bail};
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
    pull_request: Option<PullRequest>,
    pub requested_action: Option<RequestedAction>,
}

impl Payload {
    pub fn pull_request(&self) -> Result<&PullRequest> {
        let mut problems = Vec::new();

        if let Some(pr) = self.pull_request.as_ref() {
            /*
             * Unfortunately, GitHub has been known to occasionally omit the
             * repository object in web hooks that get delivered to us.  If that
             * occurs, just drop the delivery.
             */
            if pr.base.repo.is_none() {
                problems.push("missing pull_request.base.repo");
            }
            if pr.head.repo.is_none() {
                problems.push("missing pull_request.head.repo");
            }

            if problems.is_empty() {
                return Ok(pr);
            }
        } else {
            problems.push("missing pull request information");
        }

        bail!("{}", problems.join(", "));
    }
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
pub struct App {
    pub id: i64,
    pub name: String,
    pub slug: String,
    pub description: Option<String>,
}

#[derive(Deserialize, Debug, Clone, Copy)]
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
    pub app: App,
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
    pub repo: Option<Repository>,
}

impl PullRequest {
    pub fn is_open(&self) -> bool {
        matches!(self.state, PullRequestState::Open)
    }
}

impl PullRequestCommit {
    pub fn repo(&self) -> &Repository {
        /*
         * This is validated in the pull_request() routine on the Payload.
         */
        self.repo.as_ref().unwrap()
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
    pub account: Option<User>,
}

#[derive(Deserialize, Debug)]
pub struct Owner {
    pub id: i64,
    pub node_id: String,
    pub login: String,
}

#[derive(Deserialize, Debug)]
pub struct RequestedAction {
    pub identifier: String,
}
