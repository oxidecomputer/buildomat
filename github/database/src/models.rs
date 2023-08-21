use anyhow::{bail, Result};
use buildomat_database::*;
use buildomat_github_common::hooktypes;
use chrono::prelude::*;
use diesel::deserialize::FromSql;
use diesel::prelude::*;
use diesel::serialize::ToSql;
use serde::{Deserialize, Serialize};
use serde_with::{DeserializeFromStr, SerializeDisplay};
use std::{collections::HashMap, str::FromStr};

use super::schema::*;

integer_new_type!(DeliverySeq, usize, i64, BigInt, diesel::sql_types::BigInt);

ulid_new_type!(CheckSuiteId);
ulid_new_type!(CheckRunId);

#[derive(Debug, Clone, Queryable, Insertable, Identifiable)]
#[diesel(table_name = delivery)]
#[diesel(primary_key(seq))]
pub struct Delivery {
    pub seq: DeliverySeq,
    pub uuid: String,
    pub event: String,
    pub headers: Dictionary,
    pub payload: JsonValue,
    pub recvtime: IsoDate,
    pub ack: Option<i64>,
}

impl Delivery {
    pub fn recvtime_day_prefix(&self) -> String {
        self.recvtime.0.format("%Y-%m-%d").to_string()
    }
}

#[derive(Debug, Clone, Queryable, Insertable, Identifiable)]
#[diesel(table_name = repository)]
#[diesel(primary_key(id))]
pub struct Repository {
    pub id: i64,
    pub owner: String,
    pub name: String,
}

#[derive(Debug, Clone, Queryable, Insertable, Identifiable)]
#[diesel(table_name = install)]
#[diesel(primary_key(id))]
pub struct Install {
    pub id: i64,
    pub owner: i64,
}

#[derive(Debug, Clone, Queryable, Insertable, Identifiable)]
#[diesel(table_name = user)]
#[diesel(primary_key(id))]
pub struct User {
    pub id: i64,
    pub login: String,
    pub usertype: UserType,
    pub name: Option<String>,
    pub email: Option<String>,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    DeserializeFromStr,
    SerializeDisplay,
    FromSqlRow,
    diesel::expression::AsExpression,
)]
#[diesel(sql_type = diesel::sql_types::Text)]
pub enum UserType {
    User,
    Bot,
    Organisation,
}
sql_for_enum!(UserType);

impl UserType {
    pub fn is_org(&self) -> bool {
        matches!(self, UserType::Organisation)
    }

    pub fn from_github_str(ut: &str) -> Result<Self> {
        Ok(match ut {
            "User" => UserType::User,
            "Bot" => UserType::Bot,
            "Organization" => UserType::Organisation,
            x => bail!("invalid user type from GitHub: {:?}", x),
        })
    }

    pub fn from_github(ut: hooktypes::UserType) -> Self {
        match ut {
            hooktypes::UserType::User => UserType::User,
            hooktypes::UserType::Bot => UserType::Bot,
            hooktypes::UserType::Organization => UserType::Organisation,
        }
    }
}

impl FromStr for UserType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "user" => UserType::User,
            "bot" => UserType::Bot,
            "org" => UserType::Organisation,
            x => bail!("unknown user type: {:?}", x),
        })
    }
}

impl std::fmt::Display for UserType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use UserType::*;

        write!(
            f,
            "{}",
            match self {
                User => "user",
                Bot => "bot",
                Organisation => "org",
            }
        )
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    DeserializeFromStr,
    SerializeDisplay,
    FromSqlRow,
    diesel::expression::AsExpression,
)]
#[diesel(sql_type = diesel::sql_types::Text)]
pub enum CheckRunVariety {
    Control,
    AlwaysPass,
    FailFirst,
    Basic,
}
sql_for_enum!(CheckRunVariety);

impl CheckRunVariety {
    pub fn is_control(&self) -> bool {
        matches!(self, CheckRunVariety::Control)
    }
}

impl FromStr for CheckRunVariety {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "control" => CheckRunVariety::Control,
            "always_pass" => CheckRunVariety::AlwaysPass,
            "fail_first" => CheckRunVariety::FailFirst,
            "basic" => CheckRunVariety::Basic,
            x => bail!("unknown check run class: {:?}", x),
        })
    }
}

impl std::fmt::Display for CheckRunVariety {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use CheckRunVariety::*;

        write!(
            f,
            "{}",
            match self {
                Control => "control",
                AlwaysPass => "always_pass",
                FailFirst => "fail_first",
                Basic => "basic",
            }
        )
    }
}

#[derive(Debug, Clone, Copy, FromSqlRow, diesel::expression::AsExpression)]
#[diesel(sql_type = diesel::sql_types::Text)]
pub enum CheckSuiteState {
    Created,
    Parked,
    Planned,
    Running,
    Complete,
    Retired,
}
sql_for_enum!(CheckSuiteState);

impl CheckSuiteState {
    pub fn is_parked(&self) -> bool {
        matches!(self, CheckSuiteState::Parked)
    }
}

impl FromStr for CheckSuiteState {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "created" => CheckSuiteState::Created,
            "parked" => CheckSuiteState::Parked,
            "planned" => CheckSuiteState::Planned,
            "running" => CheckSuiteState::Running,
            "complete" => CheckSuiteState::Complete,
            "retired" => CheckSuiteState::Retired,
            x => bail!("unknown check suite state: {:?}", x),
        })
    }
}

impl ToString for CheckSuiteState {
    fn to_string(&self) -> String {
        use CheckSuiteState::*;

        match self {
            Created => "created",
            Parked => "parked",
            Planned => "planned",
            Running => "running",
            Complete => "complete",
            Retired => "retired",
        }
        .to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobFile {
    pub path: String,
    pub name: String,
    pub variety: CheckRunVariety,
    pub config: serde_json::Value,
    pub content: String,
    #[serde(default)]
    pub dependencies: HashMap<String, JobFileDepend>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobFileDepend {
    pub job: String,
    pub config: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Plan {
    pub jobfiles: Vec<JobFile>,
}

json_new_type!(JsonPlan, Plan);

#[derive(Debug, Clone, Queryable, Insertable, Identifiable)]
#[diesel(table_name = check_suite)]
#[diesel(primary_key(id))]
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

#[derive(Debug, Clone, Queryable, Insertable, Identifiable)]
#[diesel(table_name = check_run)]
#[diesel(primary_key(id))]
pub struct CheckRun {
    pub id: CheckRunId,
    pub check_suite: CheckSuiteId,
    /**
     * User-visible name of this Check Run.
     */
    pub name: String,
    /**
     * Which action do we need to take to perform this check run.
     */
    pub variety: CheckRunVariety,
    /**
     * Input job file content; e.g., a bash script.
     */
    pub content: Option<String>,
    /**
     * Input job file configuration; interpreted by the variety routines.
     */
    pub config: Option<JsonValue>,
    /**
     * Per-variety private state tracking data.
     */
    pub private: Option<JsonValue>,
    /**
     * Is this the current instance of this Check Run within the containing
     * Check Suite?
     */
    pub active: bool,
    /**
     * Do we believe that GitHub has received the most recent status information
     * and output about this Check Run?
     */
    pub flushed: bool,
    /**
     * What ID has GitHub given us when we created this instance of the Check
     * Run?
     */
    pub github_id: Option<i64>,
    /**
     * Dependency information.  This is a map from dependency name to
     * JobFileDepend objects.
     */
    pub dependencies: Option<JsonValue>,
}

impl CheckRun {
    pub fn get_config<T>(&self) -> Result<T>
    where
        for<'de> T: Deserialize<'de>,
    {
        let config = if let Some(config) = &self.config {
            config.0.clone()
        } else {
            serde_json::json!({})
        };
        Ok(serde_json::from_value(config)?)
    }

    pub fn get_dependencies(
        &self,
    ) -> Result<HashMap<String, CheckRunDependency>> {
        let mut jfds: HashMap<String, JobFileDepend> =
            if let Some(dependencies) = &self.dependencies {
                serde_json::from_value(dependencies.0.clone())?
            } else {
                return Ok(Default::default());
            };

        Ok(jfds
            .drain()
            .map(|(name, jfd)| (name, CheckRunDependency(jfd)))
            .collect())
    }

    pub fn get_private<T>(&self) -> Result<T>
    where
        for<'de> T: Deserialize<'de>,
    {
        let private = if let Some(private) = &self.private {
            private.0.clone()
        } else {
            serde_json::json!({})
        };
        Ok(serde_json::from_value(private)?)
    }

    pub fn set_private<T: Serialize>(&mut self, private: T) -> Result<()> {
        self.private = Some(JsonValue(serde_json::to_value(private)?));
        Ok(())
    }
}

pub struct CheckRunDependency(JobFileDepend);

impl CheckRunDependency {
    pub fn job(&self) -> &str {
        &self.0.job
    }

    pub fn get_config<T>(&self) -> Result<T>
    where
        for<'de> T: Deserialize<'de>,
    {
        Ok(serde_json::from_value(self.0.config.clone())?)
    }
}
