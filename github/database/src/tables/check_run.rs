/*
 * Copyright 2023 Oxide Computer Company
 */

use super::sublude::*;

sqlite_sql_enum!(CheckRunVariety (Serialize, Deserialize) => {
    Control,
    AlwaysPass,
    FailFirst,
    Basic,
});

impl CheckRunVariety {
    pub fn is_control(&self) -> bool {
        matches!(self, CheckRunVariety::Control)
    }
}

#[derive(Debug, Clone)]
#[enum_def(prefix = "", suffix = "Def")]
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

impl FromRow for CheckRun {
    fn columns() -> Vec<ColumnRef> {
        [
            CheckRunDef::Id,
            CheckRunDef::CheckSuite,
            CheckRunDef::Name,
            CheckRunDef::Variety,
            CheckRunDef::Content,
            CheckRunDef::Config,
            CheckRunDef::Private,
            CheckRunDef::Active,
            CheckRunDef::Flushed,
            CheckRunDef::GithubId,
            CheckRunDef::Dependencies,
        ]
        .into_iter()
        .map(|col| {
            ColumnRef::TableColumn(
                SeaRc::new(CheckRunDef::Table),
                SeaRc::new(col),
            )
        })
        .collect()
    }

    fn from_row(row: &Row) -> rusqlite::Result<Self> {
        Ok(CheckRun {
            id: row.get(0)?,
            check_suite: row.get(1)?,
            name: row.get(2)?,
            variety: row.get(3)?,
            content: row.get(4)?,
            config: row.get(5)?,
            private: row.get(6)?,
            active: row.get(7)?,
            flushed: row.get(8)?,
            github_id: row.get(9)?,
            dependencies: row.get(10)?,
        })
    }
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobFileDepend {
    pub job: String,
    pub config: serde_json::Value,
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

/*
 * These tests attempt to ensure that the concrete representation of the enum
 * does not change, as that would make the database unuseable.
 */
#[cfg(test)]
mod test {
    use super::CheckRunVariety;
    use std::str::FromStr;

    const CHECK_RUN_VARIETIES: &'static [(&'static str, CheckRunVariety)] = &[
        ("control", CheckRunVariety::Control),
        ("always_pass", CheckRunVariety::AlwaysPass),
        ("fail_first", CheckRunVariety::FailFirst),
        ("basic", CheckRunVariety::Basic),
    ];

    #[test]
    fn check_run_variety_forward() {
        for (s, e) in CHECK_RUN_VARIETIES {
            assert_eq!(*s, e.to_string());
        }
    }

    #[test]
    fn check_run_variety_backward() {
        for (s, e) in CHECK_RUN_VARIETIES {
            assert_eq!(CheckRunVariety::from_str(s).unwrap(), *e);
        }
    }
}
