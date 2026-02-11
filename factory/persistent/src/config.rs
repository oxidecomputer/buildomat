/*
 * Copyright 2026 Oxide Computer Company
 */

use std::collections::HashMap;
use std::path::PathBuf;

use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConfigFile {
    pub(crate) general: ConfigFileGeneral,
    pub(crate) factory: ConfigFileFactory,
    pub(crate) execution: ConfigFileExecution,
    #[serde(default)]
    pub(crate) target: HashMap<String, ConfigFileTarget>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConfigFileGeneral {
    pub(crate) baseurl: String,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConfigFileFactory {
    pub(crate) token: String,
}

/// Execution configuration for the external command.
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConfigFileExecution {
    /// Command to execute (e.g., "sp-runner")
    pub(crate) command: String,
    /// Arguments to pass to the command
    #[serde(default)]
    pub(crate) args: Vec<String>,
    /// Base directory for per-job subdirectories.
    /// Each job gets its own directory: {job_dir}/{job_id}/
    pub(crate) job_dir: PathBuf,
}

/// Build target configuration.
///
/// Currently empty - the factory only checks that requested targets
/// exist in the configuration. Future commits will add execution
/// configuration here.
#[derive(Deserialize, Debug, Clone, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConfigFileTarget {}

impl ConfigFile {
    pub(crate) fn targets(&self) -> Vec<String> {
        self.target.keys().cloned().collect()
    }
}
