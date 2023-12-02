use std::collections::BTreeMap;
use std::sync::Arc;

use crate::App;
use anyhow::Result;
use buildomat_common::*;
use buildomat_github_database::types::*;
use serde::{Deserialize, Serialize};

pub const CONTROL_RUN_NAME: &str = "*control";

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ControlPrivate {
    pub error: Option<String>,
    #[serde(default)]
    pub complete: bool,
    #[serde(default)]
    pub no_plans: bool,
    #[serde(default)]
    pub need_auth: bool,
}

pub(crate) async fn details(
    app: &Arc<App>,
    cs: &CheckSuite,
    _cr: &CheckRun,
    _local_time: bool,
) -> Result<String> {
    let mut out = String::new();
    let db = &app.db;

    let repo = db.load_repository(cs.repo);

    /*
     * List details about all check runs for this check suite.  Sort the runs
     * into groups by run name, as they appear on GitHub.
     */
    let mut runs: BTreeMap<String, Vec<CheckRun>> = Default::default();
    for run in db.list_check_runs_for_suite(cs.id)? {
        let set = runs.entry(run.name.to_string()).or_default();
        set.push(run);
    }

    /*
     * Within a set, sort by run ID (which, as the IDs are ULIDs, should also be
     * creation order):
     */
    for set in runs.values_mut() {
        set.sort_by(|a, b| a.id.cmp(&b.id));
    }

    /*
     * Render a list of sets of runs:
     */
    out += "<h3>Check Runs:</h3>\n";
    out += "<ul>\n";
    for (set, runs) in &runs {
        out += &format!("<li>check run \"{set}\"\n");

        out += "<ul>\n";
        for run in runs {
            out += "<li>";
            if run.active {
                out += "<b>";
            }

            let when = run
                .id
                .datetime()
                .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
            let age = run.id.age().render();

            out += &format!(
                "{when} run <a href=\"{}\">{}</a> ",
                app.make_details_url(cs, run),
                run.id,
            );

            if run.active {
                out += "</b> ";
            }

            out += &format!("[created {age} ago] ");

            if let Some(id) = &run.github_id {
                let id = if let Ok(repo) = &repo {
                    let url = format!(
                        "https://github.com/{}/{}/runs/{}",
                        repo.owner, repo.name, id,
                    );
                    format!("<a href=\"{url}\">{id}</a>")
                } else {
                    id.to_string()
                };
                out += &format!("(GitHub ID {id}) ");
            }

            if run.active {
                out += " <b>[latest instance]</b>";
            }
            out += "\n";
        }
        out += "</ul>\n";
        out += "<br>\n";
        out += "<br>\n";
    }
    out += "</ul>\n";

    Ok(out)
}
