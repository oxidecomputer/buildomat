/*
 * Copyright 2024 Oxide Computer Company
 */

use std::collections::{HashMap, HashSet};

use anyhow::{Result, anyhow, bail};
use buildomat_common::*;
use serde::Deserialize;

#[derive(Deserialize, Debug, Copy, Clone, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum Variety {
    Basic,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobFile {
    pub path: String,
    pub name: String,
    pub variety: Variety,
    pub config: serde_json::Value,
    pub content: String,
    pub dependencies: HashMap<String, JobFileDepend>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobFileDepend {
    pub job: String,
    pub config: serde_json::Value,
}

#[derive(Deserialize)]
struct FrontMatter {
    pub name: String,
    pub variety: Variety,
    #[serde(default = "true_if_missing")]
    pub enable: bool,
    #[serde(default)]
    pub dependencies: HashMap<String, FrontMatterDepend>,
    #[serde(flatten)]
    pub extra: toml::Value,
}

#[derive(Deserialize)]
struct FrontMatterDepend {
    pub job: String,
    #[serde(flatten)]
    pub extra: toml::Value,
}

impl JobFile {
    /**
     * Lift the TOML frontmatter out of a job file and parse the global values.
     * Returns None if the file is valid, but not enabled.
     */
    pub fn parse_content_at_path(
        content: &str,
        path: &str,
    ) -> Result<Option<JobFile>> {
        let mut lines = content.lines();

        if let Some(shebang) = lines.next() {
            /*
             * For now, we accept any script and assume it is effectively
             * bourne-compatible, at least with respect to comments.
             */
            if !shebang.starts_with("#!") {
                bail!("{:?} must have an interpreter line", path);
            }
        };

        /*
         * Extract lines after the interpreter line that begin with "#:".  Treat
         * this as a TOML block wrapped in something that bourne shells will
         * ignore as a comment.  Allow the use of regular comments interspersed
         * with TOML lines, as long as there are no blank lines.
         */
        let frontmatter = lines
            .by_ref()
            .take_while(|l| l.starts_with('#'))
            .filter(|l| l.starts_with("#:"))
            .map(|l| l.trim_start_matches("#:"))
            .collect::<Vec<_>>()
            .join("\n");

        /*
         * Parse the front matter as TOML:
         */
        let toml =
            toml::from_str::<FrontMatter>(&frontmatter).map_err(|e| {
                anyhow!("TOML front matter in {path:?}: {}", e.message())
            })?;

        if !toml.enable {
            /*
             * Skip job files that have been marked as disabled.
             */
            return Ok(None);
        }

        /*
         * Rule out some common mispellings of "enable", before we get
         * all the way into variety processing:
         */
        if toml.extra.get("enabled").is_some()
            || toml.extra.get("disable").is_some()
            || toml.extra.get("disabled").is_some()
        {
            bail!(
                "TOML front matter in {path:?}: \
                use \"enable\" to disable a job"
            );
        }

        Ok(Some(JobFile {
            path: path.to_string(),
            name: toml.name.to_string(),
            variety: toml.variety,
            /*
             * The use of the flattened "extra" member here is critical, as it
             * allows varieties to use "deny_unknown_fields" on their subset of
             * the frontmatter because we have subtracted the global parts here.
             */
            config: serde_json::to_value(&toml.extra)?,
            content: content.to_string(),
            dependencies: toml
                .dependencies
                .iter()
                .map(|(name, dep)| {
                    Ok((
                        name.to_string(),
                        JobFileDepend {
                            job: dep.job.to_string(),
                            config: serde_json::to_value(&dep.extra)?,
                        },
                    ))
                })
                .collect::<Result<_>>()?,
        }))
    }
}

/**
 * Some repositories define more than one job to run on each commit.  This
 * object assembles a set of job related jobs for a particular commit, ensuring
 * that each job has a unique name and that any inter-job dependencies are
 * correctly specified without dangling references or cycles.
 */
pub struct JobFileSet {
    jobfiles: Vec<JobFile>,
    max_job_count: usize,
}

impl JobFileSet {
    pub fn new(max_job_count: usize) -> Self {
        Self { jobfiles: Default::default(), max_job_count }
    }

    pub fn load(&mut self, content: &str, path: &str) -> Result<()> {
        let max = self.max_job_count;

        /*
         * Currently we know how to parse a very specific shell script with TOML
         * front matter in a specially formatted comment within the file.
         */
        if path.ends_with(".sh") {
            let Some(jf) = JobFile::parse_content_at_path(content, path)?
            else {
                /*
                 * Skip job files that have been marked as disabled.
                 */
                return Ok(());
            };

            if self.jobfiles.len() > max {
                bail!("too many job files; you can have at most {max}");
            }

            self.jobfiles.push(jf);
        } else {
            bail!("unexpected item in bagging area: {}", path);
        }

        Ok(())
    }

    pub fn complete(&self) -> Result<Vec<JobFile>> {
        /*
         * Check that the name of each job is unique, and that the job variety
         * is one that is allowed for this type of file.
         */
        let mut names = HashSet::new();
        for job in self.jobfiles.iter() {
            if !names.insert(job.name.to_string()) {
                bail!("job name {:?} is used in more than one file", job.name);
            }

            if !matches!(job.variety, Variety::Basic) {
                bail!("unexpected job variety");
            }
        }

        /*
         * We need to check each job for dependencies.  If a job has
         * dependencies, each entry must be well-formed: it must refer to
         * another job in the set by name, and it must not create a dependency
         * cycle.
         */
        for job in self.jobfiles.iter() {
            if !matches!(job.variety, Variety::Basic) {
                bail!("job variety does not support dependencies");
            }

            fn visit(
                topjob: &JobFile,
                jobfiles: &Vec<JobFile>,
                thisjob: &JobFile,
                seen: &mut HashSet<String>,
            ) -> Result<()> {
                if !seen.insert(thisjob.name.to_string()) {
                    if thisjob.name == topjob.name {
                        /*
                         * If we find our way back to the original job file that
                         * we were looking at, there is definitely a cycle.
                         */
                        bail!(
                            "job file {:?} creates a dependency cycle ({:?})",
                            topjob.path,
                            seen,
                        );
                    } else {
                        /*
                         * Otherwise, there might be a cycle or there might
                         * just be a job that appears more than once in the
                         * dependency graph; e.g.,
                         *
                         *      first <---- second-a
                         *       ^             ^
                         *       |             |
                         *       `--------- second-b
                         *
                         * Here, "second-b" depends on "second-a" and also on
                         * "first".  We will visit the "first" node twice as we
                         * flood outward from "second-b", but there is no cycle.
                         * To avoid accidentally looping forever, we only look
                         * at each job once; if there is a real cycle it will be
                         * detected when we start the walk from a job that
                         * depends eventually on itself.
                         */
                        return Ok(());
                    }
                }

                for dep in thisjob.dependencies.values() {
                    if let Some(depjob) =
                        jobfiles.iter().find(|j| j.name == dep.job)
                    {
                        visit(topjob, jobfiles, depjob, seen)?;
                    } else {
                        bail!(
                            "job file {:?} depends on job {:?} that is not \
                            present in the plan",
                            topjob.path,
                            dep.job,
                        );
                    }
                }

                Ok(())
            }

            let mut seen = HashSet::new();
            visit(job, &self.jobfiles, job, &mut seen)?;
        }

        let mut jobfiles = self.jobfiles.clone();
        jobfiles.sort_by(|a, b| a.name.cmp(&b.name));

        Ok(jobfiles)
    }
}
