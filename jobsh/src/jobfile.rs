/*
 * Copyright 2024 Oxide Computer Company
 */

use std::collections::{HashMap, HashSet};

use anyhow::{anyhow, bail, Result};
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

#[cfg(test)]
mod test {
    use super::*;

    fn make_job(name: &str) -> String {
        format!(
            "#!/bin/bash\n\
            #: name = \"{name}\"\n\
            #: variety = \"basic\"\n\
            echo hello\n"
        )
    }

    fn make_job_with_dep(name: &str, dep_name: &str, dep_job: &str) -> String {
        format!(
            "#!/bin/bash\n\
            #: name = \"{name}\"\n\
            #: variety = \"basic\"\n\
            #: [dependencies.{dep_name}]\n\
            #: job = \"{dep_job}\"\n\
            echo hello\n"
        )
    }

    #[test]
    fn jobfileset_single_job() {
        let mut jfs = JobFileSet::new(10);
        jfs.load(&make_job("test-job"), "jobs/test.sh").unwrap();
        let jobs = jfs.complete().unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].name, "test-job");
    }

    #[test]
    fn jobfileset_multiple_jobs() {
        let mut jfs = JobFileSet::new(10);
        jfs.load(&make_job("job-a"), "jobs/a.sh").unwrap();
        jfs.load(&make_job("job-b"), "jobs/b.sh").unwrap();
        jfs.load(&make_job("job-c"), "jobs/c.sh").unwrap();
        let jobs = jfs.complete().unwrap();
        assert_eq!(jobs.len(), 3);
        // Jobs should be sorted by name
        assert_eq!(jobs[0].name, "job-a");
        assert_eq!(jobs[1].name, "job-b");
        assert_eq!(jobs[2].name, "job-c");
    }

    #[test]
    fn jobfileset_duplicate_names() {
        let mut jfs = JobFileSet::new(10);
        jfs.load(&make_job("same-name"), "jobs/a.sh").unwrap();
        jfs.load(&make_job("same-name"), "jobs/b.sh").unwrap();
        let err = jfs.complete().unwrap_err();
        assert!(
            err.to_string().contains("used in more than one file"),
            "expected duplicate name error, got: {}",
            err
        );
    }

    #[test]
    fn jobfileset_valid_dependency() {
        let mut jfs = JobFileSet::new(10);
        jfs.load(&make_job("first-job"), "jobs/first.sh").unwrap();
        jfs.load(
            &make_job_with_dep("second-job", "needs", "first-job"),
            "jobs/second.sh",
        )
        .unwrap();
        let jobs = jfs.complete().unwrap();
        assert_eq!(jobs.len(), 2);
    }

    #[test]
    fn jobfileset_missing_dependency() {
        let mut jfs = JobFileSet::new(10);
        jfs.load(
            &make_job_with_dep("my-job", "needs", "nonexistent-job"),
            "jobs/my.sh",
        )
        .unwrap();
        let err = jfs.complete().unwrap_err();
        assert!(
            err.to_string().contains("not present in the plan"),
            "expected missing dependency error, got: {}",
            err
        );
    }

    #[test]
    fn jobfileset_self_dependency_cycle() {
        // A job that depends on itself
        let content = "\
            #!/bin/bash\n\
            #: name = \"self-ref\"\n\
            #: variety = \"basic\"\n\
            #: [dependencies.me]\n\
            #: job = \"self-ref\"\n\
            echo hello\n";
        let mut jfs = JobFileSet::new(10);
        jfs.load(content, "jobs/self.sh").unwrap();
        let err = jfs.complete().unwrap_err();
        assert!(
            err.to_string().contains("dependency cycle"),
            "expected cycle error, got: {}",
            err
        );
    }

    #[test]
    fn jobfileset_two_job_cycle() {
        // A -> B -> A
        let job_a = "\
            #!/bin/bash\n\
            #: name = \"job-a\"\n\
            #: variety = \"basic\"\n\
            #: [dependencies.needs_b]\n\
            #: job = \"job-b\"\n\
            echo a\n";
        let job_b = "\
            #!/bin/bash\n\
            #: name = \"job-b\"\n\
            #: variety = \"basic\"\n\
            #: [dependencies.needs_a]\n\
            #: job = \"job-a\"\n\
            echo b\n";
        let mut jfs = JobFileSet::new(10);
        jfs.load(job_a, "jobs/a.sh").unwrap();
        jfs.load(job_b, "jobs/b.sh").unwrap();
        let err = jfs.complete().unwrap_err();
        assert!(
            err.to_string().contains("dependency cycle"),
            "expected cycle error, got: {}",
            err
        );
    }

    #[test]
    fn jobfileset_diamond_dependency_ok() {
        // Diamond: D depends on B and C, both depend on A
        // This is NOT a cycle and should succeed
        let job_a = make_job("job-a");
        let job_b = make_job_with_dep("job-b", "needs", "job-a");
        let job_c = make_job_with_dep("job-c", "needs", "job-a");
        let job_d = "\
            #!/bin/bash\n\
            #: name = \"job-d\"\n\
            #: variety = \"basic\"\n\
            #: [dependencies.needs_b]\n\
            #: job = \"job-b\"\n\
            #: [dependencies.needs_c]\n\
            #: job = \"job-c\"\n\
            echo d\n";

        let mut jfs = JobFileSet::new(10);
        jfs.load(&job_a, "jobs/a.sh").unwrap();
        jfs.load(&job_b, "jobs/b.sh").unwrap();
        jfs.load(&job_c, "jobs/c.sh").unwrap();
        jfs.load(job_d, "jobs/d.sh").unwrap();
        let jobs = jfs.complete().unwrap();
        assert_eq!(jobs.len(), 4);
    }

    #[test]
    fn jobfileset_non_sh_extension() {
        let mut jfs = JobFileSet::new(10);
        let err = jfs.load(&make_job("test"), "jobs/test.txt").unwrap_err();
        assert!(
            err.to_string().contains("unexpected item"),
            "expected extension error, got: {}",
            err
        );
    }

    #[test]
    fn jobfileset_too_many_jobs() {
        let mut jfs = JobFileSet::new(2);
        jfs.load(&make_job("job-1"), "jobs/1.sh").unwrap();
        jfs.load(&make_job("job-2"), "jobs/2.sh").unwrap();
        jfs.load(&make_job("job-3"), "jobs/3.sh").unwrap();
        let err = jfs.load(&make_job("job-4"), "jobs/4.sh").unwrap_err();
        assert!(
            err.to_string().contains("too many job files"),
            "expected too many jobs error, got: {}",
            err
        );
    }

    #[test]
    fn jobfile_missing_shebang() {
        let content = "\
            #: name = \"test\"\n\
            #: variety = \"basic\"\n\
            echo hello\n";
        let err = JobFile::parse_content_at_path(content, "test.sh").unwrap_err();
        assert!(
            err.to_string().contains("interpreter line"),
            "expected shebang error, got: {}",
            err
        );
    }

    #[test]
    fn jobfile_misspelled_enable_enabled() {
        let content = "\
            #!/bin/bash\n\
            #: name = \"test\"\n\
            #: variety = \"basic\"\n\
            #: enabled = true\n\
            echo hello\n";
        let err = JobFile::parse_content_at_path(content, "test.sh").unwrap_err();
        assert!(
            err.to_string().contains("use \"enable\""),
            "expected misspelling error, got: {}",
            err
        );
    }

    #[test]
    fn jobfile_misspelled_enable_disabled() {
        let content = "\
            #!/bin/bash\n\
            #: name = \"test\"\n\
            #: variety = \"basic\"\n\
            #: disabled = true\n\
            echo hello\n";
        let err = JobFile::parse_content_at_path(content, "test.sh").unwrap_err();
        assert!(
            err.to_string().contains("use \"enable\""),
            "expected misspelling error, got: {}",
            err
        );
    }

    #[test]
    fn jobfile_with_dependencies() {
        let content = "\
            #!/bin/bash\n\
            #: name = \"dependent-job\"\n\
            #: variety = \"basic\"\n\
            #: [dependencies.build]\n\
            #: job = \"build-job\"\n\
            #: [dependencies.test]\n\
            #: job = \"test-job\"\n\
            echo hello\n";
        let jf = JobFile::parse_content_at_path(content, "test.sh")
            .unwrap()
            .unwrap();
        assert_eq!(jf.dependencies.len(), 2);
        assert_eq!(jf.dependencies.get("build").unwrap().job, "build-job");
        assert_eq!(jf.dependencies.get("test").unwrap().job, "test-job");
    }

    #[test]
    fn jobfile_empty_content() {
        // Empty file has no lines, so lines.next() returns None and
        // TOML parsing fails due to missing required fields
        let content = "";
        let err = JobFile::parse_content_at_path(content, "test.sh").unwrap_err();
        assert!(
            err.to_string().contains("TOML front matter"),
            "expected TOML parsing error for empty file, got: {}",
            err
        );
    }

    #[test]
    fn jobfile_only_shebang() {
        // Just a shebang with no frontmatter should fail TOML parsing
        let content = "#!/bin/bash\n";
        let err = JobFile::parse_content_at_path(content, "test.sh").unwrap_err();
        assert!(
            err.to_string().contains("TOML front matter"),
            "expected TOML parsing error, got: {}",
            err
        );
    }
}
