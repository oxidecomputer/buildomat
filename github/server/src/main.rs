/*
 * Copyright 2025 Oxide Computer Company
 */

#![allow(clippy::vec_init_then_push)]

use anyhow::{anyhow, bail, Context, Result};
use base64::Engine;
use buildomat_common::*;
use buildomat_github_client::types::{
    ActionsListJobsWorkflowRunFilter, ChecksCreateRequestActions,
    ChecksCreateSuiteRequest, JobStatus,
};
use buildomat_github_database::types::*;
use buildomat_github_hooktypes as hooktypes;
use buildomat_jobsh::jobfile::JobFileSet;
use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use slog::{debug, error, info, o, trace, warn, Logger};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use variety::control::{ControlPrivate, CONTROL_RUN_NAME};

mod config;
mod http;
mod templates;
mod variety;

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct RepoConfig {
    /**
     * Repository-level control for enabling or disabling all buildomat
     * activity.
     */
    pub enable: bool,

    /**
     * Should we require that users submitting jobs be members of the
     * organisation that owns the repository?  Users outside that organisation
     * will require approval from a user inside the organisation.
     *
     * If not specified, this value defaults to "true"; i.e., users outside the
     * organisation require approval by default.
     */
    #[serde(default = "true_if_missing")]
    pub org_only: bool,

    /**
     * A list of login names for users that should be authorised for jobs on
     * this repository, even if they are not members of the organisation.
     */
    #[serde(default)]
    pub allow_users: Vec<String>,
}

struct LoadedFromSha<T> {
    sha: String,
    loaded: T,
}

struct App {
    log: Logger,
    db: buildomat_github_database::Database,
    config: config::Config,
    jwt: buildomat_github_client::JWTCredentials,
    templates: templates::Templates,
}

impl App {
    fn make_url(&self, path: &str) -> String {
        format!("{}/{}", self.config.base_url, path)
    }

    fn make_details_url(&self, cs: &CheckSuite, cr: &CheckRun) -> String {
        self.make_url(&format!("details/{}/{}/{}", cs.id, cs.url_key, cr.id))
    }

    fn app_client(&self) -> buildomat_github_client::Client {
        buildomat_github_client::app_client(self.jwt.clone()).unwrap()
    }

    fn install_client(
        &self,
        install_id: i64,
    ) -> buildomat_github_client::Client {
        buildomat_github_client::install_client(self.jwt.clone(), install_id)
            .unwrap()
    }

    async fn temp_access_token(
        &self,
        install_id: i64,
        repo: &Repository,
        extra_repos: Option<&Vec<i64>>,
    ) -> Result<String> {
        use buildomat_github_client::types::{
            AppPermissions, AppsCreateInstallationAccessTokenRequest, Pages,
        };

        let gh = self.install_client(install_id);

        let mut ids = vec![repo.id];
        if let Some(repos) = extra_repos {
            for id in repos {
                if !ids.contains(id) {
                    ids.push(*id);
                }
            }
        }

        let permissions = Some(AppPermissions {
            contents: Some(Pages::Read),
            ..Default::default()
        });

        let body = AppsCreateInstallationAccessTokenRequest {
            permissions,
            repository_ids: ids,
            ..Default::default()
        };

        let t = gh
            .apps()
            .create_installation_access_token(install_id, &body)
            .await?;

        Ok(t.token)
    }

    async fn load_file(
        &self,
        gh: &buildomat_github_client::Client,
        repo: &Repository,
        sha: &str,
        path: &str,
    ) -> Result<Option<String>> {
        let f = gh
            .repos()
            .get_content_file(&repo.owner, &repo.name, path, sha)
            .await;

        match f {
            Ok(f) => {
                if f.encoding != "base64" {
                    bail!("encoding {} is not base64", f.encoding);
                }

                let encoded = f.content.trim().replace('\n', "");
                let ctx = || anyhow!("content: {:?}", &encoded);
                Ok(Some(
                    String::from_utf8(
                        base64::engine::general_purpose::STANDARD
                            .decode(&encoded)
                            .with_context(ctx)?,
                    )
                    .with_context(ctx)?,
                ))
            }
            Err(e) => {
                if e.to_string().contains("404 Not Found") {
                    /*
                     * XXX Need better error types from octorust, but for now
                     * let us assume this means the file is not in the
                     * repository.
                     */
                    return Ok(None);
                }

                bail!("could not load \"{}\" from {}: {:?}", path, sha, e);
            }
        }
    }

    async fn load_repo_job_files(
        &self,
        gh: &buildomat_github_client::Client,
        cs: &CheckSuite,
        repo: &Repository,
    ) -> Result<LoadedFromSha<Plan>> {
        /*
         * List the jobs directory in the commit under test.
         */
        let path = format!("{}/jobs", self.config.confroot);
        let entries = match gh
            .repos()
            .get_content_vec_entries(
                &repo.owner,
                &repo.name,
                &path,
                &cs.head_sha,
            )
            .await
        {
            Ok(entries) => entries,
            Err(e) => {
                if e.to_string().contains("404 Not Found") {
                    /*
                     * XXX Need better error types from octorust, but for now
                     * let us assume this means the directory does not exist
                     * within the repository.
                     */
                    return Ok(LoadedFromSha {
                        sha: cs.head_sha.to_string(),
                        loaded: Plan { jobfiles: Vec::new() },
                    });
                }

                bail!(
                    "could not load {:?} from commit {} in {}/{}",
                    path,
                    cs.head_sha,
                    repo.owner,
                    repo.name
                );
            }
        };

        let mut jobfileset = JobFileSet::new(32);

        for ent in entries {
            let f = self
                .load_file(gh, repo, &cs.head_sha, &ent.path)
                .await
                .with_context(|| {
                    anyhow!("loading {:?} from repository", &ent.path)
                })?
                .ok_or_else(|| {
                    anyhow!("{:?} missing from repository?!", &ent.path)
                })?;

            jobfileset.load(&f, &ent.path)?;
        }

        Ok(LoadedFromSha {
            sha: cs.head_sha.to_string(),
            loaded: Plan {
                jobfiles: jobfileset
                    .complete()?
                    .into_iter()
                    .map(JobFile::from)
                    .collect(),
            },
        })
    }

    async fn load_repo_config(
        &self,
        cs: &CheckSuite,
        repo: &Repository,
    ) -> Result<LoadedFromSha<RepoConfig>> {
        let gh = self.install_client(cs.install);

        /*
         * Determine the head of the default branch.
         */
        let commits = gh
            .repos()
            .list_commits(&repo.owner, &repo.name, "", "", "", None, None, 1, 0)
            .await?;
        if commits.len() != 1 {
            bail!("could not get head of default branch");
        }
        let sha = commits[0].sha.to_string();

        /*
         * Load the top-level configuration file from the default branch of the
         * repository and decode the response.
         */
        let path = format!("{}/config.toml", self.config.confroot);
        if let Some(f) = self.load_file(&gh, repo, &sha, &path).await? {
            let loaded: RepoConfig = toml::from_str(&f)
                .with_context(|| anyhow!("content: {:?}", &f))?;

            Ok(LoadedFromSha { sha, loaded })
        } else {
            /*
             * Treat the absence of the file as if checks were disabled.
             */
            Ok(LoadedFromSha { sha, loaded: RepoConfig::default() })
        }
    }

    fn buildomat_username(&self, repo: &Repository) -> String {
        /*
         * Use a separate buildomat user for each GitHub repository.  These are
         * created on demand, and we access them via delegation rather than
         * using real per-user credentials.  In this way, we can provide access
         * to expensive or security sensitive targets (e.g., hardware lab
         * resources) on a per-repository basis, while leaving low-cost or
         * unprivileged targets (e.g., ephemeral AWS instances) for public use.
         *
         * We use the GitHub repository ID to construct the buildomat username
         * in the hope that this will remain invariant across future changes in
         * the name and organisational ownership of the repository.
         */
        format!("gong-{}", repo.id)
    }

    fn buildomat(&self, repo: &Repository) -> buildomat_client::Client {
        buildomat_client::ClientBuilder::new(&self.config.buildomat.url)
            .bearer_token(&self.config.buildomat.token)
            .delegated_user(&self.buildomat_username(repo))
            .build()
            .unwrap()
    }

    fn buildomat_admin(&self) -> buildomat_client::Client {
        /*
         * The buildomat admin client is used for the overall status page.  It
         * uses the configured service user without switching to a
         * per-repository user.
         */
        buildomat_client::ClientBuilder::new(&self.config.buildomat.url)
            .bearer_token(&self.config.buildomat.token)
            .build()
            .unwrap()
    }

    fn buildomat_raw(&self) -> Result<(reqwest::Client, String)> {
        Ok((
            reqwest::ClientBuilder::new()
                .timeout(Duration::from_secs(3600))
                .tcp_keepalive(Duration::from_secs(60))
                .connect_timeout(Duration::from_secs(15))
                .build()?,
            self.config.buildomat.url.to_string(),
        ))
    }
}

async fn process_deliveries(app: &Arc<App>) -> Result<()> {
    let log = &app.log;

    /*
     * Bump this version if we changed what it means to have acked an event.
     * e.g., if we decide we need to backfill because of a bug in handling
     * check suite requests, we could reset all deliveries of event
     * "check_suite" with ack < 2.
     */
    let ack = 1;

    /*
     * Convert web hook deliveries into records we can process.
     */
    for del in app.db.list_deliveries_unacked()? {
        use hooktypes::Payload;

        if del.event == "ping" {
            /*
             * This initial event does not contain all the fields we would like,
             * so just skip it for now.
             */
            info!(log, "delivery {} is a {:?}; ignoring", del.seq, del.event);
            app.db.delivery_ack(del.seq, ack)?;
            continue;
        }

        let payload = match serde_json::from_value::<Payload>(del.payload.0) {
            Ok(payload) => {
                info!(
                    log,
                    "loaded delivery {} event {} action {}",
                    del.seq,
                    del.event,
                    payload.action
                );
                payload
            }
            Err(e) => {
                error!(log, "delivery {} event {}: {}", del.seq, del.event, e);
                continue;
            }
        };

        /*
         * Cache user information from the event sender in the database.
         */
        app.db.store_user(
            payload.sender.id,
            &payload.sender.login,
            UserType::from_github(payload.sender.type_),
            payload.sender.name.as_deref(),
            payload.sender.email.as_deref(),
        )?;

        match del.event.as_str() {
            "installation" if &payload.action == "created" => {
                let inst = if let Some(inst) = &payload.installation {
                    inst
                } else {
                    error!(
                        log,
                        "delivery {} missing installation information", del.seq
                    );
                    app.db.delivery_ack(del.seq, ack)?;
                    continue;
                };

                let owner = if let Some(acc) = &inst.account {
                    acc
                } else {
                    error!(
                        log,
                        "delivery {} missing installation account", del.seq
                    );
                    app.db.delivery_ack(del.seq, ack)?;
                    continue;
                };

                /*
                 * Store the user that owns the installation and then the
                 * installation itself:
                 */
                app.db.store_user(
                    owner.id,
                    &owner.login,
                    UserType::from_github(owner.type_),
                    owner.name.as_deref(),
                    owner.email.as_deref(),
                )?;
                app.db.store_install(inst.id, owner.id)?;
                app.db.delivery_ack(del.seq, ack)?;
                continue;
            }
            "installation" | "installation_repositories" => {
                /*
                 * XXX Skip this for right now.  Probably need to keep track of
                 * installations in the database.
                 * e.g., "created" and "new_permissions_accepted" actions.
                 */
                app.db.delivery_ack(del.seq, ack)?;
                continue;
            }
            "repository" if &payload.action == "deleted" => {
                /*
                 * Ignore deleted repositories.
                 */
                app.db.delivery_ack(del.seq, ack)?;
                continue;
            }
            "repository" => {
                if let Some(repo) = &payload.repository {
                    /*
                     * Update our cache of ID to repository owner/name mapping.
                     */
                    app.db.store_repository(
                        repo.id,
                        &repo.owner.login,
                        &repo.name,
                    )?;
                };
                app.db.delivery_ack(del.seq, ack)?;
                continue;
            }
            "pull_request"
                if &payload.action == "synchronize"
                    || &payload.action == "opened" =>
            {
                /*
                 * We want to create check suites for pull requests that come in
                 * to our repositories.  There is at most one check suite per
                 * commit per repository.  By listening for both "opened" and
                 * "synchronize" events, we should hear each time the head
                 * commit for a pull request changes.
                 */
                let repo = if let Some(repo) = &payload.repository {
                    if !app.config.allow_owners.contains(&repo.owner.login) {
                        warn!(
                            log,
                            "delivery {} from outsider: {:?}",
                            del.seq,
                            repo.owner.login
                        );
                        app.db.delivery_ack(del.seq, ack)?;
                        continue;
                    }

                    app.db.store_repository(
                        repo.id,
                        &repo.owner.login,
                        &repo.name,
                    )?;
                    repo
                } else {
                    error!(
                        log,
                        "delivery {} missing repository information", del.seq
                    );
                    continue;
                };

                let instid = if let Some(inst) = &payload.installation {
                    inst.id
                } else {
                    error!(log, "delivery {} missing install ID", del.seq);
                    continue;
                };

                let pr = match payload.pull_request() {
                    Ok(pr) => pr,
                    Err(e) => {
                        error!(log, "delivery {}: {e}", del.seq);
                        app.db.delivery_ack(del.seq, ack)?;
                        continue;
                    }
                };

                info!(
                    log,
                    "del {}: pull request from {}/{} against {}/{}",
                    del.seq,
                    &pr.head.repo().owner.login,
                    &pr.head.repo().name,
                    &pr.base.repo().owner.login,
                    &pr.base.repo().name
                );

                if pr.base.repo().id != repo.id {
                    warn!(
                        log,
                        "delivery {}: base repo {} != hook repo {}",
                        del.seq,
                        pr.base.repo().id,
                        repo.id,
                    );
                    app.db.delivery_ack(del.seq, ack)?;
                    continue;
                }

                /*
                 * Even though we do not technically need it, it may assist
                 * with debugging to store the mapping from ID to owner/name
                 * for the foreign repository.
                 */
                app.db.store_repository(
                    pr.head.repo().id,
                    &pr.head.repo().owner.login,
                    &pr.head.repo().name,
                )?;

                let gh = app.install_client(instid);

                /*
                 * First, check to see if we already created the suite for this
                 * commit.
                 * XXX Pagination.
                 */
                let suites = gh
                    .checks()
                    .list_suites_for_ref(
                        &pr.base.repo().owner.login,
                        &pr.base.repo().name,
                        &pr.head.sha,
                        app.config.id as i64,
                        "",
                        100,
                        0,
                    )
                    .await?;

                if suites.check_suites.len() > 1 {
                    warn!(
                        log,
                        "found {} checksuites for commit {}",
                        suites.check_suites.len(),
                        pr.head.sha,
                    );
                    app.db.delivery_ack(del.seq, ack)?;
                    continue;
                }

                let suite_id = if let Some(suite) = suites.check_suites.first()
                {
                    info!(
                        log,
                        "delivery {}: found check suite {} for {}",
                        del.seq,
                        suite.id,
                        pr.head.sha,
                    );
                    suite.id
                } else {
                    info!(
                        log,
                        "delivery {}: creating check suite for {}",
                        del.seq,
                        pr.head.sha,
                    );

                    let res = gh
                        .checks()
                        .create_suite(
                            &pr.base.repo().owner.login,
                            &pr.base.repo().name,
                            &ChecksCreateSuiteRequest {
                                head_sha: pr.head.sha.to_string(),
                            },
                        )
                        .await?;

                    info!(
                        log,
                        "delivery {}: check suite {} created for {}",
                        del.seq,
                        res.id,
                        pr.head.sha,
                    );
                    res.id
                };

                /*
                 * Make sure we have a local record of the check suite we found
                 * or created.
                 */
                let mut cs = app.db.ensure_check_suite(
                    pr.base.repo().id,
                    instid,
                    suite_id,
                    &pr.head.sha,
                    /*
                     * XXX What does actions do about branch names?
                     */
                    None,
                )?;

                /*
                 * Record the user who triggered this pull request event so that
                 * we can do authorisation checks later.
                 */
                if cs.pr_by.is_none() {
                    cs.pr_by = Some(payload.sender.id);
                    app.db.update_check_suite(&cs)?;
                }

                info!(
                    log,
                    "delivery {}: check suite {} -> {}",
                    del.seq,
                    suite_id,
                    cs.id,
                );

                app.db.delivery_ack(del.seq, ack)?;
                continue;
            }
            "push" | "pull_request" | "create" | "delete" | "public" => {
                /*
                 * For now, we don't process these events specifically.
                 */
                app.db.delivery_ack(del.seq, ack)?;
                continue;
            }
            "check_run" if &payload.action == "requested_action" => {
                let actid = if let Some(ra) = &payload.requested_action {
                    if ra.identifier != "auth"
                        && ra.identifier != "cancel"
                        && ra.identifier != "cancel_all"
                    {
                        /*
                         * Authorisation and cancellation are the only actions
                         * we know how to do for now.
                         */
                        error!(
                            log,
                            "delivery {} check run action {:?} unexpected",
                            del.seq,
                            ra.identifier,
                        );
                        app.db.delivery_ack(del.seq, ack)?;
                        continue;
                    }
                    ra.identifier.to_string()
                } else {
                    error!(
                        log,
                        "delivery {} missing requested action", del.seq,
                    );
                    continue;
                };

                let crid = if let Some(cr) = &payload.check_run {
                    if let Ok(id) = cr.external_id.parse() {
                        id
                    } else {
                        error!(
                            log,
                            "delivery {} invalid check run ID", del.seq
                        );
                        continue;
                    }
                } else {
                    error!(log, "delivery {} missing check run", del.seq);
                    continue;
                };

                let mut cr = if let Ok(cr) = app.db.load_check_run(crid) {
                    cr
                } else {
                    error!(
                        log,
                        "delivery {} could not load check run", del.seq
                    );
                    continue;
                };
                let mut cs = app.db.load_check_suite(cr.check_suite)?;

                match cr.variety {
                    CheckRunVariety::Control => {
                        /*
                         * The "Authorise" button is the only action that is
                         * valid for the Control check run.
                         */
                        if actid != "auth" {
                            warn!(
                                log,
                                "delivery {} for {:?} on control check run",
                                del.seq,
                                actid,
                            );
                            app.db.delivery_ack(del.seq, ack)?;
                            continue;
                        }
                    }
                    CheckRunVariety::AlwaysPass
                    | CheckRunVariety::FailFirst => {
                        warn!(
                            log,
                            "delivery {} for {:?} unsupported", del.seq, actid,
                        );
                        app.db.delivery_ack(del.seq, ack)?;
                        continue;
                    }
                    CheckRunVariety::Basic => {
                        match actid.as_str() {
                            "cancel" => {
                                /*
                                 * Cancel any work that has been queued but not
                                 * yet performed:
                                 */
                                variety::basic::cancel(app, &cs, &mut cr)
                                    .await?;
                            }
                            "cancel_all" => {
                                /*
                                 * Cancel any work that has been queued but not
                                 * yet performed for all basic variety check
                                 * runs in this suite:
                                 */
                                for mut cr in
                                    app.db.list_check_runs_for_suite(cs.id)?
                                {
                                    if !cr.active
                                        || !matches!(
                                            cr.variety,
                                            CheckRunVariety::Basic
                                        )
                                    {
                                        continue;
                                    }

                                    variety::basic::cancel(app, &cs, &mut cr)
                                        .await?;
                                }
                            }
                            other => {
                                warn!(
                                    log,
                                    "delivery {} for {:?} on basic check run",
                                    del.seq,
                                    other,
                                );
                                app.db.delivery_ack(del.seq, ack)?;
                                continue;
                            }
                        }

                        app.db.delivery_ack(del.seq, ack)?;
                        continue;
                    }
                }

                /*
                 * Determine whether this check suite still requires
                 * authorisation.
                 */
                if cs.approved_by.is_some() {
                    info!(
                        log,
                        "delivery {} was for check suite already authorised",
                        del.seq
                    );
                    app.db.delivery_ack(del.seq, ack)?;
                    continue;
                }
                if !cs.state.is_parked() {
                    warn!(
                        log,
                        "delivery {} for check suite not parked", del.seq
                    );
                    app.db.delivery_ack(del.seq, ack)?;
                    continue;
                }

                /*
                 * The sender of the webhook should be the user who pressed the
                 * "Authorise" button on the check run.  Determine if they are a
                 * member of organisation that owns this installation.
                 */
                let u = app.db.load_user(payload.sender.id)?;
                let inst = app.db.load_install(cs.install)?;
                let org = app.db.load_user(inst.owner)?;

                /*
                 * The GitHub organisational membership check regrettably (and
                 * thus predictably) does not work when applied to a repository
                 * that is not an organisation, even though it would make the
                 * user model attractively orthogonal for a user to be a member
                 * of themselves.  Check first if the authorising user and the
                 * installation owner are the same:
                 */
                if org.id == u.id && org.login == u.login {
                    info!(
                        log,
                        "delivery {} authorisation is OK (owner)", del.seq
                    );
                } else {
                    let gh = app.install_client(inst.id);

                    let res = gh
                        .orgs()
                        .check_membership_for_user(&org.login, &u.login)
                        .await;
                    if res.is_ok() {
                        info!(log, "delivery {} authorisation is OK", del.seq);
                    } else {
                        warn!(
                            log,
                            "delivery {} authorisation failure", del.seq
                        );
                        app.db.delivery_ack(del.seq, ack)?;
                        continue;
                    }
                }

                /*
                 * Mark the check suite as authorised by this user and send it
                 * back through the creation phase.
                 */
                cr.active = false;
                app.db.update_check_run(&cr)?;
                assert!(cs.approved_by.is_none());
                cs.approved_by = Some(u.id);
                assert!(matches!(cs.state, CheckSuiteState::Parked));
                cs.state = CheckSuiteState::Created;
                app.db.update_check_suite(&cs)?;

                info!(
                    log,
                    "delivery {} authorised suite {} by user {}",
                    del.seq,
                    cs.id,
                    u.login
                );

                app.db.delivery_ack(del.seq, ack)?;
                continue;
            }
            "check_run" if &payload.action == "rerequested" => {
                /*
                 * XXX A re-run of a failed check as requested.
                 */
                let crid = if let Some(cr) = &payload.check_run {
                    if let Ok(id) = cr.external_id.parse() {
                        id
                    } else {
                        error!(
                            log,
                            "delivery {} invalid check run ID", del.seq
                        );
                        continue;
                    }
                } else {
                    error!(log, "delivery {} missing check run", del.seq);
                    continue;
                };

                let mut cr = if let Ok(cr) = app.db.load_check_run(crid) {
                    cr
                } else {
                    error!(
                        log,
                        "delivery {} could not load check run", del.seq
                    );
                    continue;
                };
                let mut cs = app.db.load_check_suite(cr.check_suite)?;

                /*
                 * Mark this check run as inactive, then return the check suite
                 * to the Planned state so that it will be recreated.  Once
                 * those acts are both recorded we can acknowledge delivery.
                 */
                cr.active = false;
                app.db.update_check_run(&cr)?;
                cs.state = match cr.variety {
                    CheckRunVariety::Control => CheckSuiteState::Created,
                    _ => CheckSuiteState::Planned,
                };
                app.db.update_check_suite(&cs)?;
                info!(
                    log,
                    "re-running check {:?} for suite {}/{}",
                    cr.name,
                    cs.github_id,
                    cs.id
                );
                app.db.delivery_ack(del.seq, ack)?;
                continue;
            }
            "check_run" => {
                /*
                 * XXX I don't think we care about these events?  They seem to
                 * occur in response to requests we are making to GitHub to
                 * update check runs...
                 */
                app.db.delivery_ack(del.seq, ack)?;
                continue;
            }
            "check_suite" if &payload.action == "completed" => {
                /*
                 * XXX I don't think we care about these events?  They seem to
                 * occur in response to requests we are making to GitHub to
                 * update check runs...
                 */
                app.db.delivery_ack(del.seq, ack)?;
                continue;
            }
            "check_suite" if &payload.action == "requested" => {
                let repo = if let Some(repo) = &payload.repository {
                    if !app.config.allow_owners.contains(&repo.owner.login) {
                        warn!(
                            log,
                            "delivery {} from outsider: {:?}",
                            del.seq,
                            repo.owner.login
                        );
                        app.db.delivery_ack(del.seq, ack)?;
                        continue;
                    }

                    app.db.store_repository(
                        repo.id,
                        &repo.owner.login,
                        &repo.name,
                    )?;
                    repo
                } else {
                    error!(
                        log,
                        "delivery {} missing repository information", del.seq
                    );
                    continue;
                };

                let instid = if let Some(inst) = &payload.installation {
                    inst.id
                } else {
                    error!(log, "delivery {} missing install ID", del.seq);
                    continue;
                };

                let suite = if let Some(suite) = &payload.check_suite {
                    suite
                } else {
                    error!(log, "delivery {} missing check suite", del.seq);
                    continue;
                };

                /*
                 * GitHub will gleefully fling check suite requests at us for
                 * random other GitHub applications that happen to be working
                 * within the same repositories as us.  This can cause duplicate
                 * jobs, overlapping published files, and other confusion, so we
                 * must be very careful to correctly ignore such deliveries.
                 */
                if suite.app.id != app.config.id as i64 {
                    info!(
                        log,
                        "delivery {} from foreign GitHub Application: {:?}",
                        del.seq,
                        suite.app,
                    );
                    app.db.delivery_ack(del.seq, ack)?;
                    continue;
                }

                let mut cs = app.db.ensure_check_suite(
                    repo.id,
                    instid,
                    suite.id,
                    &suite.head_sha,
                    suite.head_branch.as_deref(),
                )?;

                /*
                 * Record the user who triggered this check suite request event
                 * so that we can do authorisation checks later.
                 */
                if cs.requested_by.is_none() {
                    cs.requested_by = Some(payload.sender.id);
                    app.db.update_check_suite(&cs)?;
                }

                app.db.delivery_ack(del.seq, ack)?;
            }
            "check_suite" if &payload.action == "rerequested" => {
                /*
                 * XXX I think we need to return the whole check suite to the
                 * CheckSuiteState::Created state, after first confirming that
                 * the request is from an appropriate party.  This would be
                 * similar to a check_run/rerequested event (see above) for the
                 * Control run.
                 *
                 * For now, though, just eat the notification:
                 */
                app.db.delivery_ack(del.seq, ack)?;
                continue;
            }
            _ => {}
        }
    }

    Ok(())
}

/**
 * Load the full set of Check Runs from the database and from GitHub.  Ensure
 * that every Run on GitHub has a local database entry and vice-versa.
 */
async fn reconcile_check_runs(app: &Arc<App>, cs: &CheckSuite) -> Result<()> {
    let log = &app.log;
    let db = &app.db;
    let repo = db.load_repository(cs.repo)?;
    let gh = app.install_client(cs.install);

    /*
     * Load the full list of Check Runs from GitHub.
     * XXX pagination
     */
    let runs = gh
        .checks()
        .list_for_suite(
            &repo.owner,
            &repo.name,
            cs.github_id,
            "",
            JobStatus::Noop,
            ActionsListJobsWorkflowRunFilter::All,
            100,
            0,
        )
        .await?;

    if runs.total_count >= 100 {
        warn!(
            log,
            "check suite {} has {} check runs; too many?",
            cs.id,
            runs.total_count
        );
    }

    let mut cancel = HashSet::new();

    /*
     * Every Check Run on GitHub should have a local database entry.
     */
    for run in runs.check_runs {
        /*
         * According to GitHub, once a job has the Completed status, we can no
         * longer update the Conclusion.
         */
        let completed = matches!(run.status, JobStatus::Completed,);

        let mut cr = match run.external_id.parse() {
            Ok(id) => db.load_check_run(id)?,
            Err(e) => {
                /*
                 * If there is no local database entry, we will cancel the Check
                 * Run on GitHub.
                 */
                warn!(
                    log,
                    "check suite {} run {} has invalid external ID {}: {:?}",
                    cs.id,
                    run.id,
                    run.external_id,
                    e,
                );
                if !completed {
                    cancel.insert(run.id);
                }
                continue;
            }
        };

        if let Some(expected) = cr.github_id {
            if run.id != expected {
                warn!(
                    log,
                    "GitHub run {} has external ID {}, but database says \
                    that run should be {} on GitHub; cancelling",
                    run.id,
                    run.external_id,
                    expected,
                );
                if !completed {
                    cancel.insert(run.id);
                }
            }
            continue;
        }

        if !cr.active {
            /*
             * If this database record has been marked inactive, then the Check
             * Run in question has been replaced by a newer version.  Cancel the
             * old version on GitHub.
             */
            if !completed {
                info!(
                    log,
                    "check suite {} run {}: cancel old run", cs.id, cr.id
                );
                cancel.insert(run.id);
            }
        }

        /*
         * Record locally the ID of this check on GitHub, as it was presumably
         * created by us in the past.
         */
        cr.github_id = Some(run.id);
        db.update_check_run(&cr)?;
    }

    /*
     * Cancel any Check Runs on GitHub that are not connected with local
     * database records.
     */
    for id in cancel {
        use buildomat_github_client::types::{
            ChecksCreateRequestConclusion::Cancelled, ChecksUpdateRequest,
            JobStatus::Completed,
        };

        info!(log, "cancelling GitHub check run {}", id);
        let body = ChecksUpdateRequest {
            conclusion: Some(Cancelled),
            status: Some(Completed),
            ..Default::default()
        };
        let res = gh.checks().update(&repo.owner, &repo.name, id, &body).await;
        if let Err(e) = res {
            warn!(log, "Could not cancel GitHub check run {}: {}", id, e);
        }
    }

    Ok(())
}

enum FlushState {
    Queued,
    Running,
    Success,
    Failure,
}

struct FlushOut {
    title: String,
    summary: String,
    detail: String,
    state: FlushState,
    actions: Vec<ChecksCreateRequestActions>,
}

async fn flush_check_runs(
    app: &Arc<App>,
    cs: &CheckSuite,
    repo: &Repository,
) -> Result<()> {
    let log = &app.log;
    let db = &app.db;
    let gh = app.install_client(cs.install);

    for mut cr in db.list_check_runs_for_suite(cs.id)? {
        if !cr.active {
            continue;
        }

        if cr.flushed {
            continue;
        }

        let out = match cr.variety {
            CheckRunVariety::Control => {
                let sha = cs.plan_sha.as_deref();
                let sha = sha.unwrap_or("<?>");
                let p: ControlPrivate = cr.get_private()?;

                let approval = if let Some(aby) = cs.approved_by {
                    let u = app.db.load_user(aby)?;
                    format!("  Plan approved by user {:?}.", u.login)
                } else {
                    "".to_string()
                };

                if let Some(e) = &p.error {
                    FlushOut {
                        title: "Failed to load plan".into(),
                        summary: e.to_string(),
                        detail: "".into(),
                        state: FlushState::Failure,
                        actions: Default::default(),
                    }
                } else if !p.complete {
                    FlushOut {
                        title: "Plan loaded, creating check runs...".into(),
                        summary: format!(
                            "Plan loaded from commit {}.{}",
                            sha, approval,
                        ),
                        detail: "".into(),
                        state: FlushState::Running,
                        actions: Default::default(),
                    }
                } else if p.need_auth {
                    FlushOut {
                        title: "Plan requires authorisation.".into(),
                        summary: "Plans submitted by users that are not a \
                            member of the organisation require explicit \
                            authorisation."
                            .into(),
                        detail: "".into(),
                        state: FlushState::Failure,
                        actions: vec![ChecksCreateRequestActions {
                            description: "Allow this plan to proceed.".into(),
                            identifier: "auth".into(),
                            label: "Authorise".into(),
                        }],
                    }
                } else if p.no_plans {
                    FlushOut {
                        title: "No job files.".into(),
                        summary: format!(
                            "Plan loaded from commit {}, but there were \
                            no job files in {}",
                            sha, app.config.confroot
                        ),
                        detail: "".into(),
                        state: FlushState::Success,
                        actions: Default::default(),
                    }
                } else {
                    FlushOut {
                        title: "Checks underway.".into(),
                        summary: format!(
                            "Plan loaded from commit {}.{}",
                            sha, approval,
                        ),
                        detail: "".into(),
                        state: FlushState::Success,
                        actions: Default::default(),
                    }
                }
            }
            CheckRunVariety::AlwaysPass => {
                let p: AlwaysPassPrivate = cr.get_private()?;
                if p.complete {
                    FlushOut {
                        title: "As always, we passed!".into(),
                        summary: "This check is not exhaustive.".into(),
                        detail: "".into(),
                        state: FlushState::Success,
                        actions: Default::default(),
                    }
                } else {
                    FlushOut {
                        title: "Going well so far...".into(),
                        summary: "This check is not exhaustive.".into(),
                        detail: "".into(),
                        state: FlushState::Running,
                        actions: Default::default(),
                    }
                }
            }
            CheckRunVariety::FailFirst => {
                let p: FailFirstPrivate = cr.get_private()?;
                if p.complete {
                    if p.failed {
                        /*
                         * XXX
                         */
                        FlushOut {
                            title: "Failed; thump to try again!".into(),
                            summary: "This check never works the first \
                                time."
                                .into(),
                            detail: "".into(),
                            state: FlushState::Failure,
                            actions: Default::default(),
                        }
                    } else {
                        FlushOut {
                            title: "Thumped; the check whirs into life!".into(),
                            summary: "This check never works the first \
                                time."
                                .into(),
                            detail: "".into(),
                            state: FlushState::Success,
                            actions: Default::default(),
                        }
                    }
                } else {
                    FlushOut {
                        title: "Hmmm....".into(),
                        summary: "This check is not exhaustive.".into(),
                        detail: "".into(),
                        state: FlushState::Running,
                        actions: Default::default(),
                    }
                }
            }
            CheckRunVariety::Basic => {
                variety::basic::flush(app, cs, &mut cr, repo).await?
            }
        };

        use buildomat_github_client::types::{
            ChecksCreateRequest,
            ChecksCreateRequestConclusion::{Failure, Success},
            ChecksCreateRequestOutput, ChecksUpdateRequest,
            ChecksUpdateRequestOutput,
            JobStatus::{Completed, InProgress, Queued},
        };

        let details_url = app.make_details_url(cs, &cr);

        let (conclusion, status) = match out.state {
            FlushState::Queued => (None, Some(Queued)),
            FlushState::Running => (None, Some(InProgress)),
            FlushState::Success => (Some(Success), Some(Completed)),
            FlushState::Failure => (Some(Failure), Some(Completed)),
        };

        if let Some(ghid) = &cr.github_id {
            /*
             * This check run exists on GitHub already, so update it.
             */
            let output = Some(ChecksUpdateRequestOutput {
                summary: out.summary,
                text: out.detail,
                title: out.title,
                ..Default::default()
            });

            let body = ChecksUpdateRequest {
                conclusion,
                details_url,
                output,
                status,
                actions: out.actions,
                ..Default::default()
            };

            gh.checks()
                .update(&repo.owner, &repo.name, *ghid, &body)
                .await
                .map_err(|e| {
                    anyhow!(
                        "updating check suite {} run {} ({}/{}): {e}",
                        cs.id,
                        cr.id,
                        repo.owner,
                        repo.name,
                    )
                })?;

            info!(log, "check suite {} run {} updated", cs.id, cr.id);
        } else {
            let output = Some(ChecksCreateRequestOutput {
                summary: out.summary,
                text: out.detail,
                title: out.title,
                ..Default::default()
            });

            let body = ChecksCreateRequest {
                conclusion,
                details_url,
                external_id: cr.id.to_string(),
                head_sha: cs.head_sha.to_string(),
                name: cr.name.to_string(),
                output,
                status,
                actions: out.actions,
                ..Default::default()
            };

            let res = gh
                .checks()
                .create(&repo.owner, &repo.name, &body)
                .await
                .map_err(|e| {
                    anyhow!(
                        "creating check suite {} run {} ({}/{}): {e}",
                        cs.id,
                        cr.id,
                        repo.owner,
                        repo.name,
                    )
                })?;

            info!(
                log,
                "check suite {} run {} created as {}", cs.id, cr.id, res.id
            );

            cr.github_id = Some(res.id);
        }

        cr.flushed = true;
        db.update_check_run(&cr)?;
    }

    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct AlwaysPassPrivate {
    #[serde(default)]
    complete: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct FailFirstPrivate {
    #[serde(default)]
    complete: bool,
    #[serde(default)]
    failed: bool,
    #[serde(default)]
    count: usize,
}

/**
 * Perform whatever actions are required to advance the state of this check run.
 * Returns true if the function should be called again, or false if this check
 * run is over.
 */
async fn check_run_run(
    app: &Arc<App>,
    cs: &CheckSuite,
    cr: &mut CheckRun,
) -> Result<bool> {
    let db = &app.db;

    Ok(match &cr.variety {
        CheckRunVariety::Control => {
            let mut p: ControlPrivate = cr.get_private()?;
            if !p.complete {
                p.complete = true;
                cr.set_private(p)?;
                cr.flushed = false;
                db.update_check_run(cr)?;
            }
            false
        }
        CheckRunVariety::AlwaysPass => {
            let mut p: AlwaysPassPrivate = cr.get_private()?;
            if !p.complete {
                p.complete = true;
                cr.set_private(p)?;
                cr.flushed = false;
                db.update_check_run(cr)?;
            }
            false
        }
        CheckRunVariety::FailFirst => {
            let mut p: FailFirstPrivate = cr.get_private()?;
            if !p.complete {
                let n = db
                    .list_check_runs_for_suite(cs.id)?
                    .iter()
                    .filter(|ocr| ocr.name == cr.name)
                    .count();
                p.count = n;
                p.failed = n < 2;
                p.complete = true;
                cr.set_private(p)?;
                cr.flushed = false;
                db.update_check_run(cr)?;
            }
            false
        }
        CheckRunVariety::Basic => variety::basic::run(app, cs, cr).await?,
    })
}

async fn process_check_suite(app: &Arc<App>, cs: CheckSuiteId) -> Result<()> {
    let log = &app.log;
    let db = &app.db;

    let mut cs = db.load_check_suite(cs)?;
    let repo = app.db.load_repository(cs.repo)?;
    let install = app.db.load_install(cs.install)?;

    match &cs.state {
        CheckSuiteState::Parked => {}
        CheckSuiteState::Complete => {}
        CheckSuiteState::Retired => {}
        CheckSuiteState::Created => {
            let gh = app.install_client(cs.install);

            /*
             * Read our top-level configuration file in the default branch of
             * the repository.  If checks are not enabled, we will do nothing
             * else.
             */
            let rc = app.load_repo_config(&cs, &repo).await?;
            if !rc.loaded.enable {
                info!(
                    log,
                    "check suite {}: ignored because in {} checks are \
                    not enabled",
                    cs.id,
                    rc.sha
                );
                cs.state = CheckSuiteState::Retired;
                db.update_check_suite(&cs)?;
                return Ok(());
            }

            /*
             * We may be configuring the check suite for the first time, or we
             * may be tearing it down to recreate it from an updated plan.  Mark
             * any non-control runs in the database as inactive to ensure they
             * get cancelled.
             */
            let mut control_name = None;
            for mut cr in db.list_check_runs_for_suite(cs.id)? {
                if cr.variety.is_control() {
                    /*
                     * If this check suite already has a control run, use the
                     * same name if we need to re-create it.
                     */
                    control_name = Some(cr.name.to_string());
                }

                if !cr.active {
                    continue;
                }

                if cr.variety.is_control() {
                    /*
                     * Any spare control runs will be cleared out, if needed, by
                     * ensure_check_run() below.
                     */
                    continue;
                }

                cr.active = false;
                cr.flushed = false;
                db.update_check_run(&cr)?;
            }

            /*
             * We may re-enter this state if the user requests that we start
             * again from an updated plan, so make sure we have an accurate
             * picture of all checks in the local database, and that all
             * outdated check run instances are cancelled.
             */
            reconcile_check_runs(app, &cs).await?;

            /*
             * We need to determine the plan for this check suite.  We do this
             * by loading job description files from the commit under test in
             * the repository.  By taking the jobs from the commit itself, one
             * can more easily test updates to the job itself.
             *
             * To allow for reporting on the status of plan creation itself
             * (e.g., if reading the file fails because the file is not
             * currently valid) we create a synthetic Check Run.  This check run
             * is updated based on our parsing of the file, and can be retried
             * by the user if they want to retry plan creation.
             */
            let control_name =
                control_name.as_deref().unwrap_or(CONTROL_RUN_NAME);
            let mut cr = db.ensure_check_run(
                cs.id,
                control_name,
                CheckRunVariety::Control,
                &Default::default(),
            )?;

            /*
             * Organisation-only authorisation can be enabled on a
             * per-repository basis.  It requires that we not create any job in
             * response to a pull request made by a user that is not a member of
             * the organisation.  Such a job will be marked as failed due to
             * lack of authorisation, with a button that an organisation member
             * may press to authorise the job.
             *
             * We need to fill in the "approved_by" field whether or not this
             * kind of authorisation check will be performed, as it is
             * subsequently used by varieties to determine if this user is
             * authorised by the organisation or not; e.g., to determine if
             * access to additional private repositories is allowed.
             */
            if cs.approved_by.is_none() {
                /*
                 * If the check suite was created by a push, we expect it to
                 * have been explicitly requested by a delivery.  Check for
                 * a requesting user first.
                 */
                if let Some(id) = cs.requested_by {
                    let u = db.load_user(id)?;
                    let iu = db.load_user(install.owner)?;
                    if u.id == iu.id && u.login == iu.login {
                        info!(
                            log,
                            "check suite {} authorised by {} (owner)",
                            cs.id,
                            u.login,
                        );
                        cs.approved_by = Some(u.id);
                    } else if rc
                        .loaded
                        .allow_users
                        .iter()
                        .any(|l| &u.login == l)
                    {
                        info!(
                            log,
                            "check suite {} authorised by {} (request, config)",
                            cs.id,
                            u.login,
                        );
                        cs.approved_by = Some(u.id);
                    } else {
                        let org = db.load_user(install.owner)?;
                        let res = gh
                            .orgs()
                            .check_membership_for_user(&org.login, &u.login)
                            .await;
                        if res.is_ok() {
                            info!(
                                log,
                                "check suite {} authorised by {} (request)",
                                cs.id,
                                u.login,
                            );
                            cs.approved_by = Some(u.id);
                        }
                    }
                }
            }
            if cs.approved_by.is_none() {
                /*
                 * Otherwise, if the check suite was created by a pull
                 * request, check whether the user that created the PR is
                 * within the organisation.
                 */
                if let Some(id) = cs.pr_by {
                    let u = db.load_user(id)?;
                    let iu = db.load_user(install.owner)?;
                    if u.id == iu.id && u.login == iu.login {
                        info!(
                            log,
                            "check suite {} authorised by {} (owner)",
                            cs.id,
                            u.login,
                        );
                        cs.approved_by = Some(u.id);
                    } else if rc
                        .loaded
                        .allow_users
                        .iter()
                        .any(|l| &u.login == l)
                    {
                        info!(
                            log,
                            "check suite {} authorised by {} (pull, config)",
                            cs.id,
                            u.login,
                        );
                        cs.approved_by = Some(u.id);
                    } else {
                        let org = db.load_user(install.owner)?;
                        let res = gh
                            .orgs()
                            .check_membership_for_user(&org.login, &u.login)
                            .await;
                        if res.is_ok() {
                            info!(
                                log,
                                "check suite {} authorised by {} (pull)",
                                cs.id,
                                u.login,
                            );
                            cs.approved_by = Some(u.id);
                        } else {
                            info!(
                                log,
                                "check suite {} by {} (pull) needs \
                                authorisation",
                                cs.id,
                                u.login,
                            );
                        }
                    }
                }
            }

            let authorised = if !rc.loaded.org_only {
                /*
                 * If organisation-only authorisation is not enabled, all jobs
                 * are implicitly authorised.
                 */
                true
            } else {
                cs.approved_by.is_some()
            };

            if authorised {
                match app.load_repo_job_files(&gh, &cs, &repo).await {
                    Ok(lp) => {
                        /*
                         * Store the new plan in the check suite record.
                         */
                        info!(
                            log,
                            "check suite {} plan from {}/{} commit {}",
                            cs.id,
                            repo.owner,
                            repo.name,
                            lp.sha
                        );

                        /*
                         * If there were no job files, be sure to make a note
                         * of this in the summary output.
                         */
                        if lp.loaded.jobfiles.is_empty() {
                            cr.flushed = false;
                            cr.set_private(ControlPrivate {
                                complete: true,
                                no_plans: true,
                                ..Default::default()
                            })?;
                            db.update_check_run(&cr)?;
                        }

                        cs.plan = Some(lp.loaded.into());
                        cs.plan_sha = Some(lp.sha);
                        cs.state = CheckSuiteState::Planned;
                    }
                    Err(e) => {
                        /*
                         * We could not load the plan for this job.  Discard any
                         * existing plan for this check suite.
                         */
                        cs.plan = None;
                        cs.plan_sha = None;

                        /*
                         * Report the failure to the user through the check run
                         * output and state.
                         */
                        cr.flushed = false;
                        cr.set_private(ControlPrivate {
                            complete: true,
                            error: Some(format!(
                                "Failed to load plan:\n```\n{:?}\n```\n",
                                e
                            )),
                            ..Default::default()
                        })?;
                        db.update_check_run(&cr)?;

                        /*
                         * Park the check suite.  If the user hits retry on the
                         * control job in the GitHub UI, it will bring us back
                         * to the Created state for another attempt.
                         */
                        cs.state = CheckSuiteState::Parked;
                    }
                }
            } else {
                /*
                 * The job is not authorised.  Discard any existing plan for
                 * this check suite.
                 */
                cs.plan = None;
                cs.plan_sha = None;

                /*
                 * Report the authorisation failure to the user through
                 * the check run output and state.
                 */
                cr.flushed = false;
                cr.set_private(ControlPrivate {
                    complete: true,
                    need_auth: true,
                    ..Default::default()
                })?;
                db.update_check_run(&cr)?;

                /*
                 * Park the check suite.  If an organisation user subsequently
                 * authorises the job, it will bring us back to the Created
                 * state for another attempt.
                 */
                cs.state = CheckSuiteState::Parked;
            }

            flush_check_runs(app, &cs, &repo).await?;

            db.update_check_suite(&cs)?;
        }
        CheckSuiteState::Planned => {
            /*
             * We may re-enter this state if the user requests that a specific
             * failed check run be retried, or if we crashed half way through
             * establishing check runs earlier.  Be sure that we have an
             * accurate picture of all existing checks in the local database.
             */
            reconcile_check_runs(app, &cs).await?;

            /*
             * XXX
             */
            let plan = if let Some(plan) = &cs.plan {
                plan
            } else {
                error!(
                    log,
                    "check suite {} should have a plan; parking", cs.id
                );
                cs.state = CheckSuiteState::Parked;
                db.update_check_suite(&cs)?;
                return Ok(());
            };

            /*
             * Walk through the entries in the plan and ensure we have an active
             * run for each one.
             */
            for jf in plan.jobfiles.iter() {
                let mut cr = db.ensure_check_run(
                    cs.id,
                    &jf.name,
                    jf.variety,
                    &jf.dependencies,
                )?;

                let mut update = false;
                if cr.config.is_none() {
                    cr.config = Some(JsonValue(jf.config.clone()));
                    debug!(log, "cr {} config -> {:?}", cr.id, cr.config);
                    update = true;
                }
                if cr.content.is_none() {
                    cr.content = Some(jf.content.clone());
                    debug!(log, "cr {} content -> {:?}", cr.id, cr.content);
                    update = true;
                }

                if update {
                    db.update_check_run(&cr)?;
                }
            }

            /*
             * Flush check run status out to GitHub.
             */
            flush_check_runs(app, &cs, &repo).await?;

            cs.state = CheckSuiteState::Running;
            db.update_check_suite(&cs)?;
        }
        CheckSuiteState::Running => {
            /*
             * Walk through the current check runs and process any
             * that are finished.
             */
            info!(log, "check suite {}: running now", cs.id);

            let mut nrunning = 0;
            for mut cr in db.list_check_runs_for_suite(cs.id)? {
                let running = match check_run_run(app, &cs, &mut cr).await {
                    Ok(running) => running,
                    Err(e) => {
                        error!(
                            log,
                            "check suite {} run {} run error: {}",
                            cs.id,
                            cr.id,
                            e
                        );
                        /*
                         * If the check run failed to advance, we must assume it
                         * needs to be run at least once more later on.
                         */
                        true
                    }
                };

                if running {
                    nrunning += 1;
                }
            }

            /*
             * Flush check run status out to GitHub.
             */
            flush_check_runs(app, &cs, &repo).await?;

            if nrunning == 0 {
                /*
                 * Once all check runs have completed their work, we can stop
                 * working on this check suite.
                 */
                info!(log, "check suite {} has completed all runs", cs.id);
                cs.state = CheckSuiteState::Complete;
            }

            db.update_check_suite(&cs)?;
        }
    }

    Ok(())
}

async fn bgtask(app: Arc<App>) {
    let log = &app.log;

    loop {
        if let Err(e) = process_deliveries(&app).await {
            error!(log, "background task: delivery processing error: {:?}", e);
        }
        match app.db.list_check_suites_active() {
            Ok(suites) => {
                for suite in suites {
                    if let Err(e) = process_check_suite(&app, suite.id).await {
                        /*
                         * Attempt to include the check suite state in the log
                         * message.
                         */
                        let st = match app.db.load_check_suite(suite.id) {
                            Ok(cs) => cs.state.to_string(),
                            Err(_) => "?".into(),
                        };

                        error!(
                            log,
                            "background task: suite {} (state {st}): {:?}",
                            suite.id,
                            e,
                        );
                    }
                }
            }
            Err(e) => {
                error!(log, "background task: listing suites: {:?}", e);
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(5_000)).await;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    usdt::register_probes().unwrap();

    let log = make_log("github-server");

    info!(log, "ok");

    /*
     * Load our files from disk...
     */
    let key = config::load_bytes("etc/privkey.pem")?;
    let key =
        pem::parse(&key).map_err(|e| anyhow!("parse privkey: {:?}", e))?;
    let config = config::load_config("etc/app.toml")?;

    let jwt = buildomat_github_client::JWTCredentials::new(
        config.id,
        key.contents().to_vec(),
    )?;

    let app0 = Arc::new(App {
        log: log.clone(),
        jwt: jwt.clone(),
        db: buildomat_github_database::Database::new(
            log.new(o!("component" => "db")),
            "var/data.sqlite3",
            config.sqlite.cache_kb,
        )?,
        config,
        templates: templates::Templates::new(log.clone())?,
    });

    /*
     * Check that we can start up and authenticate as the configured GitHub
     * application.
     *
     * XXX This might need to happen in a retry loop, in case GitHub is not
     * available at startup.
     */
    let c = app0.app_client();
    let ghapp = c.apps().get_authenticated().await?;
    println!("app slug: {}", ghapp.slug);

    /*
     * XXX Move this to a background task that periodically sweeps to ensure we
     * detect new installations even if we miss the webhook delivery.
     */
    let insts = c.apps().list_all_installations(None, "").await?;
    for i in insts.iter() {
        println!(
            "  installation: {} [{}] ({}/{})",
            i.id, i.account.simple_user.login, i.app_id, i.app_slug,
        );

        if !i.account.simple_user.login.is_empty() {
            let name = if i.account.simple_user.name.is_empty() {
                None
            } else {
                Some(i.account.simple_user.name.as_str())
            };
            let email = if i.account.simple_user.email.is_empty() {
                None
            } else {
                Some(i.account.simple_user.email.as_str())
            };

            app0.db.store_user(
                i.account.simple_user.id,
                &i.account.simple_user.login,
                UserType::from_github_str(&i.account.simple_user.type_)?,
                name,
                email,
            )?;
            app0.db.store_install(i.id, i.account.simple_user.id)?;
        }
    }

    /*
     * Start the background tasks that implement most of the CI functionality.
     */
    let app = Arc::clone(&app0);
    tokio::task::spawn(async move {
        bgtask(app).await;
    });

    /*
     * Listen for web requests from GitHub and users.
     */
    http::server(app0, "0.0.0.0:4021".parse().unwrap()).await?;

    Ok(())
}
