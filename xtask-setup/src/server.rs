/*
 * Copyright 2026 Oxide Computer Company
 */

use std::collections::HashMap;

use anyhow::{Context as _, Result};
use aws_config::BehaviorVersion;
use aws_runtime::env_config::file::EnvConfigFiles;
use aws_sdk_s3::types::{
    AbortIncompleteMultipartUpload, BucketLifecycleConfiguration,
    BucketLocationConstraint, CreateBucketConfiguration, ExpirationStatus,
    LifecycleRule, LifecycleRuleFilter, PublicAccessBlockConfiguration,
};
use aws_types::os_shim_internal::{Env, Fs};
use buildomat_client::types::UserCreate;
use buildomat_client::Client as Buildomat;
use buildomat_common::genkey;
use dialoguer::{Input, Select};

use crate::with_api::with_api;
use crate::Context;
use std::path::Path;

pub(crate) async fn setup(ctx: &Context) -> Result<()> {
    let root = ctx.root.join("server");
    if root.exists() {
        std::fs::remove_dir_all(&root)
            .context("failed to remove old server directory")?;
    }
    std::fs::create_dir_all(&root)
        .context("failed to create server directory")?;

    println!();
    println!("buildomat-server setup");
    println!("======================");

    let aws = get_aws(ctx).await?;
    let baseurl = get_base_url(ctx)?;

    /*
     * Write the configuration file used by the server.
     */
    let config = ServerConfig {
        admin: ServerConfigAdmin { token: genkey(64), hold: false },
        general: ServerConfigGeneral { baseurl },
        storage: ServerConfigStorage {
            profile: aws.profile,
            bucket: aws.bucket,
            prefix: ctx.setup_name.clone(),
            region: aws.region,
        },
        job: ServerConfigJob { max_runtime: 60 * 60 },
        sqlite: ServerConfigSqlite {},
    };
    std::fs::write(root.join("config.toml"), toml::to_string_pretty(&config)?)
        .context("failed to write the server configuration file")?;

    /*
     * Create the database file, as the server errors out if it's missing.
     */
    std::fs::create_dir_all(root.join("data"))
        .context("failed to create the data dir")?;
    std::fs::write(root.join("data/data.sqlite3"), b"")
        .context("failed to create the database")?;

    with_api(ctx, async |bmat| {
        /*
         * Create the user that will be used by the CLI.  This needs to be done
         * within with_api() as it issues API calls to the buildomat server.
         */
        create_cli_user(&root, bmat, &config).await?;
        Ok(())
    })
    .await?;

    /*
     * Mark the server setup as complete.
     */
    std::fs::write(root.join("complete"), b"true\n")
        .context("failed to mark the server setup as complete")?;

    Ok(())
}

fn get_base_url(ctx: &Context) -> Result<String> {
    println!();
    println!("Buildomat needs a PUBLICLY ACCESSIBLE domain name that proxies");
    println!("requests to localhost on port 9979.");
    println!();

    Ok(Input::with_theme(&ctx.input_theme)
        .with_prompt("Domain name")
        .validate_with(|url: &String| -> _ {
            if !url.starts_with("http://") && !url.starts_with("https://") {
                Err("missing http:// or https:// at the start")
            } else {
                Ok(())
            }
        })
        .interact()?)
}

async fn get_aws(ctx: &Context) -> Result<Aws> {
    println!();
    println!("Buildomat needs access to an AWS account to store data in an");
    println!("S3 bucket, plus run VMs if you choose to use the AWS factory.");
    println!();

    /*
     * List the AWS profiles configured on the local machine.
     */
    let fs = Fs::real();
    let env = Env::real();
    let ecf = EnvConfigFiles::default();
    let mut profiles = aws_config::profile::load(&fs, &env, &ecf, None)
        .await?
        .profiles()
        .map(|p| p.to_string())
        .collect::<Vec<_>>();
    profiles.sort();

    /*
     * Let the user choose the profile they want.
     */
    let selected = Select::with_theme(&ctx.input_theme)
        .with_prompt("Select the AWS profile to use")
        .default(profiles.iter().position(|p| p == "default").unwrap_or(0))
        .items(&profiles)
        .interact()?;
    let profile = profiles.remove(selected);

    /*
     * List S3 buckets available in the account.
     */
    let config_global = aws_config::defaults(BehaviorVersion::latest())
        .profile_name(&profile)
        .load()
        .await;
    let s3_global = aws_sdk_s3::Client::new(&config_global);
    let buckets = s3_global
        .list_buckets()
        .into_paginator()
        .send()
        .collect::<Result<Vec<_>, _>>()
        .await
        .context("failed to list S3 buckets in the account")?
        .iter()
        .flat_map(|response| response.buckets())
        .map(|bucket| bucket.name().unwrap().to_string())
        .collect::<Vec<_>>();

    /*
     * Let the user choose the S3 bucket they want.
     */
    let selected = Select::with_theme(&ctx.input_theme)
        .with_prompt("Select the S3 bucket (you can reuse existing ones)")
        .default(1)
        .item("Create a new bucket.")
        .items(&buckets)
        .interact()?;
    let (bucket, region) = if selected == 0 {
        create_s3_bucket(ctx, &profile).await?
    } else {
        let bucket = buckets[selected - 1].clone();

        /*
         * Locate the region of the bucket, we'll use it as the region of everything
         * else too.  We need to do some post-processing on it as AWS documents a
         * null result to be "us-east-1" (checks out given the availability), and
         * "EU" to be "eu-west-1".  Yay legacy.
         */
        let loc = s3_global
            .get_bucket_location()
            .bucket(&bucket)
            .send()
            .await
            .context("failed to get the location of the selected S3 bucket")?;
        let region = match loc.location_constraint() {
            Some(BucketLocationConstraint::Eu) => "eu-west-1".to_string(),
            Some(region) => region.to_string(),
            None => "us-east-1".to_string(),
        };
        (bucket, region)
    };

    Ok(Aws { profile, region, bucket })
}

async fn create_s3_bucket(
    ctx: &Context,
    profile: &str,
) -> Result<(String, String)> {
    let name: String = Input::with_theme(&ctx.input_theme)
        .with_prompt("Name of the bucket to create")
        .interact()?;

    /*
     * We *could* offer a dropdown of all regions, but that is going to be a
     * long list for little purpose.  Instead, pick some reasonable choices.
     *
     * Note that if you decide to add us-east-1 or eu-west-1 to the list (don't)
     * you will need to update the code below to set the location constraint of
     * the bucket, as AWS treats those regions in a special way.
     */
    let regions = ["us-west-2", "us-east-2", "eu-central-1"];
    let region_idx = Select::with_theme(&ctx.input_theme)
        .with_prompt("Region to create the bucket into")
        .default(0)
        .items(&regions)
        .interact()?;
    let region = regions[region_idx];

    let config_regional = aws_config::defaults(BehaviorVersion::latest())
        .profile_name(profile)
        .region(region)
        .load()
        .await;
    let s3_regional = aws_sdk_s3::Client::new(&config_regional);

    /*
     * Create a bucket with some defaults.
     */
    s3_regional
        .create_bucket()
        .bucket(&name)
        .create_bucket_configuration(
            CreateBucketConfiguration::builder()
                .location_constraint(region.parse()?)
                .build(),
        )
        .send()
        .await
        .context("failed to create the S3 bucket")?;
    s3_regional
        .put_public_access_block()
        .bucket(&name)
        .public_access_block_configuration(
            PublicAccessBlockConfiguration::builder()
                .block_public_acls(true)
                .block_public_policy(true)
                .ignore_public_acls(true)
                .restrict_public_buckets(true)
                .build(),
        )
        .send()
        .await
        .context("failed to restrict public access to the bucket")?;
    s3_regional
        .put_bucket_lifecycle_configuration()
        .bucket(&name)
        .lifecycle_configuration(
            BucketLifecycleConfiguration::builder()
                .rules(
                    LifecycleRule::builder()
                        .id("abort-multipart-uploads")
                        .status(ExpirationStatus::Enabled)
                        .filter(
                            LifecycleRuleFilter::builder().prefix("").build(),
                        )
                        .abort_incomplete_multipart_upload(
                            AbortIncompleteMultipartUpload::builder()
                                .days_after_initiation(7)
                                .build(),
                        )
                        .build()?,
                )
                .build()?,
        )
        .send()
        .await
        .context("failed to set a lifecycle rule for the bucket")?;

    Ok((name, region.to_string()))
}

async fn create_cli_user(
    root: &Path,
    bmat: &Buildomat,
    config: &ServerConfig,
) -> Result<()> {
    let user = bmat
        .user_create()
        .body(UserCreate { name: "cli".into() })
        .send()
        .await
        .context("failed to create cli user")?;

    let cli_config = CliConfig {
        default_profile: "local".into(),
        profile: [(
            "local".to_string(),
            CliConfigProfile {
                url: "http://127.0.0.1:9979".into(),
                secret: user.token.clone(),
                admin_token: config.admin.token.clone(),
            },
        )]
        .into_iter()
        .collect(),
    };

    std::fs::write(
        root.join("cli-config.toml"),
        toml::to_string(&cli_config)?.as_bytes(),
    )
    .context("failed to write the CLI config")?;
    Ok(())
}

#[derive(Debug)]
struct Aws {
    profile: String,
    region: String,
    bucket: String,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct ServerConfig {
    pub(crate) admin: ServerConfigAdmin,
    pub(crate) general: ServerConfigGeneral,
    pub(crate) storage: ServerConfigStorage,
    pub(crate) job: ServerConfigJob,
    pub(crate) sqlite: ServerConfigSqlite,
}

impl ServerConfig {
    pub(crate) fn from_context(ctx: &Context) -> Result<Self> {
        Ok(toml::from_str(
            &std::fs::read_to_string(
                ctx.root.join("server").join("config.toml"),
            )
            .context("failed to find the buildomat-server config")?,
        )?)
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct ServerConfigAdmin {
    pub(crate) token: String,
    pub(crate) hold: bool,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct ServerConfigGeneral {
    pub(crate) baseurl: String,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct ServerConfigStorage {
    pub(crate) profile: String,
    pub(crate) bucket: String,
    pub(crate) prefix: String,
    pub(crate) region: String,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct ServerConfigJob {
    pub(crate) max_runtime: u64,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct ServerConfigSqlite {}

#[derive(serde::Serialize)]
struct CliConfig {
    default_profile: String,
    profile: HashMap<String, CliConfigProfile>,
}

#[derive(serde::Serialize)]
struct CliConfigProfile {
    url: String,
    secret: String,
    admin_token: String,
}
