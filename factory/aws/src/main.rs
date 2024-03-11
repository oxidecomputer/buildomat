/*
 * Copyright 2024 Oxide Computer Company
 */

use std::sync::Arc;

use anyhow::{bail, Context, Result};
use buildomat_common::*;
use buildomat_types::metadata;
use getopts::Options;
use slog::Logger;

mod aws;
mod config;
use config::ConfigFile;

struct Central {
    log: Logger,
    config: config::ConfigFile,
    client: buildomat_client::Client,
    targets: Vec<String>,
}

impl Central {
    fn metadata(
        &self,
        t: &config::ConfigFileAwsTarget,
    ) -> metadata::FactoryMetadata {
        metadata::FactoryMetadata::V1(metadata::FactoryMetadataV1 {
            addresses: Default::default(),
            root_password_hash: self.config.aws.root_password_hash.clone(),
            dump_to_rpool: self.config.aws.dump_to_rpool,
            post_job_diagnostic_script: t.post_job_diagnostic_script.clone(),
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    usdt::register_probes().unwrap();

    let mut opts = Options::new();

    opts.optopt("f", "", "configuration file", "CONFIG");

    let p = match opts.parse(std::env::args().skip(1)) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("ERROR: usage: {}", e);
            eprintln!("       {}", opts.usage("usage"));
            std::process::exit(1);
        }
    };

    let log = make_log("factory-aws");
    let config: ConfigFile = if let Some(f) = p.opt_str("f").as_deref() {
        read_toml(f)?
    } else {
        bail!("must specify configuration file (-f)");
    };
    let targets = config.target.keys().map(String::to_string).collect();
    let client = buildomat_client::ClientBuilder::new(&config.general.baseurl)
        .bearer_token(&config.factory.token)
        .build()?;

    let c = Arc::new(Central { log, config, client, targets });

    let t_aws = tokio::task::spawn(async move {
        aws::aws_worker(c).await.context("AWS worker task failure")
    });

    tokio::select! {
        _ = t_aws => bail!("AWS worker task stopped early"),
    }
}
