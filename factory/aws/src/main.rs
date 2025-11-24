/*
 * Copyright 2024 Oxide Computer Company
 */

use std::sync::Arc;

use anyhow::{Context, Result, bail};
use buildomat_common::*;
use buildomat_types::metadata;
use getopts::Options;
use slog::Logger;

mod aws;
mod config;
use config::ConfigFile;

mod types {
    use rusty_ulid::Ulid;
    use std::str::FromStr;

    macro_rules! ulid_new_type {
        ($name:ident, $prefix:literal) => {
            #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
            #[repr(transparent)]
            pub struct $name(Ulid);

            impl FromStr for $name {
                type Err = anyhow::Error;

                fn from_str(s: &str) -> Result<Self, Self::Err> {
                    Ok($name(Ulid::from_str(s)?))
                }
            }

            impl std::fmt::Display for $name {
                fn fmt(
                    &self,
                    f: &mut std::fmt::Formatter<'_>,
                ) -> std::fmt::Result {
                    self.0.fmt(f)
                }
            }

            impl std::fmt::Debug for $name {
                fn fmt(
                    &self,
                    f: &mut std::fmt::Formatter<'_>,
                ) -> std::fmt::Result {
                    format_args!("{}:{self}", $prefix).fmt(f)
                }
            }
        };
    }

    ulid_new_type!(LeaseId, "lease");
    ulid_new_type!(WorkerId, "worker");
}

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
    ) -> Result<metadata::FactoryMetadata> {
        /*
         * Allow the per-target diagnostic configuration to override the base
         * diagnostic configuration.
         */
        self.config.diag.apply_overrides(&t.diag).build()
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
