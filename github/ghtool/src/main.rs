/*
 * Copyright 2021 Oxide Computer Company
 */

use anyhow::{anyhow, Result};
use hiercmd::prelude::*;

mod config;

#[derive(Default)]
struct Stuff {
    jwt: Option<octorust::auth::JWTCredentials>,
}

impl Stuff {
    fn make_jwt(&self) -> octorust::auth::JWTCredentials {
        self.jwt.as_ref().unwrap().clone()
    }

    fn app_client(&self) -> octorust::Client {
        octorust::Client::custom(
            "https://api.github.com",
            "jclulow/wollongong@0",
            octorust::auth::Credentials::JWT(self.make_jwt()),
            reqwest::Client::builder().build().unwrap(),
        )
    }

    fn install_client(&self, install_id: i64) -> octorust::Client {
        let iat = octorust::auth::InstallationTokenGenerator::new(
            install_id as u64,
            self.make_jwt(),
        );

        octorust::Client::custom(
            "https://api.github.com",
            "jclulow/wollongong@0",
            octorust::auth::Credentials::InstallationToken(iat),
            reqwest::Client::builder().build().unwrap(),
        )
    }
}

async fn do_installs(mut l: Level<Stuff>) -> Result<()> {
    no_args!(l);

    let s = l.context();

    let c = s.app_client();
    let installs = c.apps().list_all_installations(None, "").await?;

    println!("{:#?}", installs);

    Ok(())
}

async fn do_info(mut l: Level<Stuff>) -> Result<()> {
    no_args!(l);

    let s = l.context();

    let c = s.app_client();
    let ghapp = c.apps().get_authenticated().await?;

    println!("{:#?}", ghapp);

    Ok(())
}

async fn do_member(mut l: Level<Stuff>) -> Result<()> {
    l.usage_args(Some("ORG USER"));

    l.optopt("i", "", "install ID", "INSTALL");

    let a = args!(l);

    if a.args().len() != 2 {
        bad_args!(l, "specify organisation and user");
    }

    let org = a.args()[0].as_str();
    let user = a.args()[1].as_str();

    let c = if let Some(i) = a.opts().opt_str("i") {
        l.context().install_client(i.parse()?)
    } else {
        l.context().app_client()
    };

    let res = c.orgs().check_membership_for_user(org, user).await?;

    println!("{:#?}", res);

    Ok(())
}

async fn do_org(mut l: Level<Stuff>) -> Result<()> {
    l.cmd("member", "is this user a member of this org", cmd!(do_member))?;

    sel!(l).run().await
}

async fn do_user_info(mut l: Level<Stuff>) -> Result<()> {
    l.usage_args(Some("USER"));

    l.optopt("i", "", "install ID", "INSTALL");

    let a = args!(l);

    if a.args().len() != 1 {
        bad_args!(l, "specify user");
    }

    let user = a.args()[0].as_str();

    let c = if let Some(i) = a.opts().opt_str("i") {
        l.context().install_client(i.parse()?)
    } else {
        l.context().app_client()
    };

    let res = c.users().get_by_username(user).await?;

    println!("{:#?}", res);

    Ok(())
}

async fn do_user(mut l: Level<Stuff>) -> Result<()> {
    l.cmd("info", "dump user info", cmd!(do_user_info))?;

    sel!(l).run().await
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut l = Level::new("wollongong-ghtool", Stuff::default());

    l.cmd("info", "get information about the configured app", cmd!(do_info))?;
    l.cmd("installs", "dump info about installations", cmd!(do_installs))?;
    l.cmd("org", "organisation commands", cmd!(do_org))?;
    l.cmd("user", "user commands", cmd!(do_user))?;

    let mut s = sel!(l);

    /*
     * Load our files from disk...
     */
    let key = config::load_bytes("etc/privkey.pem")?;
    let key = nom_pem::decode_block(&key)
        .map_err(|e| anyhow!("decode_block: {:?}", e))?;
    let config = config::load_config("etc/app.toml")?;

    s.context_mut().jwt =
        Some(octorust::auth::JWTCredentials::new(config.id, key.data)?);

    s.run().await
}
