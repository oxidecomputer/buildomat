/*
 * Copyright 2023 Oxide Computer Company
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
            buildomat_github_common::USER_AGENT,
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
            buildomat_github_common::USER_AGENT,
            octorust::auth::Credentials::InstallationToken(iat),
            reqwest::Client::builder().build().unwrap(),
        )
    }
}

async fn do_again(mut l: Level<Stuff>) -> Result<()> {
    l.usage_args(Some("DELIVERY_ID"));

    let a = args!(l);
    if a.args().len() != 1 {
        bad_args!(l, "specify one delivery ID");
    }
    let id = a.args()[0].parse()?;

    let s = l.context();
    let c = s.app_client();

    c.apps().redeliver_webhook_delivery(id).await?;

    Ok(())
}

async fn do_webhooks(mut l: Level<Stuff>) -> Result<()> {
    l.optopt("N", "", "how many web hook entries to read?", "COUNT");
    l.optopt("D", "", "how many days worth of hooks to read?", "DAYS");

    let a = no_args!(l);

    let days = a.opts().opt_str("D").map(|s| s.parse::<u32>()).transpose()?;
    let count = a.opts().opt_str("N").map(|s| s.parse::<u32>()).transpose()?;
    let perpage = count.map(|c| c.min(100)).unwrap_or(100) as i64;

    let s = l.context();
    let c = s.app_client();

    let mut cursor = None;
    let mut seen = 0;
    loop {
        if let Some(count) = count {
            if seen >= count {
                return Ok(());
            }
        }

        let (recentdels, link) = c
            .apps()
            .list_webhook_deliveries(perpage, cursor.as_deref().unwrap_or(""))
            .await?;

        for del in recentdels {
            if let Some(count) = count {
                if seen >= count {
                    return Ok(());
                }
            }

            let when = del.delivered_at.unwrap();
            if let Some(days) = days {
                let age = chrono::Utc::now()
                    .signed_duration_since(when)
                    .num_seconds();
                if age > (days as i64) * 24 * 3600 {
                    return Ok(());
                }
            }

            let when = when
                .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
                .to_string();
            let flags = if del.redelivery { "R" } else { "-" };
            println!(
                "{:<16} {}  {}  {} {}",
                del.id, when, flags, del.status_code, del.status,
            );

            seen += 1;
        }

        cursor = link
            .as_ref()
            .and_then(|lm| lm.get(&Some("next".to_string())))
            .and_then(|next| next.queries.get("cursor"))
            .cloned();

        if cursor.is_none() {
            return Ok(());
        }
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

async fn do_repos(mut l: Level<Stuff>) -> Result<()> {
    l.reqopt("i", "", "install ID", "INSTALL");
    l.optflag("b", "", "brief output");

    let a = no_args!(l);

    let i = a.opts().opt_str("i").unwrap();
    let b = a.opts().opt_present("b");

    let c = l.context().install_client(i.parse()?);

    let mut p = 0;
    loop {
        let res = c.apps().list_repos_accessible_to_installation(0, p).await?;

        if res.repositories.is_empty() {
            break;
        }
        p += 1;

        if b {
            println!("page {}", p);
            for r in res.repositories {
                let o = r.owner.unwrap();
                println!("{:>16} {}/{}", r.id, o.login, r.name);
            }
        } else {
            println!("{:#?}", res);
        }
    }

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
    let mut l = Level::new("buildomat-github-ghtool", Stuff::default());

    l.cmd("info", "get information about the configured app", cmd!(do_info))?;
    l.cmd("installs", "dump info about installations", cmd!(do_installs))?;
    l.cmd("repos", "list repos visible to an installation", cmd!(do_repos))?;
    l.cmd("org", "organisation commands", cmd!(do_org))?;
    l.cmd("user", "user commands", cmd!(do_user))?;
    l.cmd("webhooks", "webhook deliveries", cmd!(do_webhooks))?;
    l.cmd("again", "webhook redelivery?", cmd!(do_again))?;

    let mut s = sel!(l);

    /*
     * Load our files from disk...
     */
    let key = config::load_bytes("etc/privkey.pem")?;
    let key =
        pem::parse(&key).map_err(|e| anyhow!("parse privkey: {:?}", e))?;
    let config = config::load_config("etc/app.toml")?;

    s.context_mut().jwt = Some(octorust::auth::JWTCredentials::new(
        config.id,
        key.contents().to_vec(),
    )?);

    s.run().await
}
