/*
 * Copyright 2024 Oxide Computer Company
 */

use anyhow::{anyhow, bail, Context, Result};
use buildomat_github_database::types::{CheckRunVariety, CheckSuiteId};
use buildomat_github_database::Database;
use buildomat_github_hooktypes as hooktypes;
use chrono::prelude::*;
use hiercmd::prelude::*;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::Write;
use std::os::unix::fs::DirBuilderExt;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::{Duration, Instant};

const SHORT_SHA_LEN: usize = 16;

#[derive(Default)]
struct Stuff {
    db: Option<Database>,
    archive: Option<PathBuf>,
}

impl Stuff {
    fn db(&self) -> &Database {
        self.db.as_ref().unwrap()
    }

    fn archive(&self, set: &str, file: &str) -> Result<PathBuf> {
        let mut out = self.archive.as_ref().unwrap().to_path_buf();
        std::fs::DirBuilder::new().mode(0o700).recursive(true).create(&out)?;
        out.push(set);
        std::fs::DirBuilder::new().mode(0o700).recursive(true).create(&out)?;
        out.push(&format!("{}.json", file));
        Ok(out)
    }
}

async fn do_delivery_unack(mut l: Level<Stuff>) -> Result<()> {
    l.usage_args(Some("SEQ..."));

    let a = args!(l);

    if a.args().is_empty() {
        bad_args!(l, "specify at least one delivery sequence number");
    }

    for seq in a.args() {
        let seq = seq.parse()?;
        l.context().db().delivery_unack(seq)?;
        println!("unacked delivery sequence {}", seq);
    }

    Ok(())
}

#[derive(Debug, Serialize)]
struct DeliveryArchive {
    v: String,
    records: Vec<Delivery>,
}

#[derive(Debug, Serialize)]
struct Delivery {
    pub seq: u64,
    pub uuid: String,
    pub event: String,
    pub headers: BTreeMap<String, String>,
    pub payload: serde_json::Value,
    pub recvtime: DateTime<Utc>,
    pub ack: i64,
}

async fn do_delivery_archive(mut l: Level<Stuff>) -> Result<()> {
    no_args!(l);

    let mut prior: Option<String> = None;

    loop {
        let first = if let Some(f) = l.context().db().delivery_earliest()? {
            f
        } else {
            println!("no deliveries?");
            return Ok(());
        };

        let start = Instant::now();

        let prefix = first.recvtime.0.format("%Y-%m-%d").to_string();
        println!("earliest delivery day is {}", prefix);

        if let Some(prior) = &prior {
            if &prefix == prior {
                bail!("should not see the same day twice");
            }
        }

        /*
         * Determine how long ago this day was:
         */
        let old = 14;
        if Utc::now().signed_duration_since(first.recvtime.0).num_days() < old {
            println!("less than {} days old, all done", old);
            return Ok(());
        }

        let wholeday = l.context().db().same_day_deliveries(&first)?;

        if wholeday.iter().any(|del| del.ack.is_none()) {
            bail!("cannot archive a day with unacked deliveries");
        }

        let out =
            l.context().archive("delivery", &first.recvtime_day_prefix())?;
        println!("archive to {:?}", out);

        let records = wholeday
            .into_iter()
            .map(|del| {
                let buildomat_github_database::types::Delivery {
                    seq,
                    uuid,
                    event,
                    headers,
                    payload,
                    recvtime,
                    ack,
                } = del;

                println!(
                    "{} seq {} event \"{}\"",
                    recvtime.0.to_rfc3339(),
                    seq,
                    event
                );

                Delivery {
                    seq: seq.0 as u64,
                    uuid,
                    event,
                    headers: headers.0.into_iter().collect(),
                    payload: payload.0,
                    recvtime: recvtime.0,
                    ack: ack.unwrap(),
                }
            })
            .collect::<Vec<_>>();
        let da = DeliveryArchive { v: "1".to_string(), records };

        let mut bw = std::io::BufWriter::new(
            std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&out)
                .with_context(|| anyhow!("creating {:?}", out))?,
        );
        serde_json::to_writer_pretty(&mut bw, &da)?;
        let mut f = bw.into_inner()?;
        f.flush()?;
        f.sync_all()?;

        let dels = da
            .records
            .into_iter()
            .map(|d| {
                (
                    buildomat_github_database::types::DeliverySeq(
                        d.seq.try_into().unwrap(),
                    ),
                    d.uuid,
                )
            })
            .collect::<Vec<_>>();
        loop {
            match l.context().db().remove_deliveries(&dels) {
                Ok(_) => break,
                Err(e) if e.is_locked_database() => {
                    eprintln!("WARNING: database locked... retrying...");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
                Err(e) => bail!("{e}"),
            }
        }

        let delta = Instant::now().duration_since(start);
        println!("took {} milliseconds", delta.as_millis());

        prior = Some(prefix);
    }
}

async fn do_delivery_dump(mut l: Level<Stuff>) -> Result<()> {
    l.optflag("x", "", "dump the whole Delivery object");

    l.usage_args(Some("SEQ..."));

    let a = args!(l);

    if a.args().is_empty() {
        bad_args!(l, "specify at least one delivery sequence number");
    }

    for seq in a.args() {
        let del = l.context().db().load_delivery(seq.parse()?)?;
        if a.opts().opt_present("x") {
            println!("{:#?}", del);
        } else {
            println!("delivery: sequence {} event \"{}\"", del.seq, del.event);

            let payload: hooktypes::Payload =
                serde_json::from_value(del.payload.0)?;
            println!("{:#?}", payload);
        }
    }

    Ok(())
}

async fn do_delivery_list(mut l: Level<Stuff>) -> Result<()> {
    l.add_column("seq", 7, true);
    l.add_column("ack", 3, true);
    l.add_column("recvtime", 20, true);
    l.add_column("event", 14, true);
    l.add_column("action", 24, true);
    l.add_column("uuid", 36, false);
    l.add_column("sender", 24, false);
    l.add_column("install", 8, false);

    l.optopt("n", "", "limit to this many of the most recent entries", "N");

    let a = args!(l);

    let mut t = a.table();

    let seqs = if let Some(n) = a.opts().opt_str("n") {
        l.context().db().list_deliveries_recent(n.parse()?)?
    } else {
        l.context().db().list_deliveries()?
    };

    for &seq in seqs.iter() {
        let del = l.context().db().delivery_load(seq)?;

        let mut r = Row::default();
        r.add_u64("seq", del.seq.0 as u64);
        r.add_str("uuid", &del.uuid);
        r.add_str("event", &del.event);
        r.add_str(
            "recvtime",
            del.recvtime.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        );
        r.add_str(
            "ack",
            &del.ack.map(|n| n.to_string()).unwrap_or_else(|| "-".to_string()),
        );

        let seq = del.seq;
        match serde_json::from_value::<hooktypes::Payload>(del.payload.0) {
            Ok(payload) => {
                r.add_str("action", &payload.action);
                r.add_str("sender", &payload.sender.login);
                if let Some(inst) = &payload.installation {
                    r.add_str("install", inst.id.to_string());
                } else {
                    r.add_str("install", "-");
                }
            }
            Err(e) => {
                if &del.event != "ping" {
                    eprintln!("WARNING: seq {}: {:?}", seq, e);
                }
                r.add_str("action", "-");
                r.add_str("sender", "-");
                r.add_str("install", "-");
            }
        }

        t.add_row(r);
    }

    print!("{}", t.output()?);
    Ok(())
}

async fn do_delivery(mut l: Level<Stuff>) -> Result<()> {
    l.cmda("list", "ls", "list deliveries", cmd!(do_delivery_list))?;
    l.cmd("dump", "inspect a delivery", cmd!(do_delivery_dump))?;
    l.cmd("unack", "process a delivery again", cmd!(do_delivery_unack))?;
    l.cmd("archive", "archive deliveries", cmd!(do_delivery_archive))?;

    sel!(l).run().await
}

async fn do_repository_list(mut l: Level<Stuff>) -> Result<()> {
    l.add_column("id", 10, true);
    l.add_column("owner", 36, true);
    l.add_column("name", 36, true);

    let a = no_args!(l);
    let mut t = a.table();

    for repo in l.context().db().list_repositories()? {
        let mut r = Row::default();
        r.add_u64("id", repo.id as u64);
        r.add_str("owner", repo.owner);
        r.add_str("name", repo.name);

        t.add_row(r);
    }

    print!("{}", t.output()?);
    Ok(())
}

async fn do_repository(mut l: Level<Stuff>) -> Result<()> {
    l.cmda("list", "ls", "list repositories", cmd!(do_repository_list))?;

    sel!(l).run().await
}

async fn do_check_run_list(_l: Level<Stuff>) -> Result<()> {
    bail!("not yet implemented");
}

async fn do_check_suite_list(mut l: Level<Stuff>) -> Result<()> {
    l.add_column("id", 26, true);
    l.add_column("ghid", 10, true);
    l.add_column("repo", 10, true);
    l.add_column("state", 14, true);
    l.add_column("ssha", SHORT_SHA_LEN, false);
    l.add_column("sha", 40, false);
    l.add_column("branch", 24, false);

    let a = no_args!(l);
    let mut t = a.table();

    for suite in l.context().db().list_check_suites()? {
        let mut r = Row::default();
        r.add_str("id", &suite.id.to_string());
        r.add_u64("ghid", suite.github_id as u64);
        r.add_u64("repo", suite.repo as u64);
        r.add_str("state", &format!("{:?}", suite.state));
        r.add_str("ssha", &suite.head_sha[0..SHORT_SHA_LEN]);
        r.add_str("sha", suite.head_sha);
        r.add_str("branch", suite.head_branch.as_deref().unwrap_or("-"));

        t.add_row(r);
    }

    print!("{}", t.output()?);
    Ok(())
}

async fn do_check_suite_dump(mut l: Level<Stuff>) -> Result<()> {
    l.usage_args(Some("CHECKSUITE"));

    let a = args!(l);
    if a.args().len() != 1 {
        bad_args!(l, "specify a buildomat check suite ULID");
    }

    let csid = CheckSuiteId::from_str(&a.args()[0])?;

    let cs = l.context().db().load_check_suite(csid)?;

    println!("check suite: {cs:#?}");
    Ok(())
}

async fn do_check_suite_find(mut l: Level<Stuff>) -> Result<()> {
    l.usage_args(Some("OWNER REPO CHECKSUITE"));

    let a = args!(l);
    if a.args().len() != 3 {
        bad_args!(l, "specify a GitHub owner, repository, and check suite ID");
    }

    let owner = a.args()[0].to_string();
    let repo = a.args()[1].to_string();
    let csid = a.args()[2].to_string().parse::<i64>()?;

    let Some(r) = l.context().db().lookup_repository(&owner, &repo)? else {
        bail!("could not locate repository {owner}/{repo}");
    };

    let cs = l.context().db().load_check_suite_by_github_id(r.id, csid)?;

    println!("check suite: {cs:#?}");
    Ok(())
}

async fn do_check_run_find(mut l: Level<Stuff>) -> Result<()> {
    l.usage_args(Some("OWNER REPO CHECKRUN"));

    let a = args!(l);
    if a.args().len() != 3 {
        bad_args!(l, "specify a GitHub owner, repository, and check run ID");
    }

    let owner = a.args()[0].to_string();
    let repo = a.args()[1].to_string();
    let crid = a.args()[2].to_string().parse::<i64>()?;

    let Some(r) = l.context().db().lookup_repository(&owner, &repo)? else {
        bail!("could not locate repository {owner}/{repo}");
    };

    let crs = l.context().db().find_check_runs_from_github_id(r.id, crid)?;
    if crs.is_empty() {
        bail!("could not locate that check run");
    }

    for (i, (cs, cr)) in crs.iter().enumerate() {
        if i > 0 {
            println!();
        }

        println!("SUITE {} RUN {}", cs.id, cr.id);
        println!("    NAME {:?}", cr.name);
        println!("    HEAD {}", cs.head_sha);
        if let Some(plan) = cs.plan_sha.as_deref() {
            println!("    PLAN {}", plan);
        }

        match cr.variety {
            CheckRunVariety::Basic => {
                if let Some(pj) = &cr.private {
                    if let Some(o) = pj.as_object() {
                        if let Some(bmid) = o.get("buildomat_id") {
                            if let Some(bmid) = bmid.as_str() {
                                println!(
                                    "    BUILDOMAT JOB {bmid} (gong-{})",
                                    r.id,
                                );
                            }
                        }
                    }
                }
            }
            CheckRunVariety::Control
            | CheckRunVariety::AlwaysPass
            | CheckRunVariety::FailFirst => {
                /*
                 * Other varieties do have extra information like buildomat jobs
                 * that we want to expose here.
                 */
            }
        }
    }

    Ok(())
}

async fn do_duplicates(mut l: Level<Stuff>) -> Result<()> {
    no_args!(l);

    let ids = l.context().db().list_check_suite_ids()?;
    let mut suites_for_commit: HashMap<String, Vec<CheckSuiteId>> =
        Default::default();

    for id in ids {
        let cs = l.context().db().load_check_suite(id)?;

        let v = suites_for_commit.entry(cs.head_sha).or_default();

        if !v.contains(&id) {
            v.push(id);
        }
    }

    let mut dups = suites_for_commit
        .into_iter()
        .filter(|(_, csids)| csids.len() > 1)
        .collect::<Vec<_>>();

    dups.sort_by(|a, b| a.1[0].cmp(&b.1[0]));

    let mut count_per_month: BTreeMap<String, u32> = Default::default();
    let mut year_min = 9999;
    let mut year_max = 0;
    let mut spread_min = None;
    let mut spread_max = None;

    for (sha, ids) in dups {
        assert!(ids.len() > 1);

        /*
         * Determine the interval of time between the creation of the first
         * check suite and the creation of the last one:
         */
        let dts = ids.iter().map(|id| id.datetime()).collect::<BTreeSet<_>>();
        let dur = dts
            .iter()
            .max()
            .unwrap()
            .signed_duration_since(*dts.iter().min().unwrap())
            .to_std()
            .unwrap();

        if let Some(old) = spread_min {
            if dur < old {
                spread_min = Some(dur);
            }
        } else {
            spread_min = Some(dur)
        }
        if let Some(old) = spread_max {
            if dur > old {
                spread_max = Some(dur);
            }
        } else {
            spread_max = Some(dur)
        }

        println!("SHA {sha} [{} msec spread]", dur.as_millis());

        for id in ids {
            let dt = id.datetime();

            println!("    {dt} {id}");

            if year_min > dt.year() {
                year_min = dt.year();
            }
            if year_max < dt.year() {
                year_max = dt.year();
            }

            let bkt = dt.format("%Y-%m").to_string();
            let c = count_per_month.entry(bkt).or_default();
            *c += 1;
        }
    }

    println!();

    for y in year_min..=year_max {
        for m in 1..=12 {
            let bkt = format!("{y:04}-{m:02}");
            let c = count_per_month.get(&bkt).unwrap_or(&0);
            println!("{bkt} {c}");
        }

        println!();
    }

    if let Some(val) = spread_min {
        println!("min. spread = {} msec", val.as_millis());
    }
    if let Some(val) = spread_max {
        println!("max. spread = {} msec", val.as_millis());
    }

    Ok(())
}

async fn do_check_suite(mut l: Level<Stuff>) -> Result<()> {
    l.cmda("list", "ls", "list check suites", cmd!(do_check_suite_list))?;
    l.cmd(
        "find",
        "locate a particular check suite",
        cmd!(do_check_suite_find),
    )?;
    l.cmd("dump", "dump a particular check suite", cmd!(do_check_suite_dump))?;
    l.cmd(
        "duplicates",
        "look for commits that had more than one check suite",
        cmd!(do_duplicates),
    )?;

    sel!(l).run().await
}

async fn do_check_run(mut l: Level<Stuff>) -> Result<()> {
    l.cmda("list", "ls", "list check runs", cmd!(do_check_run_list))?;
    l.cmd("find", "locate a particular check run", cmd!(do_check_run_find))?;

    sel!(l).run().await
}

async fn do_check(mut l: Level<Stuff>) -> Result<()> {
    l.cmd("suite", "GitHub check suites", cmd!(do_check_suite))?;
    l.cmd("run", "GitHub check runs", cmd!(do_check_run))?;

    sel!(l).run().await
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut l = Level::new("buildomat-github-dbtool", Stuff::default());

    l.cmda("delivery", "del", "webhook deliveries", cmd!(do_delivery))?;
    l.cmda("repository", "repo", "GitHub repositories", cmd!(do_repository))?;
    l.cmd("check", "GitHub checks", cmd!(do_check))?;

    let var = {
        let mut var = std::env::current_dir()?;
        var.push("var");
        var
    };

    let db = {
        let mut db = var.clone();
        db.push("data.sqlite3");
        db
    };

    l.context_mut().db = Some(Database::new(l.discard_logger(), db, None)?);
    l.context_mut().archive = Some({
        let mut db = var.clone();
        db.push("archive");
        db
    });

    sel!(l).run().await
}
