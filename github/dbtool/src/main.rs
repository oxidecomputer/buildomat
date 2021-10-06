use anyhow::{bail, Result};
use hiercmd::prelude::*;
use wollongong_common::hooktypes;
use wollongong_database::Database;

const SHORT_SHA_LEN: usize = 16;

#[derive(Default)]
struct Stuff {
    db: Option<Database>,
}

impl Stuff {
    fn db(&self) -> &Database {
        self.db.as_ref().unwrap()
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
                serde_json::from_value(del.payload)?;
            println!("{:#?}", payload);
        }
    }

    Ok(())
}

async fn do_delivery_list(mut l: Level<Stuff>) -> Result<()> {
    l.add_column("seq", 5, true);
    l.add_column("ack", 3, true);
    l.add_column("recvtime", 20, true);
    l.add_column("event", 14, true);
    l.add_column("action", 24, true);
    l.add_column("uuid", 36, false);
    l.add_column("sender", 36, false);

    let a = no_args!(l);
    let mut t = a.table();

    for del in l.context().db().list_deliveries()? {
        let mut r = Row::default();
        r.add_u64("seq", del.seq as u64);
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
        match serde_json::from_value::<hooktypes::Payload>(del.payload) {
            Ok(payload) => {
                r.add_str("action", &payload.action);
                r.add_str("sender", &payload.sender.login);
            }
            Err(e) => {
                if &del.event != "ping" {
                    eprintln!("WARNING: seq {}: {:?}", seq, e);
                }
                r.add_str("action", "-");
                r.add_str("sender", "-");
            }
        }

        t.add_row(r);
    }

    print!("{}", t.output()?);
    Ok(())
}

async fn do_delivery(mut l: Level<Stuff>) -> Result<()> {
    l.cmda("list", "ls", "list deliveries", cmd!(do_delivery_list))?;
    l.cmda("dump", "", "inspect a delivery", cmd!(do_delivery_dump))?;
    l.cmda("unack", "", "process a delivery again", cmd!(do_delivery_unack))?;

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

async fn do_check_suite(mut l: Level<Stuff>) -> Result<()> {
    l.cmda("list", "ls", "list check suites", cmd!(do_check_suite_list))?;

    sel!(l).run().await
}

async fn do_check_run(mut l: Level<Stuff>) -> Result<()> {
    l.cmda("list", "ls", "list check runs", cmd!(do_check_run_list))?;

    sel!(l).run().await
}

async fn do_check(mut l: Level<Stuff>) -> Result<()> {
    l.cmd("suite", "GitHub check suites", cmd!(do_check_suite))?;
    l.cmd("run", "GitHub check runs", cmd!(do_check_run))?;

    sel!(l).run().await
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut l = Level::new("wollongong-dbtool", Stuff::default());

    l.cmda("delivery", "del", "webhook deliveries", cmd!(do_delivery))?;
    l.cmda("repository", "repo", "GitHub repositories", cmd!(do_repository))?;
    l.cmd("check", "GitHub checks", cmd!(do_check))?;

    l.context_mut().db =
        Some(Database::new(l.discard_logger(), "var/data.sqlite3")?);

    sel!(l).run().await
}
