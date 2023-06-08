use anyhow::Result;
use hiercmd::prelude::*;

struct Stuff {}

async fn cmd_store(mut l: Level<Stuff>) -> Result<()> {
    todo!()
}

pub async fn main() -> Result<()> {
    let s = Stuff {};

    let mut l = Level::new("bmat", s);

    sel!(l).run().await
}
