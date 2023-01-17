use anyhow::Result;
use futures_util::TryStreamExt;

#[tokio::main]
async fn main() -> Result<()> {
    let log = buildomat_common::make_log("gerrit-client-example-list");

    let client = callaghan_gerrit_client::ClientBuilder::new(
        log,
        "https://code.oxide.computer",
    )
    .build()?;

    let mut changes = client.changes();

    while let Some(c) = changes.try_next().await? {
        println!("{:?}", c);
    }

    Ok(())
}
