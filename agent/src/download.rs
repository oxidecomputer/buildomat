/*
 * Copyright 2021 Oxide Computer Company
 */

use std::path::PathBuf;
use std::sync::mpsc;

pub enum Activity {
    Downloading(PathBuf),
    Downloaded(PathBuf),
    Complete,
}

pub(crate) fn download(
    cw: super::ClientWrap,
    inputs: Vec<super::WorkerPingInput>,
    inputdir: PathBuf,
) -> mpsc::Receiver<Activity> {
    let (tx, rx) = mpsc::channel::<Activity>();

    tokio::spawn(async move {
        for i in inputs.iter() {
            let mut path = inputdir.clone();
            path.push(&i.name);

            /*
             * Try our best to create any parent directories that are required
             * for names that includes slashes.
             */
            super::make_dirs_for(&path).ok();

            tx.send(Activity::Downloading(path.clone())).unwrap();

            cw.input(&i.id, &path).await;

            tx.send(Activity::Downloaded(path.clone())).unwrap();
        }

        tx.send(Activity::Complete).unwrap();
    });

    rx
}
