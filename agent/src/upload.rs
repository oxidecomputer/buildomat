use std::collections::HashSet;
use std::fs;
use std::io::Read;
use std::path::PathBuf;
use std::sync::mpsc;

pub enum Activity {
    Error(String),
    Scanned(usize),
    Uploading(PathBuf, u64),
    Uploaded(PathBuf),
    Complete,
}

struct Upload {
    path: PathBuf,
    size: u64,
}

pub(crate) fn upload(
    cw: super::ClientWrap,
    rules: Vec<String>,
) -> mpsc::Receiver<Activity> {
    let (tx, rx) = mpsc::channel::<Activity>();

    tokio::spawn(async move {
        let mut seen = HashSet::new();
        let mut uploads = Vec::new();

        /*
         * Enumerate all of the files we expect to upload.
         */
        for r in rules.iter() {
            /*
             * Walk the file system and locate output files to
             * upload.
             */
            match glob::glob(r.as_str()) {
                Ok(paths) => {
                    for p in paths {
                        match p {
                            Ok(p) => {
                                if seen.contains(&p) {
                                    continue;
                                }
                                seen.insert(p.clone());

                                match fs::metadata(&p) {
                                    Ok(md) => {
                                        if !md.is_file() {
                                            continue;
                                        }

                                        uploads.push(Upload {
                                            path: p,
                                            size: md.len(),
                                        });
                                    }
                                    Err(e) => {
                                        tx.send(Activity::Error(format!(
                                            "glob {:?} stat error: {:?}",
                                            r, e
                                        )))
                                        .unwrap();
                                        continue;
                                    }
                                }
                            }
                            Err(e) => {
                                tx.send(Activity::Error(format!(
                                    "glob {:?} path error: {:?}",
                                    r, e
                                )))
                                .unwrap();
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    tx.send(Activity::Error(format!(
                        "glob {:?} error: {:?}",
                        r, e
                    )))
                    .unwrap();
                }
            }
        }

        tx.send(Activity::Scanned(uploads.len())).unwrap();

        /*
         * Upload each scanned file.
         */
        'outer: for u in uploads.iter() {
            tx.send(Activity::Uploading(u.path.clone(), u.size)).unwrap();

            let mut f = match fs::File::open(&u.path) {
                Ok(f) => f,
                Err(e) => {
                    tx.send(Activity::Error(format!(
                        "open {:?} failed: {:?}",
                        u.path, e
                    )))
                    .unwrap();
                    continue;
                }
            };

            /*
             * Read 5MB chunks of the file and upload them to the server.
             */
            let mut total = 0;
            let mut chunks = Vec::new();
            loop {
                let mut buf = bytes::BytesMut::new();
                buf.resize(5 * 1024 * 1024, 0);

                let buf = match f.read(&mut buf) {
                    Ok(sz) if sz == 0 => break,
                    Ok(sz) => {
                        buf.truncate(sz);
                        total += sz as u64;
                        buf.freeze()
                    }
                    Err(e) => {
                        tx.send(Activity::Error(format!(
                            "read {:?} failed: {:?}",
                            u.path, e
                        )))
                        .unwrap();
                        continue 'outer;
                    }
                };

                chunks.push(cw.chunk(buf).await);
            }

            if total != u.size {
                tx.send(Activity::Error(format!(
                    "file {:?} changed size mid upload: {} -> {}",
                    u.path, u.size, total
                )))
                .unwrap();
                continue;
            }

            cw.output(&u.path, u.size, chunks.as_slice()).await;

            tx.send(Activity::Uploaded(u.path.clone())).unwrap();
        }

        tx.send(Activity::Complete).unwrap();
    });

    rx
}
