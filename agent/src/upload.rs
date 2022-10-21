/*
 * Copyright 2022 Oxide Computer Company
 */

use std::collections::HashSet;
use std::fs;
use std::io::Read;
use std::path::PathBuf;
use std::sync::mpsc;

pub enum Activity {
    Error(String),
    Warning(String),
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
    rules: Vec<super::WorkerPingOutputRule>,
) -> mpsc::Receiver<Activity> {
    let (tx, rx) = mpsc::channel::<Activity>();

    tokio::spawn(async move {
        let mut seen = HashSet::new();
        let mut uploads = Vec::new();

        /*
         * Preprocess the rule list to locate any special handling directives
         * like ignore rules or required matches.
         */
        let mut walk_patterns = Vec::new();
        let mut ignore = Vec::new();
        let mut required = Vec::new();
        let mut relaxed = Vec::new();
        for r in rules.iter() {
            let pat = match glob::Pattern::new(&r.rule) {
                Ok(pat) => pat,
                Err(e) => {
                    tx.send(Activity::Error(format!(
                        "glob {:?} error: {:?}",
                        r.rule, e,
                    )))
                    .unwrap();
                    continue;
                }
            };

            if r.ignore {
                /*
                 * The user wants to prevent the upload of any file that matches
                 * this glob pattern.  This rule type is mutually exclusive with
                 * all other special directives, and with regular processing.
                 */
                ignore.push(pat);
                continue;
            }

            if r.require_match {
                /*
                 * The user wants to make sure that this pattern matches at
                 * least one uploaded file.  If no file matches, we want to fail
                 * the job.
                 */
                required.push((pat.clone(), false));
            }

            if r.size_change_ok {
                /*
                 * By default, we will fail an upload if the file changes size
                 * while we are trying to upload it.  This is to avoid
                 * accidentally creating a job which terminates while background
                 * processes are still manipulating output artefacts; such
                 * background manipulation will likely lead to incomplete
                 * or otherwise corrupt artefacts and should result in a job
                 * failure.
                 *
                 * In some cases, the user actually doesn't care that
                 * modification may continue after the job has nominally
                 * completed; e.g., if the job is an integration test and the
                 * file to upload is an informational log generated by a process
                 * that may not have been completely torn down at the end of the
                 * job.  We will still warn about the condition, but not fail
                 * the job or the file upload.
                 */
                relaxed.push(pat.clone());
            }

            walk_patterns.push(r.rule.to_string());
        }

        /*
         * Enumerate all of the files we expect to upload.
         */
        for r in rules.iter().filter(|r| !r.ignore) {
            /*
             * Walk the file system and locate output files to
             * upload.
             */
            match glob::glob(r.rule.as_str()) {
                Ok(paths) => {
                    for p in paths {
                        match p {
                            Ok(p) => {
                                if seen.contains(&p) {
                                    continue;
                                }
                                seen.insert(p.clone());

                                if ignore.iter().any(|g| g.matches_path(&p)) {
                                    /*
                                     * This path matches an ignore rule.  We do
                                     * not want to upload it, even if it matches
                                     * other rules.
                                     */
                                    continue;
                                }

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

            /*
             * XXX For now, individual upload size is capped at 1GB.
             */
            if u.size > 1024 * 1024 * 1024 {
                tx.send(Activity::Error(format!(
                    "file {:?} is {} bytes in size, which is larger than 1GiB \
                    and cannot be uploaded at this time.",
                    u.path, u.size,
                )))
                .unwrap();
                continue;
            }

            /*
             * Determine whether we care about the file changing size during
             * upload.
             */
            let change_ok = relaxed.iter().any(|g| g.matches_path(&u.path));

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
                let msg = format!(
                    "file {:?} changed size mid upload: {} -> {}",
                    u.path, u.size, total
                );

                if change_ok {
                    tx.send(Activity::Warning(msg)).unwrap();
                } else {
                    tx.send(Activity::Error(msg)).unwrap();
                    continue;
                }

                /*
                 * XXX For now, individual upload size is capped at 1GB.
                 */
                if total > 1024 * 1024 * 1024 {
                    tx.send(Activity::Error(format!(
                        "file {:?} is {} bytes in size, which is larger \
                        than 1GiB and cannot be uploaded at this time.",
                        u.path, total,
                    )))
                    .unwrap();
                    continue;
                }
            }

            cw.output(&u.path, total, chunks.as_slice()).await;

            tx.send(Activity::Uploaded(u.path.clone())).unwrap();

            /*
             * Mark as used any rule that requires a match and which matches
             * the path of the file we just uploaded.
             */
            required.iter_mut().for_each(|(g, used)| {
                if g.matches_path(&u.path) {
                    *used = true;
                }
            });
        }

        /*
         * Make sure all rules which required at least one match were satisfied:
         */
        for (g, used) in required.iter() {
            if !used {
                tx.send(Activity::Error(format!(
                    "rule \"{}\" required a match, but was not used",
                    g,
                )))
                .unwrap();
            }
        }

        tx.send(Activity::Complete).unwrap();
    });

    rx
}
