use crate::connect;
use chrono::{DateTime, Utc};
use clap::ArgMatches;
use foundation::commands::preflight;
use foundation::{send_raw, InnerJob, Job, Response, Status};
use foundation::{AckType, Action, CommandData, PreflightFileInfo, SyncMetadata, Tag};
#[allow(unused_imports)]
use log::{debug, error, info};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::env;
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::process::exit;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use threadpool::ThreadPool;
use tokio::sync::mpsc::channel;
use tokio::time::sleep;
use walkdir::WalkDir;

pub async fn handler(args: &ArgMatches) -> Result<(), ()> {
    let mut source_dir = match args.get_one::<String>("source") {
        Some(source) => {
            let mut source = source.to_string();
            if source.starts_with("./") {
                source = source[2..].to_string();
            }
            source
        }
        None => {
            error!("Source directory is required");
            exit(1);
        }
    };

    let _base_dir = match source_dir.ends_with("/") {
        true => source_dir.clone(),
        false => format!("{}/", source_dir.clone()),
    };

    let mut base_dir = None;
    let sd = source_dir.clone();
    if !sd.ends_with("/") {
        let mut sd = sd.split("/").collect::<Vec<&str>>();
        if sd.len() > 1 {
            base_dir = sd.pop();
            source_dir = sd.join("/").to_string();
        }
        drop(sd);
    }

    env::set_current_dir(&source_dir).unwrap();

    let bucket = match args.get_one::<String>("bucket") {
        Some(bucket) => bucket,
        None => {
            error!("Bucket is required");
            exit(1);
        }
    };

    let tags_temp = match args.get_one::<String>("tags") {
        Some(tags) => tags,
        None => "",
    };

    let tags_temp = tags_temp.split(";").collect::<Vec<&str>>();
    let mut tags: Vec<Tag> = vec![];
    for tag in tags_temp.clone() {
        let tag = tag.split("=").collect::<Vec<&str>>();
        if tag.len() == 2 {
            tags.push(Tag {
                key: tag[0].to_string(),
                value: tag[1].to_string(),
            });
        }
    }

    let (mut senders, handles, mut raw_channels) = connect::sync_threaded(10).await;

    let job_id = Arc::new(Mutex::new(0));
    let jobs = Arc::new(Mutex::new(Vec::new()));
    let pool = ThreadPool::new(8); // Set the pool size to the number of threads you want

    for file in WalkDir::new(base_dir.unwrap_or("."))
        .into_iter()
        .filter_map(|e| e.ok())
    {
        if !file.metadata().unwrap().is_file() {
            continue;
        }
        let file = file.path().to_path_buf();
        let job_id = Arc::clone(&job_id);
        let jobs = Arc::clone(&jobs);
        let bucket = bucket.clone();

        pool.execute(move || {
            let mut file = file.clone();
            let file_path = Some(file.display().to_string());

            // let mut symlink_to: Option<String> = None;
            if file.is_symlink() {
                match file.read_link() {
                    Ok(link) => {
                        info!("Symlink: {:?}", link);
                        file = link;
                        // symlink_to = Some(link.display().to_string());
                    }
                    Err(e) => {
                        error!("Could not read symlink: {:?}", e);
                    }
                };
            }

            let file_hash = match fs::File::open(&file) {
                Ok(f) => {
                    let mut hasher = Sha256::new();

                    // Add more metadata to the hasher to void collision
                    let meta_modified: DateTime<Utc> =
                        file.metadata().unwrap().modified().unwrap().into();
                    hasher.update(meta_modified.format("%Y-%m-%d %H:%M:%S").to_string());

                    // Add filename
                    if let Some(filename) = file.file_name().unwrap().to_str() {
                        hasher.update(filename);
                    }

                    drop(f);
                    format!("{:x}", hasher.finalize())
                }
                Err(_) => return,
            };

            let meta = file.metadata().unwrap();
            let modified = meta.modified().unwrap();

            // Create Job struct and add to jobs
            let mut job_list = jobs.lock().unwrap();
            let mut id = job_id.lock().unwrap();
            *id += 1;

            job_list.push(Job {
                inner: InnerJob {
                    file_hash: Some(file_hash),
                    job_id: Some(*id),
                    status: Status::Pending,
                    accessed: Some(meta.accessed().unwrap()),
                    modified: Some(modified),
                    created: Some(meta.created().unwrap_or(modified)),
                    mode: Some(meta.mode()),
                    uid: Some(meta.uid()),
                    gid: Some(meta.gid()),
                    file_path,
                    bucket: Some(bucket.clone()),
                    retries: 0,
                    is_dir: false,
                },
                status_sender: None,
            });
        });
    }

    // Wait for all threads to finish
    pool.join();

    // Now `jobs` contains all the Job entries
    let mut final_jobs = jobs.lock().unwrap();

    let mut ack_count = 0;
    let mut skip_count = 0;
    let mut nack_count = 0;

    let raw_channel = raw_channels.pop().unwrap();

    let file_list: Vec<PreflightFileInfo> = final_jobs
        .iter()
        .map(|j| PreflightFileInfo {
            file_path: j.inner.clone().file_path.unwrap(),
            file_hash: j.inner.clone().file_hash.unwrap(),
        })
        .collect();

    let file_list_hashes: HashSet<_>;
    match preflight(bucket.to_string(), file_list).await {
        Ok(fl) => {
            file_list_hashes = fl.iter().map(|x| x.file_hash.clone()).collect();
            fl
        }
        _ => {
            error!("Could not complete preflight..");
            exit(1);
        }
    };

    final_jobs.iter_mut().for_each(|j| {
        if !file_list_hashes.contains(&j.inner.file_hash.clone().unwrap()) {
            j.inner.status = Status::Confirmed;
            skip_count += 1;
        }
    });

    let file_list = final_jobs
        .iter()
        .map(|j| j.inner.file_path.as_ref().unwrap().to_string())
        .collect();

    // sending retained file jobs (some may have vanished now as preflight removed unnecessarry uploads)

    'exit: loop {
        let mut left_jobs = 0;
        'next_file: while let Some(this_job) = final_jobs.iter_mut().next() {
            if this_job.inner.status == Status::Pending {
                loop {
                    for job_sender in senders.clone().iter() {
                        if job_sender.capacity() > 0 {
                            left_jobs += 1;
                            let (tx, mut rx) = channel::<Response>(1);
                            this_job.status_sender = Some(tx);
                            // info!("Sending job.. {:?}", this_job);
                            if job_sender.send(Some(this_job.clone())).await.is_err() {
                                error!("Could not send job");
                                break 'exit;
                            }
                            //info!("Waiting for response..");
                            let response = rx.recv().await.unwrap();
                            if response.ack_type == Some(AckType::Ack) {
                                ack_count += 1;
                            } else if response.ack_type == Some(AckType::Skip) {
                                skip_count += 1;
                            } else {
                                // AckType::Nack
                                nack_count += 1;
                            }
                            if response.success {
                                this_job.inner.status = Status::Confirmed;
                            } else {
                                this_job.inner.retries += 1;
                                if this_job.inner.retries > 3 {
                                    this_job.inner.status = Status::Failure;
                                }
                            }
                            break 'next_file;
                        }
                    }
                    _ = sleep(Duration::from_millis(50)).await;
                }
            }
            final_jobs.retain(|j| j.inner.status != Status::Confirmed);
        }
        if left_jobs == 0 {
            break;
        }
    }

    let _res = send_raw(
        &raw_channel,
        Some(
            serde_json::to_string(&CommandData {
                action: Action::SetSyncMetadata,
                query: None,
            })
            .unwrap()
            .into_bytes(),
        ),
    )
    .await;

    let _res = send_raw(
        &raw_channel,
        Some(
            serde_json::to_string(&SyncMetadata {
                tags,
                timestamp: std::time::SystemTime::now(),
                bucket: bucket.clone(),
                ack_count,
                nack_count,
                skip_count,
                file_list: Some(file_list),
            })
            .unwrap()
            .into_bytes(),
        ),
    )
    .await;

    for queue in senders.iter_mut() {
        _ = queue.send(None).await;
    }

    _ = futures_util::future::join_all(handles).await;
    Ok(())
}
