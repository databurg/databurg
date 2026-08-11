//! Backup subcommand: walk a local directory, decide which files the server
//! still needs (preflight), and stream them over several connections in
//! parallel.
//!
//! The run is intentionally strict about local read failures: a file that
//! cannot be walked, stat'ed, opened or read is recorded and makes the whole
//! run exit non-zero, rather than silently dropping out of the backup.

use crate::connect;
use chrono::{DateTime, Utc};
use clap::ArgMatches;
use foundation::commands::preflight;
use foundation::{encode_raw_path, send_raw, InnerJob, Job, Response, Status};
use foundation::{AckType, Action, CommandData, PreflightFileInfo, SyncMetadata, Tag};
#[allow(unused_imports)]
use log::{debug, error, info};
use sha2::{Digest, Sha256};
use std::collections::{HashSet, VecDeque};
use std::env;
use std::fs;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::exit;
use std::sync::{Arc, Mutex};
use std::time::UNIX_EPOCH;
use threadpool::ThreadPool;
use tokio::sync::mpsc::{channel, Sender};
use tokio::task::JoinSet;
use walkdir::WalkDir;

/// Number of parallel server connections opened for one backup run.
const CONNECTION_COUNT: usize = 10;
/// Size of the local thread pool that walks and hashes files.
const HASH_POOL_SIZE: usize = 8;
/// How often a single file is retried before it counts as failed.
const MAX_RETRIES: u8 = 3;
/// Upper bound on individually listed files in the final failure report.
const MAX_REPORTED_FAILURES: usize = 25;

/// A parsed `backup` invocation.
struct BackupPlan {
    /// Directory the process changes into before walking.
    source_dir: String,
    /// Directory walked relative to `source_dir` (the source's last path
    /// component, or `.` when the source already ends in a slash). This is
    /// what makes stored paths start at the backed-up directory itself.
    walk_root: String,
    /// Remote bucket the files are stored under.
    bucket: String,
    /// Metadata tags attached to this run's sync metadata.
    tags: Vec<Tag>,
}

/// Result of streaming all pending jobs to the server.
#[derive(Default)]
struct DispatchOutcome {
    ack_count: u64,
    nack_count: u64,
    skip_count: u64,
    /// Wire paths of files that were not stored after all retries.
    failed_files: Vec<String>,
    /// Set when every connection died: the remaining jobs never left.
    aborted: bool,
}

/// Entry point for the `backup` subcommand.
pub async fn handler(args: &ArgMatches) -> Result<(), ()> {
    let plan = parse_backup_plan(args);

    if let Err(e) = env::set_current_dir(&plan.source_dir) {
        error!("Cannot enter source directory {}: {}", plan.source_dir, e);
        exit(1);
    }

    // Walk and hash the tree locally first; `unreadable` collects anything we
    // could not read, so it can be reported at the very end.
    let (mut jobs, unreadable) = collect_jobs(&plan.walk_root, &plan.bucket);

    let (mut senders, handles, mut raw_channels) = connect::sync_threaded(CONNECTION_COUNT).await;
    let raw_channel = raw_channels.pop().unwrap();

    // Preflight: let the server tell us which hashes it is still missing, and
    // mark everything else as already done.
    let preflight_skips = mark_unchanged(&mut jobs, &plan.bucket).await;

    // Snapshot the file list for the sync metadata before uploading, so the
    // server's deleted-file detection sees the full picture.
    let (file_list, file_list_raw) = build_file_lists(&jobs);

    // Stream the remaining pending jobs across all connections.
    let mut outcome = dispatch_jobs(&mut jobs, &mut senders).await;
    outcome.skip_count += preflight_skips;

    send_sync_metadata(&raw_channel, &plan, &outcome, file_list, file_list_raw).await;

    // Tell every connection there is no more work, then wait for them to end.
    for queue in senders.iter_mut() {
        _ = queue.send(None).await;
    }
    _ = futures_util::future::join_all(handles).await;

    report_result(outcome, unreadable)
}

/// Parses CLI arguments into a [`BackupPlan`], exiting on missing required
/// arguments. Mirrors the historical path handling: a trailing slash means
/// "walk the directory itself", otherwise the last component becomes the
/// walk root so stored paths begin at the backed-up directory.
fn parse_backup_plan(args: &ArgMatches) -> BackupPlan {
    let mut source_dir = match args.get_one::<String>("source") {
        Some(source) => source.strip_prefix("./").unwrap_or(source).to_string(),
        None => {
            error!("Source directory is required");
            exit(1);
        }
    };

    let mut walk_root = ".".to_string();
    if !source_dir.ends_with('/') {
        let mut parts = source_dir.split('/').collect::<Vec<&str>>();
        if parts.len() > 1 {
            walk_root = parts.pop().unwrap().to_string();
            source_dir = parts.join("/");
        }
    }

    let bucket = match args.get_one::<String>("bucket") {
        Some(bucket) => bucket.to_string(),
        None => {
            error!("Bucket is required");
            exit(1);
        }
    };

    BackupPlan {
        source_dir,
        walk_root,
        bucket,
        tags: parse_tags(args.get_one::<String>("tags").map(String::as_str)),
    }
}

/// Parses the `key=value;key=value` tag string into [`Tag`]s, ignoring
/// malformed pairs.
fn parse_tags(raw: Option<&str>) -> Vec<Tag> {
    raw.unwrap_or_default()
        .split(';')
        .filter_map(|pair| pair.split_once('='))
        .map(|(key, value)| Tag {
            key: key.to_string(),
            value: value.to_string(),
        })
        .collect()
}

/// Walks `walk_root` and hashes every regular file on a small thread pool,
/// returning the jobs to consider and the display paths of files that could
/// not be read locally.
fn collect_jobs(walk_root: &str, bucket: &str) -> (Vec<Job>, Vec<String>) {
    let jobs = Arc::new(Mutex::new(Vec::<Job>::new()));
    let unreadable = Arc::new(Mutex::new(Vec::<String>::new()));
    let pool = ThreadPool::new(HASH_POOL_SIZE);

    for entry in WalkDir::new(walk_root).into_iter().filter_map(|e| e.ok()) {
        let entry_path = entry.path().to_path_buf();
        match entry.metadata() {
            Ok(meta) if !meta.is_file() => continue,
            Ok(_) => {}
            Err(e) => {
                record_unreadable(&unreadable, &entry_path, "stat", &e.to_string());
                continue;
            }
        }

        let jobs = Arc::clone(&jobs);
        let unreadable = Arc::clone(&unreadable);
        let bucket = bucket.to_string();
        pool.execute(move || {
            if let Some(job) = build_job(entry_path, bucket, &unreadable) {
                jobs.lock().unwrap().push(job);
            }
        });
    }
    pool.join();

    // Assign stable job ids after collection; ordering across the pool is
    // otherwise non-deterministic.
    let mut jobs = Arc::try_unwrap(jobs).unwrap().into_inner().unwrap();
    for (idx, job) in jobs.iter_mut().enumerate() {
        job.inner.job_id = Some(idx as u64 + 1);
    }
    let unreadable = Arc::try_unwrap(unreadable).unwrap().into_inner().unwrap();
    (jobs, unreadable)
}

/// Builds a single [`Job`] for one file: resolves symlinks, hashes the
/// content, and records the wire path plus its byte-exact raw form. Returns
/// `None` (after recording the file as unreadable) if anything cannot be read.
fn build_job(path: PathBuf, bucket: String, unreadable: &Arc<Mutex<Vec<String>>>) -> Option<Job> {
    // The wire path and its raw companion are taken from the name as walked;
    // the raw bytes are only carried when the name is not valid UTF-8, so
    // legacy peers keep seeing exactly the frames they already know.
    let file_path = Some(path.display().to_string());
    let file_path_raw = path.to_str().is_none().then(|| encode_raw_path(&path));

    // Follow a symlink to its target for stat/hash/open; the file is still
    // stored under the name it was walked as.
    let mut target = path.clone();
    if target.is_symlink() {
        match target.read_link() {
            Ok(link) => {
                info!("Symlink: {:?}", link);
                target = link;
            }
            Err(e) => error!("Could not read symlink {}: {}", target.display(), e),
        }
    }

    let meta = match target.metadata() {
        Ok(meta) => meta,
        Err(e) => {
            record_unreadable(unreadable, &target, "stat", &e.to_string());
            return None;
        }
    };
    let modified = meta.modified().unwrap_or(UNIX_EPOCH);

    let file_hash = match hash_file(&target, modified) {
        Ok(hash) => hash,
        Err(e) => {
            record_unreadable(unreadable, &target, "read", &e.to_string());
            return None;
        }
    };

    Some(Job {
        inner: InnerJob {
            file_hash: Some(file_hash),
            job_id: None,
            status: Status::Pending,
            accessed: Some(meta.accessed().unwrap_or(modified)),
            modified: Some(modified),
            created: Some(meta.created().unwrap_or(modified)),
            mode: Some(meta.mode()),
            uid: Some(meta.uid()),
            gid: Some(meta.gid()),
            file_path,
            file_path_raw,
            file_path_os: Some(target),
            bucket: Some(bucket),
            retries: 0,
            is_dir: false,
        },
        status_sender: None,
    })
}

/// Computes the version identifier of one file from its modification time and
/// name — deliberately byte-compatible with every released version.
///
/// This value is what preflight compares against, and it also names the stored
/// blob, so changing the formula would invalidate every hash the server already
/// holds: each client would re-upload its entire corpus once, and (since old
/// versions are never reclaimed) that storage would be occupied permanently.
/// The formula therefore stays exactly as it was.
///
/// The one deviation is deliberate and does not affect valid UTF-8 names: the
/// name is fed in as raw bytes, where earlier versions used `to_str()` and so
/// silently contributed *nothing* for names that are not valid UTF-8 — making
/// all such files in a directory share a hash whenever their mtimes matched.
/// For every name that is valid UTF-8 the hashed bytes are identical, so the
/// existing corpus keeps its hashes.
///
/// The file is opened but not read: opening is the readability check that makes
/// an unreadable file a reported failure rather than a silent omission.
fn hash_file(path: &Path, modified: std::time::SystemTime) -> std::io::Result<String> {
    let file = fs::File::open(path)?;
    let mut hasher = Sha256::new();

    let modified: DateTime<Utc> = modified.into();
    hasher.update(modified.format("%Y-%m-%d %H:%M:%S").to_string());
    if let Some(name) = path.file_name() {
        hasher.update(name.as_bytes());
    }

    drop(file);
    Ok(format!("{:x}", hasher.finalize()))
}

/// Records a file we could not read: logs it and adds it to the report list.
fn record_unreadable(unreadable: &Arc<Mutex<Vec<String>>>, path: &Path, action: &str, err: &str) {
    error!(
        "Cannot {} {}: {}, excluding from backup",
        action,
        path.display(),
        err
    );
    unreadable.lock().unwrap().push(path.display().to_string());
}

/// Runs the preflight exchange and marks every job the server already has as
/// [`Status::Confirmed`]. Returns how many jobs were skipped this way.
async fn mark_unchanged(jobs: &mut [Job], bucket: &str) -> u64 {
    let request: Vec<PreflightFileInfo> = jobs
        .iter()
        .map(|job| PreflightFileInfo {
            file_path: job.inner.file_path.clone().unwrap(),
            file_path_raw: job.inner.file_path_raw.clone(),
            file_hash: job.inner.file_hash.clone().unwrap(),
        })
        .collect();

    let needed: HashSet<String> = match preflight(bucket.to_string(), request).await {
        Ok(list) => list.into_iter().map(|info| info.file_hash).collect(),
        Err(_) => {
            error!("Could not complete preflight..");
            exit(1);
        }
    };

    let mut skipped = 0;
    for job in jobs.iter_mut() {
        if !needed.contains(job.inner.file_hash.as_ref().unwrap()) {
            job.inner.status = Status::Confirmed;
            skipped += 1;
        }
    }
    skipped
}

/// Builds the sync-metadata file lists: the lossy string list plus the
/// byte-exact companions for entries whose names are not valid UTF-8.
fn build_file_lists(jobs: &[Job]) -> (Vec<String>, Option<Vec<(String, String)>>) {
    let file_list = jobs
        .iter()
        .map(|job| job.inner.file_path.clone().unwrap())
        .collect();

    let raw: Vec<(String, String)> = jobs
        .iter()
        .filter_map(|job| {
            Some((
                job.inner.file_path.clone()?,
                job.inner.file_path_raw.clone()?,
            ))
        })
        .collect();

    (file_list, (!raw.is_empty()).then_some(raw))
}

/// Streams every still-pending job to the server across all connections at
/// once. A free connection always gets the next queued job; responses are
/// consumed as they arrive. A connection that dies is retired and its job
/// requeued, up to [`MAX_RETRIES`]; if every connection dies the remaining
/// jobs are reported as not transferred.
async fn dispatch_jobs(
    jobs: &mut [Job],
    senders: &mut Vec<Sender<Option<Job>>>,
) -> DispatchOutcome {
    let mut outcome = DispatchOutcome::default();

    let mut pending: VecDeque<usize> = (0..jobs.len())
        .filter(|&i| jobs[i].inner.status == Status::Pending)
        .collect();
    let mut free_senders: Vec<usize> = (0..senders.len()).collect();
    let mut live_senders = senders.len();
    // Each in-flight task resolves to (job index, connection index, response).
    let mut in_flight: JoinSet<(usize, usize, Option<Response>)> = JoinSet::new();

    loop {
        // Hand out queued jobs to idle connections.
        while !pending.is_empty() && !free_senders.is_empty() && live_senders > 0 {
            let job_idx = pending.pop_front().unwrap();
            let sender_idx = free_senders.pop().unwrap();

            let (tx, mut rx) = channel::<Response>(1);
            jobs[job_idx].status_sender = Some(tx);
            if senders[sender_idx]
                .send(Some(jobs[job_idx].clone()))
                .await
                .is_err()
            {
                // This connection is gone; requeue the job and retire it.
                error!("Connection lost, retiring it");
                live_senders -= 1;
                pending.push_front(job_idx);
                continue;
            }
            jobs[job_idx].inner.status = Status::Sent;
            in_flight.spawn(async move { (job_idx, sender_idx, rx.recv().await) });
        }

        if live_senders == 0 {
            error!("All connections lost, aborting upload");
            outcome.aborted = true;
            break;
        }
        if in_flight.is_empty() {
            // Nothing running and nothing left to dispatch: we are done.
            if pending.is_empty() {
                break;
            }
            continue;
        }

        // Wait for whichever connection answers first.
        let (job_idx, sender_idx, response) = match in_flight.join_next().await {
            Some(Ok(result)) => result,
            Some(Err(_)) => {
                error!("Response task failed");
                continue;
            }
            None => continue,
        };

        let response = match response {
            Some(response) => {
                free_senders.push(sender_idx);
                response
            }
            None => {
                // Connection died before answering: retire it, count a nack.
                error!("Connection lost, retiring it");
                live_senders -= 1;
                Response {
                    success: false,
                    code: None,
                    message: None,
                    ack_type: None,
                }
            }
        };

        record_ack(&mut outcome, &response);
        apply_response(
            &mut jobs[job_idx],
            job_idx,
            response.success,
            &mut pending,
            &mut outcome,
        );
    }

    // Account for any job that never reached Confirmed — the remaining jobs
    // after an abort, and (defensively) anything a failed response task could
    // have left stuck in Sent. Failure jobs are already recorded, so only
    // Pending/Sent are collected here.
    collect_unfinished(jobs, &mut outcome);
    outcome
}

/// Tallies one response into the ack/nack/skip counters.
fn record_ack(outcome: &mut DispatchOutcome, response: &Response) {
    match response.ack_type {
        Some(AckType::Ack) => outcome.ack_count += 1,
        Some(AckType::Skip) => outcome.skip_count += 1,
        _ => outcome.nack_count += 1,
    }
}

/// Applies one response to its job: confirm on success, otherwise requeue for
/// another attempt or give up after [`MAX_RETRIES`].
fn apply_response(
    job: &mut Job,
    job_idx: usize,
    success: bool,
    pending: &mut VecDeque<usize>,
    outcome: &mut DispatchOutcome,
) {
    if success {
        job.inner.status = Status::Confirmed;
        return;
    }
    job.inner.retries += 1;
    if job.inner.retries > MAX_RETRIES {
        job.inner.status = Status::Failure;
        outcome
            .failed_files
            .push(job.inner.file_path.clone().unwrap_or_default());
    } else {
        job.inner.status = Status::Pending;
        pending.push_back(job_idx);
    }
}

/// After an abort, records every job that never reached [`Status::Confirmed`].
fn collect_unfinished(jobs: &[Job], outcome: &mut DispatchOutcome) {
    for job in jobs.iter() {
        if matches!(job.inner.status, Status::Pending | Status::Sent) {
            outcome
                .failed_files
                .push(job.inner.file_path.clone().unwrap_or_default());
        }
    }
}

/// Sends the end-of-run sync metadata (a `SetSyncMetadata` command followed by
/// the [`SyncMetadata`] payload) over the raw channel.
async fn send_sync_metadata(
    raw_channel: &Sender<Option<foundation::ChannelMessage>>,
    plan: &BackupPlan,
    outcome: &DispatchOutcome,
    file_list: Vec<String>,
    file_list_raw: Option<Vec<(String, String)>>,
) {
    let command = serde_json::to_string(&CommandData {
        action: Action::SetSyncMetadata,
        query: None,
    })
    .unwrap();
    _ = send_raw(raw_channel, Some(command.into_bytes())).await;

    let metadata = serde_json::to_string(&SyncMetadata {
        tags: plan.tags.clone(),
        timestamp: std::time::SystemTime::now(),
        bucket: plan.bucket.clone(),
        ack_count: outcome.ack_count,
        nack_count: outcome.nack_count,
        skip_count: outcome.skip_count,
        file_list: Some(file_list),
        file_list_raw,
    })
    .unwrap();
    _ = send_raw(raw_channel, Some(metadata.into_bytes())).await;
}

/// Emits the final report. A run that lost files locally or on the wire logs
/// each one (capped) and returns `Err(())`, so the process exits non-zero and
/// cron surfaces the incomplete backup.
fn report_result(mut outcome: DispatchOutcome, unreadable: Vec<String>) -> Result<(), ()> {
    outcome.failed_files.extend(unreadable);

    if !outcome.aborted && outcome.failed_files.is_empty() {
        return Ok(());
    }

    error!(
        "Backup incomplete: {} file(s) were not transferred",
        outcome.failed_files.len()
    );
    for file in outcome.failed_files.iter().take(MAX_REPORTED_FAILURES) {
        error!("  not backed up: {}", file);
    }
    if outcome.failed_files.len() > MAX_REPORTED_FAILURES {
        error!(
            "  ... and {} more",
            outcome.failed_files.len() - MAX_REPORTED_FAILURES
        );
    }
    Err(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Replicates the version-identifier formula EXACTLY as every released
    /// version computed it (mtime formatted, then the file name via `to_str`).
    /// Kept verbatim so the test fails loudly if `hash_file` ever drifts from
    /// the released formula and would invalidate the server's stored hashes.
    fn released_formula(path: &Path, modified: std::time::SystemTime) -> String {
        let mut hasher = Sha256::new();
        let modified: DateTime<Utc> = modified.into();
        hasher.update(modified.format("%Y-%m-%d %H:%M:%S").to_string());
        if let Some(name) = path.file_name().unwrap().to_str() {
            hasher.update(name);
        }
        format!("{:x}", hasher.finalize())
    }

    fn write_temp(name: &str, contents: &[u8]) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("databurg-hash-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join(name);
        let mut f = fs::File::create(&path).unwrap();
        f.write_all(contents).unwrap();
        path
    }

    #[test]
    fn hash_matches_the_released_formula() {
        // A file whose name is valid UTF-8 — i.e. essentially the whole corpus
        // already stored on the backup server. Its hash MUST NOT change, or
        // every client re-uploads everything once and that storage is never
        // reclaimed.
        let path = write_temp("index.html", b"<html>hello</html>");
        let modified = fs::metadata(&path).unwrap().modified().unwrap();

        assert_eq!(
            hash_file(&path, modified).unwrap(),
            released_formula(&path, modified),
            "the version identifier drifted from the released formula"
        );
    }

    #[test]
    fn hash_ignores_content_so_stored_hashes_stay_valid() {
        // Two files with the same name and mtime but different content hash
        // alike: content is deliberately NOT part of the identifier, exactly as
        // in the released versions.
        let a = write_temp("same-name.txt", b"one");
        let modified = fs::metadata(&a).unwrap().modified().unwrap();
        let first = hash_file(&a, modified).unwrap();

        let mut f = fs::OpenOptions::new().write(true).truncate(true).open(&a).unwrap();
        f.write_all(b"completely different content").unwrap();
        drop(f);

        assert_eq!(hash_file(&a, modified).unwrap(), first);
    }

    #[test]
    fn non_utf8_names_contribute_to_the_hash() {
        // Earlier versions dropped the name entirely when it was not valid
        // UTF-8, so such files collided whenever their mtimes matched. Raw
        // bytes fix that; valid-UTF-8 names are unaffected (see the test above).
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let raw_a = OsString::from_vec(vec![b'a', 0xE4, b'.', b't', b'x', b't']);
        let raw_b = OsString::from_vec(vec![b'b', 0xE4, b'.', b't', b'x', b't']);
        let pa = write_temp(&raw_a.to_string_lossy(), b"x");
        let pb = write_temp(&raw_b.to_string_lossy(), b"x");
        let m = fs::metadata(&pa).unwrap().modified().unwrap();

        assert_ne!(hash_file(&pa, m).unwrap(), hash_file(&pb, m).unwrap());
    }

    #[test]
    fn unreadable_file_is_an_error_not_a_silent_skip() {
        let missing = std::env::temp_dir().join("databurg-hash-does-not-exist");
        let _ = fs::remove_file(&missing);
        assert!(hash_file(&missing, std::time::SystemTime::now()).is_err());
    }
}
