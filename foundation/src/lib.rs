use log::error;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::{channel, Receiver, Sender};

pub mod actor;
pub mod commands;
pub mod env;
pub mod fs;
pub mod protocol;

type TlsStream<S> = tokio_rustls::TlsStream<S>;

#[derive(Debug, Clone)]
pub struct FileInfo {
    pub file_name: String,
    pub path_internal: Option<PathBuf>,
    pub bucket: String,
    pub meta: std::fs::Metadata,
}

#[derive(Debug, Clone)]
pub struct Job {
    pub inner: InnerJob,
    pub status_sender: Option<Sender<Response>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InnerJob {
    pub job_id: Option<u64>,
    pub file_path: Option<String>,
    pub bucket: Option<String>,
    pub file_hash: Option<String>,
    pub mode: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub accessed: Option<std::time::SystemTime>,
    pub modified: Option<std::time::SystemTime>,
    pub created: Option<std::time::SystemTime>,
    pub status: Status,
    pub retries: u8,
    pub is_dir: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectMetadata {
    pub mode: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub accessed: Option<std::time::SystemTime>,
    pub modified: Option<std::time::SystemTime>,
    pub created: Option<std::time::SystemTime>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum AckType {
    Ack,
    Nack,
    Skip,
}

impl Default for InnerJob {
    fn default() -> Self {
        InnerJob {
            mode: None,
            file_hash: None,
            job_id: None,
            file_path: None,
            bucket: None,
            accessed: None,
            modified: None,
            created: None,
            uid: None,
            gid: None,
            status: Status::Pending,
            retries: 0,
            is_dir: false,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum Status {
    Pending,
    Sent,
    Confirmed,
    Failure,
}

#[derive(Debug)]
pub struct Actor<S> {
    pub socket_r: Option<tokio::io::ReadHalf<TlsStream<S>>>,
    pub socket_w: Option<tokio::io::WriteHalf<TlsStream<S>>>,
    pub job_receiver: Option<Receiver<Option<Job>>>,
    pub raw_sender_tx: Sender<ChannelMessage>,
    raw_sender_rx: Receiver<ChannelMessage>,
    handshake_done: bool,
    auth_done: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChannelMessage {
    pub buf: Option<Vec<u8>>,
    #[serde(skip_serializing, skip_deserializing)]
    pub status: Option<Sender<Response>>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Response {
    pub success: bool,
    pub code: Option<u32>,
    pub message: Option<String>,
    pub ack_type: Option<AckType>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Action {
    Preflight,
    SetSyncMetadata,
    Sync,
    Recover,
    Status,
    List,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileActionData {
    pub job: InnerJob,
    pub file_size: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FileSelector {
    pub path: String,
    pub bucket: String,
    // For Point In Time Recovery
    pub point_in_time: Option<SystemTime>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CommandData {
    pub action: Action,
    pub query: Option<FileSelector>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JobStatusMessage {
    pub job_id: u64,
    pub status: Status,
    pub bytes_sent: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct HandshakeData {
    pub client_version: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AuthenticationData {
    pub token: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BucketStatus {
    pub meta: Vec<SyncMetadata>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PointInTime {
    pub point_in_time: SystemTime,
    pub changed_files: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SyncMetadata {
    pub tags: Vec<Tag>,
    pub timestamp: SystemTime,
    pub bucket: String,
    pub ack_count: u64,
    pub nack_count: u64,
    pub skip_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_list: Option<Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Tag {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PreflightRequestData {
    pub file_list: Vec<PreflightFileInfo>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PreflightFileInfo {
    pub file_path: String,
    pub file_hash: String,
}

/// Normalize the remote directory path and makes relative path an absolute
/// path relative to the buckets root
pub fn normalize_relative_save(dir: &str) -> String {
    // If the path is already absolute, return it as is
    if dir.starts_with("/") {
        return dir.to_string();
    }
    // If the path is empty, return the root path
    if dir.is_empty() {
        return "/".to_string();
    }
    // If the path is relative, make it absolute
    if dir.starts_with("./") {
        return format!("/{}", dir[2..dir.len()].to_string());
    }
    if dir.starts_with(".") {
        return format!("/{}", dir[1..dir.len()].to_string());
    }
    // If the path contains .., remove this part
    return dir.replace("..", "").replace("//", "/");
}

/// Normalize the local directory path and allows for relative paths
pub fn normalize_localdir(dir: &str) -> Result<PathBuf, std::io::Error> {
    return Path::new(dir).canonicalize();
}

pub async fn send_raw(
    raw_channel: &Sender<Option<ChannelMessage>>,
    buf: Option<Vec<u8>>,
) -> Result<Option<Response>, ()> {
    let (tx, mut rx) = channel::<Response>(1);
    let raw = ChannelMessage {
        buf,
        status: Some(tx),
    };
    if raw_channel.send(Some(raw)).await.is_err() {
        error!("Could not send metadata");
        return Err(());
    }
    Ok(rx.recv().await)
}

fn truncate_to_seconds(time: SystemTime) -> SystemTime {
    // Calculate the duration since the UNIX_EPOCH
    match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => {
            // Truncate to seconds by taking only the seconds part
            let truncated_duration = Duration::from_secs(duration.as_secs());
            UNIX_EPOCH + truncated_duration
        }
        Err(_) => time, // Handle the unlikely case where time is before the UNIX epoch
    }
}
