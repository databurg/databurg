use base64::Engine as _;
use log::error;
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Component, Path, PathBuf};
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
    /// Byte-exact local path of the file. `file_path` is lossy for names that
    /// are not valid UTF-8; this one is used to actually open the file. Never
    /// serialized, so the wire format stays unchanged.
    #[serde(skip)]
    pub file_path_os: Option<PathBuf>,
    /// Base64 of the raw wire-path bytes. Only set when the name is not valid
    /// UTF-8, so peers that predate the field keep seeing exactly the frames
    /// they already know.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_path_raw: Option<String>,
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
            file_path_os: None,
            file_path_raw: None,
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

impl InnerJob {
    /// The path used to open the file locally: the byte-exact OS path when
    /// present, otherwise the (possibly lossy) wire string.
    pub fn source_path(&self) -> PathBuf {
        match &self.file_path_os {
            Some(path) => path.clone(),
            None => PathBuf::from(self.file_path.clone().unwrap_or_default()),
        }
    }

    /// Sets the wire path from a local relative path: the lossy string for
    /// display and legacy peers, plus the raw bytes when (and only when) the
    /// name is not valid UTF-8.
    pub fn set_wire_path(&mut self, path: &Path) {
        self.file_path = Some(path.display().to_string());
        self.file_path_raw = path.to_str().is_none().then(|| encode_raw_path(path));
    }

    /// The peer-supplied path, decoded byte-exact where possible and stripped
    /// of absolute and parent components so it can never leave the directory
    /// it is joined to.
    pub fn wire_path(&self) -> PathBuf {
        let path = self
            .file_path_raw
            .as_deref()
            .and_then(decode_raw_path)
            .unwrap_or_else(|| PathBuf::from(self.file_path.clone().unwrap_or_default()));
        sanitize_wire_path(&path)
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
    auth_token: Option<String>,
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
    /// Byte-exact companions for `file_list` entries whose names are not
    /// valid UTF-8: pairs of (lossy entry, base64 raw bytes). Legacy peers
    /// ignore it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_list_raw: Option<Vec<(String, String)>>,
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
    /// Base64 of the raw path bytes; only set when the name is not valid
    /// UTF-8. Legacy peers ignore it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_path_raw: Option<String>,
    pub file_hash: String,
}

impl PreflightFileInfo {
    /// Same contract as [`InnerJob::wire_path`]: byte-exact where possible,
    /// stripped of absolute and parent components.
    pub fn wire_path(&self) -> PathBuf {
        let path = self
            .file_path_raw
            .as_deref()
            .and_then(decode_raw_path)
            .unwrap_or_else(|| PathBuf::from(self.file_path.clone()));
        sanitize_wire_path(&path)
    }
}

/// Encodes the raw bytes of a path for the wire (base64).
pub fn encode_raw_path(path: &Path) -> String {
    base64::engine::general_purpose::STANDARD.encode(path.as_os_str().as_bytes())
}

/// Decodes a wire-encoded raw path back into its byte-exact form.
pub fn decode_raw_path(encoded: &str) -> Option<PathBuf> {
    base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .ok()
        .map(|bytes| PathBuf::from(OsString::from_vec(bytes)))
}

/// Reduces a peer-supplied path to its normal components: root markers, `.`
/// and `..` are dropped, so the result can be joined to a local base
/// directory without ever escaping it.
pub fn sanitize_wire_path(path: &Path) -> PathBuf {
    path.components()
        .filter_map(|component| match component {
            Component::Normal(part) => Some(part),
            _ => None,
        })
        .collect()
}

/// True for a well-formed content hash: exactly 64 lowercase hex characters.
/// Peer-supplied hashes become the on-disk blob file name, so anything else
/// (path separators, `..`, wrong length) must be rejected before use.
pub fn is_valid_hash(hash: &str) -> bool {
    hash.len() == 64
        && hash
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Returns the last `count` normal components of `path` as a relative path.
/// Used to recover a byte-exact relative path from a longer absolute one when
/// only the trailing depth is known.
pub fn tail_components(path: &Path, count: usize) -> PathBuf {
    let parts: Vec<Component> = path
        .components()
        .filter(|c| matches!(c, Component::Normal(_)))
        .collect();
    let start = parts.len().saturating_sub(count);
    parts[start..].iter().collect()
}

/// Resolves a `file_list` entry to its byte-exact path: if `file_list_raw`
/// maps the lossy string to raw bytes, those win, otherwise the string
/// itself is the path.
pub fn resolve_raw_entry(entry: &str, raw_list: &Option<Vec<(String, String)>>) -> PathBuf {
    raw_list
        .as_ref()
        .and_then(|pairs| pairs.iter().find(|(lossy, _)| lossy == entry))
        .and_then(|(_, encoded)| decode_raw_path(encoded))
        .unwrap_or_else(|| PathBuf::from(entry))
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;

    #[test]
    fn source_path_prefers_byte_exact_os_path() {
        // "prä.txt" with a Latin-1 0xE4, which is not valid UTF-8
        let raw = OsString::from_vec(vec![b'p', b'r', 0xE4, b'.', b't', b'x', b't']);
        let job = InnerJob {
            file_path: Some("pr\u{FFFD}.txt".to_string()),
            file_path_os: Some(PathBuf::from(raw.clone())),
            ..Default::default()
        };
        assert_eq!(job.source_path(), PathBuf::from(raw));
    }

    #[test]
    fn source_path_falls_back_to_the_wire_string() {
        let job = InnerJob {
            file_path: Some("some/file.txt".to_string()),
            ..Default::default()
        };
        assert_eq!(job.source_path(), PathBuf::from("some/file.txt"));
    }

    #[test]
    fn os_path_never_hits_the_wire() {
        let job = InnerJob {
            file_path: Some("a.txt".to_string()),
            file_path_os: Some(PathBuf::from("a.txt")),
            ..Default::default()
        };
        let json = serde_json::to_value(&job).unwrap();
        assert!(json.get("file_path_os").is_none());

        // Frames from peers that predate the field must still parse
        let legacy = r#"{"job_id":1,"file_path":"a.txt","bucket":"b","file_hash":"h","mode":420,"uid":33,"gid":33,"accessed":null,"modified":null,"created":null,"status":"Pending","retries":0,"is_dir":false}"#;
        let parsed: InnerJob = serde_json::from_str(legacy).unwrap();
        assert!(parsed.file_path_os.is_none());
        assert_eq!(parsed.file_path.as_deref(), Some("a.txt"));
        assert!(parsed.file_path_raw.is_none());
    }

    #[test]
    fn raw_wire_path_roundtrip() {
        let raw = OsString::from_vec(vec![
            b'a', b'/', b'p', b'r', 0xE4, b's', b'.', b'p', b'p', b't',
        ]);
        let path = PathBuf::from(raw);
        let mut job = InnerJob::default();
        job.set_wire_path(&path);
        assert!(job.file_path_raw.is_some());
        assert_eq!(job.wire_path(), path);

        // Clean UTF-8 names carry no raw companion
        let mut clean = InnerJob::default();
        clean.set_wire_path(Path::new("a/b.txt"));
        assert!(clean.file_path_raw.is_none());
        assert_eq!(clean.wire_path(), PathBuf::from("a/b.txt"));
    }

    #[test]
    fn wire_path_neutralizes_traversal() {
        let mut job = InnerJob::default();
        job.file_path = Some("../../etc/passwd".to_string());
        assert_eq!(job.wire_path(), PathBuf::from("etc/passwd"));

        job.file_path = Some("/etc/cron.d/evil".to_string());
        assert_eq!(job.wire_path(), PathBuf::from("etc/cron.d/evil"));

        // The raw field is sanitized exactly like the string
        let mut raw_job = InnerJob::default();
        raw_job.file_path_raw = Some(encode_raw_path(Path::new("../up/x")));
        assert_eq!(raw_job.wire_path(), PathBuf::from("up/x"));
    }

    #[test]
    fn is_valid_hash_rejects_non_hex_and_traversal() {
        assert!(is_valid_hash(&"a".repeat(64)));
        assert!(is_valid_hash(&"0123456789abcdef".repeat(4)));
        assert!(!is_valid_hash(&"A".repeat(64))); // uppercase
        assert!(!is_valid_hash(&"a".repeat(63))); // too short
        assert!(!is_valid_hash("../../../../etc/cron.d/evil"));
        assert!(!is_valid_hash("deadbeef"));
    }

    #[test]
    fn tail_components_takes_the_trailing_path() {
        let path = Path::new("/srv/storage/wh/src/sub/file.txt");
        assert_eq!(tail_components(path, 2), PathBuf::from("sub/file.txt"));
        assert_eq!(tail_components(path, 1), PathBuf::from("file.txt"));
        // Asking for more than exist yields all normal components
        assert_eq!(tail_components(Path::new("a/b"), 9), PathBuf::from("a/b"));
    }

    #[test]
    fn resolve_raw_entry_prefers_mapped_bytes() {
        let raw = OsString::from_vec(vec![b'p', b'r', 0xE4, b's']);
        let pairs = Some(vec![(
            "pr\u{FFFD}s".to_string(),
            encode_raw_path(Path::new(&raw)),
        )]);
        assert_eq!(resolve_raw_entry("pr\u{FFFD}s", &pairs), PathBuf::from(raw));
        assert_eq!(
            resolve_raw_entry("plain.txt", &pairs),
            PathBuf::from("plain.txt")
        );
    }
}
