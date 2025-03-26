use crate::{
    env, truncate_to_seconds, Action, Actor, AuthenticationData, CommandData, FileActionData,
    FileInfo, HandshakeData, InnerJob, Job, ObjectMetadata, PreflightRequestData, Status,
};
use crate::{BucketStatus, FileSelector, SyncMetadata};
use flate2::read::GzDecoder;
#[allow(unused_imports)]
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt::{self, Display, Formatter};
use std::fs;
use std::io::prelude::*;
use std::os::unix::fs::{chown, symlink, PermissionsExt};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::{Duration, SystemTime};
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use walkdir::WalkDir;

// Decompress the file data after receiving
fn decompress_data(data: &[u8]) -> Vec<u8> {
    let mut decoder = GzDecoder::new(data);
    let mut decompressed_data = Vec::new();
    decoder.read_to_end(&mut decompressed_data).unwrap();
    decompressed_data
}

pub struct Handler<S> {
    pub actor: Actor<S>,
    storage_dir: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeletionHistoryItem {
    pub deleted_at: SystemTime,
    pub deleted_til: SystemTime,
}

#[derive(Debug, Clone)]
pub enum Stage {
    Handshake,
    Authentication,
    Command,
    ServerPreflightRequest,
    ServerFileReceiver,
    ServerFileBucketStatus,
    ServerFileList,
    ServerFileSender,
    ServerSetSyncMetadata,
    ServerDataReceiver,
    ClientFileReceiver,
    ClientDataReceiver,
}

impl Display for Stage {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Stage::Handshake => write!(f, "Handshake"),
            Stage::Authentication => write!(f, "Authentication"),
            Stage::Command => write!(f, "Command"),
            Stage::ServerPreflightRequest => write!(f, "ServerPreflightRequest"),
            Stage::ServerFileReceiver => write!(f, "ServerFileReceiver"),
            Stage::ServerFileBucketStatus => write!(f, "ServerFileBucketStatus"),
            Stage::ServerFileList => write!(f, "ServerFileList"),
            Stage::ServerSetSyncMetadata => write!(f, "ServerSetSyncMetadata"),
            Stage::ClientFileReceiver => write!(f, "ClientFileReceiver"),
            Stage::ServerDataReceiver => write!(f, "ServerDataReceiver"),
            Stage::ServerFileSender => write!(f, "ServerFileSender"),
            Stage::ClientDataReceiver => write!(f, "ClientDataReceiver"),
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> Handler<S> {
    pub fn new(actor: Actor<S>, storage_dir: &str) -> Handler<S> {
        Handler {
            actor,
            storage_dir: storage_dir.to_string(),
        }
    }

    /// Constructs the file path based on the storage directory, bucket, and file name.
    fn file_path(&self, bucket: String, file: String) -> PathBuf {
        let mut path = PathBuf::new();
        path.push(&self.storage_dir);
        path.push(bucket);
        path.push(file);
        path
    }

    /// Sets a delete marker for the specified file in the given bucket.
    /// This function creates a `.deleted` file in the file's directory.
    async fn set_delete_marker(&mut self, bucket: String, file: String) -> tokio::io::Result<()> {
        // Construct the path to the .deleted file
        let mut path = self.file_path(bucket, file);
        path.push(".deleted");

        // Create the .deleted file and write "deleted" to it
        let mut f = File::create(path).await?;
        f.write_all(b"deleted").await?;

        Ok(())
    }

    /// Checks if a delete marker exists for the specified file at a given point in time.
    /// If no point in time is provided, it checks if the `.deleted` file exists.
    /// If a point in time is provided, it checks the deletion history to see if the file was deleted at that time.
    async fn has_delete_marker(
        &mut self,
        mut path: PathBuf, // full file path (no hash)
        point_in_time: Option<SystemTime>,
    ) -> Option<PathBuf> {
        if let Some(pit) = point_in_time {
            // Check at specific point in time
            let history_items = self.get_deletion_history_items(path.clone()).await.unwrap();
            let pit = truncate_to_seconds(pit);
            for item in history_items.iter() {
                if truncate_to_seconds(item.deleted_at).le(&pit)
                    && truncate_to_seconds(item.deleted_til).gt(&pit)
                {
                    return Some(path);
                }
            }
        } else {
            // No point in time, so check if .deleted exists
            path.push(".deleted");
            if path.exists() {
                return Some(path);
            }
        }
        None
    }

    /// Unsets the delete marker for the specified file.
    /// This function removes the `.deleted` file and appends the deletion history.
    async fn unset_delete_marker(
        &mut self,
        path: PathBuf, // full file path (no hash)
    ) -> tokio::io::Result<()> {
        let mut deleted_path = path.clone();
        deleted_path.push(".deleted");

        if deleted_path.exists() {
            // Append to deletion history before removing the delete marker
            self.append_to_deletion_history(path).await?;
            fs::remove_file(deleted_path)?;
        }
        Ok(())
    }

    /// Retrieves the deletion history items for the specified file.
    /// This function reads the `.deletion.history` file and parses its content.
    async fn get_deletion_history_items(
        &mut self,
        path: PathBuf, // full file path (no hash)
    ) -> Result<Vec<DeletionHistoryItem>, tokio::io::Error> {
        let mut history_path = path.clone();
        history_path.push(".deletion.history");
        if !history_path.exists() {
            return Ok(vec![]);
        }

        let history_file_content = fs::read_to_string(history_path)?;
        let history_items: Vec<DeletionHistoryItem> =
            serde_json::from_str(&history_file_content).unwrap_or_else(|_| vec![]);

        Ok(history_items)
    }

    /// Sets the deletion history items for the specified file.
    /// This function writes the provided deletion history items to the `.deletion.history` file.
    async fn set_deletion_history_items(
        &mut self,
        path: PathBuf, // full file path (no hash)
        items: Vec<DeletionHistoryItem>,
    ) -> tokio::io::Result<()> {
        let mut history_path = path.clone();
        history_path.push(".deletion.history");
        let history_file_content = serde_json::to_string(&items).unwrap();
        let mut f = File::create(history_path).await?;
        f.write_all(history_file_content.as_bytes()).await?;
        Ok(())
    }

    /// Appends a new deletion history item to the deletion history file.
    /// This function reads the current deletion history, appends a new item,
    /// and writes the updated history back to the `.deletion.history` file.
    async fn append_to_deletion_history(
        &mut self,
        path: PathBuf, // full file path (no hash)
    ) -> tokio::io::Result<()> {
        // Retrieve current deletion history items
        let mut history_items = self.get_deletion_history_items(path.clone()).await?;

        // Create a new deletion history item
        let new_item = DeletionHistoryItem {
            deleted_at: path.metadata()?.created()?,
            deleted_til: SystemTime::now(),
        };

        // Append the new item to the history
        history_items.push(new_item);

        // Write the updated history back to the file
        self.set_deletion_history_items(path, history_items).await
    }

    /// Checks if the given path is the latest version of the file.
    /// This function compares the canonicalized path of the file with the canonicalized path of the "latest" symlink.
    fn is_latest(&self, path: PathBuf) -> bool {
        let mut latest_file = path.clone();
        latest_file.pop();
        latest_file.push("latest");
        latest_file
            .canonicalize()
            .map_or(false, |compare_to| compare_to == path)
    }

    /// Sets the "latest" symlink to point to the given file path.
    /// This function removes any existing "latest" symlink and creates a new one.
    fn set_latest(&mut self, path: PathBuf) -> Result<(), ()> {
        let mut latest_file = path.clone();
        latest_file.pop();
        latest_file.push("latest");

        // Remove existing "latest" symlink if it exists
        if latest_file.is_symlink() {
            if let Err(e) = fs::remove_file(&latest_file) {
                error!(
                    "Could not remove symlink {}: {:?}",
                    latest_file.display(),
                    e
                );
            }
        }

        let file_hash = path.file_name().unwrap().to_str().unwrap();

        info!(
            "Creating symlink from {} to {}",
            latest_file.display(),
            file_hash
        );

        // Create new "latest" symlink
        if let Err(e) = symlink(file_hash, &latest_file) {
            error!(
                "Could not create symlink {}: {:?}",
                latest_file.display(),
                e
            );
        }

        Ok(())
    }

    /// Applies the specified permissions, user ID, and group ID to the given path.
    /// This function sets the file permissions and changes the owner and group of the file.
    async fn apply_permissions(
        &self,
        path: &std::path::PathBuf,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
    ) {
        if let Some(mode) = mode {
            if let Err(e) = fs::set_permissions(path, fs::Permissions::from_mode(mode)) {
                error!("Could not set permissions for {}: {:?}", path.display(), e);
            }
        }

        if let Err(e) = chown(path, uid, gid) {
            error!("Could not set owner/group for {}: {:?}", path.display(), e);
        }
    }

    /// Writes the provided metadata to a `.meta` file in the same directory as the given path.
    /// This function serializes the metadata and writes it to the `.meta` file.
    async fn to_metadata(&self, path: &std::path::PathBuf, mode: InnerJob) {
        let meta = ObjectMetadata {
            mode: mode.mode,
            uid: mode.uid,
            gid: mode.gid,
            accessed: mode.accessed,
            modified: mode.modified,
            created: mode.created,
        };

        let mut meta_path = path.clone();
        meta_path.pop();
        meta_path.push(".meta");
        match fs::write(
            meta_path.clone(),
            serde_json::to_string(&meta).unwrap().into_bytes(),
        ) {
            Ok(_) => {}
            Err(e) => {
                error!(
                    "Could not write metadata to file: {:?} {:?}",
                    meta_path.display().to_string(),
                    e
                );
            }
        }
    }

    /// Retrieves the metadata from the `.meta` file in the same directory as the given path.
    /// This function reads the `.meta` file and parses its content into an `ObjectMetadata` struct.
    async fn from_metadata(&self, path: &std::path::PathBuf) -> Option<ObjectMetadata> {
        let mut meta_path = path.clone();
        meta_path.pop();
        meta_path.push(".meta");

        if !meta_path.exists() {
            return None;
        }

        let file_content = fs::read(meta_path).ok()?;
        serde_json::from_str(&String::from_utf8_lossy(&file_content)).ok()
    }

    /// Constructs the path to the metadata file for the specified bucket.
    /// This function creates a `.meta` file path in the storage directory.
    fn meta_path(&self, bucket: String) -> PathBuf {
        let mut path = PathBuf::from(&self.storage_dir);
        path.push(format!(".{}.meta", bucket));
        path
    }

    /// Reads the metadata for the specified bucket.
    /// This function reads the `.meta` file, parses its content, and returns a vector of `SyncMetadata`.
    async fn read_meta(&mut self, bucket: String) -> Vec<SyncMetadata> {
        let path = self.meta_path(bucket);
        if path.exists() {
            if let Ok(content) = fs::read_to_string(path) {
                return serde_json::from_str(&content).unwrap_or_default();
            }
        }
        vec![]
    }

    /// Appends new metadata to the existing metadata file for the specified bucket.
    /// This function reads the current metadata, appends the new metadata, and writes the updated metadata back to the file.
    async fn append_meta(&mut self, meta: SyncMetadata) {
        let bucket = meta.bucket.clone();
        let mut existing_meta: Vec<SyncMetadata> = self.read_meta(bucket.clone()).await;

        // Remove file_list data for previous entries to save space
        for item in &mut existing_meta {
            item.file_list = None;
        }

        // Append the new item
        existing_meta.push(meta);

        // Write the updated metadata back to the file
        let path = self.meta_path(bucket);
        if let Err(e) = fs::write(
            path,
            serde_json::to_string(&existing_meta).unwrap().into_bytes(),
        ) {
            error!("Could not write SyncMetadata to file: {:?}", e);
        }
    }

    /// Retrieves files based on the provided query.
    /// This function processes the query to determine the files that match the criteria,
    /// including the bucket, path, and point in time.
    pub async fn get_files_from_query(&mut self, query: &FileSelector) -> Vec<FileInfo> {
        let query_path = query.path.clone();
        let query_bucket = query.bucket.clone();
        let point_in_time: Option<SystemTime> = query
            .point_in_time
            .map(|p| p.checked_add(Duration::from_secs(1)).unwrap());

        let prefix = query_bucket.as_str();
        let mut path = std::path::PathBuf::from(self.storage_dir.clone());
        path.push(prefix);

        if query_path != "." && query_path != "/" {
            path.push(query_path);
        }

        let mut files_vec: Vec<FileInfo> = vec![];

        // Additional security
        let ls = format!("{}", self.storage_dir.clone());
        if !path.starts_with(ls.clone()) {
            error!("Path does not start with {}.. {:?}", ls, path.to_str());
            return files_vec;
        }

        // If |path|.latest exists, client wants to recover a single file
        let mut path_latest = path.clone();
        path_latest.push("latest");
        if path_latest.is_symlink() {
            // Requested data is a specific file in the backup
            if let Ok(f) = self
                .process_item(path.as_path(), prefix, query_bucket.clone(), point_in_time)
                .await
            {
                if let Some(f) = f {
                    files_vec.push(f);
                }
            } else {
                error!("Could not process item");
            }
        } else {
            debug!("Walkdir within {}", path.display().to_string());
            for e in WalkDir::new(path).into_iter().filter_map(|e| e.ok()) {
                let p = e.path();
                debug!("Walkdir: {:?}", p);
                if let Ok(f) = self
                    .process_item(p, prefix, query_bucket.clone(), point_in_time)
                    .await
                {
                    if let Some(f) = f {
                        files_vec.push(f);
                    }
                } else {
                    error!("Could not process item");
                }
            }
        }
        files_vec
    }

    /// Processes a directory entry to determine if it matches the query criteria.
    /// This function checks if the entry is a directory, if it was created before the point in time,
    /// and if it does not have a delete marker. If all conditions are met, it constructs a `FileInfo` object.
    async fn process_item(
        &mut self,
        e: &std::path::Path,
        prefix: &str,
        query_bucket: String,
        point_in_time: Option<SystemTime>,
    ) -> Result<Option<FileInfo>, ()> {
        let meta = e.metadata().map_err(|_| ())?;

        // Check if the entry is a directory
        if meta.is_dir() {
            // Check if the directory was created before the point in time
            if let Some(pit) = point_in_time {
                if meta.created().map_err(|_| ())?.gt(&pit) {
                    return Ok(None);
                }
            }

            // Check if the directory has a delete marker
            if self
                .has_delete_marker(e.to_path_buf(), point_in_time)
                .await
                .is_some()
            {
                return Ok(None);
            }

            // Construct the FileInfo object
            let physical_path = e.display().to_string();
            let file_info = FileInfo {
                file_name: physical_path.replace(prefix, ""),
                path_internal: self.pick_version(&physical_path, point_in_time),
                bucket: query_bucket,
                meta,
            };

            // Return the FileInfo object if it has an internal path
            if file_info.path_internal.is_some() {
                return Ok(Some(file_info));
            }
        }
        Ok(None)
    }

    /// Picks the appropriate version of a file based on the provided directory and point in time.
    /// If a point in time is provided, it selects the latest version before that time.
    /// If no point in time is provided, it selects the "latest" symlink if it exists.
    fn pick_version(&mut self, dir: &str, point_in_time: Option<SystemTime>) -> Option<PathBuf> {
        let mut latest = None;
        let mut latest_time = SystemTime::UNIX_EPOCH;

        for entry in WalkDir::new(dir)
            .max_depth(1)
            .into_iter()
            .filter_map(Result::ok)
        {
            let metadata = match entry.metadata() {
                Ok(meta) => meta,
                Err(_) => continue,
            };

            if !metadata.is_file() && !metadata.is_symlink() {
                continue;
            }

            let filename = match entry.file_name().to_str() {
                Some(name) => name,
                None => continue,
            };

            if let Some(pit) = point_in_time {
                if filename != "latest" {
                    let created = match metadata.created() {
                        Ok(time) => time,
                        Err(_) => continue,
                    };
                    let modified = match metadata.modified() {
                        Ok(time) => time,
                        Err(_) => continue,
                    };

                    if modified < pit && created < pit && modified > latest_time {
                        latest_time = modified;
                        latest = Some(entry.path().to_path_buf());
                    }
                }
            } else if filename == "latest" {
                let mut path = PathBuf::from(dir);
                if let Ok(link) = entry.path().read_link() {
                    path.push(link);
                }
                latest = Some(path);
                break;
            }
        }
        latest
    }

    /// Initializes a file for writing based on the provided `FileActionData`.
    /// This function creates the necessary directories, creates the file, writes metadata,
    /// and sets the "latest" symlink.
    async fn init_file(
        &mut self,
        cad: Option<FileActionData>,
    ) -> Result<Option<tokio::fs::File>, ()> {
        let job_inner = cad.unwrap().job.clone();
        let new_file_hash = job_inner.file_hash.clone().unwrap();
        let mut path = PathBuf::new();

        // Construct the full path to the file
        path.push(&self.storage_dir);
        path.push(job_inner.bucket.clone().unwrap());
        path.push(job_inner.file_path.clone().unwrap());

        // Create necessary directories
        if let Err(e) = fs::create_dir_all(&path) {
            info!("Could not create directory {}: {:?}", path.display(), e);
            return Err(());
        }

        // Append the file hash to the path
        path.push(&new_file_hash);

        // Create the file
        let real_file_path = path.clone();
        let fd = match File::create(&real_file_path).await {
            Ok(f) => Some(f),
            Err(e) => {
                error!("Could not open file for write {}: {:?}", path.display(), e);
                return Err(());
            }
        };

        // Canonicalize the file path
        let real_file_path = match real_file_path.canonicalize() {
            Ok(p) => p,
            Err(e) => {
                error!(
                    "Could not canonicalize file path {}: {:?}",
                    path.display(),
                    e
                );
                return Err(());
            }
        };

        // Write metadata and set the "latest" symlink
        self.to_metadata(&real_file_path, job_inner).await;
        self.set_latest(path).unwrap_or_else(|e| {
            error!("Failed to set latest symlink: {:?}", e);
        });

        Ok(fd)
    }

    async fn client_init_file(
        &mut self,
        cad: Option<FileActionData>,
    ) -> Result<std::option::Option<tokio::fs::File>, ()> {
        let job_inner = cad.unwrap().job.clone();
        let mut path = std::path::PathBuf::new();
        // /path/to/file.txt will become
        // /data_dir/path/to/file.txt/
        path.push(&self.storage_dir);
        path.push(job_inner.bucket.unwrap());
        path.push(job_inner.file_path.clone().unwrap());

        let path1 = path.clone();
        let filename = path1.iter().last().unwrap().to_str().unwrap();
        path.pop();

        match fs::create_dir_all(path.clone()) {
            Ok(_) => {}
            Err(e) => {
                info!(
                    "Could not create directory {}: {:?}",
                    path.display().to_string(),
                    e
                );
                return Err(());
            }
        }

        path.push(filename);

        let real_file_path = path.clone();
        let fd = match File::create(real_file_path.clone()).await {
            Ok(f) => Some(f),
            Err(e) => {
                error!(
                    "Could not open file for write {}: {:?}",
                    path.display().to_string(),
                    e
                );
                None
            }
        };

        let real_file_path = real_file_path.canonicalize().unwrap();

        _ = self
            .apply_permissions(
                &real_file_path,
                job_inner.mode.clone(),
                job_inner.uid.clone(),
                job_inner.gid.clone(),
            )
            .await;

        return Ok(fd);
    }

    pub async fn run(&mut self, initial_stage: Option<Stage>) {
        let mut current_command_data: Option<CommandData> = None;
        let mut current_stage = initial_stage.unwrap_or(Stage::Handshake);

        let mut current_action_data: Option<FileActionData> = None;
        let mut await_bytes = 0;
        let mut fd: Option<File> = None;
        let mut fd_pos: usize = 0;

        loop {
            match current_stage {
                Stage::Handshake => {
                    if self.handle_handshake().await.is_err() {
                        return;
                    }
                    current_stage = Stage::Authentication;
                }

                Stage::Authentication => {
                    let auth_res = self.handle_authentication().await;
                    if auth_res.is_err() {
                        return;
                    }
                    current_stage = Stage::Command;
                }

                Stage::Command => {
                    if let Ok(cmd) = self.actor.get_command().await {
                        self.reset_fd(&mut fd).await;

                        if let Some(st) = self.handle_command(cmd, &mut current_command_data).await
                        {
                            current_stage = st;
                        } else {
                            // Authentication failed due to bucket
                            // mismatch or bucket authentication issues.
                            return;
                        }
                    } else {
                        return;
                    }
                }

                Stage::ServerPreflightRequest => {
                    // KEIN BUCKET NAME IM current_command_data

                    if self
                        .handle_preflight(&mut current_command_data)
                        .await
                        .is_err()
                    {
                        return;
                    }
                    current_stage = Stage::Command;
                }

                Stage::ServerSetSyncMetadata => {
                    // KEIN BUCKET NAME IM current_command_data
                    if self.handle_sync_metadata().await.is_err() {
                        return;
                    }
                    current_stage = Stage::Command;
                }

                Stage::ServerFileSender => {
                    let bucket = current_command_data.clone().unwrap().query.unwrap().bucket;
                    if self.handle_bucket_auth(bucket, false).await.is_err() {
                        // Auth for bucket/auth_token combination failed.
                        return;
                    }
                    _ = self
                        .handle_server_file_sender(&mut current_command_data)
                        .await;
                    return;
                }

                Stage::ServerFileReceiver => {
                    // KEIN BUCKET NAME IM current_command_data
                    if let Ok(new_stage) = self
                        .handle_server_file_receiver(
                            &mut fd,
                            &mut await_bytes,
                            &mut current_action_data,
                        )
                        .await
                    {
                        current_stage = new_stage;
                    } else {
                        return;
                    }
                }

                Stage::ServerDataReceiver => {
                    if let Ok(new_stage) = self
                        .handle_server_data_receiver(
                            &mut fd,
                            &mut fd_pos,
                            await_bytes,
                            &mut current_action_data,
                        )
                        .await
                    {
                        if let Some(new_stage) = new_stage {
                            current_stage = new_stage;
                        }
                    } else {
                        return;
                    }
                }

                Stage::ServerFileList => {}

                Stage::ServerFileBucketStatus => {
                    let bucket = current_command_data.clone().unwrap().query.unwrap().bucket;
                    if self.handle_bucket_auth(bucket, false).await.is_err() {
                        // Auth for bucket/auth_token combination failed.
                        debug!("Bucket Auth Failed in handle_bucket_auth() - sending nack");
                        self.actor.send_nack().await;
                    } else {
                        debug!("Bucket Auth OK in handle_bucket_auth()");
                        self.handle_server_bucket_status(&mut current_command_data)
                            .await;
                    }
                    return;
                }

                Stage::ClientFileReceiver => {
                    // Receive files from binary stream from Server
                    if let Ok(new_stage) = self
                        .handle_client_file_receiver(
                            &mut fd,
                            &mut await_bytes,
                            &mut current_action_data,
                        )
                        .await
                    {
                        current_stage = new_stage;
                    } else {
                        return;
                    }
                }

                Stage::ClientDataReceiver => {
                    if let Ok(new_stage) = self
                        .handle_client_data_receiver(
                            &mut fd,
                            &mut fd_pos,
                            await_bytes,
                            &mut current_action_data,
                        )
                        .await
                    {
                        if let Some(new_stage) = new_stage {
                            current_stage = new_stage;
                        }
                    } else {
                        return;
                    }
                }
            }
        }
    }

    async fn handle_handshake(&mut self) -> Result<(), ()> {
        match self.actor.next_msg().await {
            Some(buf) => {
                // It suppose to be a HandshakeData object
                let buf_str = String::from_utf8_lossy(&buf);
                let hd = serde_json::from_str::<HandshakeData>(&buf_str);
                if hd.is_err() {
                    // Close connection
                    error!("Could not parse HandshakeData object");
                    return Err(());
                }
                // debug!("Handshake succeeded - {:?}", hd);
                self.actor.send_ack().await;
                return Ok(());
            }
            None => {
                error!("Could not get self.actor.next_msg()");
                Err(())
            }
        }
    }

    async fn handle_bucket_auth(&mut self, bucket: String, write_op: bool) -> Result<(), ()> {
        match self.actor.auth_token.clone() {
            Some(auth_token) => {
                let mut hasher = Sha256::new();
                hasher.update(auth_token);
                let auth_token_hash = format!("{:x}", hasher.finalize());

                // Find bucket's (|bucket|) auth token hash

                // - Build Path
                let mut bucket_path = std::path::PathBuf::new();
                bucket_path.push(&self.storage_dir);
                bucket_path.push(bucket.clone());

                let mut auth_path = PathBuf::from(&self.storage_dir);
                auth_path.push(format!(".{}.auth", bucket));

                // - Check if Path exists
                // - If path does not exists, add a .auth file and write the hash (only on write operations, not on list/status/preflight)
                if bucket_path.exists() {
                    // - Is there a .auth file?
                    if !auth_path.exists() {
                        return Err(()); // We do have a auth_token (no master user) but no .auth-file -> refuse access to bucket
                    }
                    // - read content and compare hashes
                    if let Ok(auth_token_hash_from_bucket) = fs::read_to_string(auth_path) {
                        if auth_token_hash_from_bucket.trim() == auth_token_hash.trim() {
                            return Ok(());
                        }
                    }

                    // Cannot read auth file or hash does not match
                    return Err(());
                } else {
                    // - path does not exists PLUS
                    if write_op {
                        // - this is a write operation
                        // = create the initial .auth file
                        // write |auth_token_hash| into the .auth file
                        if fs::write(auth_path, auth_token_hash.into_bytes()).is_ok() {
                            return Ok(());
                        } else {
                            error!(
                                "Could not write new auth token hash to .auth-file on bucket {:?}",
                                bucket
                            );
                        }
                    }
                }

                Err(())
            }
            // No self.actor.auth_token means the client
            // is authenticated with a master key by (handle_authentication())
            None => Ok(()),
        }
    }

    async fn handle_authentication(&mut self) -> Result<(), ()> {
        match self.actor.next_msg().await {
            Some(buf) => {
                // It suppose to be a HandshakeData object
                let buf_str = String::from_utf8_lossy(&buf);
                let ad = serde_json::from_str::<AuthenticationData>(&buf_str);
                if ad.is_err() {
                    // Close connection
                    error!("Could not parse AuthenticationData object");
                    self.actor.send_nack().await;
                    return Err(());
                }
                let ad = ad.unwrap();
                if ad.token
                    != env::var("PRE_SHARED_SECURITY_TOKEN")
                        .expect("PRE_SHARED_SECURITY_TOKEN not set")
                {
                    // Maybe this authentication token is specific to the bucket.
                    debug!("No matching master token. Limiting access to on bucket-level");
                    self.actor.auth_token = Some(String::from(ad.token));
                } else {
                    debug!("Authentication succeeded - {:?}", ad);
                }
                self.actor.send_ack().await;
                Ok(())
            }
            None => {
                error!("Could not get self.actor.next_msg()");
                // Close connection
                return Err(());
            }
        }
    }

    async fn reset_fd(&self, fd: &mut Option<File>) {
        if fd.is_some() {
            _ = fd.as_mut().unwrap().shutdown().await;
            *fd = None;
        }
    }

    async fn handle_command(
        &mut self,
        cmd: CommandData,
        current_command_data: &mut Option<CommandData>,
    ) -> Option<Stage> {
        *current_command_data = Some(cmd.clone());
        if cmd.action != Action::List {
            self.actor.send_ack().await;
        }
        Some(match cmd.action {
            Action::Preflight => Stage::ServerPreflightRequest,
            Action::SetSyncMetadata => Stage::ServerSetSyncMetadata,
            Action::Sync => Stage::ServerFileReceiver,
            Action::Status => Stage::ServerFileBucketStatus,
            Action::Recover => Stage::ServerFileSender,
            Action::List => {
                self.actor.send_nack().await;
                Stage::Command
            }
        })
    }

    async fn handle_preflight(
        &mut self,
        current_command_data: &mut Option<CommandData>,
    ) -> Result<(), ()> {
        // Client will send a PreflightRequestData{} structure with a file list we need to compare.
        // We will answer with a list of files we acutally want to receive.
        // Client should only retain those files in the list of files that will be sent over.
        // Server will not accept other files for that bucket (as they already exist)
        match self.actor.next_msg().await {
            Some(buf) => {
                // It suppose to be a HandshakeData object
                let buf_str = String::from_utf8_lossy(&buf);

                match serde_json::from_str::<PreflightRequestData>(&buf_str) {
                    Ok(preflight) => {
                        self.actor.send_ack().await;

                        let ca = current_command_data.clone();
                        let ca = ca.unwrap();
                        let query = ca.query.clone().unwrap();
                        let point_in_time = query.point_in_time.clone();
                        let mut file_list = preflight.file_list.clone();
                        let mut skipped_files = vec![];

                        let mut retained_files = vec![];
                        for f in file_list.iter() {
                            let bucket = query.bucket.clone();
                            let mut path = std::path::PathBuf::new();
                            path.push(&self.storage_dir);
                            path.push(&bucket);
                            path.push(&f.file_path);
                            let path = path.canonicalize();

                            // Do authentication
                            if self.handle_bucket_auth(bucket, false).await.is_err()
                                || path.is_err()
                            {
                                retained_files.push(f.clone());
                                continue;
                            }

                            let mut path = path.unwrap();
                            let path_nohash = path.clone();
                            path.push(&f.file_hash);

                            let res = !path.exists()
                                || self
                                    .has_delete_marker(path_nohash.clone(), point_in_time)
                                    .await
                                    .is_some()
                                || !self.is_latest(path.clone());

                            if res {
                                retained_files.push(f.clone());
                            } else {
                                if path.exists() {
                                    skipped_files.push(path);
                                }
                            }
                        }
                        file_list = retained_files;

                        for path in skipped_files.iter() {
                            let mut path_nohash = path.clone();
                            path_nohash.pop();
                            _ = self.unset_delete_marker(path_nohash).await;
                            if !self.is_latest(path.clone()) {
                                _ = self.set_latest(path.clone());
                            }
                        }
                        drop(skipped_files);

                        // Send file list
                        let file_list = serde_json::to_string(&file_list).unwrap();
                        let file_list = file_list.into_bytes();
                        _ = self.actor.write(file_list).await;

                        // current_stage = Stage::Command;
                        // continue 'mainloop;
                        Ok(())
                    }
                    Err(e) => {
                        error!("Could not parse PreflightRequestData object: {:?}", e);
                        self.actor.send_nack().await;
                        // Close connection
                        Err(())
                    }
                }
            }
            None => {
                error!("Could not get self.actor.next_msg()");
                // Close connection
                return Err(());
            }
        }
    }

    async fn handle_sync_metadata(&mut self) -> Result<(), ()> {
        match self.actor.next_msg().await {
            Some(buf) => {
                // It suppose to be a HandshakeData object
                let buf_str = String::from_utf8_lossy(&buf);
                match serde_json::from_str::<SyncMetadata>(&buf_str) {
                    Ok(meta) => {
                        // Get all files associated available on the client
                        // side and figure out which files where deleted in
                        // the meantime
                        if let Some(all_files) = meta.clone().file_list {
                            let existing_meta: Vec<SyncMetadata> =
                                self.read_meta(meta.bucket.clone()).await;
                            if existing_meta.len() > 0 {
                                let last_sync = existing_meta.last().unwrap();
                                if let Some(last_file_list) = last_sync.file_list.clone() {
                                    for file in last_file_list.iter() {
                                        if !all_files.contains(&file) {
                                            // File was deleted
                                            _ = self
                                                .set_delete_marker(meta.bucket.clone(), file.into())
                                                .await;
                                        }
                                    }
                                }
                            }
                        }
                        // Write metadata to file
                        self.append_meta(meta).await;
                    }
                    Err(e) => {
                        error!("Could not parse SyncMetadata object: {:?}", e);
                        return Err(());
                    }
                }

                self.actor.send_ack().await;
                Ok(())
            }
            None => {
                error!("Could not get self.actor.next_msg()");
                // Close connection
                return Err(());
            }
        }
    }

    async fn handle_server_file_receiver(
        &mut self,
        fd: &mut Option<File>,
        await_bytes: &mut usize,
        current_action_data: &mut Option<FileActionData>,
    ) -> Result<Stage, ()> {
        match self.actor.next_msg().await {
            Some(buf) => {
                let buf_str = String::from_utf8_lossy(&buf);
                let ad = serde_json::from_str::<FileActionData>(&buf_str);
                if ad.is_err() {
                    error!("Could not parse FileActionData object");
                    return Err(());
                }
                *current_action_data = ad.ok();
                self.reset_fd(fd).await;
                let cad = current_action_data.clone().unwrap();
                let mut path = std::path::PathBuf::new();
                let bucket = cad.job.bucket.clone().unwrap();

                path.push(&self.storage_dir);
                path.push(bucket.clone());

                if self.handle_bucket_auth(bucket, true).await.is_err() {
                    // Auth for bucket/auth_token combination failed.
                    self.actor.send_nack().await;
                    return Ok(Stage::Command);
                }

                path.push(cad.job.file_path.clone().unwrap());
                path.push(cad.job.file_hash.clone().unwrap());

                if path.exists() {
                    _ = self.unset_delete_marker(path.clone()).await;
                    self.to_metadata(&path, cad.job.clone()).await;
                    if !self.is_latest(path.clone()) {
                        _ = self.set_latest(path);
                    }
                    self.actor.send_skip().await;
                    return Ok(Stage::Command);
                }

                *await_bytes = cad.file_size.unwrap() as usize;
                if *await_bytes == 0 {
                    // clear the file
                    if let Ok(ld) = self.init_file(current_action_data.clone()).await {
                        *fd = ld;
                        _ = fd.as_mut().unwrap().sync_all();
                        *fd = None;
                        self.actor.send_skip().await;
                    } else {
                        self.actor.send_nack().await;
                    }
                    return Ok(Stage::Command);
                }
                self.actor.send_ack().await;
                return Ok(Stage::ServerDataReceiver);
            }
            None => {
                // Close connection
                return Err(());
            }
        };
    }

    async fn handle_server_data_receiver(
        &mut self,
        fd: &mut Option<File>,
        fd_pos: &mut usize,
        await_bytes: usize,
        current_action_data: &mut Option<FileActionData>,
    ) -> Result<Option<Stage>, ()> {
        match self.actor.next_msg().await {
            Some(buf) => {
                let buf = decompress_data(&buf);
                if fd.is_none() {
                    *fd_pos = 0;
                    if let Ok(ld) = self.init_file(current_action_data.clone()).await {
                        *fd = ld;
                    } else {
                        self.actor.send_nack().await;
                        return Ok(Some(Stage::Command));
                    }
                }

                let res = fd.as_mut().unwrap().write_all(&buf).await;

                if res.is_ok() {
                    _ = fd.as_mut().unwrap().flush().await;
                    *fd_pos += buf.len();
                } else {
                    error!("Could not write to file: {:?}", res.err().unwrap());
                    *fd = None;
                    self.actor.send_nack().await;
                    return Ok(Some(Stage::Command));
                }

                if *fd_pos == await_bytes {
                    _ = fd.as_mut().unwrap().sync_all();
                    *fd = None;
                    // Reset state
                    self.actor.send_ack().await;
                    return Ok(Some(Stage::Command));
                }

                // Want more data
                self.actor.send_ack().await;
                return Ok(None);
            }
            _ => return Err(()),
        }
    }

    async fn handle_server_bucket_status(
        &mut self,
        current_command_data: &mut Option<CommandData>,
    ) {
        let ca = current_command_data.clone();
        let ca = ca.unwrap();

        let query = ca.query.clone().unwrap();
        let mut point_in_time = query.point_in_time.clone();

        let mut bucket_status = BucketStatus {
            meta: self.read_meta(query.bucket.clone()).await,
        };

        // Remove the file_list from the meta-data.
        bucket_status
            .meta
            .iter_mut()
            .for_each(|item| item.file_list = None);

        // point_in_time is currently either None or just a value,
        // the client told us - we need to get a real value from
        // our meta-data that is close to the point in time
        if point_in_time.is_some() {
            let mut closest = SystemTime::UNIX_EPOCH;
            for meta in bucket_status.meta.iter() {
                if truncate_to_seconds(meta.timestamp).le(&point_in_time.unwrap()) {
                    if meta.timestamp.gt(&closest) {
                        closest = meta.timestamp;
                    }
                }
            }
            if closest != SystemTime::UNIX_EPOCH {
                point_in_time = Some(closest);
            }

            // bucket_status.meta potentially contains meta-data from after the point in time
            // we need to filter out the files that were deleted before the point in time
            bucket_status
                .meta
                .retain(|meta| truncate_to_seconds(meta.timestamp).le(&point_in_time.unwrap()));
        }

        let bucket_status = serde_json::to_string(&bucket_status).unwrap();
        let buf_bytes = bucket_status.into_bytes();
        _ = self.actor.write(buf_bytes).await;
        self.actor.send_ack().await;
    }

    async fn handle_server_file_sender(
        &mut self,
        current_command_data: &mut Option<CommandData>,
    ) -> Result<(), ()> {
        // Send files to binary stream

        let ca = current_command_data.clone();
        let ca = ca.unwrap();

        let mut query = ca.query.clone().unwrap();
        let mut files: Vec<FileInfo> = vec![];

        if query.path.starts_with("./") {
            query.path = query.path.replacen("./", "", 1);
        } else if query.path.starts_with("/") {
            query.path = query.path.replacen("/", "", 1);
        }

        if query.path.contains("..") {
            error!("Request path contains ..; exiting!");
            return Err(());
        }
        files.extend(self.get_files_from_query(&query).await);

        let mut job_id = 1;
        for file in files.iter() {
            // 1. Send FileActionData (like the client currently does)
            //    to Stage::ClientFileReceiver on client side
            let bucket = file.clone().bucket;

            let file = file.clone().path_internal.unwrap();
            let file_path = file.display().to_string();
            let file_hash = file.as_path().to_string_lossy().to_string();
            let file_hash = file_hash[file_hash.rfind('/').unwrap() + 1..].to_string();

            // full internal storage path of the file
            let target = file_path.clone();
            // only til the last / (cut away the file hash)
            let target = target[0..target.rfind('/').unwrap()].to_string();
            // remove local storage dir including the bucket name form the path
            let local_root = format!("{}/{}/", self.storage_dir, bucket);
            let mut target = target.replace(&local_root, "");
            // cut away everything from query.path until the last / if there
            // is more than one /
            let mut sp = query.path.split("/").collect::<Vec<&str>>();
            if sp.len() > 1 {
                sp.pop().unwrap();
                let query_path_str = format!("/{}", sp.join("/"));
                if target.starts_with(query_path_str.as_str()) {
                    target = target[query_path_str.len()..].to_string();
                }
            }

            let job: Job;
            if let Some(meta) = self.from_metadata(&file).await {
                job = Job {
                    inner: InnerJob {
                        file_hash: Some(file_hash),
                        job_id: Some(job_id),
                        status: Status::Pending,
                        accessed: meta.accessed,
                        modified: meta.modified,
                        created: meta.created,
                        mode: meta.mode,
                        uid: meta.uid,
                        gid: meta.gid,
                        file_path: Some(file_path.clone()),
                        bucket: Some(bucket),
                        retries: 0,
                        is_dir: false,
                    },
                    status_sender: None,
                };
            } else {
                let mut inner = InnerJob::default();
                inner.bucket = Some(bucket.clone());
                inner.file_path = Some(file_path.clone());
                inner.file_hash = Some(file_hash);
                inner.job_id = Some(job_id);
                job = Job {
                    inner,
                    status_sender: None,
                };
            }

            job_id += 1;

            // Send Binary Chunks (like the client currently does)
            // for job in jobs.iter_mut() {
            match self.actor.send_file(job.clone(), Some(target)).await {
                Ok(bytes_sent) => {
                    debug!("File sent successfully ({} bytes)", bytes_sent);
                }
                Err(e) => {
                    error!("Could not send file: {:?}", e);
                }
            };
        }

        Ok(())
    }

    async fn handle_client_file_receiver(
        &mut self,
        fd: &mut Option<File>,
        await_bytes: &mut usize,
        current_action_data: &mut Option<FileActionData>,
    ) -> Result<Stage, ()> {
        match self.actor.next_msg().await {
            Some(buf) => {
                // It suppose to be a ActionData object
                let buf_str = String::from_utf8_lossy(&buf);
                let ad = serde_json::from_str::<FileActionData>(&buf_str);
                if ad.is_err() {
                    error!("Could not parse FileActionData object");
                    return Err(());
                }
                *current_action_data = ad.ok();
                self.reset_fd(fd).await;

                let mut cad = current_action_data.clone().unwrap();
                let mut path = std::path::PathBuf::new();

                path.push(&self.storage_dir.clone().as_str());
                path.push(cad.job.file_path.clone().unwrap().to_string());
                cad.job.file_path = Some(path.display().to_string());

                *await_bytes = cad.file_size.unwrap() as usize;
                if *await_bytes == 0 {
                    if let Ok(ld) = self.client_init_file(Some(cad.clone())).await {
                        *fd = ld;
                        _ = fd.as_mut().unwrap().sync_all();
                        *fd = None;
                        self.actor.send_skip().await;
                    } else {
                        self.actor.send_nack().await;
                    }
                    return Ok(Stage::ClientFileReceiver);
                }

                *current_action_data = Some(cad);
                self.actor.send_ack().await;
                return Ok(Stage::ClientDataReceiver);
            }
            None => {
                return Err(());
            }
        };
    }

    async fn handle_client_data_receiver(
        &mut self,
        fd: &mut Option<File>,
        fd_pos: &mut usize,
        await_bytes: usize,
        current_action_data: &mut Option<FileActionData>,
    ) -> Result<Option<Stage>, ()> {
        // Receiving binary chunks from the Server
        match self.actor.next_msg().await {
            Some(buf) => {
                let buf = decompress_data(&buf);
                if fd.is_none() {
                    *fd_pos = 0;
                    //let job_inner = current_action_data.clone().unwrap().job.clone();
                    let target_filename = current_action_data
                        .clone()
                        .unwrap()
                        .job
                        .file_path
                        .clone()
                        .unwrap();

                    let target_dir =
                        target_filename[..target_filename.rfind("/").unwrap()].to_string();

                    let path = std::path::PathBuf::from_str(&target_filename).unwrap();
                    match fs::create_dir_all(target_dir) {
                        Ok(_) => {}
                        Err(e) => {
                            info!(
                                "Could not create directory {}: {:?}",
                                path.display().to_string(),
                                e
                            );
                            // current_stage = Stage::Command;
                            self.actor.send_nack().await;
                            // continue 'mainloop;
                            return Ok(Some(Stage::Command));
                        }
                    }

                    let real_file_path = path.clone();
                    *fd = match File::create(real_file_path.clone()).await {
                        Ok(f) => Some(f),
                        Err(e) => {
                            error!(
                                "Could not open file for write {}: {:?}",
                                path.display().to_string(),
                                e
                            );
                            None
                        }
                    };
                }

                let res = fd.as_mut().unwrap().write_all(&buf).await;
                if res.is_ok() {
                    *fd_pos += buf.len();
                } else {
                    error!("Could not write to file: {:?}", res.err().unwrap());
                    // current_stage = Stage::Command;
                    *fd = None;
                    self.actor.send_nack().await;
                    return Ok(Some(Stage::Command));
                }

                if *fd_pos == await_bytes as usize {
                    _ = fd.as_mut().unwrap().sync_all();
                    *fd = None;

                    // Reset state
                    self.actor.send_ack().await;
                    return Ok(Some(Stage::ClientFileReceiver));
                }
                self.actor.send_ack().await;
                // continue 'mainloop;
                return Ok(None);
            }
            _ => return Err(()),
        }
    }
}
