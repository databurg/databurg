use crate::env;
use crate::{
    AckType, Action, Actor, AuthenticationData, ChannelMessage, CommandData, FileActionData,
    HandshakeData, Job, Response,
};
use byteorder::{BigEndian, ByteOrder};
use flate2::read::GzEncoder;
use flate2::Compression;
use log::{debug, error};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use std::{error::Error, io::prelude::*, sync::Arc, time::Duration};
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{channel, Receiver};
use tokio::time::sleep;
use tokio_rustls::{TlsConnector, TlsStream};

/// Constants used throughout the Actor.
pub const MESSAGE_LENGTH_BYTES: usize = 4;
pub const DEFAULT_CHUNK_SIZE: usize = 2_097_152; // 2 MB

/// Custom certificate verifier that skips server verification.
/// **Warning:** Using this in production is insecure.
/// This is only for testing / non-production use.
#[derive(Debug)]
struct SkipServerVerification;

impl SkipServerVerification {
    /// Creates a new instance of the custom certificate verifier.
    fn new() -> Arc<Self> {
        Arc::new(Self)
    }
}

impl ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer,
        _intermediates: &[CertificateDer],
        _server_name: &ServerName,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::RSA_PKCS1_SHA1,
            SignatureScheme::ECDSA_SHA1_Legacy,
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::ECDSA_NISTP521_SHA512,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::ED25519,
            SignatureScheme::ED448,
        ]
    }
}

/// Compresses the provided data using gzip compression.
///
/// # Arguments
///
/// * `data` - A slice of bytes to compress.
///
/// # Returns
///
/// A `Vec<u8>` containing the compressed data.
fn compress_data(data: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(data, Compression::default());
    let mut compressed_data = Vec::new();
    encoder
        .read_to_end(&mut compressed_data)
        .expect("Failed to compress data");
    compressed_data
}

/// Retrieves the current region (optional).
///
/// # Returns
///
/// `Ok(String)` containing the region if successful, otherwise `Err(())`.
async fn region() -> Result<String, ()> {
    Err(())
}

/// Establishes a TLS connection to the server.
///
/// # Errors
///
/// Returns an error if the connection or TLS handshake fails.
///
/// # Returns
///
/// `Ok(TlsStream<TcpStream>)` on success.
pub async fn connect() -> Result<tokio_rustls::client::TlsStream<TcpStream>, Box<dyn Error>> {
    // Configure TLS with custom certificate verifier
    let config = Arc::new(
        ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(SkipServerVerification::new())
            .with_no_client_auth(),
    );
    let tls_connector = TlsConnector::from(config);

    // Retrieve server hostname and port from environment variables
    let env_server_hostname =
        env::var("SERVER_HOSTNAME").unwrap_or_else(|_| "127.0.0.1".to_string());
    let env_server_port = env::var("SERVER_PORT").unwrap_or_else(|_| "2403".to_string());

    // Construct the server address, optionally including the region
    let addr = match region().await {
        Ok(region) => format!("{}.{}:{}", region, env_server_hostname, env_server_port),
        Err(_) => format!("{}:{}", env_server_hostname, env_server_port),
    };

    // Establish TCP connection
    let socket = TcpStream::connect(&addr).await.map_err(|e| {
        error!("Could not connect to server at {}: {:?}", addr, e);
        std::io::Error::new(std::io::ErrorKind::Other, "Could not connect to server")
    })?;

    // Parse the server name for TLS
    let server_name = ServerName::try_from(env_server_hostname).map_err(|e| {
        error!("Invalid server name: {:?}", e);
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid server name")
    })?;

    // Perform TLS handshake
    let tls_stream = tls_connector
        .connect(server_name, socket)
        .await
        .map_err(|e| {
            error!("TLS handshake failed: {:?}", e);
            std::io::Error::new(std::io::ErrorKind::Other, "TLS handshake failed")
        })?;

    Ok(tls_stream)
}

/// Represents the Actor responsible for managing communication and file transfers.
impl<S: AsyncRead + AsyncWrite + Unpin> Actor<S> {
    /// Creates a new Actor instance by splitting the TLS stream into reader and writer.
    ///
    /// # Arguments
    ///
    /// * `stream` - A TLS stream wrapping a TCP stream.
    ///
    /// # Returns
    ///
    /// A new instance of `Actor`.
    pub fn new(stream: TlsStream<S>) -> Self {
        let (socket_r, socket_w) = tokio::io::split(stream);
        let (raw_sender_tx, raw_sender_rx) = channel::<ChannelMessage>(1);

        Actor {
            socket_r: Some(socket_r),
            socket_w: Some(socket_w),
            job_receiver: None,
            raw_sender_rx,
            raw_sender_tx,
            handshake_done: false,
            auth_done: false,
            auth_token: None,
        }
    }

    /// Performs the handshake process with the server.
    ///
    /// This function serializes the handshake data, sends it to the server,
    /// waits for an acknowledgment, and marks the handshake as completed.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails, writing to the server fails,
    /// or acknowledgment is not received.
    pub async fn handshake(&mut self) -> Result<(), Box<dyn Error>> {
        // Serialize handshake data with client version
        let hd = serde_json::to_string(&HandshakeData {
            client_version: Some(env!("CARGO_PKG_VERSION").to_string()),
        })?;

        // Send handshake data to the server
        self.write(hd.into_bytes()).await.map_err(|_| {
            error!("Could not write handshake data");
            std::io::Error::new(std::io::ErrorKind::Other, "Could not write handshake data")
        })?;

        // Await acknowledgment from the server
        self.acked().await.map_err(|_| {
            error!("Could not get ack for handshake");
            std::io::Error::new(std::io::ErrorKind::Other, "Could not get ack for handshake")
        })?;

        // Mark handshake as done
        self.handshake_done = true;
        Ok(())
    }

    /// Authenticates the client with the server using a pre-shared token.
    ///
    /// This function serializes the authentication data, sends it to the server,
    /// waits for an acknowledgment, and marks authentication as completed.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails, the environment variable is not set,
    /// writing to the server fails, or acknowledgment is not received.
    pub async fn authenticate(&mut self) -> Result<(), Box<dyn Error>> {
        // Retrieve the pre-shared security token from environment variables
        let hd = serde_json::to_string(&AuthenticationData {
            token: env::var("PRE_SHARED_SECURITY_TOKEN")
                .expect("PRE_SHARED_SECURITY_TOKEN not set"),
        })?;

        // Send authentication data to the server
        self.write(hd.into_bytes()).await.map_err(|_| {
            error!("Could not write authentication data");
            std::io::Error::new(
                std::io::ErrorKind::Other,
                "Could not write authentication data",
            )
        })?;

        // Await acknowledgment from the server
        self.acked().await.map_err(|_| {
            error!("Could not get ack for authentication");
            std::io::Error::new(
                std::io::ErrorKind::Other,
                "Could not get ack for authentication",
            )
        })?;

        // Mark authentication as done
        self.auth_done = true;
        Ok(())
    }

    /// Runs the command handler, waiting for incoming commands from the server.
    ///
    /// This function continuously listens for incoming `CommandData` messages
    /// and processes them accordingly. It ensures that the handshake is completed
    /// before processing any commands.
    ///
    /// # Arguments
    ///
    /// * `sender_rx` - A receiver channel for incoming `CommandData` messages.
    pub async fn command_runner(&mut self, mut sender_rx: Receiver<Option<CommandData>>) {
        // Wait until the handshake is completed
        while !self.handshake_done {
            debug!("Waiting for handshake...");
            sleep(Duration::from_secs(1)).await;
        }

        loop {
            debug!("Waiting for command...");
            tokio::select! {
                Some(msg) = sender_rx.recv() => {
                    debug!("Received command: {:?}", msg);
                    if let Some(command) = msg {
                        // Serialize the command data
                        let cmd = serde_json::to_string(&command).unwrap();

                        // Send the command to the server
                        if let Err(e) = self.write(cmd.into_bytes()).await {
                            error!("Failed to send command: {:?}", e);
                            continue;
                        }

                        // Await acknowledgment from the server
                        if let Err(e) = self.acked().await {
                            error!("Failed to receive acknowledgment for command: {:?}", e);
                            continue;
                        }
                    } else {
                        debug!("No more commands to process");
                        return;
                    }
                },
                else => {
                    debug!("Command channel closed");
                    return;
                }
            }
        }
    }

    /// Handles sending jobs and raw messages to the server.
    ///
    /// This function listens for incoming `Job` and `ChannelMessage` instances
    /// and processes them accordingly. It ensures that the handshake is completed
    /// before processing any jobs.
    ///
    /// # Arguments
    ///
    /// * `sender_rx` - A receiver channel for incoming `Job` messages.
    /// * `raw_sender_rx` - A receiver channel for incoming `ChannelMessage` messages.
    pub async fn sending(
        &mut self,
        mut sender_rx: Receiver<Option<Job>>,
        mut raw_sender_rx: Receiver<Option<ChannelMessage>>,
    ) {
        // Wait until the handshake is completed
        while !self.handshake_done {
            debug!("Waiting for handshake...");
            sleep(Duration::from_secs(1)).await;
        }

        loop {
            debug!("Waiting for job or raw message...");
            tokio::select! {
                Some(msg) = sender_rx.recv() => {
                    debug!("Received job: {:?}", msg);
                    if let Some(job) = msg {
                        // Attempt to send the file
                        match self.send_file(job.clone(), None).await {
                            Ok(bytes_sent) => {
                                debug!("File sent successfully: {} bytes", bytes_sent);
                                // Send a success response back
                                if let Some(status_sender) = job.status_sender {
                                    let ack_type = if bytes_sent == 0 { AckType::Skip } else { AckType::Ack };
                                    let response = Response {
                                        success: true,
                                        message: Some(format!("Sent {} bytes", bytes_sent)),
                                        code: None,
                                        ack_type: Some(ack_type),
                                    };
                                    if let Err(e) = status_sender.send(response).await {
                                        error!("Failed to send acknowledgment: {:?}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Could not send file: {:?}", e);
                                // Send a failure response back
                                if let Some(status_sender) = job.status_sender {
                                    let response = Response {
                                        success: false,
                                        message: Some("Could not send file".to_string()),
                                        code: None,
                                        ack_type: None,
                                    };
                                    if let Err(e) = status_sender.send(response).await {
                                        error!("Failed to send failure acknowledgment: {:?}", e);
                                    }
                                }
                                return;
                            }
                        }
                    } else {
                        debug!("No more jobs to process");
                        return;
                    }
                },
                Some(msg) = raw_sender_rx.recv() => {
                    debug!("Received raw message: {:?}", msg);
                    if let Some(channel_message) = msg {
                        if let Some(buf) = channel_message.buf {
                            // Write the raw buffer and await acknowledgment
                            if let Ok(ack_type) = self.write_wait_ack(buf).await {
                                if let Some(status) = channel_message.status {
                                    let response = Response {
                                        success: true,
                                        message: None,
                                        code: None,
                                        ack_type: Some(ack_type),
                                    };
                                    if let Err(e) = status.send(response).await {
                                        error!("Failed to send raw acknowledgment: {:?}", e);
                                    }
                                }
                            } else {
                                error!("Failed to write and acknowledge raw message");
                            }
                        }
                    }
                },
                // Receives ChannelMessage{} struct
                Some(msg) = self.raw_sender_rx.recv() => {
                    debug!("Received raw message (self.raw_sender_rx): {:?}", msg);
                    if let Some(buf) = msg.buf {
                        _=self.write_wait_ack(buf).await;
                    }
                },
                else => {
                    debug!("Raw sender channel closed");
                    return;
                }
            }
        }
    }

    /// Sends a command action to the server and awaits acknowledgment.
    ///
    /// # Arguments
    ///
    /// * `action` - The `Action` to send.
    ///
    /// # Returns
    ///
    /// `Ok(AckType)` if acknowledgment is received, otherwise `Err(())`.
    pub async fn send_cmd(&mut self, action: Action) -> Result<AckType, ()> {
        debug!("Sending command: {:?}", action);
        let cmd_data = CommandData {
            action,
            query: None,
        };
        let ad = serde_json::to_string(&cmd_data).unwrap();
        self.write_wait_ack(ad.into_bytes()).await
    }

    /// Sends file action data to the server and awaits acknowledgment.
    ///
    /// # Arguments
    ///
    /// * `job` - The `Job` containing file information.
    /// * `len` - The length of the file in bytes.
    ///
    /// # Returns
    ///
    /// `Ok(AckType)` if acknowledgment is received, otherwise `Err(())`.
    pub async fn send_file_action_data(&mut self, job: Job, len: u64) -> Result<AckType, ()> {
        let file_action_data = FileActionData {
            job: job.inner.clone(),
            file_size: Some(len),
        };
        let ad = serde_json::to_string(&file_action_data).unwrap();
        self.write_wait_ack(ad.into_bytes()).await
    }

    /// Writes data to the server and awaits acknowledgment.
    ///
    /// # Arguments
    ///
    /// * `buf` - The buffer to write.
    ///
    /// # Returns
    ///
    /// `Ok(AckType)` if acknowledgment is received, otherwise `Err(())`.
    pub async fn write_wait_ack(&mut self, buf: Vec<u8>) -> Result<AckType, ()> {
        // Write the buffer to the server
        self.write(buf).await?;
        // Await acknowledgment
        self.acked().await
    }

    /// Sends a file to the server in chunks, handling compression and acknowledgments.
    ///
    /// # Arguments
    ///
    /// * `job` - The `Job` containing file information.
    /// * `target_filename` - An optional target filename to rename the file on the server.
    ///
    /// # Returns
    ///
    /// `Ok(u64)` containing the number of bytes sent, or `Err(())` on failure.
    pub async fn send_file(
        &mut self,
        mut job: Job,
        target_filename: Option<String>,
    ) -> Result<u64, ()> {
        // Retrieve the source file path from the job
        let source = job.inner.file_path.clone().unwrap();

        // Open the file asynchronously
        let mut f = File::open(source.clone()).await.map_err(|_| {
            error!("Cannot open file {}", source);
            ()
        })?;

        // Get the file length
        let len = f.metadata().await.unwrap().len();
        let mut len_read = 0;

        // If target_filename is provided, update the job's file_path
        if target_filename.is_none() {
            // Send a sync command to the server
            self.send_cmd(Action::Sync).await?;
        } else {
            job.inner.file_path = target_filename.clone();
        }

        // Send file action data and check if the server wants to skip
        let skip_indicator = self.send_file_action_data(job, len).await?;
        if skip_indicator == AckType::Skip {
            return Ok(0);
        }

        // Send the file in chunks
        while len > len_read {
            let rest = (len - len_read) as usize;
            let buf_size = if rest > DEFAULT_CHUNK_SIZE {
                DEFAULT_CHUNK_SIZE
            } else {
                rest
            };
            let mut buf = vec![0; buf_size];

            // Read a chunk from the file
            f.read_exact(&mut buf).await.map_err(|_| {
                error!("Could not read chunk from file");
                ()
            })?;
            len_read += buf.len() as u64;

            // Compress the chunk
            let compressed = compress_data(&buf);

            // Write the compressed chunk to the server
            self.write(compressed).await.map_err(|_| {
                error!("Failed to write compressed data");
                ()
            })?;

            // Await acknowledgment for the chunk
            self.acked().await.map_err(|_| {
                error!("Failed to receive acknowledgment for chunk");
                ()
            })?;
        }

        Ok(len_read)
    }

    /// Retrieves a command from the server.
    ///
    /// This function reads the next message from the server and attempts to parse it as `CommandData`.
    ///
    /// # Returns
    ///
    /// `Ok(CommandData)` if successful, otherwise `Err(())`.
    pub async fn get_command(&mut self) -> Result<CommandData, ()> {
        if let Some(buf) = self.next_msg().await {
            let buf_str = String::from_utf8_lossy(&buf);
            let cmd = serde_json::from_str::<CommandData>(&buf_str).map_err(|e| {
                error!("Could not parse CommandData object: {} -> {}", e, buf_str);
                ()
            })?;
            Ok(cmd)
        } else {
            Err(())
        }
    }

    /// Writes a string to the server.
    ///
    /// # Arguments
    ///
    /// * `buf` - The string to write.
    ///
    /// # Returns
    ///
    /// `Ok(())` if successful, otherwise `Err(())`.
    pub async fn write_string(&mut self, buf: String) -> Result<(), ()> {
        self.write(buf.into_bytes()).await
    }

    /// Writes a byte buffer to the server.
    ///
    /// This function prepares the buffer with a length prefix and sends it over the socket.
    ///
    /// # Arguments
    ///
    /// * `buf` - The byte buffer to write.
    ///
    /// # Returns
    ///
    /// `Ok(())` if successful, otherwise `Err(())`.
    pub async fn write(&mut self, buf: Vec<u8>) -> Result<(), ()> {
        let send_buf = self.prepare_buffer(buf)?;
        self.send_to_socket(send_buf).await
    }

    /// Prepares the buffer by prefixing it with its length.
    ///
    /// # Arguments
    ///
    /// * `buf` - The byte buffer to prepare.
    ///
    /// # Returns
    ///
    /// `Ok(Vec<u8>)` containing the prepared buffer, otherwise `Err(())`.
    fn prepare_buffer(&self, buf: Vec<u8>) -> Result<Vec<u8>, ()> {
        let buf_len = buf.len();
        let mut send_buf = vec![0; MESSAGE_LENGTH_BYTES];
        BigEndian::write_u32(&mut send_buf, buf_len.try_into().unwrap());
        send_buf.extend(buf);
        Ok(send_buf)
    }

    /// Sends the prepared buffer to the socket.
    ///
    /// # Arguments
    ///
    /// * `send_buf` - The prepared byte buffer to send.
    ///
    /// # Returns
    ///
    /// `Ok(())` if successful, otherwise `Err(())`.
    pub async fn send_to_socket(&mut self, send_buf: Vec<u8>) -> Result<(), ()> {
        if let Some(s) = self.socket_w.as_mut() {
            s.write_all(&send_buf).await.map_err(|_| {
                error!("Could not write to socket");
                ()
            })?;
            s.flush().await.map_err(|_| {
                error!("Could not flush socket");
                ()
            })?;
            Ok(())
        } else {
            error!("Socket writer is None");
            Err(())
        }
    }

    /// Constructs a response message.
    ///
    /// # Arguments
    ///
    /// * `success` - Indicates if the operation was successful.
    /// * `message` - An optional message.
    /// * `code` - An optional status code.
    ///
    /// # Returns
    ///
    /// `Option<Vec<u8>>` containing the serialized response, or `None`.
    pub async fn response(
        self,
        success: bool,
        message: Option<String>,
        code: Option<u32>,
    ) -> Option<Vec<u8>> {
        let response = Response {
            success,
            message,
            code,
            ack_type: None,
        };
        let serialized = serde_json::to_string(&response).unwrap();
        Some(serialized.into_bytes())
    }

    /// Generates an acknowledgment message.
    ///
    /// # Returns
    ///
    /// A byte vector representing "ACK".
    pub fn ack(&mut self) -> Vec<u8> {
        b"ACK".to_vec()
    }

    /// Generates a negative acknowledgment message.
    ///
    /// # Returns
    ///
    /// A byte vector representing "NACK".
    pub fn nack(&mut self) -> Vec<u8> {
        b"NACK".to_vec()
    }

    /// Generates a skip acknowledgment message.
    ///
    /// # Returns
    ///
    /// A byte vector representing "SKIP".
    pub fn skip(&mut self) -> Vec<u8> {
        b"SKIP".to_vec()
    }

    /// Sends an acknowledgment message to the server.
    ///
    /// # Asynchronous Behavior
    ///
    /// This is an asynchronous function and should be awaited.
    pub async fn send_ack(&mut self) {
        let buf = self.ack();
        _ = self.write(buf).await;
    }

    /// Sends a negative acknowledgment message to the server.
    ///
    /// # Asynchronous Behavior
    ///
    /// This is an asynchronous function and should be awaited.
    pub async fn send_nack(&mut self) {
        let buf = self.nack();
        _ = self.write(buf).await;
    }

    /// Sends a skip acknowledgment message to the server.
    ///
    /// # Asynchronous Behavior
    ///
    /// This is an asynchronous function and should be awaited.
    pub async fn send_skip(&mut self) {
        let buf = self.skip();
        _ = self.write(buf).await;
    }

    /// Waits for an acknowledgment from the server.
    ///
    /// This function reads the acknowledgment message from the server,
    /// interprets it, and returns the corresponding `AckType`.
    ///
    /// # Returns
    ///
    /// - `Ok(AckType::Ack)` if an "ACK" message is received.
    /// - `Ok(AckType::Skip)` if a "SKIP" message is received.
    /// - `Err(())` for any other message or if reading fails.
    ///
    /// # Errors
    ///
    /// Returns `Err(())` if:
    /// - The socket is not readable.
    /// - Reading the message length or content fails.
    /// - The received message does not match "ACK" or "SKIP".
    pub async fn acked(&mut self) -> Result<AckType, ()> {
        // Ensure the socket is in a readable state
        self.readable().await.unwrap();

        // Buffer to hold the message length prefix
        let mut buf = vec![0; MESSAGE_LENGTH_BYTES];
        // Get a mutable reference to the socket reader
        let s = self.socket_r.as_mut().unwrap();
        // Read the length prefix from the socket
        s.read_exact(&mut buf).await.map_err(|_| ())?;
        // Decode the message length using BigEndian
        let msg_len = BigEndian::read_u32(&buf);
        // Buffer to hold the actual acknowledgment message
        let mut recv_buf = vec![0; msg_len as usize];
        // Read the acknowledgment message based on the decoded length
        s.read_exact(&mut recv_buf).await.map_err(|_| ())?;
        // Compare the received message to known acknowledgment types
        if recv_buf == self.ack() {
            Ok(AckType::Ack)
        } else if recv_buf == self.skip() {
            Ok(AckType::Skip)
        } else {
            Err(())
        }
    }

    /// Placeholder for checking if the socket is readable.
    ///
    /// # Returns
    ///
    /// Always returns `Ok(())`. To be implemented.
    pub async fn readable(&mut self) -> Result<(), Box<dyn Error>> {
        // TODO: Implement this
        Ok(())
    }

    /// Retrieves the length of the next incoming message.
    ///
    /// This function reads the first few bytes to determine the length of the upcoming message.
    ///
    /// # Returns
    ///
    /// - `Some(usize)` with the message length if successful.
    /// - `None` if reading the length fails or the socket is not available.
    pub async fn next_msg_len(&mut self) -> Option<usize> {
        // Buffer to hold the message length prefix
        let mut buf = vec![0; MESSAGE_LENGTH_BYTES];
        // Get a mutable reference to the socket reader
        let s = self.socket_r.as_mut()?;
        // Attempt to read the length prefix; return `None` on failure
        s.read_exact(&mut buf).await.ok()?;
        // Decode the message length using BigEndian
        let msg_len = BigEndian::read_u32(&buf);
        Some(msg_len as usize)
    }

    /// Retrieves the next incoming message from the server.
    ///
    /// This function first determines the length of the incoming message and then reads the message.
    ///
    /// # Returns
    ///
    /// - `Some(Vec<u8>)` containing the message bytes if successful.
    /// - `None` if reading fails or the socket is not available.
    pub async fn next_msg(&mut self) -> Option<Vec<u8>> {
        // Get the length of the next message
        let msg_len = self.next_msg_len().await?;
        // Buffer to hold the actual message
        let mut recv_buf = vec![0; msg_len];
        // Get a mutable reference to the socket reader
        let s = self.socket_r.as_mut()?;
        // Attempt to read the message; return `None` on failure
        s.read_exact(&mut recv_buf).await.ok()?;
        Some(recv_buf)
    }
}
