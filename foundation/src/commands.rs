use std::time::SystemTime;

use log::{error, info};

use crate::{
    actor, protocol, Action, Actor, BucketStatus, CommandData, FileSelector, PreflightFileInfo,
    PreflightRequestData,
};

pub async fn status(bucket: String, point_in_time: Option<SystemTime>) -> Result<BucketStatus, ()> {
    let job = CommandData {
        action: Action::Status,
        query: Some(FileSelector {
            path: "".to_string(),
            point_in_time,
            bucket,
        }),
    };

    let tls_stream = actor::connect().await.unwrap();
    let mut a = Actor::new(tokio_rustls::TlsStream::Client(tls_stream));
    if a.handshake().await.is_err() {
        return Err(());
    }
    if a.authenticate().await.is_err() {
        return Err(());
    }
    let mut h = protocol::Handler::new(a, "");

    let job = serde_json::to_string(&job).unwrap();
    let buf_bytes = job.into_bytes();
    _ = h.actor.write(buf_bytes).await;
    if h.actor.acked().await.is_err() {
        // Could not receive ACK for this transaction
        return Err(());
    }

    if let Some(msg) = h.actor.next_msg().await {
        let msg = String::from_utf8_lossy(&msg);
        let msg = msg.trim();
        if let Ok(bucket_status) = serde_json::from_str(msg) {
            return Ok(bucket_status);
        }

        error!("Bucket access denied");
    }

    Err(())
}

pub async fn preflight(
    bucket: String,
    file_list: Vec<PreflightFileInfo>,
) -> Result<Vec<PreflightFileInfo>, ()> {
    let tls_stream = actor::connect().await.unwrap();
    let mut a = Actor::new(tokio_rustls::TlsStream::Client(tls_stream));
    if a.handshake().await.is_err() {
        return Err(());
    }
    if a.authenticate().await.is_err() {
        return Err(());
    }
    let mut h = protocol::Handler::new(a, "");

    // 1.Send the preflight command
    info!("Sending preflight command");
    let job = serde_json::to_string(&CommandData {
        action: Action::Preflight,
        query: Some(FileSelector {
            path: "".to_string(),
            point_in_time: None,
            bucket,
        }),
    })
    .unwrap();
    _ = h.actor.write(job.into_bytes()).await;
    if h.actor.acked().await.is_err() {
        // Could not receive ACK for this transaction
        return Err(());
    }

    // 2. Send the actual preflight input data
    info!("Sending preflight input data");
    let request = serde_json::to_string(&PreflightRequestData { file_list })
        .unwrap()
        .into_bytes();
    _ = h.actor.write(request).await;
    if h.actor.acked().await.is_err() {
        return Err(());
    }

    // 3. Receive the preflight response data
    info!("Receiving preflight response");
    if let Some(msg) = h.actor.next_msg().await {
        let msg = String::from_utf8_lossy(&msg);
        let msg = msg.trim();
        let file_list = serde_json::from_str(msg).unwrap();
        return Ok(file_list);
    }

    Err(())
}
