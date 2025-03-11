use foundation::{actor, Actor, ChannelMessage, Job};
#[allow(unused_imports)]
use tokio::sync::mpsc::{channel, Sender};
use tokio::task::JoinHandle;

const MAX_THREADS: usize = 40;

/// Starts multiple asynchronous connections for syncing jobs.
///
/// # Arguments
///
/// * `job_count` - The number of jobs to be processed.
///
/// # Returns
///
/// A tuple containing:
/// - A vector of job senders.
/// - A vector of join handles for the spawned tasks.
/// - A vector of raw message senders.
pub async fn sync_threaded(
    job_count: usize,
) -> (
    Vec<Sender<Option<Job>>>,
    Vec<JoinHandle<()>>,
    Vec<Sender<Option<ChannelMessage>>>,
) {
    // Ensure job count is not zero
    if job_count == 0 {
        panic!("Job count cannot be 0");
    }

    // Determine the maximum number of connections to create
    let max_connection = if job_count < MAX_THREADS {
        job_count
    } else {
        MAX_THREADS
    };

    log::debug!("Starting {} connections", max_connection);

    // Vectors to hold the senders and handles
    let mut handles = vec![];
    let mut senders = vec![];
    let mut raw_senders = vec![];

    // Create the specified number of connections
    for _ in 0..max_connection {
        let (job_sender_tx, job_sender_rx) = channel::<Option<Job>>(1);
        let (raw_sender_tx, raw_sender_rx) = channel::<Option<ChannelMessage>>(1);

        // Spawn an asynchronous task for each connection
        let h = tokio::spawn(async move {
            // Build Socket and spawn Actor with Socket as argument
            let tls_stream = actor::connect().await.unwrap();
            let mut a = Actor::new(tokio_rustls::TlsStream::Client(tls_stream));

            // Perform handshake
            if a.handshake().await.is_err() {
                return;
            }

            // Perform authentication
            if a.authenticate().await.is_err() {
                return;
            }

            // Start sending jobs and raw messages
            a.sending(job_sender_rx, raw_sender_rx).await;
        });

        // Store the senders and handles
        senders.push(job_sender_tx);
        raw_senders.push(raw_sender_tx);
        handles.push(h);
    }

    (senders, handles, raw_senders)
}
