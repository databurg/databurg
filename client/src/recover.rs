use clap::ArgMatches;
use foundation::protocol::Stage;
use foundation::{
    actor, normalize_localdir, normalize_relative_save, protocol, Action, Actor, CommandData,
    FileSelector,
};
#[allow(unused_imports)]
use log::{error, info};
use std::{process::exit, time::SystemTime};

fn get_required_arg(args: &ArgMatches, key: &str, required: bool) -> String {
    args.get_one::<String>(key)
        .map(|s| s.to_string())
        .unwrap_or_else(|| {
            if required {
                error!("{} is required", key);
                exit(1);
            } else {
                "".to_string()
            }
        })
}

pub async fn handler(args: &ArgMatches) -> Result<(), ()> {
    // Destination directory on the clients machine
    let dest_dir = normalize_localdir(get_required_arg(args, "dest", true).as_str());
    if dest_dir.is_err() {
        error!("Destination path does not exist: {:?}", dest_dir.err());
        exit(1);
    }
    let dest_dir = dest_dir.unwrap().to_str().unwrap().to_string();

    // Source directory on the Databurg server, relative ot the bucket root
    let source_dir = normalize_relative_save(get_required_arg(args, "source", true).as_str());
    let bucket = get_required_arg(args, "bucket", true);

    // Point in time to recover files from
    let point_in_time_str = get_required_arg(args, "time", false);
    let mut point_in_time: Option<SystemTime> = None;
    if !point_in_time_str.is_empty() {
        point_in_time = match point_in_time_str.parse::<u64>() {
            Ok(time) => Some(SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(time)),
            Err(_) => {
                error!("Invalid time format - Use UNIX timestamp");
                exit(1);
            }
        };
    }

    let job = CommandData {
        action: Action::Recover,
        query: Some(FileSelector {
            path: source_dir.clone(),
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
    let mut h = protocol::Handler::new(a, dest_dir.as_str());

    let job = serde_json::to_string(&job).unwrap();
    let buf_bytes = job.into_bytes();
    _ = h.actor.write(buf_bytes).await;
    if h.actor.acked().await.is_err() {
        // Could not receive ACK for this transaction
        error!("Could not receive ACK for this transaction");
        return Err(());
    }

    let handle = tokio::spawn(async move {
        h.run(Some(Stage::ClientFileReceiver)).await;
    });

    _ = tokio::join!(handle);

    Ok(())
}
