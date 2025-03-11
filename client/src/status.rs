use clap::ArgMatches;
use foundation::commands::status;
#[allow(unused_imports)]
use log::{error, info};
use std::{process::exit, time::SystemTime};

pub async fn handler(args: &ArgMatches) -> Result<(), ()> {
    let point_in_time = match args.get_one::<String>("time") {
        Some(time) => match time.parse::<u64>() {
            Ok(time) => Some(SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(time)),
            Err(_) => {
                error!("Invalid time format - Use UNIX timestamp");
                exit(1);
            }
        },
        None => None,
    };

    let bucket = match args.get_one::<String>("bucket") {
        Some(bucket) => bucket.to_string(),
        None => {
            error!("Bucket is required");
            exit(1);
        }
    };

    let bucket_status = status(bucket, point_in_time).await;
    println!("{}", serde_json::to_string(&bucket_status).unwrap());

    Ok(())
}
