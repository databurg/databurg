use clap::{Arg, Command};
use std::process::exit;
mod connect;
mod recover;
mod status;
mod sync;
use foundation::env;
use log::info;

// Reusable argument for source directory
fn source_arg() -> Arg {
    Arg::new("source")
        .short('s')
        .long("source")
        .help("Specify source directory")
        .global(true)
}

// Reusable argument for bucket
fn bucket_arg() -> Arg {
    Arg::new("bucket")
        .short('b')
        .long("bucket")
        .help("Specify remote bucket")
        .global(true)
}

// Reusable argument for time
fn time_arg() -> Arg {
    Arg::new("time")
        .short('t')
        .long("time")
        .help("Specify point in time to recover files from")
        .global(true)
}

// Reusable argument for destination
fn dest_arg() -> Arg {
    Arg::new("dest")
        .short('d')
        .long("destination")
        .help("Specify destination directory")
        .global(true)
}

// Reusable argument for config file.
//
// Global so that it is accepted both before and after the subcommand. Release
// 0.0.1 registered it on the subcommands, so deployed scripts pass it as
// `databurg backup -c FILE ...`; rejecting that form would break every existing
// cron runner on upgrade.
fn conf_arg() -> Arg {
    Arg::new("conf")
        .short('c')
        .long("config")
        .help("Specify config file")
        .global(true)
}

// Reusable argument for tags
fn tags_arg() -> Arg {
    Arg::new("tags")
        .short('t')
        .long("tags")
        .help("Specify meta tags")
        .global(true)
}

#[tokio::main]
async fn main() -> Result<(), ()> {
    // Initialize environment and logger
    initialize_environment();

    // Define the CLI command structure using Clap
    let cmd = Command::new("databurg")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or("NOT_FOUND"))
        .before_help("Databurg Backup Client")
        .subcommand(
            Command::new("backup")
                .about("Copy files to a Databurg server")
                .arg(source_arg())
                .arg(bucket_arg())
                .arg(tags_arg()),
        )
        .subcommand(
            Command::new("recover")
                .about("Get files from a Databurg server")
                .arg(source_arg())
                .arg(dest_arg())
                .arg(bucket_arg())
                .arg(time_arg()),
        )
        .subcommand(
            Command::new("status")
                .about("Get bucket status")
                .arg(source_arg())
                .arg(bucket_arg())
                .arg(time_arg()),
        )
        .arg(conf_arg());

    // Parse the CLI arguments
    let matches = cmd.get_matches();

    // Load configuration from the config file. There is no embedded fallback,
    // so a missing or unreadable config is fatal.
    //
    // Look in the subcommand's matches as well: `-c` is accepted on both sides
    // of the subcommand, and which set of matches carries it depends on where
    // it was given. Scripts written for 0.0.1 pass it after the subcommand.
    let config_file = matches
        .get_one::<String>("conf")
        .or_else(|| {
            matches
                .subcommand()
                .and_then(|(_, sub)| sub.get_one::<String>("conf"))
        })
        .map(|s| s.as_str())
        .unwrap_or("/etc/databurg.cnf");
    if let Err(e) = env::load_config(config_file) {
        eprintln!("Could not read config file {}: {}", config_file, e);
        exit(1);
    }

    // Match and handle the provided subcommand
    let subcommand = matches.subcommand();
    if let Some(("backup", args)) = subcommand {
        info!("Backing up files");
        match sync::handler(args).await {
            Ok(_) => {
                exit(0);
            }
            Err(_) => {
                exit(1);
            }
        };
    } else if let Some(("recover", args)) = subcommand {
        info!("Recovering files");
        match recover::handler(args).await {
            Ok(_) => {
                exit(0);
            }
            Err(_) => {
                exit(1);
            }
        };
    } else if let Some(("status", args)) = subcommand {
        info!("Check bucket status and list recovery points");
        match status::handler(args).await {
            Ok(_) => {
                exit(0);
            }
            Err(_) => {
                exit(1);
            }
        };
    }

    Ok(())
}

/// Initializes logging.
fn initialize_environment() {
    env_logger::init();
}
