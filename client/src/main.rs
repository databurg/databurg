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

// Reusable argument for config file
fn conf_arg() -> Arg {
    Arg::new("conf")
        .short('c')
        .long("config")
        .help("Specify config file")
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
                .arg(conf_arg())
                .arg(source_arg())
                .arg(bucket_arg())
                .arg(tags_arg()),
        )
        .subcommand(
            Command::new("recover")
                .about("Get files from a Databurg server")
                .arg(conf_arg())
                .arg(source_arg())
                .arg(dest_arg())
                .arg(bucket_arg())
                .arg(time_arg()),
        )
        .subcommand(
            Command::new("status")
                .about("Get bucket status")
                .arg(conf_arg())
                .arg(source_arg())
                .arg(bucket_arg())
                .arg(time_arg()),
        );

    // Parse the CLI arguments
    let matches = cmd.get_matches();

    // Handle configuration file loading if specified
    if let Some(config_file) = matches.get_one::<String>("conf") {
        // Load the specified config file
        env::load_config(config_file.to_string());
    } else {
        // Use built in config? We prefer to use the config file from the default path
        env::load_config("/etc/databurg.cnf".to_string());
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

/// Initializes environment variables and logging
fn initialize_environment() {
    env::init();
    env_logger::init();
}
