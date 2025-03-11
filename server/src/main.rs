use clap::{Arg, ArgAction, Command};
use foundation::env;

mod serve;

#[tokio::main]
async fn main() -> Result<(), ()> {
    // Initialize environment and logger
    initialize_environment();

    // Define and parse command-line arguments
    let matches = create_command().get_matches();

    // Handle configuration file loading if specified
    if let Some(config_file) = matches.get_one::<String>("conf") {
        // Load the specified config file
        env::load_config(config_file.to_string());
    } else {
        // Use built in config? We prefer to use the config file from the default path
        env::load_config("/etc/databurg.cnf".to_string());
    }

    // Start the server
    serve::serve().await;

    Ok(())
}

/// Initializes environment variables and logging
fn initialize_environment() {
    env::init();
    env_logger::init();
}

/// Creates and returns the CLI command configuration
fn create_command() -> Command {
    Command::new("databurgd")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or("NOT_FOUND"))
        .before_help("Databurg Backup Server")
        .arg(
            Arg::new("conf")
                .short('c')
                .long("config")
                .help("Specify config file"),
        )
        .arg(
            Arg::new("daemonize")
                .short('d')
                .long("daemon")
                .action(ArgAction::SetTrue),
        )
}
