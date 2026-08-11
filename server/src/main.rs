use clap::{Arg, ArgAction, Command};
use foundation::env;

mod serve;

#[tokio::main]
async fn main() -> Result<(), ()> {
    // Initialize environment and logger
    initialize_environment();

    // Define and parse command-line arguments
    let matches = create_command().get_matches();

    // Load configuration from the config file. There is no embedded fallback,
    // so a missing or unreadable config is fatal.
    let config_file = matches
        .get_one::<String>("conf")
        .map(|s| s.as_str())
        .unwrap_or("/etc/databurg.cnf");
    if let Err(e) = env::load_config(config_file) {
        eprintln!("Could not read config file {}: {}", config_file, e);
        std::process::exit(1);
    }

    // Start the server
    serve::serve().await;

    Ok(())
}

/// Initializes logging.
fn initialize_environment() {
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
