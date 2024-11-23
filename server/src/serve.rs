use crate::env;
use foundation::{protocol::Handler, Actor};
#[allow(unused_imports)]
use log::{error, info};
use rcgen::{generate_simple_self_signed, CertifiedKey};
use rustls::ServerConfig;
use rustls_pemfile::{self, certs, private_key};
use std::{fs, io::Cursor, sync::Arc};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

/// Main server function to start serving
pub async fn serve() {
    let server_listen_addr = get_server_address();
    let storage_base_dir = get_storage_base_dir();
    let tls_acceptor = setup_tls_acceptor();

    let listener = TcpListener::bind(&server_listen_addr)
        .await
        .expect("Cannot bind to address");

    log::info!("Databurg Server Listening on: {}", server_listen_addr);
    log::info!("Databurg Storage Basedir: {}", storage_base_dir);

    loop {
        match listener.accept().await {
            Ok((socket, _addr)) => {
                let tls_acceptor = tls_acceptor.clone();
                let storage_base_dir = storage_base_dir.clone();

                tokio::spawn(async move {
                    match tls_acceptor.accept(socket).await {
                        Ok(tls_stream) => {
                            log::debug!(
                                "Client connected: {:?}",
                                tls_stream.get_ref().0.peer_addr()
                            );
                            handle_client(tls_stream, &storage_base_dir).await;
                        }
                        Err(e) => error!("TLS handshake failed: {:?}", e),
                    }
                });
            }
            Err(e) => error!("Could not accept connection: {:?}", e),
        }
    }
}

/// Retrieves server address from environment variables
fn get_server_address() -> String {
    format!(
        "{}:{}",
        env::var("SERVER_LISTEN").unwrap_or("0.0.0.0".to_string()),
        env::var("SERVER_PORT").unwrap_or("2403".to_string()),
    )
}

/// Retrieves storage base directory from environment variables
fn get_storage_base_dir() -> String {
    let base_dir = env::var("STORAGE_BASE_DIR").unwrap_or("".to_string());
    ensure_storage_base_dir(&base_dir);
    set_working_directory(&base_dir);
    base_dir
}

/// Ensures that the storage base directory exists
fn ensure_storage_base_dir(storage_base_dir: &str) {
    if !std::path::Path::new(storage_base_dir).exists() {
        match fs::create_dir_all(storage_base_dir) {
            Ok(_) => info!("Base dir created: {}", storage_base_dir),
            Err(e) => {
                error!("Could not create base dir: {:?}", e);
                std::process::exit(1);
            }
        }
    }
}

/// Sets the current working directory to the storage base directory
fn set_working_directory(storage_base_dir: &str) {
    if let Err(e) = std::env::set_current_dir(storage_base_dir) {
        error!("Could not set base dir: {:?}", e);
        std::process::exit(1);
    }
}

fn setup_tls_acceptor_self_signed() -> (String, String) {
    let (c, k) = generate_certificate();
    let cert = std::str::from_utf8(&c).unwrap();
    let key = std::str::from_utf8(&k).unwrap();
    (cert.to_string(), key.to_string())
}

/// Sets up TLS acceptor with a self-signed certificate
fn setup_tls_acceptor() -> TlsAcceptor {
    // If config contains path to cert and key, use them
    // (load them dynamically at start-up, not compile-time)
    let key_file = env::var("PRIVATE_KEY_FILE")
        .unwrap_or("".to_string())
        .as_str()
        .to_string();
    let cert_file = env::var("CERTIFICATE_FILE")
        .unwrap_or("".to_string())
        .as_str()
        .to_string();

    let mut key = String::new();
    let mut cert = String::new();

    if !cert_file.is_empty() && !key_file.is_empty() {
        key = fs::read_to_string(key_file).expect("Could not read private key file");
        cert = fs::read_to_string(cert_file).expect("Could not read certificate file");
    }

    if key.is_empty() || cert.is_empty() {
        log::info!("Generating self-signed certificate");
        (cert, key) = setup_tls_acceptor_self_signed();
    }

    let cert_chain = certs(&mut Cursor::new(cert))
        .map(|x| x.unwrap())
        .collect::<Vec<_>>();
    let keys = private_key(&mut Cursor::new(key)).unwrap().unwrap();

    // Create a server config and set the certificate and private key
    let config = ServerConfig::builder().with_no_client_auth();
    let config = config.with_single_cert(cert_chain, keys);
    let config = Arc::new(config.unwrap());

    TlsAcceptor::from(config)
}

/// Handles an individual client connection
async fn handle_client(
    tls_stream: tokio_rustls::server::TlsStream<tokio::net::TcpStream>,
    storage_base_dir: &str,
) {
    let actor = Actor::new(tokio_rustls::TlsStream::Server(tls_stream));
    let mut handler = Handler::new(actor, storage_base_dir);
    handler.run(None).await;
    log::debug!("Client disconnected");
}

/// Generates a self-signed certificate and private key
fn generate_certificate() -> (Vec<u8>, Vec<u8>) {
    // Define parameters for the certificate
    let CertifiedKey { cert, key_pair } =
        generate_simple_self_signed(vec![env::var("SERVER_HOSTNAME")
            .unwrap_or("databurgd".to_string())
            .to_string()])
        .expect("Failed to create certificate params");
    let cert_pem = cert.pem();
    let key_pem = key_pair.serialize_pem();
    (cert_pem.into_bytes(), key_pem.into_bytes())
}
