#!/bin/sh
if [ ! -f .env ]; then
    tee .env <<EOF
RUST_LOG=error
SERVER_HOSTNAME=127.0.0.1
SERVER_PORT=2403
SERVER_LISTEN=0.0.0.0
STORAGE_BASE_DIR=./test-storage
PRE_SHARED_SECURITY_TOKEN=your_secret
#CERTIFICATE_FILE=/path/to/cert.pem
#PRIVATE_KEY_FILE=/path/to/key.pem
EOF
fi
cargo build -p server && \
cargo build -p client
