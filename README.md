# Databurg Backup System

![Build And Release](https://github.com/databurg/databurg/actions/workflows/release.yml/badge.svg)

Databurg is an open-source backup system designed for efficient local and remote data management. It balances ease of use with robust data security and integrity, making it suitable for both small projects and large-scale deployments. Databurg provides flexible and reliable file backup options, advanced data recovery features, and tools for monitoring storage status, serving as a simpler alternative to traditional systems like  S3.

## Key Features

- **Advanced Backup Capabilities**: Back up your files and folders locally or to on-premises storage, supporting anything from small setups to large-scale environments.
- **Granular Recovery Options**: Restore data from specific past versions for precise control over accidental changes or deletions. Either entire buckets or single files.
- **Real-Time Status Monitoring**: Track synchronization, recovery points, and storage metrics to keep your backups reliable.
- **Command-Line Interface (CLI)**: Manage backups, recoveries, and monitor status easily using CLI commands.
- **Customizable Configurations**: Tailor configurations to fit various workflows, with built-in security features for safe data management.
- **TypeScript Integration**: Provides a demo for connecting to the Databurg server using TypeScript, including listing recovery points for a bucket.

## Getting Started

### Cloning the Repository

To start using Databurg, clone the repository:

```sh
git clone https://github.com/databurg/databurg.git
cd databurg
```

This provides the complete source code for customization and collaborative development.

### Building Databurg

#### Local Build

To build a Databurg release locally:

```sh
cargo build -p server -r
cargo build -p client -r
```

The executables are located at `target/release/databurg` and `target/release/databurgd`.

#### Cross-Compilation for Linux Targets on macOS

1. **Install Zig and Zigbuild**:

   ```sh
   brew install zig zigbuild
   ```

2. **Add Rust Targets**:

   ```sh
   rustup target add x86_64-unknown-linux-gnu
   # Optional: rustup target add aarch64-unknown-linux-musl
   ```

3. **Compile Using Zig**:

   ```sh
   cargo zigbuild -p server -r --target x86_64-unknown-linux-gnu
   cargo zigbuild -p client -r --target x86_64-unknown-linux-gnu
   ```

This will generate Linux-compatible binaries.

## Databurg Server

### Starting the Server

To start the Databurg server:

```sh
databurgd -d
```

By default, a self-signed certificate is generated. For production, use custom certificates specified in `server/src/serve.rs`.

### Installing as a System Service

To install Databurg as a system service:

```sh
cp databurgd.service /etc/systemd/system/databurgd.service
systemctl daemon-reload
systemctl start databurgd
systemctl enable databurgd
```

> Ensure that you have configured a /etc/databurg.cnf file (refer to the instructions below).

This ensures the server runs automatically on system reboot.

### Purging Obsolete Data

To keep your storage efficient, use a script to remove outdated data: [Purge Script](https://gist.github.com/amallek/749fd7d4da8e23a4319a147705298215).

## Databurg Client

### Performing Directory Backups

To back up a directory to a Databurg bucket:

```sh
databurg backup -b MyBucket -s ./test-source
```

Add tags for metadata using `-t "key=value;key1=value1"`.

### Monitoring Bucket Status

To check the status and metadata of a bucket:

```sh
databurg status -b MyBucket
```

### Listing Recovery Points

To convert status output into a CSV list of recovery points using `jq`:

```sh
databurg status -b MyBucket | jq -r '.Ok.meta[] | select(.ack_count > 0) | [.bucket, .ack_count, .nack_count, .skip_count, .timestamp.secs_since_epoch] | @csv'
```

### Recovering Files

To restore all files from a bucket to a specific directory:

```sh
databurg recover -b MyBucket -s ./ -d ./test-recover
```

To recover data from a specific point in time, specify the timestamp:

```sh
databurg recover -b MyBucket -s ./ -d ./test-recover -t 1730925459
```

## Configuration Guidelines

Databurg is configured using environment variables set in a `.env` file located in the project's root directory:

- `SERVER_HOSTNAME`: Server IP address or hostname.
- `SERVER_PORT`: Server listening port (default: 2403).
- `PRE_SHARED_SECURITY_TOKEN`: Security token for authentication.
- `SERVER_LISTEN`: IP address for server binding (default: 0.0.0.0).
- `STORAGE_BASE_DIR`: Base directory for data storage.
- `CERTIFICATE_FILE`: Path to a custom TLS certificate (optional).
- `PRIVATE_KEY_FILE`: Path to a custom TLS private key (optional).

> **Note**: Avoid setting `PRE_SHARED_SECURITY_TOKEN` in your `.env` file during compile time, as it may be embedded in the compiled binary, posing a security risk.

To use a custom configuration file:

```sh
databurg -c /etc/databurg.cnf
```

This allows different settings for various environments, including `PRE_SHARED_SECURITY_TOKEN`.

## License

Databurg is licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0). For more details, see the [LICENSE](./LICENSE) file.

