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

There is currently no supported way to reclaim old versions, and **no external
script should be pointed at the storage tree**. The layout has invariants that
are not obvious from the outside: a blob that is not the `latest` target is
still the correct answer for a point-in-time restore, so removing an
intermediate version makes recovery return *older bytes with no error*. Object
directories, `.deleted`, `.deletion.history` and `.<bucket>.meta` are equally
load-bearing — the deletion timeline exists nowhere else, and a lost
`.<bucket>.meta` permanently stops deleted files from ever being marked deleted.

Only `.tmp-*` files inside object directories are provably unreferenced (crash
residue from an interrupted upload) and safe to remove at any time.

Storage grows with real churn: an unchanged file costs nothing on a nightly run.
The dominant avoidable growth comes from deploys that rewrite modification times
(`tar` extracts, non-preserving copies) — every touched file becomes a new
version even when its bytes are unchanged. Preferring `rsync -a` on the source
side reclaims more than any purge would.

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
databurg recover -b MyBucket -s ./ -d ./test-recover [-c /etc/databurg.cnf]
```

To recover data from a specific point in time, specify the timestamp:

```sh
databurg recover -b MyBucket -s ./ -d ./test-recover -t 1730925459 [-c /etc/databurg.cnf]
```

## Configuration Guidelines

Databurg is configured at runtime from a config file (`/etc/databurg.cnf` by
default, or `-c <path>`). Nothing is compiled into the binaries, and a missing or
unreadable config file is a fatal error. See [`databurg.cnf.sample`](./databurg.cnf.sample).

> `-c` is accepted on either side of the subcommand — both
> `databurg -c FILE backup …` and `databurg backup -c FILE …` work.
> Release 0.0.1 accepted only the latter and then silently ignored the value,
> always reading `/etc/databurg.cnf`; from 0.3.1 on the path is honoured
> wherever it is given, so scripts written for 0.0.1 keep working unchanged.

- `SERVER_HOSTNAME`: Server IP address or hostname.
- `SERVER_PORT`: Server listening port (default: 2403).
- `PRE_SHARED_SECURITY_TOKEN`: Security token for authentication.
- `SERVER_CERT_SHA256`: **Client, required.** SHA-256 of the server's TLS
  certificate — the pin the client verifies the server against. The client
  refuses to connect without it, and rejects any server whose certificate does
  not match. `databurgd` logs this value at startup (`RUST_LOG=info`), or derive
  it with `openssl x509 -in cert.pem -outform DER | sha256sum`.
- `SERVER_LISTEN`: IP address for server binding (default: 0.0.0.0).
- `STORAGE_BASE_DIR`: Base directory for data storage.
- `MAX_CONNECTIONS`: Server: cap on concurrent connections (default: 128).
- `CERTIFICATE_FILE`: Path to a TLS certificate. **Required in production** —
  without it the server generates an ephemeral self-signed certificate whose pin
  changes on every restart, which breaks pinned clients.
- `PRIVATE_KEY_FILE`: Path to the matching TLS private key.

To use a custom configuration file:

```sh
databurg backup -c /etc/databurg.cnf ..
databurg recover -c /etc/databurg.cnf ..
databurg status -c /etc/databurg.cnf ..
```

This allows different settings for various environments, including `PRE_SHARED_SECURITY_TOKEN`.

## License

Databurg is licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0). For more details, see the [LICENSE](./LICENSE) file.

