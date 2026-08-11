# Changelog

All notable changes to this project are documented here.

## 0.3.1

**Upgrade from 0.0.x directly to this release. Do not deploy 0.3.0.**

### Fixed

- **The per-file version identifier is compatible with earlier releases again.**
  0.3.0 mixed the file size and the first 256 KiB of content into it, so every
  hash differed from the ones a server already stores: the first backup after a
  client upgrade would have re-uploaded every file of every account, and since
  old versions are never reclaimed, that storage would stay occupied
  permanently. The released formula (modification time + file name) is restored
  and pinned by a test that replicates it verbatim.
  This is a client-side change; servers and their stored data are untouched.
- **`MAX_CONNECTIONS` no longer throttles a normal fleet.** The default of 128
  was below what a single hosting node produces (a backup process opens eleven
  connections and the runner starts ten in parallel — about 110 per node), and
  because the semaphore is acquired inside the accept loop, exhausting it
  stalled accept entirely until clients hit their idle timeout. Default raised
  to 2048; the systemd unit sets `LimitNOFILE=65535` so the cap is reachable,
  and gains `RestartSec` and `After=network-online.target`.
- **`acked()` enforces the frame-size limit and idle timeout** that the rest of
  the read path already had. It is reachable after authentication on the
  recover path, where a peer could announce a length and stall to pin a
  connection.
- The TypeScript binding verifies the server certificate against a pin instead
  of accepting any certificate, and no longer swallows connection errors.

### Removed

- The README's link to an external "purge script". Removing an intermediate
  version makes point-in-time recovery return older bytes with no error, and
  several sidecar files are load-bearing in non-obvious ways. See
  [CONTEXT.md](./CONTEXT.md) for what the storage layout guarantees.

## 0.3.0 — withdrawn

Do not deploy. Its version identifier is incompatible with previously stored
hashes and causes a full one-time re-upload of every account. Use 0.3.1.

### Added

- **Files whose names are not valid UTF-8** (Latin-1/CP1252, common in older
  uploads) are backed up and recovered byte-exact. Previously such a name
  aborted the whole account run while the process still exited 0, silently
  leaving every remaining file out of the backup.
- **Backups report failure.** A file that cannot be walked, stat'ed, opened or
  read is listed and the run exits non-zero instead of vanishing silently.
- **Uploads use all connections concurrently** instead of effectively
  serialising through one.
- **TLS certificate pinning.** The client verifies the server against
  `SERVER_CERT_SHA256` and refuses to connect without it. Previously it
  accepted any certificate, so anyone in the network path could impersonate the
  server, read customer data in transit, or feed arbitrary bytes to the client
  during a recover.
- **Configuration and secrets are read only at runtime.** Nothing is compiled
  into the binaries any more, and a missing or unreadable config file is fatal
  rather than silently falling back to an embedded copy.

### Fixed

- **Cross-tenant backup destruction.** `SetSyncMetadata` was the only
  bucket-scoped stage that ran without authorization, so any peer could mark
  another tenant's files deleted and overwrite that bucket's metadata history.
- **Torn blobs reported as complete.** Uploads published `.meta` and the
  `latest` symlink before any data arrived, and never actually issued an
  `fsync`. An interrupted upload left a truncated blob that preflight then
  treated as present — never re-sent, and served corrupt on recover. Blobs are
  now streamed to a temporary file and committed only after every declared byte
  has arrived: fsync, atomic rename, then metadata.
- **Point-in-time recovery** was wrong in both directions: deletion times were
  taken from the object directory's creation time, a currently-deleted file was
  still included, and a file re-added with changed content stayed marked
  deleted forever.
- **Path traversal.** Every peer-supplied bucket, path and content hash is now
  validated where it enters the storage layer, instead of at scattered call
  sites that could be forgotten.
- Denial-of-service limits: frame size, idle connections, decompression output
  and connection count are all bounded.

## 0.2.x and earlier

See the commit history. Note that 0.0.1 silently ignores `-c/--config` and
always reads `/etc/databurg.cnf`; from 0.3.0 on, `-c` is a top-level option
(`databurg -c FILE backup ...`) and is honoured.
