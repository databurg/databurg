# databurg — domain glossary

Shared vocabulary for the codebase. Names here are load-bearing: use them exactly
in code, comments, and architecture reviews.

## Storage domain (planned — the `ObjectStore` deepening)

Designed but not yet implemented; lands after the server-hardening work merges.
It concentrates ~21 scattered `Handler` methods (path building, delete markers,
point-in-time recovery, version selection, metadata) behind one deep interface.

- **ObjectStore** — root-scoped owner of *all* server-side storage semantics, and
  the single place peer input is validated (the traversal boundary). Holds only
  the storage root; imports no protocol wire types. Hands out `Bucket`s.
- **Bucket** — a per-operation, already-sanitized handle for one bucket. The
  bucket name is sanitized exactly once (`ObjectStore::bucket`); object operations
  hang off the handle and are themselves bucket-free. Minted fresh per request,
  never bound to a connection.
- **Object** — a logical file in a bucket, addressed by its bucket-relative path.
  On disk it is a directory `<root>/<bucket>/<rel>/` holding one blob file per
  content version plus sidecars.
- **Version** — a resolved, streamable version of an object: `hash`, `blob_path`,
  `stored_at`, `meta`. What point-in-time selection returns.
- **ObjectMeta** — owned twin of the on-disk `.meta` (permissions/ownership/times);
  serde-compatible with the wire `ObjectMetadata`, adapted from `InnerJob` at the
  session edge.
- **Deletion interval** — a window during which an object was absent. Closed
  intervals `[deleted_at, deleted_til)` live in `.deletion.history`
  (`DeletionInterval`); the *open* interval `[marker-mtime, ∞)` is implied by a
  live `.deleted` marker and is never stored. `deleted_at` is the marker's own
  timestamp, never the object directory's creation time.
- **Present-set** — the set of object paths a client reports as currently present
  in one sync. `reconcile_deletions(present)` marks deleted everything the bucket
  held at the previous sync that is absent from the present-set (diff against the
  last `SyncRecord`, preserving today's behavior — NOT a disk walk; see the
  compatibility contract below).
- **PendingBlob** — a reserved-but-uncommitted blob write (a `.tmp-*` file).
  `begin` mints it; the session streams bytes into `tmp_path()` and fsyncs;
  `publish` finalizes atomically (rename → `.meta` → `latest` → close interval);
  `Drop`/`discard` aborts. This is the only place async touches storage.
- **adopt** — the dedup / re-add fast path: the bucket already holds the exact
  blob → promote it to `latest`, clear the delete marker, skip receiving bytes.
- **SyncRecord** / **SyncTag** — an owned bucket sync-log entry in
  `<root>/.<bucket>.meta`. A **byte-identical twin** of the wire `SyncMetadata`
  (same fields — `tags`, `timestamp`, `bucket`, `ack/nack/skip_count`,
  `file_list`, `file_list_raw` — and same serde attributes), so it serializes to
  exactly today's on-disk JSON. Mapped to/from the wire type at the session edge.

### On-disk layout the store owns
```
<root>/<bucket>/<rel-path>/
    <hash>              one blob file per content version (name = 64-hex content id)
    latest              symlink → the current <hash>
    .meta               ObjectMeta JSON for the object
    .deleted            delete marker; its mtime = the deletion time
    .deletion.history   JSON array of DeletionInterval
    .tmp-*              in-flight PendingBlob writes
<root>/.<bucket>.meta   bucket sync-log (JSON array of SyncRecord)
<root>/.<bucket>.auth   auth token hash — owned by the authorizer, NOT the store
```

### The on-disk format is a COMPATIBILITY CONTRACT

Live backup servers hold customer data written by earlier releases. New code MUST
read, resolve and restore that existing data unchanged, and nightly backups must
keep working across an upgrade. Improving the layout is worthless if one customer
backup becomes unfindable.

Verified (2026-08-11) across every release — v0.0.1, v0.0.2, v0.2.0, v0.2.1:
`ObjectMetadata` and `DeletionHistoryItem` are byte-identical in all of them, and
`SyncMetadata` is field-identical in all of them. No release ever changed the
format. The single change is additive: `file_list_raw` on `SyncMetadata`
(`#[serde(default, skip_serializing_if = "Option::is_none")]`). Nothing anywhere
uses `deny_unknown_fields`, so old readers ignore it and new readers default it —
compatible in both directions, including a rollback.

Therefore, binding rules for the `ObjectStore`:

1. **The store's owned types are byte-identical twins of the wire types.**
   `ObjectMeta` ≡ `ObjectMetadata`, `DeletionInterval` ≡ `DeletionHistoryItem`,
   `SyncRecord` ≡ `SyncMetadata` (all fields, same serde attributes). They exist
   to keep the store free of wire imports — never to change the JSON.
2. **Never drop a persisted field** (notably `file_list` / `file_list_raw`) and
   never add `deny_unknown_fields`. New fields must be additive and defaulted.
3. **Keep the existing semantics** where they are observable on disk: deletion
   reconciliation stays a diff against the previous sync's file list, not a disk
   walk, so an upgrade cannot produce a different set of delete markers.
4. **Acceptance test:** a fixture laying down a storage tree in the released
   format (blobs + all sidecars + `.<bucket>.meta`) must prove that `current`,
   `list_at`, `read_sync_log` and a full recover work on that legacy data. No
   release ships without it.

Known limits of existing data (not caused by, and not fixable by, the store):
pre-upgrade `.deletion.history` entries carry the old, wrong `deleted_at` value
baked in, so point-in-time queries over deletions from before the fix stay as
imprecise as they were; only new deletions are correct. Separately, the client
hash change already in `main` gives every file one new version on the first
backup after a client upgrade — old versions remain recoverable.

## Roles

- **Server session** — drives the receive/serve protocol state machine over one
  TLS connection; owns the async byte-streaming. Adapts wire types
  (`SyncMetadata`, `InnerJob`, `FileActionData`) to/from the store's domain types
  at the edge. (Today this is `Handler` in server mode.)
- **Client receiver** — writes recovered files to a local destination directory.
  A separate concern from the storage root; outside the `ObjectStore`. (Today the
  client-side stages of `Handler`.)
