# liters-mobile

**Continuous SQLite replication you can embed in an iOS or Android app.**

[![test](https://github.com/vishk23/liters-mobile/actions/workflows/test.yml/badge.svg)](https://github.com/vishk23/liters-mobile/actions/workflows/test.yml)
&nbsp;·&nbsp; MIT &nbsp;·&nbsp; pre-1.0

[Litestream](https://litestream.io) is the standard answer to "how do I get a
SQLite database off this machine, continuously, without running a replication
server?" — but it is a Go daemon that watches files, which is not something you
can run inside an app that iOS suspends thirty seconds after the user swipes
away. liters-mobile is a Rust library that reads and writes Litestream v0.5's
LTX file format and bucket layout, exposed to Swift and Kotlin through UniFFI,
with no daemon and no file watching: the app calls `push()` when it commits and
`sync()` when it wants to read, every call is short, resumable and crash-safe,
and the bucket that comes out restores with stock `litestream restore`. It is
for people shipping a mobile app with a real local SQLite database — health
data, offline-first notes, a local-first sync engine — who want that database
continuously mirrored to storage they control, at page granularity, without
inventing a row-level sync protocol.

This is a derivative of [`mrkurt/liters`](https://github.com/mrkurt/liters) by
Kurt Mackey, MIT-licensed and published with his permission. See
[Relationship to mrkurt/liters](#relationship-to-mrkurtliters).

## How it works

- **Page-level LTX, not row-level sync.** A push encodes the SQLite pages that
  changed into an LTX file — Litestream v0.5's transaction format: a 100-byte
  header, one standalone LZ4 frame per page, a page index, and a CRC-64/GO-ISO
  checksum over the uncompressed page bytes. There is no schema knowledge, no
  conflict resolution and no merge. A bucket is the byte-exact history of one
  database, written by one writer.
- **The WAL is the change feed.** `liters-wal` reads SQLite's write-ahead log
  directly — salt- and checksum-verified frames, folded into a page map per
  committed transaction — so a push is "the pages committed since TXID N",
  read out of a file SQLite already wrote. Nothing hooks your queries and
  nothing watches the filesystem.
- **The writer holds a long-running read lock and checkpoints itself.** That
  is how Litestream guarantees no foreign checkpointer restarts the WAL out
  from under the replication position, and liters ports it thresholds and all
  (`min_checkpoint_page_n = 1000`, matching upstream `db.go`). It is also why
  [which SQLite you link](#sqlite-linkage--bundled-sqlite) is a correctness
  question on iOS rather than a packaging preference.
- **Restore produces a real file, then follows incrementally.**
  `Replica::sync()` materializes the newest snapshot plus the LTX chain above
  it into an ordinary SQLite database you can open read-only with any SQLite;
  every later sync applies only the new files. Each fetched file's CRC is
  verified *before* its pages touch the live replica, and a pruned level or a
  reseeded bucket is detected rather than silently followed.
- **Compaction and retention run on the device.** Each writer is the sole
  writer of its bucket prefix, so it is also its own compactor:
  `Writer::maintain()` rolls L0 files up through the levels, takes snapshots,
  and applies retention while preserving the invariants stock Litestream
  readers depend on. There is no server-side component at all — a directory,
  an S3 bucket, or another liters process over HTTP is the entire backend.

## Quickstart

### Rust

```toml
[dependencies]
liters = { git = "https://github.com/vishk23/liters-mobile" }
```

```rust
use liters::{Writer, WriterOptions, Replica, ReplicaOptions, DirReplicaClient};

// Write side: the app owns app.db and calls push() after commits.
let mut w = Writer::open("app.db", Box::new(DirReplicaClient::new("/bucket")),
                         WriterOptions::default())?;
w.push()?;                                  // WAL → L0 LTX → upload
w.maintain(&Default::default())?;           // compaction/snapshots/retention, when due

// Read side: a live-updating local materialization of a bucket.
let mut r = Replica::open("replica.db", Box::new(DirReplicaClient::new("/bucket")),
                          ReplicaOptions::default());
r.sync()?;                                  // restore on first call, then incremental
// open replica.db read-only with any SQLite
```

The bucket that produces is Litestream's: `litestream restore -o out.db
file:///bucket` works against it, and the test suite asserts exactly that
against the real Go binaries — see [Compatibility](#compatibility).

Cargo features, defined on `liters` and re-exported by `liters-ffi`:

| feature | default | what it does |
|---|---|---|
| `bundled-sqlite` | **on** | compile SQLite's amalgamation into the library. Turn it off (`--no-default-features`) to link the platform `libsqlite3` — [required if anything else in the process links SQLite](#sqlite-linkage--bundled-sqlite) |
| `system-sqlite-bindgen` | off | with the above off, regenerate bindings from the platform's own `sqlite3.h` (needs libclang) |
| `http` | off (on for `liters-ffi`) | the liters HTTP replication protocol: serve, follow, push |
| `s3` | off | S3-compatible object storage in Litestream's S3 layout |
| `cli` (`liters-ffi` only) | off | build the `uniffi-bindgen` binary. Deliberately not a default: `uniffi/cli` would otherwise become a normal dependency of every cross-compiled mobile slice |

### iOS

```sh
SQLITE=system scripts/build-ios.sh
```

Produces `target/apple/Liters.xcframework` plus Swift sources in
`target/apple/swift/`; add both to your SPM target. `SQLITE=system` builds
`--no-default-features` and links Apple's system `libsqlite3` — **use it if
your app already links SQLite** (GRDB does), and read
[SQLite linkage](#sqlite-linkage--bundled-sqlite) for why that is not
optional. `SQLITE=bundled` (the default) is correct only when nothing else in
the process carries its own SQLite.

```swift
// A writer that hands every transfer to your own URLSession-backed client,
// so an upload in flight when iOS suspends the app is not lost.
let writer = try LitersWriter.newWithHttpClient(
    dbPath: dbPath,
    storage: .http(url: "https://sync.example/db", authToken: token),
    httpClient: MyURLSessionClient())

let summary = try writer.push()
if summary.snapshotted {
    log("full snapshot: \(summary.snapshotReason ?? "?"), \(summary.bytesUploaded) bytes")
}
```

The FFI exports `LitersWriter`, `LitersReplica`, and `LitersManager` — the
last for several databases at once, with `sleepAll()` / `resumeAll()` for
backgrounding and a `ManagerListener` callback interface for state changes and
push completions. `HttpClient` is a foreign trait: implement it in Swift and
the platform owns TLS, the system trust store, keepalive, and background
transfers. Without one, the built-in socket transport is `http://`-only.

**Set `wal_autocheckpoint = 0` on every connection your app opens to a
replicated database.** liters takes over checkpointing; a foreign
autocheckpoint firing while the app is backgrounded (when liters has released
its read lock) restarts the WAL and forces the next push to upload a full
snapshot.

### Android

```sh
scripts/build-android.sh
```

Produces `target/android/jniLibs/{arm64-v8a,armeabi-v7a,x86_64}/libliters_ffi.so`
plus Kotlin sources in `target/android/kotlin/` (package into your AAR;
requires JNA). SQLite is always bundled here — the NDK exposes no public
`libsqlite3`, so there is no platform library to link against.

Both scripts run `cargo run -p liters-ffi --features cli --bin uniffi-bindgen`
for the binding-generation step, which is the entire reason the `cli` feature
exists.

### Building and testing from a clone

```sh
make reference   # fetch the pinned litestream checkout (git only, no Go needed)
make test        # builds the Go oracle if available, then cargo test --workspace
```

`make reference` is required once: `reference/` is not checked in, and the
wal-reader fixture tests read Litestream's own testdata out of it. Without it
those tests print `SKIP:` instead of failing. Without a Go toolchain the
oracle-backed tests skip the same way, and `make test` still runs.

## Production use

liters-mobile has been running in production since 2026-07-30 in an
open-source WHOOP-client iOS app (a fork of
[ryanbr/noop](https://github.com/ryanbr/noop)), continuously replicating a
live health-metrics SQLite database from an iPhone to a Fly.io server over the
HTTP replication protocol, on the unbundled (`SQLITE=system`) linkage
alongside GRDB.

That is one app, one deployment, one person's device. It is real evidence that
the mobile path works end to end on real hardware under real backgrounding. It
is not a claim of broad production hardening, and it should not be read as one.

## Relationship to mrkurt/liters

liters is Kurt Mackey's work. The LTX v0.5 codec, the SQLite WAL reader, the
storage backends, the `Writer`/`Replica`/`Manager` surface, the HTTP
replication protocol and the UniFFI bindings are all his, and they are the
substantial majority of the code in this repository. His commits keep their
original SHAs, authorship and dates (`git log --author=kurt`), and `LICENSE`
carries his copyright.

This repository exists because embedding liters in a shipping mobile app
turned up a handful of concrete fixes, and having them in one installable
place was useful while they were in review. Every one of them is also offered
upstream as a pull request:

| upstream PR | what it fixes |
|---|---|
| [#2](https://github.com/mrkurt/liters/pull/2) | four in-tree doc claims that disagree with their own cited sources, plus a draft MIT `LICENSE` |
| [#3](https://github.com/mrkurt/liters/pull/3) | `rusqlite/bundled` hardcoded in `[workspace.dependencies]`, which made the platform-SQLite linkage impossible tree-wide |
| [#5](https://github.com/mrkurt/liters/pull/5) | a fresh clone could run neither `cargo test --workspace` nor `make test` |
| [#6](https://github.com/mrkurt/liters/pull/6) | a restored replica presented as a WAL-mode file with no `-shm`, which newer SQLite refuses to open |
| [#7](https://github.com/mrkurt/liters/pull/7) | `uniffi/cli` as an unconditional dependency, building a host code generator once per cross-compiled mobile slice |
| [#9](https://github.com/mrkurt/liters/pull/9) | the replica-file lock blocked forever (`F_SETLKW`) with no deadline and no cancellation |

Upstream has been quiet since 2026-07-24 and those PRs are open. That is
entirely fine. This is not a hostile fork and not a competing project: if Kurt
merges them, the right move for most people is to depend on `mrkurt/liters`
directly and for this repository to go back to being thin, or to nothing at
all. **If you want liters itself, go to
[mrkurt/liters](https://github.com/mrkurt/liters).**

Upstream PRs are proposed from [vishk23/liters](https://github.com/vishk23/liters),
which stays the vehicle for them; this repository is the packaged, installable
form.

## Status and roadmap

**Pre-1.0. The API will move.** Nothing here is published to crates.io yet and
no compatibility promise is made across commits — pin a revision. The on-disk
and on-the-wire formats are considerably more stable than the Rust API,
because they are Litestream's rather than ours.

Verified on `main` (rustc 1.97.1, macOS aarch64, Go 1.26.5):

```
make test                                                 202 passed, 0 failed
cargo clippy --workspace --all-targets -- -D warnings      clean (exit 0)
cargo test -p liters -p liters-ffi --no-default-features    ok (platform SQLite)
```

Known gaps, in rough order of how much they are likely to bother a new user:

- **No crates.io release.** Git dependency only.
- **The HTTP replication protocol is liters' own**, not Litestream's — it
  interoperates with other liters instances, not with the `litestream` binary.
  The *files* it moves are Litestream-format LTX either way. Specified
  normatively in [docs/http-protocol.md](docs/http-protocol.md).
- **No TLS in the built-in server.** Front it with a reverse proxy.
- **Write leases are in-memory** — a dual-writer detector, not a distributed
  lock. Bucket integrity across server restarts rests on L0 TXID monotonicity.
- **No VFS read replica.** Litestream v0.5 has one; liters materializes a real
  file instead.
- **Single writer per bucket prefix**, by construction. That is Litestream's
  model, not something liters adds.
- **No published Swift package or AAR.** Run the build scripts yourself.

---

# Reference

## Crates

| crate | contents |
|---|---|
| `ltx` | LTX v0.5.1 codec: encoder, decoder, page index, k-way compactor, CRC-64/GO-ISO checksums |
| `liters-wal` | SQLite WAL reader: salt/checksum-verified frames, committed-transaction page maps |
| `liters-storage` | `ReplicaClient` trait; `dir` backend (litestream `file` layout), `s3` backend (litestream S3 layout, feature `s3`), and liters-native HTTP serving + source with auth, a mountable `Mount` handler, and write fencing (feature `http`) |
| `liters` | `Writer` (push pipeline, checkpointing, device-side compaction/retention), `Replica` (restore + incremental follow), and `Manager` (background replication for N databases with sleep/resume) |
| `liters-ffi` | UniFFI bindings for Swift/Kotlin: `LitersWriter`, `LitersReplica`, `LitersManager` + event listener (`scripts/build-ios.sh`, `scripts/build-android.sh`) |

## SQLite linkage — `bundled-sqlite`

`liters` links SQLite through rusqlite, and **which** SQLite matters more here
than it does for most crates.

| build | result |
|---|---|
| default (`bundled-sqlite`) | SQLite's amalgamation is compiled into the library. Self-contained, reproducible, and the only option on Android (the NDK exposes no public libsqlite3). |
| `--no-default-features` | Link the platform's `libsqlite3` instead. |
| `--features system-sqlite-bindgen` (with `--no-default-features`) | As above, but regenerate the bindings from the platform's own `sqlite3.h` (needs libclang). |

**Use the unbundled build whenever another component in the same process
already links its own SQLite.** POSIX advisory locks are dropped when *any*
descriptor to the file is closed by the process; SQLite works around that with
a process-global `unixInodeInfo` table — but only within **one copy** of the
library. Two copies in one process each keep their own table and can silently
drop each other's locks.

That is a latent hazard for any SQLite user and a **correctness** issue for
liters specifically: the writer's guarantee that no foreign checkpointer can
restart the WAL under it *is* a long-running read lock (`sqlite::ReadLock`). If
that lock is dropped, a foreign checkpoint restarts the WAL, the resume frame
is overwritten, and `verify.rs` has to recover by uploading a full snapshot.

The concrete case is an iOS app using **GRDB**, which links Apple's system
`libsqlite3` via `.systemLibrary(name: "CSQLite")`. Build for it with:

```sh
SQLITE=system scripts/build-ios.sh
```

which is `cargo build -p liters-ffi --no-default-features`. Every SQLite symbol
the unbundled `aarch64-apple-ios` staticlib leaves undefined (41 of them) is
exported by the iOS SDK's `usr/lib/libsqlite3.tbd`, so it resolves against the
same library GRDB uses.

## HTTP replication (liters-native)

With the `http` feature, a liters process can serve its bucket over HTTP and
other liters instances can restore from it and **follow it live** — new
transactions stream to followers over a single long-lived connection, with
no object store in between. The files on the move are still litestream-format
LTX, but the protocol carrying them is liters' own: stock litestream v0.5.x
has no HTTP scheme, so this is the **liters HTTP replication protocol** —
liters-proprietary, not a litestream protocol — specified normatively in
[docs/http-protocol.md](docs/http-protocol.md).

```rust
use std::sync::Arc;
use liters_storage::{DirReplicaClient, HttpReplicaClient, HttpServer, HttpServerOptions};

// Serving side: writer pushes to a local dir bucket; the server serves it.
// The notifying tee wakes followers the instant a push lands.
let srv = HttpServer::bind("0.0.0.0:9736", Arc::new(DirReplicaClient::new("/bucket")),
                           HttpServerOptions::default())?;
let mut w = Writer::open("app.db", srv.notifying_client(Box::new(DirReplicaClient::new("/bucket"))),
                         WriterOptions::default())?;

// Following side: restore over HTTP, then apply changes as they arrive.
let mut r = Replica::open("replica.db", Box::new(HttpReplicaClient::new("http://host:9736")?),
                          ReplicaOptions::default());
let cancel = CancelToken::new();
r.follow(&cancel, &FollowOptions::default())?;   // blocks; cancel() ends it cleanly
```

`Replica::sync()` polling works over HTTP too (the server is a full
read-side `ReplicaClient`), and the server can serve a bucket some other
process writes — including stock `litestream replicate` — with poll-bounded
latency. TLS is not built in: front with a reverse proxy (auth below).

Roles also reverse (**push replication**): a server started with
`writable: true` *accepts* replication, and a writer whose destination is an
`HttpReplicaClient` dials out and pushes — the shape you want when the
writer is behind NAT or on a mobile network:

```rust
// Receiver: listens, materializes pushed data into a local bucket.
let srv = HttpServer::bind("0.0.0.0:9736", Arc::new(DirReplicaClient::new("/bucket")),
                           HttpServerOptions { writable: true, ..Default::default() })?;

// Pusher (elsewhere): a normal Writer, destination is the receiver.
let mut w = Writer::open("app.db", Box::new(HttpReplicaClient::new("http://receiver:9736")?),
                         WriterOptions::default())?;
w.push()?;      // L0 upload over HTTP; maintain() compacts/retains remotely too
```

Accepted pushes wake the receiver's `/stream` followers, so a writable
server is also a relay: devices push in, downstream replicas stream out,
and the receiver can follow its own server over loopback for a live local
copy. The pushed bucket stays litestream-exact — stock `litestream restore`
works against it.

### Mounting, embedding, auth, fencing

A server serves one database. Mount it under a URL path prefix
(`HttpServerOptions::base_path`) so liters can share an origin with unrelated
apps behind a path-routing reverse proxy — `http://host/db/...` reaches
liters while `http://host/users/...` reaches something else. Requests outside
the prefix are `404`; clients just point at the deeper URL, and the base path
they already parse from the URL lines up with the server's mount.
(Alternatively, strip the prefix at the proxy and leave `base_path` unset —
do one or the other, not both.) With `auth_token` set, every route except the
`GET /` health check requires `authorization: Bearer <token>`.

```rust
let srv = HttpServer::bind("0.0.0.0:9736", Arc::new(DirReplicaClient::new("/bucket")),
                           HttpServerOptions { base_path: Some("/db".into()),
                                               auth_token: Some("secret".into()),
                                               ..Default::default() })?;
let client = HttpReplicaClient::new("http://host:9736/db")?;
```

To serve liters from **your own** Rust HTTP server — one listener shared with
other routes, or several databases dispatched by path — skip `HttpServer` and
embed `Mount`, the transport-agnostic handler that is the protocol for one
database. `HttpServer` is itself just a thin `TcpListener` driver over one:

```rust
let mount = Mount::new(Arc::new(DirReplicaClient::new("/bucket")), MountOptions::default());
// in your router, for a request matched to this mount (prefix already stripped):
let resp = mount.handle(Request { method, path, query, headers,
                                  body: &mut req_body, cancel });
// write resp.status + resp.headers, then the Body (Bytes / Reader / Stream).
```

`Mount::handle` is a pure request-in / response-out function: it owns all
wire-format knowledge; your host owns the socket, the listener, and routing.

Writable servers also *fence* pushers: a writer that sends an
`x-liters-writer-id` header (`HttpClientOptions::writer_id`; the `Manager`
fills it automatically with a per-database persisted id) holds a lease on
its bucket, and pushes from other writer ids are rejected with 409 until
the lease ages out (`HttpServerOptions::lease_ttl`, default 24h) or a
takeover is forced. Leases are in-memory — a dual-writer detector, not a
distributed lock; L0 TXID monotonicity checks protect bucket integrity
across server restarts. Details in
[docs/http-protocol.md](docs/http-protocol.md).

## Manager: many databases, sleep/resume

For apps replicating several databases, `liters::Manager` runs one worker
thread per registered database — pushing on an interval (or only on
`push_now()`) or following live — with transient failures retried on a
jittered exponential `Backoff` and fatal errors parked in a `Failed` state
you can observe and nudge:

```rust
use std::time::Duration;
use liters::{Manager, ManagerOptions, PushConfig, FollowConfig, StorageConfig};

let mgr = Manager::new(ManagerOptions::default());
mgr.register_push("app", "app.db", PushConfig {
    storage: StorageConfig::Http {
        url: "http://sync.example:9736/db".into(),
        options: liters::HttpClientOptions {
            auth_token: Some("secret".into()),
            ..Default::default()
        },
    },
    writer_options: Default::default(),
    push_interval: Some(Duration::from_secs(30)),
    maintenance: Some((Default::default(), Duration::from_secs(3600))),
    backoff: None,
})?;
mgr.register_follow("catalog", "catalog.db", FollowConfig {
    storage: StorageConfig::Dir { path: "/buckets/catalog".into() },
    replica_options: Default::default(),
    follow_options: Default::default(),
})?;

mgr.set_observer(Some(observer));  // state changes, push completions, errors
mgr.statuses();                    // per-DB state, position, last error

// Mobile power management: sleeping cancels in-flight transfers and drops
// the Writer — releasing the WAL read lock and every fd — then parks with
// zero storage traffic; resume schedules an immediate catch-up round.
mgr.sleep_all();                   // app went to background
mgr.resume_all();                  // app returned to foreground
```

Writers open offline: pushes convert the WAL into local L0 files even with
the bucket unreachable, and the first successful push uploads the backlog.
Every long operation also has a `_with` variant taking a `CancelToken`
(`push_with`, `sync_with`, `maintain_with`, `follow`); flipping the token
makes the call return `Error::Cancelled` promptly — mid-transfer on the
HTTP backend — and retrying after a cancel is indistinguishable from
resuming after a crash. Tokens are one-shot: a cancelled token stays
cancelled, and a new session gets a fresh one. This is the mechanism behind
`sleep()`/`resume()` and bounded shutdown.

The same surface ships over FFI: `liters-ffi` exports `LitersWriter` /
`LitersReplica` (with `cancel()` and `close()`) and `LitersManager`
(register/sleep/resume/status plus a `ManagerListener` callback interface —
callbacks arrive on worker threads and must not block). Live follow over
FFI goes through `LitersManager.register_follow`.

### Per-push telemetry

`PushResult` (FFI: `PushSummary`) carries `snapshotted`, `snapshot_reason` and
`bytes_uploaded` alongside the position, so a host can tell a 400-byte
incremental delta from a whole-database re-upload without watching byte
counters and guessing. `snapshot_reason` is a small fixed set drawn from
liters' verify decision tree, so it is safe to aggregate across devices. This
is what makes "is liters viable on this device, on this network, at this
database size?" an empirically answerable question rather than one that needs
a patched crate.

## Compatibility

Liters has two compatibility surfaces, and only one is litestream's. The
**LTX file format and bucket layout** are litestream v0.5-compatible,
implemented from the `superfly/ltx` v0.5.1 **source** (the version litestream
v0.5.14 pins) and litestream's replica-client sources — not from the docs,
several of which are stale. (The **liters HTTP replication protocol** above is
the other surface: liters' own, interoperating only with other liters
instances, moving litestream-format LTX files over a protocol litestream does
not define.) Where ltx source and docs disagree, the oracle decides: the test
suite builds the real Go `litestream` and `ltx` binaries (`make oracle`) and
asserts, among others, that

- every push is restorable by `litestream restore` (file and S3/MinIO),
- Rust-encoded LTX files pass Go `ltx verify` and apply byte-identically,
- liters continues seamlessly from a database litestream was replicating
  (same meta-dir layout) and vice versa,
- the reader follows buckets written by live `litestream replicate`,
  surviving compaction races, pruned levels, and bucket reseeds.

Run everything with `make test`. It first runs `make reference`, which fetches
the pinned litestream checkout (`LITESTREAM_REF` in the `Makefile`) into
`reference/` — untracked, git-only, no Go — then builds the oracle binaries and
runs the suite. Two groups of tests degrade to a `SKIP:` notice rather than
failing: the oracle-backed ones without a Go toolchain, and the wal-reader
fixture tests without that checkout, since they read litestream's own testdata
out of it. Plain `cargo test --workspace` therefore wants `make reference` to
have run at least once. `docs/research/` holds the format/internals notes the
implementation was built from.

## Design notes

- Files are written with litestream's `HeaderFlagNoChecksum` — the pre/
  post-apply checksum chain is inert in real litestream buckets, and enabling
  it would break Go's restore path (its compactor rejects mixed checksums).
  Continuity is TXID contiguity plus per-file CRC-64, exactly as upstream.
- The writer holds litestream's long-running read transaction, so it **must**
  checkpoint the database itself; thresholds mirror upstream defaults.
- The reader verifies each fetched file's CRC **before** any page touches the
  live replica, falls back to applying a newer snapshot in place when
  levels 0–8 are pruned, and detects bucket reseeds as divergence — three
  deliberate hardenings over upstream's follow mode.
- A restored replica is left presenting as a rollback-journal file rather than
  a WAL-mode file with no `-shm`, which SQLite 3.51 refuses to open read-only.
- The replica-file lock is taken with a deadline and a cancellation check
  rather than a bare `F_SETLKW`. A daemon on a server can be restarted when a
  reader wedges it; a library inside someone else's app cannot.
- Compaction/retention run device-side (`Writer::maintain`): each device is
  the sole writer of its prefix, hence also its sole compactor. Retention
  preserves the invariants stock readers rely on (newest snapshot kept, ≥1
  file per level, no L0 gaps).

## License

MIT — see [LICENSE](LICENSE), which carries Kurt Mackey's copyright for the
original work alongside the derivative one.
