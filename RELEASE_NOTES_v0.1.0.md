# liters-mobile v0.1.0

First tagged release.

liters-mobile is continuous, page-level SQLite replication you can embed in an
iOS or Android app: a Rust library that reads and writes Litestream v0.5's LTX
file format and bucket layout, exposed to Swift and Kotlin through UniFFI, with
no daemon and no file watching. The app calls `push()` when it commits and
`sync()` when it wants to read; every call is short, resumable and crash-safe;
the bucket that comes out restores with stock `litestream restore`.

This is a derivative of [`mrkurt/liters`](https://github.com/mrkurt/liters) by
Kurt Mackey, MIT-licensed and published with his permission. The LTX codec, the
WAL reader, the storage backends, the `Writer`/`Replica`/`Manager` surface, the
HTTP replication protocol and the UniFFI bindings are his work and are the
substantial majority of the code. His commits keep their original SHAs,
authorship and dates.

## What's in this tag

Upstream [`108e1df`](https://github.com/mrkurt/liters/commit/108e1df) plus the
fixes below. Each is also offered upstream as a pull request — this repository
is the packaged, installable form, not a competing project.

### Embedding fixes

- **SQLite linkage is now a choice** (`bundled-sqlite`, on by default; turn it
  off to link the platform `libsqlite3`, with a `system-sqlite-bindgen`
  companion). `rusqlite/bundled` was hardcoded in `[workspace.dependencies]`
  and workspace dependency features are additive, so linking the platform
  SQLite was impossible tree-wide. This is what lets an iOS app on GRDB link
  **one** SQLite instead of two — a correctness requirement for liters, whose
  guarantee that no foreign checkpointer restarts the WAL under the writer *is*
  a long-running advisory read lock, and two SQLite copies in one process can
  silently drop each other's locks.
  [#3](https://github.com/mrkurt/liters/pull/3)
- **`scripts/build-ios.sh` takes `SQLITE=bundled|system|system-bindgen`**,
  applied to the device, simulator *and* bindings builds.
- **The `uniffi-bindgen` binary is behind a `cli` feature.** `uniffi/cli`
  otherwise became a normal dependency of every cross-compiled mobile slice —
  a host code generator built at opt-level 3, once per target, that the phone
  cannot run. [#7](https://github.com/mrkurt/liters/pull/7)
- **A plain `LitersWriter`/`LitersReplica` can use the host HTTP transport.**
  `Storage::into_config()` hardcoded `transport: None` and only
  `LitersManager` patched one back in, so everything else fell to the built-in
  socket transport: `https://` rejected, and no way to hand a transfer to a
  background `URLSession`, so an iOS upload in flight at suspend was lost.

### Correctness fixes

- **The replica-file lock is bounded and cancellable** instead of a bare
  `F_SETLKW` that could wedge a `sync()` forever behind a reader in another
  process. [#9](https://github.com/mrkurt/liters/pull/9)
- **A restored replica presents as a rollback-journal file.** It previously
  inherited the origin's WAL journal-mode header while the `-wal`/`-shm` files
  that header implies had just been deleted, so every read-only open failed
  with `SQLITE_CANTOPEN` under SQLite 3.51 — and silently worked under the
  bundled 3.50.2, which is how it survived.
  [#6](https://github.com/mrkurt/liters/pull/6)
- **`ForeignBody::read` no longer reports every empty body as `Idle`**,
  which violated `Idle`'s long-lived-only contract and spun on a possibly-dead
  stream instead of raising an error the caller can act on.

### Observability

- **`PushResult` / FFI `PushSummary` carry `snapshotted`, `snapshot_reason`
  and `bytes_uploaded`.** The decision and its reason already existed inside
  `verify()`; nothing read them. A host can now tell a 400-byte incremental
  delta from a whole-database re-upload, and `snapshot_reason` is a small fixed
  set so it aggregates across devices.

### Build and test

- **A fresh clone can run the test suite.** `make reference` fetches the pinned
  litestream checkout the wal-reader fixture tests read from (git only, no Go);
  CI calls the same target, so the two cannot drift.
  [#5](https://github.com/mrkurt/liters/pull/5)
- **`make test-system-sqlite`** runs the suite against the platform
  `libsqlite3`. The package selection is load-bearing: the other crates
  dev-depend on `rusqlite` with `bundled`, and cargo unions features across the
  build, so a `--workspace --no-default-features` run silently tests the
  amalgamation anyway.
  [#3](https://github.com/mrkurt/liters/pull/3)
- **The test suite passes on the platform-SQLite linkage**, not only the
  bundled one. Sixteen tests across four targets failed under
  `--no-default-features` on macOS — not a liters defect, but the Go
  `litestream restore` output carrying a WAL header with no `-shm`, which
  SQLite 3.51 refuses to open read-only. The oracle helpers now normalize that
  header instead of weakening the read-only assertions.
- **`LICENSE` added** (MIT) and `Cargo.toml` corrected from `Apache-2.0`.
  [#2](https://github.com/mrkurt/liters/pull/2)

### Documentation

- README rewritten as a landing page: pitch, how it works, quickstart for
  Rust / iOS / Android, production status, relationship to upstream, and an
  explicit pre-1.0 roadmap with known gaps.
- Four in-tree claims corrected against the sources they cite.
  [#2](https://github.com/mrkurt/liters/pull/2)

## Verification

On rustc 1.97.1, macOS aarch64, Go 1.26.5:

```
make test                                                 202 passed, 0 failed
cargo test -p liters -p liters-ffi --no-default-features    98 passed, 0 failed
cargo clippy --workspace --all-targets -- -D warnings      clean (exit 0)
```

The suite includes an oracle: it builds the real Go `litestream` and `ltx`
binaries and asserts that every push is restorable by `litestream restore`
(file and S3/MinIO), that Rust-encoded LTX passes Go `ltx verify` and applies
byte-identically, that liters continues from a database litestream was
replicating and vice versa, and that the reader follows buckets written by live
`litestream replicate` across compaction races, pruned levels and reseeds.

## Status

**Pre-1.0. The API will move.** Not published to crates.io; depend on it by git
revision. The on-disk and on-the-wire formats are considerably more stable than
the Rust API, because they are Litestream's rather than ours.

Known gaps: no crates.io release, no published Swift package or AAR, no TLS in
the built-in HTTP server (front it with a reverse proxy), in-memory write
leases (a dual-writer detector, not a distributed lock), no VFS read replica,
and one writer per bucket prefix by construction. The HTTP replication protocol
is liters' own and interoperates with other liters instances, not with the
`litestream` binary — the *files* it moves are Litestream-format LTX either
way.

## Production use

Running in production since 2026-07-30 in an open-source WHOOP-client iOS app
(a fork of [ryanbr/noop](https://github.com/ryanbr/noop)), continuously
replicating a live health-metrics SQLite database from an iPhone to a Fly.io
server over the HTTP replication protocol, on the unbundled (`SQLITE=system`)
linkage alongside GRDB. That is one app, one deployment, one device — real
evidence that the mobile path works end to end, not a claim of broad production
hardening.

## Upstream

Upstream has been quiet since 2026-07-24 and the pull requests above are open.
If they land, the right move for most people is to depend on `mrkurt/liters`
directly. If you want liters itself, go to
[mrkurt/liters](https://github.com/mrkurt/liters).
