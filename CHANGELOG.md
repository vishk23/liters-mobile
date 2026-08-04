# Changelog

All notable changes to this project are documented here. This is a derivative
of [`mrkurt/liters`](https://github.com/mrkurt/liters); entries below describe
what this repository changes relative to upstream, not liters itself.

The format is loosely [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project is pre-1.0 and does not yet follow semantic versioning — the API
may break between any two commits.

## [0.1.0] — unreleased

First tagged release. Baseline is upstream
[`108e1df`](https://github.com/mrkurt/liters/commit/108e1df); every fix below
is also offered upstream as a pull request.

### Fixed

- **Replica-file lock could block forever.** The lock was taken with a bare
  `F_SETLKW` and no deadline or cancellation check, so a reader in another
  process could wedge a `sync()` indefinitely. It now takes a bound
  (`ReplicaOptions::lock_timeout`, defaulting to the usual SQLite
  `busy_timeout` convention) and observes cancellation within ~20 ms. A daemon
  on a server can be restarted when a reader wedges it; a library inside
  someone else's app cannot.
  ([upstream #9](https://github.com/mrkurt/liters/pull/9))
- **A restored replica presented as a WAL-mode file with no `-shm`.**
  `Replica::restore` wrote page 1 verbatim out of the snapshot, so the replica
  inherited the origin's journal-mode header while the `-wal`/`-shm` files that
  header implies were deleted. Reading a WAL database needs the `-shm` index
  and a read-only connection may not create one, so every read-only open of a
  freshly restored replica failed with `SQLITE_CANTOPEN` under SQLite 3.51
  (Apple's system libsqlite3); the bundled 3.50.2 tolerated it. The restore
  path now applies the same rollback-journal fixup the incremental path always
  did. ([upstream #6](https://github.com/mrkurt/liters/pull/6))
- **The FFI's HTTP path ignored the host transport, and misreported an empty
  body.** `Storage::into_config()` hardcoded `transport: None` and only
  `LitersManager` patched a host transport back in, so a plain `LitersWriter`
  or `LitersReplica` always ran on the built-in socket transport — which
  rejects `https://` and cannot hand a transfer to a background `URLSession`,
  losing any iOS upload in flight at suspend. Added `into_config_with()` and
  `new_with_http_client()` constructors. Separately, `ForeignBody::read`
  mapped an empty `BodyChunk::Data` to `BodyRead::Idle` on every request,
  violating `Idle`'s long-lived-only contract and spinning on a possibly-dead
  stream instead of raising an error.
- **A dev-dependency silently re-bundled the SQLite under test.**
  `liters-ffi`'s dev-dependency pinned `rusqlite = { features = ["bundled"] }`.
  Cargo unifies features across the graph, so the pin switched `bundled` back
  on for `liters` itself: `cargo test -p liters -p liters-ffi
  --no-default-features` reported 97 passed while linking the bundled
  amalgamation, and the platform linkage could not be tested at all. The pin is
  gone; fixtures now follow whatever linkage is under test.
- **Four in-tree documentation claims disagreed with their own cited sources**
  — the LZ4 frame settings attributed to `ltx encoder.go`, "byte-compatible
  with superfly/ltx v0.5.1", `PageHeader::flags` "reserved; must be zero", and
  an FFI claim that a foreign `wal_autocheckpoint` can never fire.
  ([upstream #2](https://github.com/mrkurt/liters/pull/2))
- **The test suite could not open the Go oracle's own restore output
  read-only.** `litestream restore` writes page 1 verbatim out of the
  snapshot, so its output carries a WAL journal-mode header with no
  `-wal`/`-shm` alongside it — the same shape the replica fix above addresses,
  but on the Go side, where liters cannot fix it. Sixteen tests across four
  targets failed under `--no-default-features` on macOS for this reason and
  passed on Linux CI, purely because of the linked SQLite version. The test
  helpers now normalize the restored header rather than weakening the
  read-only open the assertions rest on.

### Added

- **`bundled-sqlite` feature (on by default) for SQLite linkage.**
  `rusqlite`'s `bundled` feature was hardcoded in `[workspace.dependencies]`,
  and workspace dependency features are additive — a member cannot remove them
  — so linking the *platform* SQLite was impossible tree-wide. The choice now
  lives in a `bundled-sqlite` feature with a `system-sqlite-bindgen`
  companion, re-exported through `liters-ffi`, and `scripts/build-ios.sh`
  takes a `SQLITE=bundled|system|system-bindgen` switch applied to the device,
  simulator *and* bindings builds. This is what lets an iOS app link one
  SQLite instead of two, which for liters is a correctness requirement rather
  than a packaging preference: its guarantee that no foreign checkpointer
  restarts the WAL under the writer *is* a long-running advisory read lock,
  and two SQLite copies in one process can silently drop each other's locks.
  ([upstream #3](https://github.com/mrkurt/liters/pull/3))
- **Per-push snapshot and byte telemetry.** `verify()` already decided per
  push whether a full snapshot was required instead of an incremental WAL
  delta, and already recorded why, but neither the flag nor the reason reached
  `PushResult`. `PushResult` (and FFI `PushSummary`) now carry `snapshotted`,
  `snapshot_reason` (a small fixed set, so it aggregates) and
  `bytes_uploaded`. No behaviour change — the values come from state that
  already existed.
- **`cli` feature on `liters-ffi`** gating the `uniffi-bindgen` binary and the
  `uniffi/cli` feature it needs. `uniffi/cli` pulls in `uniffi_bindgen`
  (askama, cargo_metadata, goblin, clap), and cargo unifies features across
  the graph, so declaring it unconditionally made a developer code generator a
  normal dependency of every cross-compiled mobile slice — built at
  opt-level 3, once per target, for a host tool the phone cannot run.
  ([upstream #7](https://github.com/mrkurt/liters/pull/7))

### Changed

- **A fresh clone can run the test suite.** Neither `cargo test --workspace`
  nor `make test` worked from a clean checkout, because the pinned litestream
  checkout the wal-reader fixture tests read testdata from was fetched only by
  CI. `make reference` now fetches it (git only, no Go), CI calls that same
  target, and the pinned ref lives in one place so CI and a local clone cannot
  drift. ([upstream #5](https://github.com/mrkurt/liters/pull/5))
- **`LICENSE` added and `Cargo.toml` corrected** from `Apache-2.0` to `MIT`.
  The license file carries Kurt Mackey's copyright for the original work
  alongside the derivative one.
- **README rewritten** as a landing page: what liters-mobile is, how it works,
  a quickstart matching the actual API for Rust / iOS / Android, production
  status, the relationship to upstream, and an explicit pre-1.0 roadmap with
  known gaps.
