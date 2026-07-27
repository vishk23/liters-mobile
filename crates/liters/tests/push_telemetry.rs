//! `PushResult`'s snapshot telemetry, on the sequence that decides whether a
//! device can replicate incrementally at all.
//!
//! A snapshotting push ships the entire database, so a client that snapshots on
//! most pushes is doing full uploads with extra steps. Whether that happens is
//! not observable from the other `PushResult` fields — `synced`, `uploaded` and
//! `bytes_uploaded` look the same either way, just larger — so it has to be
//! reported explicitly. These tests pin both the flag and the reason string,
//! because the reason is what tells an operator which branch of `verify()` they
//! are hitting and therefore what to change.

use std::path::Path;

use liters::{DirReplicaClient, Writer, WriterOptions};
use rusqlite::Connection;

fn create_db(path: &Path) -> Connection {
    let conn = Connection::open(path).unwrap();
    conn.pragma_update(None, "journal_mode", "WAL").unwrap();
    conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").unwrap();
    conn
}

fn insert(conn: &Connection, n: usize) {
    for _ in 0..n {
        conn.execute("INSERT INTO t (v) VALUES (?1)", ["x".repeat(200)]).unwrap();
    }
}

fn open_writer(db: &Path, bucket: &Path) -> Writer {
    Writer::open(db, Box::new(DirReplicaClient::new(bucket)), WriterOptions::default()).unwrap()
}

/// The first push has no local position to resume from, so it snapshots by
/// definition; the second, with the writer still holding its read lock, must
/// not. That contrast is the whole measurement.
#[test]
fn first_push_snapshots_and_names_why_then_steady_state_does_not() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("app.db");
    let bucket = dir.path().join("bucket");
    let conn = create_db(&db);
    // A base big enough that "snapshot the whole database" and "ship the last
    // few commits" are different orders of magnitude — which is the entire
    // reason the distinction is worth measuring.
    insert(&conn, 3_000);

    let mut w = open_writer(&db, &bucket);

    let first = w.push().unwrap();
    assert!(first.snapshotted, "the first push has no resume point and must snapshot");
    assert_eq!(first.snapshot_reason, Some("first sync, no local position"));
    assert!(first.bytes_uploaded > 0, "a snapshot must have uploaded bytes");
    assert_eq!(first.uploaded, 1);

    insert(&conn, 5);
    let second = w.push().unwrap();
    assert!(!second.snapshotted, "steady state must be incremental, got a snapshot");
    assert_eq!(second.snapshot_reason, None);
    assert!(second.synced);
    assert!(
        second.bytes_uploaded * 10 < first.bytes_uploaded,
        "an incremental push must be far smaller than the snapshot it followed \
         (incremental={}, snapshot={})",
        second.bytes_uploaded,
        first.bytes_uploaded
    );
}

/// The mobile failure mode, reproduced exactly: the writer is dropped (an app
/// suspend/kill, or `Manager::sleep`, which documents releasing the WAL-pinning
/// read transaction), something else checkpoints the WAL while it is gone, and
/// the writer comes back. `verify()` finds its resume point past the end of a
/// truncated WAL, and — because `synced_to_wal_end` is deliberately not
/// persisted across a close/reopen — cannot take the "expected truncation"
/// shortcut. The push degrades to a full snapshot, and now says so.
#[test]
fn a_foreign_checkpoint_across_a_reopen_reports_the_snapshot_and_its_reason() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("app.db");
    let bucket = dir.path().join("bucket");
    let conn = create_db(&db);
    insert(&conn, 50);

    let mut w = open_writer(&db, &bucket);
    assert!(w.push().unwrap().snapshotted);
    insert(&conn, 50);
    assert!(!w.push().unwrap().snapshotted, "sanity: incremental before the interference");

    // The app goes away; its replicator's read lock goes with it.
    drop(w);

    // A foreign connection checkpoints — exactly what SQLite's own
    // wal_autocheckpoint does at 1000 pages on any connection that is not
    // liters'.
    conn.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
    conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(())).unwrap();
    insert(&conn, 50);

    let mut w = open_writer(&db, &bucket);
    let after = w.push().unwrap();
    assert!(
        after.snapshotted,
        "a foreign checkpoint across a reopen must be reported as a snapshot"
    );
    let reason = after.snapshot_reason.expect("a snapshot must carry a reason");
    assert!(
        reason.contains("wal"),
        "the reason must name the WAL so an operator can act on it, got {reason:?}"
    );
}

/// A push with nothing new must not claim to have snapshotted, and must report
/// zero bytes — otherwise an aggregate over pushes would be dominated by no-ops.
#[test]
fn a_no_op_push_reports_no_snapshot_and_no_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("app.db");
    let bucket = dir.path().join("bucket");
    let conn = create_db(&db);
    insert(&conn, 10);

    let mut w = open_writer(&db, &bucket);
    w.push().unwrap();

    let idle = w.push().unwrap();
    assert!(!idle.snapshotted);
    assert_eq!(idle.snapshot_reason, None);
    assert_eq!(idle.uploaded, 0);
    assert_eq!(idle.bytes_uploaded, 0);
}
