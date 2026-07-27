//! Contention tests for the replica applier's SQLite-compatible file locks.
//!
//! The applier takes SQLite's EXCLUSIVE lock pair on the replica file before
//! writing any page. POSIX record locks never conflict within one process,
//! so the only thing that can contend is a *separate process* — which is the
//! normal case for liters' stated target (an iOS/Android app, or any host,
//! reading the replica while a worker follows it). These tests therefore
//! spawn a real child process that holds a conflicting lock, and assert the
//! applier gives up or cancels instead of blocking forever.
//!
//! The child is this same test binary re-executed with `--exact
//! lock_holder_child` and `LITERS_TEST_LOCK_DIR` set; without that variable
//! the helper test is a no-op, so a normal run just passes it.
//!
//! Conventions (mirroring cancellation.rs): every blocking wait carries a
//! deadline that fails the test instead of hanging it, and the child is
//! released and reaped by a `Drop` guard so a panicking test body cannot
//! leave a lock holder behind.
//!
//! Regression coverage: acquisition used to be `fcntl(F_SETLKW)`, which
//! blocks in the kernel with no timeout and no interruption point. Every
//! test here except `uncontended_*` hangs forever against that version —
//! swap `F_SETLK` back to `F_SETLKW` in `replica.rs` to see it.

use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use liters::{
    CancelToken, DirReplicaClient, Error, Replica, ReplicaOptions, Writer, WriterOptions,
};
use ltx::Txid;
use rusqlite::Connection;

/// Set on the child to turn the helper test into a lock holder; its value is
/// the directory holding `replica.db` and the two sentinel files.
const LOCK_DIR_ENV: &str = "LITERS_TEST_LOCK_DIR";
/// `raw` = a bare `F_RDLCK` over SQLite's SHARED range; `sqlite` = a real
/// read transaction through SQLite itself.
const LOCK_MODE_ENV: &str = "LITERS_TEST_LOCK_MODE";

/// The child never outlives this even if the parent dies mid-test, so a
/// crashed run cannot leave a process holding a lock on a temp file.
const HOLDER_WATCHDOG: Duration = Duration::from_secs(60);
/// Ceiling on every wait in a parent test. Comfortably above the timeouts
/// under test and far below "hung": the pre-fix code blocks indefinitely, so
/// tripping this deadline is the hang being caught rather than tolerated.
const TEST_DEADLINE: Duration = Duration::from_secs(20);

// SQLite's lock byte ranges, mirrored from `replica.rs` (which mirrors
// SQLite's `os_unix.c`). Duplicated rather than exported: if these ever
// drift apart, the contention these tests rely on simply stops happening and
// they fail loudly.
const SQLITE_PENDING_BYTE: i64 = 0x4000_0000;
const SQLITE_SHARED_FIRST: i64 = SQLITE_PENDING_BYTE + 2;
const SQLITE_SHARED_SIZE: i64 = 510;

// ---------------------------------------------------------------------------
// child helper
// ---------------------------------------------------------------------------

/// Not a test in its own right: a no-op unless [`LOCK_DIR_ENV`] is set, in
/// which case this process is a child spawned by one of the tests below. It
/// takes a conflicting lock on `replica.db`, announces itself by creating
/// `holder.ready`, and holds until `holder.release` appears (or the watchdog
/// fires). The lock goes away when the process exits.
#[test]
fn lock_holder_child() {
    let Ok(dir) = std::env::var(LOCK_DIR_ENV) else { return };
    let dir = PathBuf::from(dir);
    let db = dir.join("replica.db");
    let mode = std::env::var(LOCK_MODE_ENV).unwrap_or_else(|_| "raw".into());

    // Kept alive for the whole hold: dropping either handle releases the
    // lock and would silently defeat the test that spawned us.
    let _held = match mode.as_str() {
        "raw" => {
            let f = OpenOptions::new().read(true).write(true).open(&db).unwrap();
            // Byte-for-byte what SQLite's unix VFS holds for a SHARED lock:
            // a read lock over the whole shared range.
            set_lock(&f, libc::F_RDLCK as libc::c_short, SQLITE_SHARED_FIRST, SQLITE_SHARED_SIZE)
                .expect("child could not take the shared-range read lock");
            Held::Raw(f)
        }
        "sqlite" => {
            let conn =
                Connection::open_with_flags(&db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                    .unwrap();
            // A deferred BEGIN takes no lock; the SELECT is what makes SQLite
            // acquire SHARED, and it stays held until the transaction ends.
            conn.execute_batch("BEGIN").unwrap();
            let _: i64 = conn.query_row("SELECT COUNT(1) FROM t", [], |r| r.get(0)).unwrap();
            Held::Sqlite(conn)
        }
        other => panic!("unknown lock mode {other:?}"),
    };

    fs::write(dir.join("holder.ready"), b"1").unwrap();
    let deadline = Instant::now() + HOLDER_WATCHDOG;
    while !dir.join("holder.release").exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Whatever the child is holding the conflicting lock through. Only its
/// lifetime matters; the variants are never read.
#[allow(dead_code)]
enum Held {
    Raw(File),
    Sqlite(Connection),
}

fn set_lock(f: &File, lock_type: libc::c_short, start: i64, len: i64) -> std::io::Result<()> {
    use std::os::unix::io::AsRawFd;
    let fl = libc::flock {
        l_start: start,
        l_len: len,
        l_pid: 0,
        l_type: lock_type,
        l_whence: libc::SEEK_SET as libc::c_short,
    };
    if unsafe { libc::fcntl(f.as_raw_fd(), libc::F_SETLK, &fl) } == -1 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

/// Releases and reaps the child on drop, so no test path leaves a lock
/// holder running against the temp directory.
struct Holder {
    child: Child,
    dir: PathBuf,
    pid: i32,
}

impl Holder {
    /// Spawns the holder and blocks until it reports the lock is taken.
    fn start(dir: &Path, mode: &str) -> Holder {
        let exe = std::env::current_exe().expect("current_exe");
        let child = Command::new(exe)
            .arg("lock_holder_child")
            .arg("--exact")
            .env(LOCK_DIR_ENV, dir)
            .env(LOCK_MODE_ENV, mode)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn lock holder");
        let pid = child.id() as i32;
        let mut h = Holder { child, dir: dir.to_path_buf(), pid };

        let ready = dir.join("holder.ready");
        let deadline = Instant::now() + TEST_DEADLINE;
        while !ready.exists() {
            if let Ok(Some(status)) = h.child.try_wait() {
                panic!("lock holder exited before taking the lock: {status}");
            }
            assert!(Instant::now() < deadline, "lock holder never took the lock");
            std::thread::sleep(Duration::from_millis(5));
        }
        h
    }

    /// Tells the child to drop the lock and waits for the process to go
    /// away — the lock is only certainly gone once it has exited.
    fn release(&mut self) {
        let _ = fs::write(self.dir.join("holder.release"), b"1");
        let deadline = Instant::now() + TEST_DEADLINE;
        loop {
            match self.child.try_wait() {
                Ok(Some(_)) => return,
                Ok(None) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(5))
                }
                _ => {
                    let _ = self.child.kill();
                    let _ = self.child.wait();
                    return;
                }
            }
        }
    }
}

impl Drop for Holder {
    fn drop(&mut self) {
        self.release();
    }
}

// ---------------------------------------------------------------------------
// fixtures
// ---------------------------------------------------------------------------

fn rows_of(path: &Path) -> Vec<(i64, String)> {
    let conn =
        Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let mut stmt = conn.prepare("SELECT id, v FROM t ORDER BY id").unwrap();
    stmt.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap()
}

fn sidecar_txid(db_path: &Path) -> u64 {
    let mut p = db_path.as_os_str().to_owned();
    p.push("-txid");
    match fs::read_to_string(PathBuf::from(p)) {
        Ok(s) => u64::from_str_radix(s.trim(), 16).unwrap_or(0),
        Err(_) => 0,
    }
}

/// Source db + writer + a replica already restored to txid 3, so the next
/// `sync()` takes the *incremental* apply path — the one that locks. (A full
/// restore materializes to a temp file and renames, and never contends.)
struct Fixture {
    dir: PathBuf,
    db_path: PathBuf,
    replica_path: PathBuf,
    conn: Connection,
    writer: Writer,
    /// Last field on purpose: struct fields drop in declaration order, so the
    /// directory goes away only after the handles into it are closed.
    _tmp: tempfile::TempDir,
}

fn fixture(lock_timeout: Duration) -> (Fixture, Replica) {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    let db_path = dir.join("app.db");
    let bucket = dir.join("bucket");
    let replica_path = dir.join("replica.db");

    let conn = Connection::open(&db_path).unwrap();
    conn.pragma_update(None, "journal_mode", "WAL").unwrap();
    conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").unwrap();

    let mut writer =
        Writer::open(&db_path, Box::new(DirReplicaClient::new(&bucket)), WriterOptions::default())
            .unwrap();
    for i in 0..3 {
        conn.execute("INSERT INTO t (v) VALUES (?1)", [format!("seed-{i}")]).unwrap();
        writer.push().unwrap();
    }

    let mut replica = Replica::open(
        &replica_path,
        Box::new(DirReplicaClient::new(&bucket)),
        ReplicaOptions { lock_timeout, ..Default::default() },
    );
    assert_eq!(replica.sync().unwrap().to_txid, Txid(3));

    (Fixture { dir, db_path, replica_path, conn, writer, _tmp: tmp }, replica)
}

impl Fixture {
    /// One more transaction in the bucket, so the next sync has something to
    /// apply through the locking path.
    fn advance(&mut self) {
        self.conn.execute("INSERT INTO t (v) VALUES ('after-hold')", []).unwrap();
        self.writer.push().unwrap();
    }
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

/// L1: a reader process holding the replica's SHARED range makes the applier
/// give up at `lock_timeout` with a diagnosable error instead of blocking
/// forever, writes nothing, and syncs cleanly once the reader lets go.
///
/// Against the `F_SETLKW` version this call never returns at all.
#[test]
fn contended_apply_gives_up_instead_of_hanging() {
    let timeout = Duration::from_millis(400);
    let (mut fx, mut replica) = fixture(timeout);
    let mut holder = Holder::start(&fx.dir, "raw");
    fx.advance();

    let began = Instant::now();
    let err = replica.sync().expect_err("a held lock must not be applied through");
    let elapsed = began.elapsed();

    assert!(elapsed < TEST_DEADLINE, "sync took {elapsed:?}: the applier is blocking, not bounded");
    match err {
        Error::LockBusy { lock, waited, holder_pid } => {
            // PENDING is uncontended (the reader holds only SHARED), so the
            // applier gets that far and stalls on the shared range.
            assert_eq!(lock, "SHARED", "wrong range reported contended");
            assert!(waited >= timeout.mul_f64(0.8), "gave up after only {waited:?}");
            assert_eq!(
                holder_pid,
                Some(holder.pid),
                "the error must name the process actually holding the lock"
            );
        }
        other => panic!("expected LockBusy, got {other:?}"),
    }
    // The lock is taken before the first page is written, so a refusal is a
    // no-op: position and contents are untouched.
    assert_eq!(sidecar_txid(&fx.replica_path), 3);
    assert_eq!(rows_of(&fx.replica_path).len(), 3);

    // And it is genuinely transient — the same sync succeeds once the reader
    // commits.
    holder.release();
    assert_eq!(replica.sync().unwrap().to_txid, Txid(4));
    assert_eq!(rows_of(&fx.replica_path), rows_of(&fx.db_path));
}

/// L2: cancellation is observed *while* waiting for a contended lock, with a
/// timeout long enough that only the token can end the wait.
///
/// This is the property `F_SETLKW` could never provide: `cancel()` sets a
/// flag, and a thread blocked in the kernel on `fcntl` never reads it.
#[test]
fn contended_apply_observes_cancellation() {
    // An hour: if this returns promptly it is because of the token.
    let (mut fx, mut replica) = fixture(Duration::from_secs(3600));
    let _holder = Holder::start(&fx.dir, "raw");
    fx.advance();

    let cancel = CancelToken::new();
    let (err, latency) = std::thread::scope(|s| {
        let canceller = s.spawn({
            let cancel = cancel.clone();
            move || {
                std::thread::sleep(Duration::from_millis(200));
                let at = Instant::now();
                cancel.cancel();
                at
            }
        });
        let err = replica.sync_with(&cancel).expect_err("cancelled sync must not report success");
        let cancelled_at = canceller.join().expect("canceller panicked");
        (err, cancelled_at.elapsed())
    });

    assert!(matches!(err, Error::Cancelled), "expected Cancelled, got {err:?}");
    assert!(latency < Duration::from_secs(2), "cancel took {latency:?} to be observed");
    assert_eq!(sidecar_txid(&fx.replica_path), 3, "a cancelled wait must apply nothing");
}

/// L3: the uncontended path is untouched. `lock_timeout` is zero — the
/// tightest possible bound — and the sync still applies normally, because a
/// free lock is taken on the first `fcntl` and the deadline is never
/// consulted.
#[test]
fn uncontended_apply_ignores_the_lock_timeout() {
    let (mut fx, mut replica) = fixture(Duration::ZERO);
    fx.advance();

    assert_eq!(replica.sync().unwrap().to_txid, Txid(4));
    assert_eq!(rows_of(&fx.replica_path), rows_of(&fx.db_path));
}

/// L4: contention is a wait, not a failure. With a bound far above the hold,
/// the applier retries until the reader releases and then applies — so the
/// fix did not trade a hang for a permanently stalled replica.
#[test]
fn applier_waits_out_a_reader_that_releases() {
    let (mut fx, mut replica) = fixture(Duration::from_secs(30));
    let mut holder = Holder::start(&fx.dir, "raw");
    fx.advance();

    let began = Instant::now();
    let hold_for = Duration::from_millis(500);
    let result = std::thread::scope(|s| {
        s.spawn(|| {
            std::thread::sleep(hold_for);
            holder.release();
        });
        replica.sync()
    });

    let elapsed = began.elapsed();
    assert_eq!(result.unwrap().to_txid, Txid(4));
    assert!(elapsed >= hold_for.mul_f64(0.8), "sync finished in {elapsed:?}, before the release");
    assert!(elapsed < TEST_DEADLINE, "sync took {elapsed:?} for a {hold_for:?} hold");
    assert_eq!(rows_of(&fx.replica_path), rows_of(&fx.db_path));
}

/// L5: the same contention through a real SQLite reader in another process —
/// `BEGIN` plus a `SELECT` on the replica, which is exactly what an app or a
/// sidecar query server does — rather than a hand-rolled lock. Proves the
/// byte ranges in `replica.rs` are the ones SQLite actually uses, and that
/// the motivating scenario (a host process reading the replica while a
/// worker follows it) is the one covered.
#[test]
fn a_real_sqlite_reader_in_another_process_contends() {
    let timeout = Duration::from_millis(400);
    let (mut fx, mut replica) = fixture(timeout);
    let mut holder = Holder::start(&fx.dir, "sqlite");
    fx.advance();

    let began = Instant::now();
    let err = replica.sync().expect_err("an open read transaction must block the apply");
    assert!(began.elapsed() < TEST_DEADLINE, "the applier blocked on a SQLite reader");
    match err {
        Error::LockBusy { lock, holder_pid, .. } => {
            assert_eq!(lock, "SHARED");
            assert_eq!(holder_pid, Some(holder.pid));
        }
        other => panic!("expected LockBusy, got {other:?}"),
    }

    holder.release();
    assert_eq!(replica.sync().unwrap().to_txid, Txid(4));
    assert_eq!(rows_of(&fx.replica_path), rows_of(&fx.db_path));
}
