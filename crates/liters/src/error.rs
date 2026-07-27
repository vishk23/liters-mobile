use std::time::Duration;

use ltx::Txid;

/// Errors from liters replication.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("ltx: {0}")]
    Ltx(#[from] ltx::Error),

    #[error("wal: {0}")]
    Wal(#[from] liters_wal::WalError),

    #[error("storage: {0}")]
    Storage(#[source] liters_storage::StorageError),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// The database is not in WAL journal mode and could not be switched.
    /// (db.go:1021)
    #[error("enable wal failed, mode={0:?}")]
    EnableWalFailed(String),

    /// A local L0 LTX file expected by the writer is missing or corrupt.
    /// Recoverable by resetting local state (which forces a snapshot).
    #[error("local ltx file missing or corrupt: txid {txid}: {msg}")]
    LocalLtx { txid: Txid, msg: String },

    /// The local replica has diverged from the bucket (bucket wiped or
    /// reseeded at a lower TXID); a reset + full re-restore is required.
    #[error("replica diverged: local txid {local} ahead of bucket max {remote}")]
    Diverged { local: Txid, remote: Txid },

    /// The requested transaction cannot be reconstructed from the bucket —
    /// typically an empty (not-yet-seeded) bucket. Mirrors litestream's
    /// `ErrTxNotAvailable` (store.go:27); the display string is litestream's
    /// error text.
    #[error("transaction not available")]
    TxNotAvailable,

    /// A conflicting `fcntl` lock on the replica file outlived
    /// [`ReplicaOptions::lock_timeout`](crate::ReplicaOptions::lock_timeout),
    /// so the applier gave up instead of waiting indefinitely. The holder is
    /// another *process* — POSIX record locks never conflict within one
    /// process — typically a reader with an open SQLite transaction against
    /// the replica.
    ///
    /// Nothing was written: the lock is taken before the first page, so the
    /// local replica and its position sidecar are exactly as they were. The
    /// sync is safe to retry, and [`Error::is_transient`] reports it as
    /// retryable so a `follow` loop with a `retry` backoff resumes on its own
    /// once the reader commits.
    ///
    /// `holder_pid` is a best-effort `F_GETLK` probe: `None` when the kernel
    /// named no owner (Linux open-file-description locks report none) or the
    /// holder released in the interim.
    #[error("replica file lock busy: could not take the SQLite {lock} lock within {waited:?}{}",
            .holder_pid.map(|p| format!(" (conflicting lock held by pid {p})")).unwrap_or_default())]
    LockBusy {
        /// Which of SQLite's two lock ranges was contended: `"PENDING"` or
        /// `"SHARED"`.
        lock: &'static str,
        /// How long acquisition of that range was attempted before giving up.
        waited: Duration,
        /// PID of a process holding a conflicting lock, if the kernel
        /// reported one.
        holder_pid: Option<i32>,
    },

    /// The operation was cancelled via a
    /// [`CancelToken`](liters_storage::CancelToken).
    #[error("operation cancelled")]
    Cancelled,

    #[error("{0}")]
    Other(String),
}

/// Storage-level cancellation surfaces as [`Error::Cancelled`] so callers
/// match a single variant no matter which layer observed the token.
impl From<liters_storage::StorageError> for Error {
    fn from(e: liters_storage::StorageError) -> Error {
        match e {
            liters_storage::StorageError::Cancelled => Error::Cancelled,
            e => Error::Storage(e),
        }
    }
}

impl Error {
    /// Whether retrying the same operation later can plausibly succeed:
    /// transient storage failures (per
    /// [`StorageError::is_transient`](liters_storage::StorageError::is_transient)),
    /// local I/O hiccups, and a busy replica-file lock (the holding reader
    /// commits eventually). Divergence, integrity errors, and cancellation
    /// are never transient.
    pub fn is_transient(&self) -> bool {
        match self {
            Error::Storage(e) => e.is_transient(),
            Error::Io(_) | Error::LockBusy { .. } => true,
            _ => false,
        }
    }
}
