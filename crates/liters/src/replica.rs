//! The read side: a local, read-only materialization of a replica bucket,
//! kept current by applying LTX files incrementally. Ports litestream's
//! restore + follow machinery (replica.go:544-994) with three hardening
//! changes for mobile:
//!
//! - every fetched file's CRC is verified *before* its pages touch the live
//!   replica (litestream verifies after writing);
//! - a stalled L0/L1-8 chain falls back to applying a newer snapshot in
//!   place, and a bucket whose max TXID went backwards is detected as
//!   divergence (litestream's follow mode stalls forever on both);
//! - the replica-file lock is acquired with a deadline and a cancellation
//!   check instead of blocking forever (internal/lock_unix.go:48 uses
//!   `F_SETLKW`). A daemon on a server can be restarted when a reader wedges
//!   it; a library inside someone's app cannot, and a concurrent reader
//!   process is the normal case there. See [`FcntlLock`].

use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use liters_storage::{CancelToken, ReplicaClient, StreamEvent, SNAPSHOT_LEVEL};
use ltx::{is_contiguous, Compactor, Decoder, FileInfo, Txid, HEADER_FLAG_NO_CHECKSUM, HEADER_SIZE};
use rand::RngCore;

use crate::{Backoff, Error, Result, StorageError};

/// Post-restore integrity checking. (replica.go IntegrityCheck modes)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntegrityCheck {
    None,
    Quick,
    Full,
}

#[derive(Debug, Clone)]
pub struct ReplicaOptions {
    /// Integrity check to run after a full restore.
    pub integrity_check: IntegrityCheck,
    /// Take SQLite-compatible fcntl locks on the replica file during page
    /// application. Protects readers in *other processes*; fcntl locks
    /// cannot exclude readers in this process (POSIX locks are per-process),
    /// so same-process readers must not hold read transactions across
    /// `sync()`.
    pub use_file_locks: bool,
    /// How long to wait for those locks before giving up with
    /// [`Error::LockBusy`]. Only reached when `use_file_locks` is set and a
    /// reader in another process holds a conflicting lock; an uncontended
    /// acquisition never consults it.
    ///
    /// This is a bound, not a target: acquisition returns the instant the
    /// lock is free, and cancellation is observed within ~20ms regardless of
    /// how long the bound is. Zero means try once and fail immediately, like
    /// SQLite with `busy_timeout=0`. Values above a day are clamped.
    ///
    /// The default matches the usual SQLite `busy_timeout` convention: long
    /// enough to sit out an ordinary read transaction, short enough that a
    /// wedged reader is reported rather than waited on forever.
    pub lock_timeout: Duration,
    /// On divergence (bucket reseeded below our position) or unresumable
    /// local state, restore from scratch instead of returning
    /// [`Error::Diverged`]. The restore materializes to a temp file and
    /// atomically replaces the local replica, so the existing replica
    /// survives if the restore fails partway.
    pub auto_reset: bool,
}

impl Default for ReplicaOptions {
    fn default() -> Self {
        ReplicaOptions {
            integrity_check: IntegrityCheck::Quick,
            use_file_locks: true,
            lock_timeout: Duration::from_secs(5),
            auto_reset: false,
        }
    }
}

/// Options for [`Replica::follow`].
#[derive(Debug, Clone)]
pub struct FollowOptions {
    /// Cadence for backends without streaming support, for waiting out an
    /// empty (not-yet-seeded) bucket, and the backoff after a round that
    /// made no progress (prevents hot resync loops against a pruned or
    /// stalled bucket).
    pub poll_interval: Duration,
    /// `Some(backoff)`: transient storage/network errors (per
    /// [`Error::is_transient`]) sleep `backoff.delay(n)` — where `n` counts
    /// consecutive failures and resets to zero whenever a round makes
    /// progress — and retry instead of returning. For followers that must
    /// survive server restarts and flaky links. `None`: fail fast on the
    /// first error. Non-transient errors always return immediately.
    pub retry: Option<Backoff>,
}

impl Default for FollowOptions {
    fn default() -> Self {
        FollowOptions { poll_interval: Duration::from_secs(1), retry: None }
    }
}

/// Result of a [`Replica::sync`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncResult {
    /// Whether a full restore (not an incremental apply) ran.
    pub restored: bool,
    pub from_txid: Txid,
    pub to_txid: Txid,
}

/// A local materialized read replica of one database's bucket.
pub struct Replica {
    db_path: PathBuf,
    client: Box<dyn ReplicaClient>,
    opts: ReplicaOptions,
}

impl Replica {
    pub fn open(
        db_path: impl Into<PathBuf>,
        client: Box<dyn ReplicaClient>,
        opts: ReplicaOptions,
    ) -> Replica {
        Replica { db_path: db_path.into(), client, opts }
    }

    /// The local database file. Open it read-only with plain SQLite; it is a
    /// rollback-journal-mode file (no -wal/-shm needed).
    ///
    /// Both writers of this file guarantee that: [`Replica::apply_spooled`]
    /// rewrites page 1 on the incremental path, and [`Replica::restore`] runs
    /// [`present_as_rollback_journal`] over the materialized image. A
    /// snapshot's page 1 is otherwise a verbatim copy of the origin's, WAL
    /// header and all, which a read-only reader cannot open once the side
    /// files are gone.
    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    /// Last applied TXID from the sidecar; zero if never synced.
    pub fn position(&self) -> Result<Txid> {
        read_txid_file(&self.db_path)
    }

    /// Deletes the local replica and its sidecar.
    pub fn reset(&self) -> Result<()> {
        for suffix in ["", "-txid", ".tmp", ".apply.tmp"] {
            let mut p = self.db_path.as_os_str().to_owned();
            p.push(suffix);
            match fs::remove_file(PathBuf::from(p)) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }

    /// Brings the local replica up to date: a full restore when the local
    /// file is missing, otherwise incremental application of new LTX files.
    ///
    /// Equal to [`Replica::sync_with`] with a token that never cancels.
    pub fn sync(&mut self) -> Result<SyncResult> {
        self.sync_with(&CancelToken::new())
    }

    /// [`Replica::sync`], cancellable: the token is installed on the storage
    /// client and checked between fetched files, and while waiting for the
    /// replica file lock — but never mid-apply. A started page application
    /// always runs to completion: a torn apply is healed by re-apply anyway,
    /// but deliberately widening that window buys nothing. Waiting for the
    /// lock is not mid-apply — it happens before the first page is written,
    /// so nothing is torn by giving up there.
    /// A cancelled sync returns [`Error::Cancelled`]; the local replica is
    /// exactly as consistent as after a kill (tmp spools removed, position
    /// sidecar only ever advanced after a completed apply).
    ///
    /// If a reader in another process holds a conflicting lock on the replica
    /// for longer than [`ReplicaOptions::lock_timeout`], the sync returns
    /// [`Error::LockBusy`] rather than blocking indefinitely. Nothing was
    /// applied and the sync is safe to retry.
    pub fn sync_with(&mut self, cancel: &CancelToken) -> Result<SyncResult> {
        self.client.set_cancel(cancel.clone());

        if !self.db_path.exists() {
            let to = self.full_restore(cancel)?;
            return Ok(SyncResult { restored: true, from_txid: Txid(0), to_txid: to });
        }

        let from = read_txid_file(&self.db_path)?;
        if from.is_zero() {
            // Local file without a sidecar: unresumable. (replica.go:566-568)
            if self.opts.auto_reset {
                let to = self.full_restore(cancel)?;
                return Ok(SyncResult { restored: true, from_txid: Txid(0), to_txid: to });
            }
            return Err(Error::Other(format!(
                "replica exists but has no -txid sidecar; delete {} to re-restore",
                self.db_path.display()
            )));
        }

        match self.incremental_sync(from, cancel) {
            Ok(to) => Ok(SyncResult { restored: false, from_txid: from, to_txid: to }),
            // The healthy replica is never deleted up front: full_restore
            // replaces it atomically only once a complete new image exists,
            // so a failed restore leaves the old replica readable.
            Err(Error::Diverged { .. }) if self.opts.auto_reset => {
                let to = self.full_restore(cancel)?;
                Ok(SyncResult { restored: true, from_txid: from, to_txid: to })
            }
            Err(e) => Err(e),
        }
    }

    /// Follows the bucket continuously until `cancel` is cancelled or a
    /// fatal error occurs. Uses the backend's live stream
    /// ([`ReplicaClient::open_ltx_stream`], e.g. a liters HTTP server's
    /// `/stream`) when available — new transactions apply as they arrive
    /// over a single connection — and falls back to polling [`Replica::sync`]
    /// otherwise.
    ///
    /// Every stream anomaly (gap, reseed, non-contiguous frame, corrupt
    /// file) routes back through `sync()`, which owns the hardened
    /// restore/bridge/divergence logic; `follow` never invents its own
    /// recovery. The position sidecar advances after every applied
    /// transaction, so a killed follower resumes exactly where it stopped.
    ///
    /// Blocking: run it on a dedicated thread and cancel the token to end
    /// it. Cancellation is a clean stop — follow returns `Ok(())`, never
    /// [`Error::Cancelled`] — normally observed within ~a second (or after
    /// the in-flight file finishes applying); the worst case is the stream
    /// dead-man bound (~45s) if a frame stalls mid-transfer on a dead link
    /// and the backend is not token-aware. A follower waiting on a replica
    /// file lock held by another process is *not* part of that worst case:
    /// it notices the cancel within ~20ms, whatever
    /// [`ReplicaOptions::lock_timeout`] is set to.
    ///
    /// A lock held past that timeout surfaces as a transient
    /// [`Error::LockBusy`]: with `retry` set the follower backs off and picks
    /// up again once the reader commits; with `retry: None` it returns the
    /// error, like any other failed round.
    pub fn follow(&mut self, cancel: &CancelToken, opts: &FollowOptions) -> Result<()> {
        self.client.set_cancel(cancel.clone());

        // Consecutive transient-failure count driving the `retry` backoff;
        // any progress resets it. Empty-bucket waits use poll_interval and
        // never touch it (an unseeded bucket is not a failure).
        let mut attempt: u32 = 0;
        while !cancel.is_cancelled() {
            let before = self.position()?;
            match self.sync_with(cancel) {
                Ok(_) => attempt = 0,
                // Cancellation is a clean stop, not an error.
                Err(Error::Cancelled) => return Ok(()),
                // Empty bucket: the writer has not seeded it yet. Wait,
                // matching finish_incremental's empty-is-not-divergence
                // stance.
                Err(Error::TxNotAvailable) => {
                    if sleep_checking(cancel, opts.poll_interval) {
                        return Ok(());
                    }
                    continue;
                }
                Err(e) if e.is_transient() && opts.retry.is_some() => {
                    if sleep_checking(cancel, opts.retry.as_ref().unwrap().delay(attempt)) {
                        return Ok(());
                    }
                    attempt = attempt.saturating_add(1);
                    continue;
                }
                Err(e) => return Err(e),
            }
            let mut position = self.position()?;
            let mut made_progress = position > before;

            let stream = match self.client.open_ltx_stream(Txid(position.0 + 1)) {
                Ok(stream) => stream,
                Err(StorageError::Cancelled) => return Ok(()),
                Err(e) if e.is_transient() && opts.retry.is_some() => {
                    if sleep_checking(cancel, opts.retry.as_ref().unwrap().delay(attempt)) {
                        return Ok(());
                    }
                    attempt = attempt.saturating_add(1);
                    continue;
                }
                Err(e) => return Err(e.into()),
            };

            let Some(mut stream) = stream else {
                // No streaming support: plain sync() polling.
                if sleep_checking(cancel, opts.poll_interval) {
                    return Ok(());
                }
                continue;
            };

            // (Re)open the database and page size after every sync():
            // a full restore replaces the inode, and a reseeded bucket may
            // change the page size.
            let (db, page_size) = match open_db_for_apply(&self.db_path) {
                Ok(pair) => pair,
                Err(e) if e.is_transient() && opts.retry.is_some() => {
                    if sleep_checking(cancel, opts.retry.as_ref().unwrap().delay(attempt)) {
                        return Ok(());
                    }
                    attempt = attempt.saturating_add(1);
                    continue;
                }
                Err(e) => return Err(e),
            };
            let spool_path = tmp_sibling(&self.db_path, ".apply.tmp");

            // Runs until the stream ends or misbehaves. `None` = fall back
            // to sync() (the stream told us to, or a frame didn't fit);
            // `Some(e)` = this session failed — the error goes through the
            // same transient/retry classification as sync() errors, so
            // local I/O hiccups honor `retry` too.
            let stream_err: Option<Error> = loop {
                if cancel.is_cancelled() {
                    let _ = fs::remove_file(&spool_path);
                    return Ok(());
                }
                let mut spool = match File::create(&spool_path) {
                    Ok(f) => f,
                    Err(e) => break Some(e.into()),
                };
                match stream.next(&mut spool) {
                    Ok(StreamEvent::Ltx(info)) => {
                        if info.max_txid <= position {
                            continue; // stale frame, already applied
                        }
                        if !is_contiguous(position, info.min_txid, info.max_txid) {
                            break None; // gapped frame: bridge via sync()
                        }
                        if let Err(e) = spool.sync_all() {
                            break Some(e.into());
                        }
                        drop(spool);
                        match self.apply_spooled(&db, &spool_path, page_size, cancel) {
                            Ok(()) => {
                                position = info.max_txid;
                                if let Err(e) = write_txid_file(&self.db_path, position) {
                                    break Some(e);
                                }
                                made_progress = true;
                            }
                            // Divergence, CRC/decode failures, and storage
                            // errors re-run through sync(), which owns the
                            // hardened routing (auto_reset, re-fetch by
                            // listing, ...).
                            Err(Error::Diverged { .. } | Error::Ltx(_) | Error::Storage(_)) => {
                                break None
                            }
                            Err(e) => break Some(e),
                        }
                    }
                    // An idle ping carrying a non-empty bucket max below our
                    // position is positive divergence evidence: let sync()
                    // confirm and route it. (Empty buckets are a
                    // wipe-then-reseed window, not divergence.)
                    Ok(StreamEvent::Idle { bucket_max: Some(m) })
                        if !m.is_zero() && m < position =>
                    {
                        break None
                    }
                    Ok(StreamEvent::Idle { .. }) => continue,
                    Ok(StreamEvent::Gap { .. } | StreamEvent::Reset { .. } | StreamEvent::Closed) => {
                        break None
                    }
                    // Unknown future event (StreamEvent is non-exhaustive):
                    // safest response is drop-the-stream and resync.
                    Ok(_) => break None,
                    Err(e) => break Some(e.into()),
                }
            };
            let _ = fs::remove_file(&spool_path);

            if made_progress {
                attempt = 0;
            }
            if let Some(e) = stream_err {
                // A cancelled stream (token-aware backend) is a clean stop.
                if matches!(e, Error::Cancelled) {
                    return Ok(());
                }
                match &opts.retry {
                    Some(backoff) if e.is_transient() => {
                        if sleep_checking(cancel, backoff.delay(attempt)) {
                            return Ok(());
                        }
                        attempt = attempt.saturating_add(1);
                    }
                    _ => return Err(e),
                }
            } else if !made_progress {
                // The whole round moved nothing: back off before resyncing
                // so a pruned/stalled bucket can't induce a hot spin.
                if sleep_checking(cancel, opts.poll_interval) {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    /// Full restore: plan → k-way merge → materialize → rename → verify.
    /// (replica.go:622-731)
    fn full_restore(&mut self, cancel: &CancelToken) -> Result<Txid> {
        let plan = crate::plan::calc_restore_plan(self.client.as_ref(), Txid(0))?;

        // Minimum plausible size check. (replica.go:640-643)
        for info in &plan {
            if info.size < HEADER_SIZE as u64 {
                return Err(Error::Other(format!(
                    "invalid ltx file: level={} min={} max={} has size {} bytes",
                    info.level, info.min_txid, info.max_txid, info.size
                )));
            }
        }

        let mut rdrs: Vec<Box<dyn Read + Send>> = Vec::with_capacity(plan.len());
        for info in &plan {
            cancel.check()?;
            rdrs.push(self.client.open_ltx_file(info.level, info.min_txid, info.max_txid, 0, 0)?);
        }

        if let Some(parent) = self.db_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp_path = tmp_sibling(&self.db_path, ".tmp");
        let cleanup = TmpGuard(&tmp_path);

        // Merge the chain and materialize the database image in one pass:
        // Compactor -> pipe -> DecodeDatabaseTo in Go; here the compactor
        // writes to an in-file buffer first (single-threaded).
        let compacted_path = tmp_sibling(&self.db_path, ".compact.tmp");
        let compact_cleanup = TmpGuard(&compacted_path);
        {
            let mut compactor = Compactor::new(rdrs);
            compactor.header_flags = HEADER_FLAG_NO_CHECKSUM;
            let out = File::create(&compacted_path)?;
            compactor.compact(std::io::BufWriter::new(out))?;
        }

        let to_txid = plan.last().unwrap().max_txid;
        {
            let mut f = File::create(&tmp_path)?;
            let dec = Decoder::new(BufReader::new(File::open(&compacted_path)?));
            dec.decode_database_to(&mut f)?;
            present_as_rollback_journal(&mut f)?;
            f.sync_all()?;
        }
        drop(compact_cleanup);
        cancel.check()?;
        fs::rename(&tmp_path, &self.db_path)?;
        drop(cleanup);
        fsync_parent(&self.db_path);

        // Never leave stale sqlite side files next to a fresh image.
        // (replica.go:1258-1293)
        for suffix in ["-wal", "-shm"] {
            let mut p = self.db_path.as_os_str().to_owned();
            p.push(suffix);
            let _ = fs::remove_file(PathBuf::from(p));
        }

        self.check_integrity()?;
        write_txid_file(&self.db_path, to_txid)?;
        Ok(to_txid)
    }

    /// One round of follow-mode application: L0 from our position, bridging
    /// gaps through levels 1..8, with snapshot fallback and divergence
    /// detection. (replica.go:798-869 + hardening)
    fn incremental_sync(&mut self, from: Txid, cancel: &CancelToken) -> Result<Txid> {
        let mut f = OpenOptions::new().read(true).write(true).open(&self.db_path)?;
        let page_size = read_page_size(&mut f)?;

        let mut current = from;
        // A 404 on a file we just listed is a race with compaction/GC, not a
        // pruned chain: suppress the snapshot fallback for this sync and
        // re-list next time, like Go's retry-next-tick. (replica.go:844-849)
        let mut saw_404 = false;

        // Poll L0 for incremental files. (replica.go:801-851)
        let l0 = self.client.ltx_files(0, Txid(current.0 + 1), false)?;

        let saw_level0 = !l0.is_empty();
        for info in l0 {
            cancel.check()?;
            if info.min_txid.0 > current.0 + 1 {
                current =
                    self.fill_follow_gap(&f, current, info.min_txid, page_size, &mut saw_404, cancel)?;
                if info.max_txid <= current {
                    continue;
                }
                if info.min_txid.0 > current.0 + 1 {
                    // Still gapped; try again next sync (or snapshot below).
                    return self.finish_incremental(&f, from, current, page_size, !saw_404, cancel);
                }
            }
            if info.max_txid <= current {
                continue;
            }
            match self.apply_ltx_file(&f, &info, page_size, cancel) {
                Ok(()) => current = info.max_txid,
                // A listed file may 404 mid-race with compaction/GC:
                // re-list next sync; never advance past it. (resumable_reader.go:75)
                Err(Error::Storage(StorageError::NotFound { .. })) => {
                    return self.finish_incremental(&f, from, current, page_size, false, cancel)
                }
                Err(e) => return Err(e),
            }
        }

        if !saw_level0 {
            current =
                self.fill_follow_gap(&f, current, Txid(current.0 + 1), page_size, &mut saw_404, cancel)?;
        }

        self.finish_incremental(&f, from, current, page_size, !saw_404, cancel)
    }

    /// Post-pass: snapshot fallback and divergence detection, then persist
    /// the new position. `allow_snapshot_fallback` is false when this sync
    /// hit a transient 404 — no-progress then means "retry next sync", not
    /// "the chain was pruned".
    fn finish_incremental(
        &mut self,
        f: &File,
        from: Txid,
        mut current: Txid,
        page_size: u32,
        allow_snapshot_fallback: bool,
        cancel: &CancelToken,
    ) -> Result<Txid> {
        if current == from && allow_snapshot_fallback {
            // No progress. Distinguish up-to-date / snapshot-only-newer /
            // diverged via the bucket-wide max TXID.
            let mut bucket_max = Txid(0);
            let mut newest_snapshot: Option<FileInfo> = None;
            for level in (0..=SNAPSHOT_LEVEL).rev() {
                for info in self.client.ltx_files(level, Txid(0), false)? {
                    if info.max_txid > bucket_max {
                        bucket_max = info.max_txid;
                    }
                    if level == SNAPSHOT_LEVEL
                        && newest_snapshot.as_ref().is_none_or(|s| info.max_txid > s.max_txid)
                    {
                        newest_snapshot = Some(info);
                    }
                }
            }

            // A completely empty bucket is a no-op sync, not divergence: it
            // is the transient window of a wipe-then-reseed, and there is
            // nothing to restore from anyway. Go's follow loop likewise
            // no-ops on empty listings. Divergence is only declared on
            // positive evidence: files present whose max is below ours.
            if bucket_max.is_zero() {
                return Ok(current);
            }
            if bucket_max < current {
                return Err(Error::Diverged { local: current, remote: bucket_max });
            }

            // Newer data reachable only via a snapshot (levels 0-8 pruned):
            // a snapshot is contiguous with any position (min=1) and contains
            // every page, so apply it in place. (Litestream's follow mode
            // stalls here; see docs/research/restore-read-path.md §9.)
            if let Some(snap) = newest_snapshot {
                if snap.max_txid > current {
                    cancel.check()?;
                    self.apply_ltx_file(f, &snap, page_size, cancel)?;
                    current = snap.max_txid;
                }
            }
        }

        if current > from {
            write_txid_file(&self.db_path, current)?;
        }
        Ok(current)
    }

    /// Bridges an L0 gap through levels 1..8 (never the snapshot level).
    /// (replica.go:932-994)
    fn fill_follow_gap(
        &mut self,
        f: &File,
        after: Txid,
        gap_min: Txid,
        page_size: u32,
        saw_404: &mut bool,
        cancel: &CancelToken,
    ) -> Result<Txid> {
        let mut current = after;
        for level in 1..SNAPSHOT_LEVEL {
            for info in self.client.ltx_files(level, Txid(0), false)? {
                if info.min_txid.0 > current.0 + 1 {
                    break; // gap at this level too
                }
                if info.max_txid <= current {
                    continue;
                }
                cancel.check()?;
                match self.apply_ltx_file(f, &info, page_size, cancel) {
                    Ok(()) => current = info.max_txid,
                    Err(Error::Storage(StorageError::NotFound { .. })) => {
                        *saw_404 = true;
                        return Ok(current);
                    }
                    Err(e) => return Err(e),
                }
                if current.0 + 1 >= gap_min.0 {
                    return Ok(current);
                }
            }
            // Progress at this level: let the caller re-evaluate L0.
            if current > after {
                return Ok(current);
            }
        }
        Ok(current)
    }

    /// Applies one LTX file's pages to the replica in place: fetches to the
    /// spool file, then [`Replica::apply_spooled`]. (replica.go:879-930)
    fn apply_ltx_file(
        &mut self,
        f: &File,
        info: &FileInfo,
        page_size: u32,
        cancel: &CancelToken,
    ) -> Result<()> {
        let spool_path = tmp_sibling(&self.db_path, ".apply.tmp");
        let _cleanup = TmpGuard(&spool_path);
        {
            let mut rc = self.client.open_ltx_file(info.level, info.min_txid, info.max_txid, 0, 0)?;
            let mut spool = File::create(&spool_path)?;
            std::io::copy(&mut rc, &mut spool)?;
            spool.sync_all()?;
        }
        self.apply_spooled(f, &spool_path, page_size, cancel)
    }

    /// Applies one complete, already-spooled LTX file. The spool is
    /// CRC-verified in full *before* any page is written (hardening over Go,
    /// which verifies after). Page 1 gets the journal mode + change counter
    /// fixups; the file is truncated to the commit size. Shared by the
    /// fetch-by-listing path and streaming follow.
    fn apply_spooled(
        &mut self,
        f: &File,
        spool_path: &Path,
        page_size: u32,
        cancel: &CancelToken,
    ) -> Result<()> {
        {
            let dec = Decoder::new(BufReader::new(File::open(spool_path)?));
            dec.verify()?;
        }

        let mut dec = Decoder::new(BufReader::new(File::open(spool_path)?));
        dec.decode_header()?;
        let hdr = *dec.header();
        if hdr.page_size != page_size {
            // Page-size change implies a bucket reset. (compat: ltx
            // compactor rejects mismatched page sizes)
            return Err(Error::Diverged { local: hdr.min_txid, remote: hdr.max_txid });
        }

        // Taken before the first page is written, so a `LockBusy` or
        // `Cancelled` here leaves the replica untouched.
        let _lock = if self.opts.use_file_locks {
            Some(FcntlLock::exclusive(f, self.opts.lock_timeout, cancel)?)
        } else {
            None
        };

        let mut data = vec![0u8; page_size as usize];
        while let Some(phdr) = dec.decode_page(&mut data)? {
            if phdr.pgno == 1 && data.len() >= 28 {
                // Present as a rollback-journal database and invalidate other
                // connections' caches. (replica.go:907-910)
                data[18] = 0x01;
                data[19] = 0x01;
                rand::rng().fill_bytes(&mut data[24..28]);
            }
            let off = (phdr.pgno as u64 - 1) * page_size as u64;
            std::os::unix::fs::FileExt::write_all_at(f, &data, off)?;
        }

        if hdr.commit > 0 {
            f.set_len(hdr.commit as u64 * page_size as u64)?;
        }

        dec.finish()?;
        f.sync_all()?;
        Ok(())
    }

    /// `PRAGMA quick_check`/`integrity_check` over the freshly restored
    /// image. (replica.go:1259-1293)
    ///
    /// This is the crate's only read-only open, and until
    /// [`present_as_rollback_journal`] existed it was also the line every
    /// restore under a system libsqlite3 died on: the image still carried the
    /// origin's WAL journal-mode header while its `-shm` had just been
    /// deleted, and a read-only connection may not rebuild one. Read-only is
    /// the right flag for a check that must not mutate what it is checking —
    /// it is safe here because the image is normalized before it lands, not
    /// because the flag was ever the problem.
    fn check_integrity(&self) -> Result<()> {
        let pragma = match self.opts.integrity_check {
            IntegrityCheck::None => return Ok(()),
            IntegrityCheck::Quick => "quick_check",
            IntegrityCheck::Full => "integrity_check",
        };
        let conn = rusqlite::Connection::open_with_flags(
            &self.db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        let result: String = conn.query_row(&format!("PRAGMA {pragma}"), [], |r| r.get(0))?;
        if result != "ok" {
            let _ = fs::remove_file(&self.db_path);
            return Err(Error::Other(format!("post-restore integrity check failed: {result}")));
        }
        Ok(())
    }
}

/// SQLite-compatible exclusive byte-range locks on the replica file, taken
/// for the duration of a page application. (internal/lock_unix.go)
///
/// Acquisition is deadline-bounded and cancellable. It used to be neither:
/// both ranges were taken with `F_SETLKW`, which blocks in the kernel with
/// no timeout and no interruption point, so a single reader process holding
/// a SQLite transaction against the replica could wedge replication forever
/// — `cancel()` sets a flag that nothing in a blocked `fcntl` ever reads,
/// and the blocked thread is also what `Manager::unregister` joins on. A
/// concurrent reader is the normal case on the embedded hosts liters targets
/// (an iOS or Android app reading the replica while a worker follows it), so
/// the wait is now `F_SETLK` (the non-blocking command) on a poll schedule
/// that re-checks the token between attempts.
struct FcntlLock<'a> {
    f: &'a File,
}

const SQLITE_PENDING_BYTE: i64 = 0x4000_0000;
const SQLITE_SHARED_FIRST: i64 = SQLITE_PENDING_BYTE + 2;
const SQLITE_SHARED_SIZE: i64 = 510;

/// One of the two byte ranges SQLite's locking protocol uses. Named so a
/// failure can say *which* range was contended.
#[derive(Clone, Copy)]
struct LockRange {
    name: &'static str,
    start: i64,
    len: i64,
}

/// Held by a writer to stop *new* readers from starting.
const PENDING_RANGE: LockRange =
    LockRange { name: "PENDING", start: SQLITE_PENDING_BYTE, len: 1 };
/// Read-locked by every reader; write-locking it is what EXCLUSIVE means.
const SHARED_RANGE: LockRange =
    LockRange { name: "SHARED", start: SQLITE_SHARED_FIRST, len: SQLITE_SHARED_SIZE };

/// Poll schedule while a lock is contended: doubling from 1ms up to a 20ms
/// ceiling. The ceiling bounds both the delay after the holder releases and
/// the latency of noticing cancellation; the 1ms floor keeps a brief
/// overlap (the common case) cheap.
const LOCK_POLL_MIN: Duration = Duration::from_millis(1);
const LOCK_POLL_MAX: Duration = Duration::from_millis(20);

/// Ceiling on [`ReplicaOptions::lock_timeout`], so a nonsense value cannot
/// overflow the deadline arithmetic. Even here the wait stays cancellable.
const LOCK_TIMEOUT_MAX: Duration = Duration::from_secs(24 * 60 * 60);

/// One `fcntl(F_SETLK)` — the non-blocking command. A conflicting lock in
/// another process is reported as `EAGAIN`/`EACCES` rather than parking the
/// thread. `F_UNLCK` never conflicts, so releasing is a single call.
fn set_fcntl_lock(f: &File, lock_type: libc::c_short, r: LockRange) -> std::io::Result<()> {
    use std::os::unix::io::AsRawFd;
    let fl = libc::flock {
        l_start: r.start,
        l_len: r.len,
        l_pid: 0,
        l_type: lock_type,
        l_whence: libc::SEEK_SET as libc::c_short,
    };
    let rc = unsafe { libc::fcntl(f.as_raw_fd(), libc::F_SETLK, &fl) };
    if rc == -1 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

/// "Another process holds a conflicting lock" — the one `F_SETLK` failure
/// worth retrying. POSIX allows either errno here, so both are matched.
fn is_lock_contention(e: &std::io::Error) -> bool {
    matches!(e.raw_os_error(), Some(libc::EAGAIN) | Some(libc::EACCES))
}

/// `fcntl(F_GETLK)`: the pid of a process holding a lock that would block
/// this request. Diagnostics only, and inherently racy — the holder may
/// release between the failed `F_SETLK` and this probe — so every failure
/// mode collapses to `None`.
fn conflicting_pid(f: &File, lock_type: libc::c_short, r: LockRange) -> Option<i32> {
    use std::os::unix::io::AsRawFd;
    let mut fl = libc::flock {
        l_start: r.start,
        l_len: r.len,
        l_pid: 0,
        l_type: lock_type,
        l_whence: libc::SEEK_SET as libc::c_short,
    };
    let rc = unsafe { libc::fcntl(f.as_raw_fd(), libc::F_GETLK, &mut fl) };
    if rc == -1 || fl.l_type == libc::F_UNLCK as libc::c_short {
        return None;
    }
    // Linux reports -1 for open-file-description locks, which name no single
    // owning process.
    (fl.l_pid > 0).then_some(fl.l_pid)
}

/// Write-locks one range, retrying for as long as another process holds a
/// conflicting lock.
///
/// Returns the moment the lock is ours. Gives up with [`Error::Cancelled`]
/// as soon as `cancel` flips and with [`Error::LockBusy`] at `deadline`,
/// whichever comes first. An uncontended acquisition issues exactly one
/// `fcntl` and never sleeps, so the fast path is what the old `F_SETLKW`
/// did, instruction for instruction.
fn acquire_write_lock(
    f: &File,
    r: LockRange,
    deadline: Instant,
    cancel: &CancelToken,
) -> Result<()> {
    const WRLCK: libc::c_short = libc::F_WRLCK as libc::c_short;
    let began = Instant::now();
    let mut delay = LOCK_POLL_MIN;
    loop {
        match set_fcntl_lock(f, WRLCK, r) {
            Ok(()) => return Ok(()),
            Err(e) if is_lock_contention(&e) => {}
            Err(e) => return Err(Error::Io(e)),
        }
        // Cancellation outranks the deadline: a caller that asked us to stop
        // gets `Cancelled`, not a contention report it never waited for.
        cancel.check()?;
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Err(Error::LockBusy {
                lock: r.name,
                waited: began.elapsed(),
                holder_pid: conflicting_pid(f, WRLCK, r),
            });
        }
        std::thread::sleep(delay.min(left));
        delay = (delay * 2).min(LOCK_POLL_MAX);
    }
}

impl<'a> FcntlLock<'a> {
    /// Takes SQLite's EXCLUSIVE pair — PENDING, then the SHARED range —
    /// under one shared deadline, checking `cancel` between attempts.
    ///
    /// PENDING is held across the SHARED retries rather than dropped and
    /// re-taken between them. That is what the old blocking `F_SETLKW` on
    /// SHARED did implicitly, and it is load-bearing: holding PENDING stops
    /// *new* readers from starting, so the readers already inside drain and
    /// the applier is guaranteed to get in. Releasing it each round would
    /// let a steady stream of arriving readers starve the applier until the
    /// deadline, every time.
    fn exclusive(f: &'a File, timeout: Duration, cancel: &CancelToken) -> Result<FcntlLock<'a>> {
        let deadline = Instant::now() + timeout.min(LOCK_TIMEOUT_MAX);
        acquire_write_lock(f, PENDING_RANGE, deadline, cancel)?;
        // PENDING is ours from here: bind the guard before the second
        // acquisition so every exit path, `?` included, releases it.
        let guard = FcntlLock { f };
        acquire_write_lock(f, SHARED_RANGE, deadline, cancel)?;
        Ok(guard)
    }
}

impl Drop for FcntlLock<'_> {
    fn drop(&mut self) {
        const UNLCK: libc::c_short = libc::F_UNLCK as libc::c_short;
        let _ = set_fcntl_lock(self.f, UNLCK, SHARED_RANGE);
        let _ = set_fcntl_lock(self.f, UNLCK, PENDING_RANGE);
    }
}

/// Rewrites the two journal-mode bytes in the database header so the image
/// reads as a rollback-journal database. (replica.go:876-910, vfs.go:802)
///
/// A snapshot's page 1 is a byte-for-byte copy of the origin's, so a restored
/// image inherits the origin's journal mode — WAL, for anything liters
/// replicates. WAL is a claim about files that are not there: the restore
/// deletes `-wal`/`-shm`, and reading a WAL database requires the `-shm`
/// shared-memory index. A reader that may not create one — any
/// `SQLITE_OPEN_READONLY` connection — then fails on its first statement with
/// `SQLITE_CANTOPEN`, and which SQLite is linked decides whether it does: the
/// bundled amalgamation (3.50.2) tolerates it, Apple's system libsqlite3
/// (3.51.0) does not. That made every restore path in the suite fail under
/// `--no-default-features`, the linkage an iOS app whose Swift side already
/// links SQLite through GRDB is required to use.
///
/// [`Replica::apply_spooled`] has always done this to page 1 on the
/// incremental path, and [`Replica::db_path`] documents the result as the
/// replica's contract. Only [`Replica::restore`] skipped it, so the two
/// writers of the same file disagreed about what they produced.
fn present_as_rollback_journal(f: &mut File) -> Result<()> {
    // A database is at least one page, and the header is 100 bytes, so a
    // shorter file is not one we can fix up. `restore` can produce it only
    // from an empty snapshot, which has nothing to read either way.
    if f.metadata()?.len() < 100 {
        return Ok(());
    }
    f.seek(SeekFrom::Start(18))?;
    f.write_all(&[0x01, 0x01])?;
    Ok(())
}

/// Fresh handle + page size for in-place page application.
fn open_db_for_apply(db_path: &Path) -> Result<(File, u32)> {
    let mut db = OpenOptions::new().read(true).write(true).open(db_path)?;
    let page_size = read_page_size(&mut db)?;
    Ok((db, page_size))
}

/// Sleeps `total` in short slices so cancellation stays responsive; returns
/// true if cancelled.
fn sleep_checking(cancel: &CancelToken, total: Duration) -> bool {
    let deadline = Instant::now() + total;
    loop {
        if cancel.is_cancelled() {
            return true;
        }
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return false;
        }
        std::thread::sleep(left.min(Duration::from_millis(100)));
    }
}

/// Reads the page size from the SQLite header. (replica.go:747-755)
fn read_page_size(f: &mut File) -> Result<u32> {
    let mut buf = [0u8; 2];
    f.seek(SeekFrom::Start(16))?;
    f.read_exact(&mut buf)?;
    let ps = u32::from(u16::from_be_bytes(buf));
    Ok(if ps == 1 { 65536 } else { ps })
}

/// `{db}-txid` sidecar: 16-hex TXID + newline, written atomically.
/// Byte-compatible with litestream's follow-mode sidecar. (replica.go:1645-1703)
pub fn read_txid_file(db_path: &Path) -> Result<Txid> {
    let path = txid_path(db_path);
    match fs::read_to_string(&path) {
        Ok(s) => Txid::parse(s.trim())
            .ok_or_else(|| Error::Other(format!("parse txid file {path:?}: {s:?}"))),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Txid(0)),
        Err(e) => Err(e.into()),
    }
}

pub fn write_txid_file(db_path: &Path, txid: Txid) -> Result<()> {
    let path = txid_path(db_path);
    let tmp = tmp_sibling(&path, ".tmp");
    {
        let mut f = File::create(&tmp)?;
        writeln!(f, "{txid}")?;
        f.sync_all()?;
    }
    fs::rename(&tmp, &path)?;
    fsync_parent(&path);
    Ok(())
}

pub(crate) fn txid_path(db_path: &Path) -> PathBuf {
    let mut p = db_path.as_os_str().to_owned();
    p.push("-txid");
    PathBuf::from(p)
}

fn tmp_sibling(path: &Path, suffix: &str) -> PathBuf {
    let mut p = path.as_os_str().to_owned();
    p.push(suffix);
    PathBuf::from(p)
}

fn fsync_parent(path: &Path) {
    if let Some(parent) = path.parent() {
        if let Ok(d) = File::open(parent) {
            let _ = d.sync_all();
        }
    }
}

struct TmpGuard<'a>(&'a Path);

impl Drop for TmpGuard<'_> {
    fn drop(&mut self) {
        let _ = fs::remove_file(self.0);
    }
}
