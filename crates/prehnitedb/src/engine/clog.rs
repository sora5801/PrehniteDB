//! The commit log — a per-database, append-only file of transaction outcomes.
//!
//! Every row in the storage layer carries an MVCC `(tx_min, tx_max)`. The
//! visibility check has to answer two questions about each:
//!
//! 1. Is this TX committed? (Or rolled back? Or still in flight?)
//! 2. If committed, did it commit *before* the reader's snapshot?
//!
//! Question 2 is `tx_id < snapshot.next_tx`. Question 1 is what the clog
//! exists to answer: a single source of truth for what every TX ID resolved
//! to, persistent across crashes.
//!
//! The clog is a tiny file (`<db>-clog`) of fixed-size 9-byte records: an
//! 8-byte little-endian TX ID and a 1-byte status (`1` = committed, `2` =
//! rolled back). It is append-only and `fsync`ed on every write — the
//! durability story is the same as the WAL's, just on its own file.
//!
//! On open, the whole clog is scanned into an in-memory `HashMap<u64, Status>`
//! so lookups are O(1). A TX ID *not* in the map is either (a) still in
//! flight (its writer holds it in [`crate::engine::transaction::TxState`]'s
//! `in_flight` set), or (b) never assigned. Either way, it is not visible
//! to any snapshot.
//!
//! Crash recovery: on open, every TX ID `<= next_tx_id` (from the database
//! header) that does *not* appear in the clog is treated as **rolled back**.
//! A writer that crashed mid-flight will have stamped rows with its TX ID
//! but never written its commit/rollback record, so those rows become
//! invisible to every future snapshot.
//!
//! ## Group commit (v0.42)
//!
//! Under N concurrent writers, the v0.26 design serialised every commit
//! through one fsync each — the mutex round-tripped per call, so N
//! commits cost N fsyncs (often the per-commit bottleneck on real
//! storage, where an fsync can take 100µs–10ms).
//!
//! v0.42 splits commit into two stages:
//!
//! 1. **Enqueue.** Under the state mutex, push the `(tx_id, status)`
//!    onto a `pending` buffer and claim a monotonic `LSN`. Fast — no
//!    I/O — so the mutex is held for microseconds.
//! 2. **Flush.** The first writer to find no one else flushing becomes
//!    the **leader**: it drains the entire `pending` buffer into a
//!    local batch, releases the mutex, writes the batch and fsyncs
//!    once, then reacquires the mutex to update the in-memory map and
//!    publish the new `durable_lsn`. Every other writer arriving while
//!    the leader is flushing parks on a `Condvar`; on wake, each
//!    checks whether its LSN is now durable (`durable_lsn >= my_lsn`).
//!
//! The natural batch size is whatever stacks up in `pending` during
//! one leader's I/O window: at idle, one record per fsync (no overhead
//! vs v0.26); under contention with 32 writers, ~32 records per fsync.
//! Throughput becomes I/O-bandwidth-bound instead of fsync-latency-bound.
//!
//! Durability semantics are preserved: a record's entry in the in-memory
//! `map` is only inserted *after* fsync returns successfully, so a
//! reader can never see a "committed" status for a TX whose fsync hasn't
//! landed.
//!
//! ## Truncation (v0.57)
//!
//! Before v0.57 the clog grew unboundedly: every committed or
//! rolled-back transaction added a 9-byte record, never reclaimed.
//! A million-TX workload left a 9 MB clog file that was fully
//! resident in memory (the in-memory `HashMap<u64, Status>` mirrors
//! the file on open).
//!
//! v0.57 gives the file a tiny header (16 bytes: 8-byte magic +
//! 8-byte `min_tx_id`) and adds [`Clog::truncate_below`], which
//! atomically rewrites the file to drop every record with
//! `tx_id < floor` and bumps `min_tx_id` to the new floor.
//! Subsequent [`Clog::status`] queries for any `tx_id < min_tx_id`
//! return `Committed` by convention.
//!
//! **Why "committed" is the safe default below the floor.** The
//! truncation is meant to run only after the v0.36 background
//! reclaimer has processed every row below the floor — at which
//! point any tx_id we just forgot was either (a) a committed
//! insert (rows still present in tables, correctly visible), (b) a
//! committed delete (tombstone reclaimed, rows correctly absent),
//! or (c) a rolled-back insert (rows reclaimed, never visible).
//! All three reach the right behaviour with the `Committed`
//! default. The orchestration of "reclaim everything below F, then
//! truncate to F" lives in [`crate::engine::database::Database::truncate_clog`].
//!
//! **Crash safety.** Truncation writes the new image to
//! `<clog>.tmp`, fsyncs it, drops our handle to the canonical
//! clog file (Windows can't replace an open file), renames the
//! tmp over the canonical path, and reopens it. Any leftover
//! `.tmp` from a crashed truncation is deleted on the next open —
//! the canonical file is the truth; partial truncations are
//! discarded.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};

use crate::error::{Error, Result};

/// The on-disk size of one clog record: 8 bytes for the TX ID, 1 for status.
const RECORD_SIZE: usize = 9;

const STATUS_COMMITTED: u8 = 1;
const STATUS_ROLLED_BACK: u8 = 2;

/// v0.57: 8-byte magic at the start of every clog file. Distinguishes
/// the v0.57+ format from a (now-unsupported) v0.56-and-earlier flat
/// record file. `Clog::open` errors with a clear message if the magic
/// is wrong, including against an old format — the pre-1.0 README
/// already warns that on-disk formats may break across versions.
const CLOG_MAGIC: &[u8; 8] = b"PREHCLG1";

/// v0.57: the on-disk header size — 8 bytes magic + 8 bytes
/// `min_tx_id` (LE u64). Records start at file offset `HEADER_SIZE`.
const HEADER_SIZE: u64 = 16;

/// The outcome a TX resolved to. A TX still in flight has no clog entry yet;
/// see [`Clog::status_or_rolled_back`] for the "treat absent as rolled back"
/// helper crash recovery uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Committed,
    RolledBack,
}

/// The path of the clog file beside the database file.
pub fn clog_path(db: &Path) -> PathBuf {
    let mut name = db.as_os_str().to_os_string();
    name.push("-clog");
    PathBuf::from(name)
}

/// v0.57: the scratch file [`Clog::truncate_below`] writes the
/// new image to before atomically renaming it over the canonical
/// path. Any leftover at open time is a crashed-mid-truncate
/// remnant and is deleted.
fn clog_tmp_path(clog: &Path) -> PathBuf {
    let mut name = clog.as_os_str().to_os_string();
    name.push(".tmp");
    PathBuf::from(name)
}

/// v0.57: write the 16-byte header `[magic][min_tx_id LE]` at offset 0.
/// The caller is responsible for any fsync.
fn write_header(file: &mut File, min_tx_id: u64) -> Result<()> {
    let mut hdr = [0u8; HEADER_SIZE as usize];
    hdr[0..8].copy_from_slice(CLOG_MAGIC);
    hdr[8..16].copy_from_slice(&min_tx_id.to_le_bytes());
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&hdr)?;
    Ok(())
}

/// v0.57: read and validate the 16-byte header at offset 0. Returns
/// the on-disk `min_tx_id`, or an error if the magic doesn't match —
/// the latter catches both bit-rot corruption and an older clog
/// format (pre-v0.57 files have no header and so the first 8 bytes
/// are a TX ID's bytes, almost certainly not `PREHCLG1`).
fn read_header(file: &mut File) -> Result<u64> {
    let mut hdr = [0u8; HEADER_SIZE as usize];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut hdr).map_err(|e| {
        Error::corruption(format!(
            "clog file is too short to hold the v0.57 header ({e}); \
             pre-v0.57 clog files are not supported — see README"
        ))
    })?;
    if &hdr[0..8] != CLOG_MAGIC {
        return Err(Error::corruption(
            "clog file lacks the v0.57 magic — pre-v0.57 clog files \
             are not supported; see README's pre-1.0 format-stability note",
        ));
    }
    Ok(u64::from_le_bytes(hdr[8..16].try_into().unwrap()))
}

/// The clog handle, shared via [`Arc`] so every `Database` open on one file
/// uses the same instance. v0.42 uses a leader/follower group-commit
/// protocol — see the module docs.
///
/// **Two separate mutexes** are essential to the design: a fast
/// `state` mutex covers the in-memory map and the pending queue
/// (microsecond-scoped: every enqueue is one push), and a slower
/// `file` mutex covers the write+fsync. The leader takes `state`
/// briefly to drain `pending`, releases it, takes `file` to do the
/// slow I/O (during which other writers can still enqueue freely
/// into `state.pending`), releases it, then re-takes `state` to
/// publish the new `durable_lsn`. If they shared one mutex, the
/// fsync would block all concurrent enqueues — defeating the
/// batching the design exists for.
#[derive(Clone)]
pub struct Clog {
    state: Arc<Mutex<ClogState>>,
    /// `Option` so [`Clog::truncate_below`] can briefly take the
    /// `File` out (close the handle), rename a fresh image over
    /// the canonical path, and put a freshly-opened handle back.
    /// On Windows the rename-over-an-open-file fails, so the
    /// close-and-reopen dance is mandatory; this also keeps
    /// behaviour identical on Unix.
    file: Arc<Mutex<Option<File>>>,
    /// Followers park here while a leader is flushing; the leader signals
    /// `notify_all()` once `durable_lsn` advances.
    flush_done: Arc<Condvar>,
    /// v0.57: the canonical clog file path, remembered so
    /// `truncate_below` can derive its scratch path and reopen
    /// after the rename without needing the original `db_path`
    /// threaded through.
    path: Arc<PathBuf>,
}

impl std::fmt::Debug for Clog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let len = self.state.lock().map(|s| s.map.len()).unwrap_or(0);
        write!(f, "Clog({len} records)")
    }
}

struct ClogState {
    /// `tx_id -> Status`, populated only **after** the record's fsync
    /// has landed. v0.42 group commit: enqueueing a record into
    /// `pending` does *not* update this map; the leader inserts on
    /// behalf of the whole batch after fsync returns.
    ///
    /// v0.57: entries with `tx_id < min_tx_id` are *not* in this map —
    /// they were dropped by [`Clog::truncate_below`]. Lookups for
    /// `tx_id < min_tx_id` return `Committed` by convention; see
    /// [`Clog::status`].
    map: HashMap<u64, Status>,
    /// Records that have been enqueued but not yet fsynced. Drained
    /// wholesale by the next leader.
    pending: Vec<(u64, Status)>,
    /// Monotonic ticket counter. Each enqueue claims `next_lsn + 1`
    /// before the increment; the leader's drain snapshot covers every
    /// LSN ≤ `next_lsn`.
    next_lsn: u64,
    /// Highest LSN whose record has been durably fsynced. A follower's
    /// wait condition is `durable_lsn >= my_lsn`.
    durable_lsn: u64,
    /// True while a leader holds the flush slot. Followers see this
    /// and park on `flush_done`.
    flushing: bool,
    /// v0.57: the truncation floor. Any `tx_id < min_tx_id` was
    /// dropped from the on-disk file (and from `map`) by a
    /// previous [`Clog::truncate_below`] call; queries for such
    /// IDs answer `Committed` by convention. See the module docs
    /// for why that's the safe default. Starts at 0 (no truncation
    /// yet) for a fresh clog.
    min_tx_id: u64,
}

impl Clog {
    /// Open or create the clog file at `<path>-clog`, reading every existing
    /// record into the in-memory map. The file's records are append-only;
    /// the in-memory map mirrors them.
    ///
    /// v0.57: the file format gained a 16-byte header (magic +
    /// `min_tx_id`). A fresh file is created with the header written.
    /// An existing file's first 8 bytes must match [`CLOG_MAGIC`] or
    /// `open` errors clearly — v0.56-and-earlier clog files lack the
    /// header and won't open under v0.57 (the README's pre-1.0
    /// format-stability disclaimer covers this).
    ///
    /// Also: any leftover `<clog>.tmp` from a crashed
    /// [`Clog::truncate_below`] is deleted here. The canonical file
    /// is the truth; the tmp was a partial rewrite that never
    /// completed its atomic rename.
    pub fn open(db_path: &Path) -> Result<Clog> {
        let path = clog_path(db_path);
        // Discard any leftover from a crashed truncation. Doing this
        // before opening the canonical file means a v0.57 server
        // recovers automatically from a mid-truncate crash.
        let tmp = clog_tmp_path(&path);
        let _ = std::fs::remove_file(&tmp);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        let file_len = file.metadata()?.len();

        // Read or write the header.
        let min_tx_id = if file_len == 0 {
            // Fresh file: write a v0.57 header with min_tx_id=0 and fsync.
            // We do the fsync so a crash immediately after open doesn't
            // leave a zero-length file that the next open would also
            // treat as fresh — defence in depth, since open()-create
            // failing isn't catastrophic anyway (no records lost).
            write_header(&mut file, 0)?;
            file.sync_all()?;
            0
        } else {
            read_header(&mut file)?
        };

        let mut map = HashMap::new();
        let mut buf = [0u8; RECORD_SIZE];
        file.seek(SeekFrom::Start(HEADER_SIZE))?;
        loop {
            match file.read_exact(&mut buf) {
                Ok(()) => {
                    let tx_id = u64::from_le_bytes(buf[0..8].try_into().unwrap());
                    let status = match buf[8] {
                        STATUS_COMMITTED => Status::Committed,
                        STATUS_ROLLED_BACK => Status::RolledBack,
                        other => {
                            return Err(Error::corruption(format!(
                                "unknown clog status tag {other}"
                            )))
                        }
                    };
                    map.insert(tx_id, status);
                }
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
        }
        // Position at the end so future appends go in the right place.
        file.seek(SeekFrom::End(0))?;
        Ok(Clog {
            state: Arc::new(Mutex::new(ClogState {
                map,
                pending: Vec::new(),
                next_lsn: 0,
                durable_lsn: 0,
                flushing: false,
                min_tx_id,
            })),
            file: Arc::new(Mutex::new(Some(file))),
            flush_done: Arc::new(Condvar::new()),
            path: Arc::new(path),
        })
    }

    /// The status of `tx_id`. `None` means "not in the clog" — either still
    /// in flight (held by `TxState`), pending an in-progress group-commit
    /// fsync, or never assigned.
    ///
    /// v0.57: a `tx_id` below the truncation floor (`min_tx_id`) is no
    /// longer in the file or in `map` — it returns
    /// `Some(Status::Committed)` by convention. See the module doc's
    /// "Truncation" section for why `Committed` is the safe default.
    pub fn status(&self, tx_id: u64) -> Option<Status> {
        let state = self.state.lock().expect("poisoned clog");
        if tx_id < state.min_tx_id {
            return Some(Status::Committed);
        }
        state.map.get(&tx_id).copied()
    }

    /// Bulk lookup: status of `tx_id` for snapshot visibility. A TX ID below
    /// `oldest_active` (the watermark of "everything has resolved") that has
    /// no clog entry is treated as **rolled back** — this is the crash
    /// recovery rule. Above the watermark, "not in clog" means "still in
    /// flight" and the caller (the snapshot) is expected to know which.
    ///
    /// v0.57: the `min_tx_id` (truncation floor) takes precedence over
    /// the crash-recovery rule — `tx_id < min_tx_id` always returns
    /// `Committed`, even if `tx_id < oldest_active` would otherwise
    /// have returned `RolledBack`. This is correct because the
    /// truncation orchestrator ensures every rolled-back row below the
    /// floor has been physically reclaimed before bumping `min_tx_id`,
    /// so no row reachable via this lookup is one that was rolled back.
    pub fn status_or_rolled_back(&self, tx_id: u64, oldest_active: u64) -> Option<Status> {
        let state = self.state.lock().expect("poisoned clog");
        if tx_id < state.min_tx_id {
            return Some(Status::Committed);
        }
        match state.map.get(&tx_id) {
            Some(&status) => Some(status),
            None if tx_id < oldest_active => Some(Status::RolledBack),
            None => None,
        }
    }

    /// Append a record marking `tx_id` as committed. fsynced before return.
    pub fn record_commit(&self, tx_id: u64) -> Result<()> {
        self.append(tx_id, Status::Committed)
    }

    /// Append a record marking `tx_id` as rolled back. fsynced before return.
    pub fn record_rollback(&self, tx_id: u64) -> Result<()> {
        self.append(tx_id, Status::RolledBack)
    }

    /// Append `(tx_id, status)` and ensure it is durable before return.
    ///
    /// Two-stage: enqueue under the state mutex (fast), then either
    /// flush as leader or wait as follower. Under contention, the
    /// leader's single fsync covers every record queued during its
    /// I/O window — see the module docs for the full protocol.
    fn append(&self, tx_id: u64, status: Status) -> Result<()> {
        // Stage 1: enqueue. Claim an LSN, push the record onto pending,
        // release the state mutex.
        let my_lsn = {
            let mut state = self.state.lock().expect("poisoned clog");
            state.next_lsn += 1;
            let lsn = state.next_lsn;
            state.pending.push((tx_id, status));
            lsn
        };

        // Stage 2: ensure my LSN is durable.
        self.flush_until(my_lsn)
    }

    /// Block until every enqueued record with LSN ≤ `target` is on
    /// disk. The first arriver becomes the leader and drains the
    /// pending batch; subsequent arrivers park on `flush_done` until
    /// the leader's fsync completes, then re-check and either succeed
    /// (their LSN is now covered) or become the next leader.
    fn flush_until(&self, target_lsn: u64) -> Result<()> {
        let mut state = self.state.lock().expect("poisoned clog");
        loop {
            // Fast path: another leader's earlier fsync already covered us.
            if state.durable_lsn >= target_lsn {
                return Ok(());
            }
            // Some peer is mid-fsync. Park; on wake, re-check.
            if state.flushing {
                state = self
                    .flush_done
                    .wait(state)
                    .expect("poisoned clog (flush_done wait)");
                continue;
            }
            // We're the leader. Snapshot the batch we'll cover, mark
            // the leader slot taken, release the state mutex so peers
            // can keep enqueueing into `pending` during our I/O.
            state.flushing = true;
            let batch: Vec<(u64, Status)> = std::mem::take(&mut state.pending);
            let snapshot_lsn = state.next_lsn;
            drop(state);

            // The slow part: one write covering every batch record,
            // one fsync. Held under the `file` mutex, which doesn't
            // block enqueues on `state`.
            let result = self.write_and_fsync(&batch);

            // Re-acquire the state mutex to publish results.
            let mut state = self.state.lock().expect("poisoned clog");
            if result.is_ok() {
                // Durability has landed for the whole batch. Update
                // the in-memory map (visibility follows durability —
                // never the other way round) and advance the watermark.
                for (id, status) in &batch {
                    state.map.insert(*id, *status);
                }
                state.durable_lsn = snapshot_lsn;
            }
            // On error: the batch's records didn't make it to disk.
            // We leave them out of the map — they'll be treated as
            // in-flight by readers, and on next open the crash-recovery
            // rule (TX ID ≤ next_tx_id with no clog entry = rolled
            // back) classifies them correctly. Followers waiting on
            // us see `durable_lsn` unchanged, wake up, and either retry
            // (if their LSN is now > snapshot_lsn) or hit the same
            // error path with their own batch.
            state.flushing = false;
            self.flush_done.notify_all();
            return result;
        }
    }

    /// Encode every record in `batch` into one buffer and `write_all`
    /// it, then `sync_all` once. Records are 9 bytes apiece, so a batch
    /// of N records is one write of `9 * N` bytes followed by one
    /// fsync — the whole point of group commit.
    ///
    /// The `file` mutex is taken only here, so concurrent writers can
    /// keep pushing onto `state.pending` while the leader is doing
    /// I/O. That's where the batching opportunity arises: the next
    /// leader drains a fatter `pending`.
    fn write_and_fsync(&self, batch: &[(u64, Status)]) -> Result<()> {
        if batch.is_empty() {
            // Edge case: a peer arrived between our `mem::take` and
            // our drop(state) and pushed nothing — or the test path
            // called us with an empty batch. Either way, no-op.
            return Ok(());
        }
        let mut buf = Vec::with_capacity(batch.len() * RECORD_SIZE);
        for (tx_id, status) in batch {
            let tag = match status {
                Status::Committed => STATUS_COMMITTED,
                Status::RolledBack => STATUS_ROLLED_BACK,
            };
            buf.extend_from_slice(&tx_id.to_le_bytes());
            buf.push(tag);
        }
        let mut file_guard = self.file.lock().expect("poisoned clog (file)");
        // v0.57: the `Option` is only `None` for a microsecond during
        // `truncate_below`'s close-rename-reopen dance, AND that path
        // serialises against us via `state.flushing`, so by the time
        // we're here the file is always `Some`.
        let file = file_guard
            .as_mut()
            .expect("clog file should be present during write_and_fsync");
        file.write_all(&buf)?;
        file.sync_all()?;
        Ok(())
    }

    /// Whether `tx_id` is recorded as committed.
    pub fn is_committed(&self, tx_id: u64) -> bool {
        self.status(tx_id) == Some(Status::Committed)
    }

    /// Whether `tx_id` is recorded as rolled back.
    pub fn is_rolled_back(&self, tx_id: u64) -> bool {
        self.status(tx_id) == Some(Status::RolledBack)
    }

    /// How many records the in-memory map holds. Diagnostic. Does not
    /// count records still in the v0.42 `pending` buffer awaiting
    /// fsync — that's intentional, since those records aren't yet
    /// durable and aren't visible to readers.
    pub fn len(&self) -> usize {
        self.state.lock().expect("poisoned clog").map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// v0.57: the current truncation floor. Records below this were
    /// dropped from the file by a previous [`Clog::truncate_below`];
    /// queries for those `tx_id`s answer `Committed`.
    pub fn min_tx_id(&self) -> u64 {
        self.state.lock().expect("poisoned clog").min_tx_id
    }

    /// v0.57: physically remove every record with `tx_id < floor` from
    /// the on-disk file and from the in-memory map, and bump the
    /// `min_tx_id` header to `floor`. After this returns, any
    /// subsequent [`Clog::status`] for those `tx_id`s answers
    /// `Committed` by convention.
    ///
    /// **The caller is responsible for the safety contract** — see
    /// the module's "Truncation" section. In short:
    ///
    /// - `floor` must be `≤` the oldest-active TX, so no live snapshot
    ///   can ever ask about a truncated `tx_id`;
    /// - every rolled-back row with `tx_min < floor` must have been
    ///   physically reclaimed (by the v0.36 background reclaimer or
    ///   by `VACUUM`) BEFORE this call. Otherwise such a row would
    ///   suddenly become "visible" after truncation, because the
    ///   below-floor default flips from `RolledBack` (per the
    ///   crash-recovery rule) to `Committed`.
    ///
    /// [`crate::engine::database::Database::truncate_clog`] is the
    /// production orchestrator that satisfies both invariants.
    ///
    /// **Crash safety.** Writes the new image to `<clog>.tmp`,
    /// fsyncs, closes our handle to `<clog>` (mandatory on Windows
    /// — can't rename over an open file), atomically renames `.tmp`
    /// over `<clog>`, and reopens. A crash anywhere in this dance
    /// leaves at worst a `.tmp` file, which the next `open` deletes;
    /// the canonical file is either pre-truncation or
    /// post-truncation, never partially-truncated.
    ///
    /// **Concurrency.** Coordinates with the v0.42 group-commit
    /// leader/follower protocol via the same `state.flushing` flag.
    /// Peer `record_commit` / `record_rollback` calls can still
    /// enqueue into `pending` during the rewrite; their `flush_until`
    /// waits on the condvar. When `truncate_below` finishes it
    /// notifies all, and the next leader writes any pending records
    /// to the freshly-truncated file. By construction every TX being
    /// recorded right now has `tx_id ≥ oldest_active ≥ floor`, so
    /// no concurrent append introduces a below-floor record.
    pub fn truncate_below(&self, floor: u64) -> Result<()> {
        if floor == 0 {
            return Ok(());
        }
        // Step 1: take the flush slot so no leader can write to the
        // file we're about to replace. If a leader is already mid-fsync,
        // wait for it to finish (it'll set `flushing = false` and notify).
        let mut state = self.state.lock().expect("poisoned clog");
        while state.flushing {
            state = self
                .flush_done
                .wait(state)
                .expect("poisoned clog (truncate_below wait)");
        }
        state.flushing = true;
        if floor <= state.min_tx_id {
            // No-op: floor doesn't advance. Release the flush slot.
            state.flushing = false;
            self.flush_done.notify_all();
            return Ok(());
        }
        // Snapshot the records to keep (tx_id >= floor) under the
        // state lock — these are durable, so they're the truth.
        let kept: Vec<(u64, Status)> = state
            .map
            .iter()
            .filter(|(&id, _)| id >= floor)
            .map(|(&id, &s)| (id, s))
            .collect();
        drop(state);

        // Step 2: write the new image to <clog>.tmp + fsync. Done
        // entirely on a fresh File so no interaction with self.file.
        let tmp_path = clog_tmp_path(&self.path);
        // Best-effort: remove any leftover tmp from a previous
        // crashed truncate (open() does this too, but we run after
        // open).
        let _ = std::fs::remove_file(&tmp_path);
        let write_result: Result<()> = (|| {
            let mut tmp = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp_path)?;
            write_header(&mut tmp, floor)?;
            // After write_header, the cursor is at offset 16. Write
            // records sequentially.
            let mut buf = Vec::with_capacity(kept.len() * RECORD_SIZE);
            for (tx_id, status) in &kept {
                let tag = match status {
                    Status::Committed => STATUS_COMMITTED,
                    Status::RolledBack => STATUS_ROLLED_BACK,
                };
                buf.extend_from_slice(&tx_id.to_le_bytes());
                buf.push(tag);
            }
            tmp.write_all(&buf)?;
            tmp.sync_all()?;
            Ok(())
        })();
        if let Err(e) = write_result {
            // Release the flush slot so peers can resume.
            let _ = std::fs::remove_file(&tmp_path);
            let mut state = self.state.lock().expect("poisoned clog");
            state.flushing = false;
            self.flush_done.notify_all();
            return Err(e);
        }

        // Step 3: take the file lock, close our handle, rename, reopen.
        let swap_result: Result<()> = (|| {
            let mut file_guard = self.file.lock().expect("poisoned clog (file)");
            // Drop the old File: on Windows the rename below would
            // fail with "file in use" if we kept it open.
            let old = file_guard.take();
            drop(old);

            // Atomic rename over the canonical path.
            std::fs::rename(&tmp_path, &*self.path)?;

            // Reopen the (now post-truncate) canonical file.
            let mut new = OpenOptions::new()
                .read(true)
                .write(true)
                .create(false)
                .truncate(false)
                .open(&*self.path)?;
            new.seek(SeekFrom::End(0))?;
            *file_guard = Some(new);
            Ok(())
        })();
        if let Err(e) = swap_result {
            // Worst-case error path. We may have already renamed
            // (in which case .tmp is gone but our handle is None);
            // or the rename failed (.tmp survives). Try to recover:
            // re-open the canonical file so the clog remains usable.
            let recover = OpenOptions::new()
                .read(true)
                .write(true)
                .create(false)
                .truncate(false)
                .open(&*self.path);
            if let Ok(mut f) = recover {
                let _ = f.seek(SeekFrom::End(0));
                let mut file_guard = self.file.lock().expect("poisoned clog (file)");
                if file_guard.is_none() {
                    *file_guard = Some(f);
                }
            }
            let mut state = self.state.lock().expect("poisoned clog");
            state.flushing = false;
            self.flush_done.notify_all();
            return Err(e);
        }

        // Step 4: publish the new state. Drop below-floor entries
        // from the map, advance `min_tx_id`, release the flush slot.
        {
            let mut state = self.state.lock().expect("poisoned clog");
            state.map.retain(|&id, _| id >= floor);
            state.min_tx_id = floor;
            state.flushing = false;
            self.flush_done.notify_all();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn tmp_db() -> PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir().join(format!("prehnite-clog-{}-{n}.db", std::process::id()))
    }

    fn cleanup(path: &Path) {
        let _ = std::fs::remove_file(clog_path(path));
    }

    #[test]
    fn records_round_trip_across_reopen() {
        let db = tmp_db();
        cleanup(&db);
        {
            let clog = Clog::open(&db).unwrap();
            clog.record_commit(1).unwrap();
            clog.record_rollback(2).unwrap();
            clog.record_commit(3).unwrap();
        }
        // Reopen reads every record back.
        let clog = Clog::open(&db).unwrap();
        assert_eq!(clog.status(1), Some(Status::Committed));
        assert_eq!(clog.status(2), Some(Status::RolledBack));
        assert_eq!(clog.status(3), Some(Status::Committed));
        assert_eq!(clog.status(4), None);
        cleanup(&db);
    }

    #[test]
    fn status_or_rolled_back_handles_crash_recovery() {
        // A TX below the watermark with no clog entry is treated as
        // rolled back — the crash-recovery rule.
        let db = tmp_db();
        cleanup(&db);
        let clog = Clog::open(&db).unwrap();
        clog.record_commit(5).unwrap();
        // TX 3 was in flight at "crash"; watermark is 6.
        assert_eq!(clog.status_or_rolled_back(3, 6), Some(Status::RolledBack));
        assert_eq!(clog.status_or_rolled_back(5, 6), Some(Status::Committed));
        // TX 7 hasn't been assigned; above the watermark.
        assert_eq!(clog.status_or_rolled_back(7, 6), None);
        cleanup(&db);
    }

    #[test]
    fn append_is_durable() {
        // Each append fsyncs; the next open must see every record even
        // without a clean close.
        let db = tmp_db();
        cleanup(&db);
        let clog = Clog::open(&db).unwrap();
        for i in 1..=100u64 {
            clog.record_commit(i).unwrap();
        }
        assert_eq!(clog.len(), 100);
        // Mid-flight reopen sees all 100 records.
        drop(clog);
        let clog = Clog::open(&db).unwrap();
        assert_eq!(clog.len(), 100);
        for i in 1..=100u64 {
            assert!(clog.is_committed(i));
        }
        cleanup(&db);
    }

    /// v0.42 group commit: 32 threads concurrently calling
    /// `record_commit` must all succeed, every record must end up in
    /// the in-memory map (so visible), and a reopen must find every
    /// record on disk (so durable). The leader/follower protocol must
    /// not lose a record, deadlock, or double-fsync to confusion.
    #[test]
    fn concurrent_appenders_all_succeed_and_are_durable() {
        use std::thread;
        let db = tmp_db();
        cleanup(&db);
        let clog = Clog::open(&db).unwrap();
        const THREADS: u64 = 32;
        const PER_THREAD: u64 = 50;

        let mut handles = Vec::with_capacity(THREADS as usize);
        for t in 0..THREADS {
            let clog = clog.clone();
            handles.push(thread::spawn(move || {
                for i in 0..PER_THREAD {
                    let id = t * PER_THREAD + i + 1; // 1..=THREADS*PER_THREAD
                    clog.record_commit(id).expect("record_commit");
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let expected = THREADS * PER_THREAD;
        assert_eq!(
            clog.len() as u64,
            expected,
            "every appender's record should be in the map"
        );
        for id in 1..=expected {
            assert!(clog.is_committed(id), "id {id} should be committed");
        }

        // Durability check: drop the clog and reopen — every record
        // must still be there.
        drop(clog);
        let clog = Clog::open(&db).unwrap();
        assert_eq!(clog.len() as u64, expected, "post-reopen count");
        for id in 1..=expected {
            assert!(clog.is_committed(id), "post-reopen id {id} committed");
        }
        cleanup(&db);
    }

    /// v0.42 durability-before-visibility: while a record is in
    /// `pending` (not yet fsynced), it must NOT be visible via
    /// `status()`. Today we verify this indirectly by asserting the
    /// invariant holds at steady state — every successfully returned
    /// `record_commit` leaves a map entry; every map entry corresponds
    /// to a fsynced record.
    #[test]
    fn map_only_holds_fsynced_records() {
        // After every record_commit returns, the record is in the map
        // AND on disk. We verify by inserting one record at a time
        // and reopening — the on-disk state must match the in-memory
        // map exactly.
        let db = tmp_db();
        cleanup(&db);
        for i in 1..=10u64 {
            let clog = Clog::open(&db).unwrap();
            clog.record_commit(i).unwrap();
            assert!(clog.is_committed(i), "post-commit visible");
            drop(clog);
            // Reopen reads from disk only — the map equals what landed.
            let reopened = Clog::open(&db).unwrap();
            assert!(reopened.is_committed(i), "post-reopen visible");
        }
        cleanup(&db);
    }

    /// v0.42 wait/notify correctness: a follower whose LSN was covered
    /// by a leader's batch must return from `flush_until` once the
    /// leader's fsync completes — no spurious blocking, no missed
    /// wake-ups. Approximated by spawning two threads where one is
    /// guaranteed to be a follower (synchronised via a barrier).
    #[test]
    fn follower_wakes_when_leader_finishes() {
        use std::sync::Barrier;
        use std::thread;
        let db = tmp_db();
        cleanup(&db);
        let clog = Clog::open(&db).unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let clog_a = clog.clone();
        let barrier_a = barrier.clone();
        let a = thread::spawn(move || {
            barrier_a.wait();
            clog_a.record_commit(1).unwrap();
        });
        let clog_b = clog.clone();
        let barrier_b = barrier.clone();
        let b = thread::spawn(move || {
            barrier_b.wait();
            clog_b.record_commit(2).unwrap();
        });
        a.join().unwrap();
        b.join().unwrap();
        // Both records made it through.
        assert!(clog.is_committed(1));
        assert!(clog.is_committed(2));
        cleanup(&db);
    }

    // ------------------------------------------------------------------
    // v0.57: truncation.

    /// File size of the clog as it sits on disk. Used to verify
    /// truncation actually shrinks the file.
    fn clog_file_size(db: &Path) -> u64 {
        std::fs::metadata(clog_path(db))
            .expect("clog file should exist")
            .len()
    }

    #[test]
    fn fresh_clog_has_just_a_header() {
        let db = tmp_db();
        cleanup(&db);
        let _clog = Clog::open(&db).unwrap();
        assert_eq!(clog_file_size(&db), HEADER_SIZE);
        cleanup(&db);
    }

    #[test]
    fn header_round_trips_across_reopen() {
        let db = tmp_db();
        cleanup(&db);
        {
            let clog = Clog::open(&db).unwrap();
            for i in 1..=10u64 {
                clog.record_commit(i).unwrap();
            }
            clog.truncate_below(6).unwrap();
            assert_eq!(clog.min_tx_id(), 6);
        }
        // Reopen reads the same min_tx_id back from the header.
        let clog = Clog::open(&db).unwrap();
        assert_eq!(clog.min_tx_id(), 6);
        // Records below the floor return Committed by convention.
        assert_eq!(clog.status(1), Some(Status::Committed));
        assert_eq!(clog.status(5), Some(Status::Committed));
        // Records >= floor still resolve to their durable status.
        assert_eq!(clog.status(6), Some(Status::Committed));
        assert_eq!(clog.status(10), Some(Status::Committed));
        cleanup(&db);
    }

    #[test]
    fn truncate_shrinks_the_file() {
        let db = tmp_db();
        cleanup(&db);
        let clog = Clog::open(&db).unwrap();
        for i in 1..=1000u64 {
            clog.record_commit(i).unwrap();
        }
        let size_before = clog_file_size(&db);
        // 16 (header) + 1000 * 9 (records) = 9016 bytes expected
        assert_eq!(size_before, HEADER_SIZE + 1000 * RECORD_SIZE as u64);

        clog.truncate_below(501).unwrap();

        let size_after = clog_file_size(&db);
        // 16 (header) + 500 * 9 (kept) = 4516 bytes expected
        assert_eq!(size_after, HEADER_SIZE + 500 * RECORD_SIZE as u64);
        assert!(
            size_after < size_before,
            "file should shrink: before={size_before} after={size_after}"
        );

        // Behavior preserved across the truncation:
        for i in 1..=500u64 {
            assert_eq!(
                clog.status(i),
                Some(Status::Committed),
                "below-floor i={i} should answer Committed"
            );
        }
        for i in 501..=1000u64 {
            assert_eq!(
                clog.status(i),
                Some(Status::Committed),
                "above-floor i={i} should be Committed (from before truncate)"
            );
        }
        cleanup(&db);
    }

    #[test]
    fn truncate_preserves_rolled_back_status_above_floor() {
        // Below the floor, everything answers Committed. Above the
        // floor, the actual recorded status must survive.
        let db = tmp_db();
        cleanup(&db);
        let clog = Clog::open(&db).unwrap();
        clog.record_commit(1).unwrap();
        clog.record_rollback(2).unwrap();
        clog.record_commit(3).unwrap();
        clog.record_rollback(4).unwrap();
        clog.record_commit(5).unwrap();

        clog.truncate_below(3).unwrap();

        // Below floor: Committed by convention (we lost the
        // distinction; this is safe because the orchestrator made
        // sure no rolled-back row below the floor still exists).
        assert_eq!(clog.status(1), Some(Status::Committed));
        assert_eq!(clog.status(2), Some(Status::Committed));
        // Above floor: actual recorded statuses preserved.
        assert_eq!(clog.status(3), Some(Status::Committed));
        assert_eq!(clog.status(4), Some(Status::RolledBack));
        assert_eq!(clog.status(5), Some(Status::Committed));
        cleanup(&db);
    }

    #[test]
    fn status_or_rolled_back_respects_floor() {
        let db = tmp_db();
        cleanup(&db);
        let clog = Clog::open(&db).unwrap();
        clog.record_commit(5).unwrap();
        clog.record_rollback(7).unwrap();
        clog.truncate_below(4).unwrap();
        // Below the floor: Committed wins over the crash-recovery
        // RolledBack default. The orchestration contract ensures
        // this is correct.
        assert_eq!(
            clog.status_or_rolled_back(1, 10),
            Some(Status::Committed)
        );
        assert_eq!(
            clog.status_or_rolled_back(3, 10),
            Some(Status::Committed)
        );
        // Above the floor: behaviour identical to v0.56.
        assert_eq!(
            clog.status_or_rolled_back(5, 10),
            Some(Status::Committed)
        );
        assert_eq!(
            clog.status_or_rolled_back(7, 10),
            Some(Status::RolledBack)
        );
        // No clog entry, above floor, below oldest_active: still
        // crash-recovery RolledBack.
        assert_eq!(
            clog.status_or_rolled_back(6, 10),
            Some(Status::RolledBack)
        );
        cleanup(&db);
    }

    #[test]
    fn truncate_below_is_idempotent_for_smaller_or_equal_floor() {
        let db = tmp_db();
        cleanup(&db);
        let clog = Clog::open(&db).unwrap();
        for i in 1..=10u64 {
            clog.record_commit(i).unwrap();
        }
        clog.truncate_below(5).unwrap();
        let size_after_first = clog_file_size(&db);

        // Same floor: no-op.
        clog.truncate_below(5).unwrap();
        assert_eq!(clog_file_size(&db), size_after_first);

        // Smaller floor: also a no-op (floor doesn't go backwards).
        clog.truncate_below(2).unwrap();
        assert_eq!(clog_file_size(&db), size_after_first);
        assert_eq!(clog.min_tx_id(), 5);
        cleanup(&db);
    }

    #[test]
    fn appends_after_truncate_land_at_the_end() {
        // The reopened file handle is sought to End; new appends
        // must go after the truncated records, not overwrite them.
        let db = tmp_db();
        cleanup(&db);
        let clog = Clog::open(&db).unwrap();
        for i in 1..=10u64 {
            clog.record_commit(i).unwrap();
        }
        clog.truncate_below(6).unwrap();
        // Append more.
        for i in 11..=20u64 {
            clog.record_commit(i).unwrap();
        }
        // File holds 5 kept (6..=10) + 10 new (11..=20) = 15 records.
        assert_eq!(
            clog_file_size(&db),
            HEADER_SIZE + 15 * RECORD_SIZE as u64
        );
        // Reopen: every appended record must be visible too.
        drop(clog);
        let clog = Clog::open(&db).unwrap();
        for i in 11..=20u64 {
            assert!(clog.is_committed(i));
        }
        cleanup(&db);
    }

    #[test]
    fn leftover_tmp_file_is_discarded_on_open() {
        // Simulate a crash mid-truncate: a `.tmp` file with garbage
        // sits beside the canonical clog. `open()` must delete it
        // and use the canonical file.
        let db = tmp_db();
        cleanup(&db);
        {
            let clog = Clog::open(&db).unwrap();
            for i in 1..=5u64 {
                clog.record_commit(i).unwrap();
            }
        }
        // Drop a bogus .tmp next to the clog — simulating a crash
        // partway through `truncate_below`'s write_and_fsync.
        let tmp = clog_tmp_path(&clog_path(&db));
        std::fs::write(&tmp, b"this would be a partial truncation image").unwrap();
        assert!(tmp.exists(), "test sanity: tmp file was created");

        // Reopen: tmp should be cleaned up; canonical clog still works.
        let clog = Clog::open(&db).unwrap();
        assert!(!tmp.exists(), "open() should have deleted the leftover .tmp");
        for i in 1..=5u64 {
            assert!(clog.is_committed(i));
        }
        cleanup(&db);
    }

    /// v0.57 stress: 8 threads committing while a 9th thread runs
    /// truncate_below repeatedly. Every record must end up correct,
    /// no record may be lost, and the in-memory map must agree with
    /// the on-disk file after the dust settles.
    #[test]
    fn truncate_is_safe_against_concurrent_commits() {
        use std::thread;
        let db = tmp_db();
        cleanup(&db);
        let clog = Clog::open(&db).unwrap();
        const WRITERS: u64 = 8;
        const PER_WRITER: u64 = 200;
        const TRUNCATIONS: u64 = 10;

        let mut handles = Vec::new();
        for t in 0..WRITERS {
            let clog = clog.clone();
            handles.push(thread::spawn(move || {
                for i in 0..PER_WRITER {
                    let id = t * PER_WRITER + i + 1;
                    clog.record_commit(id).expect("commit");
                }
            }));
        }
        // Truncator thread: occasionally truncate below a low floor
        // so we don't actually drop anything most workers will care
        // about, but we exercise the truncation path concurrently.
        let truncator = {
            let clog = clog.clone();
            thread::spawn(move || {
                for k in 0..TRUNCATIONS {
                    // Pick a floor that grows over time but stays
                    // below the highest committed id.
                    let floor = (k + 1) * 10;
                    clog.truncate_below(floor).expect("truncate");
                    thread::yield_now();
                }
            })
        };

        for h in handles {
            h.join().unwrap();
        }
        truncator.join().unwrap();

        // Final truncation pass to a known floor.
        clog.truncate_below(100).unwrap();
        // Every id >= 100 must be committed.
        for id in 100..=WRITERS * PER_WRITER {
            assert!(
                clog.is_committed(id),
                "id {id} should still be committed after truncate"
            );
        }
        // Every id < 100 reads as Committed by convention.
        for id in 1..100u64 {
            assert_eq!(
                clog.status(id),
                Some(Status::Committed),
                "below-floor id {id} should answer Committed"
            );
        }
        // The file size will be at MOST the full-history size — we
        // can't pin it exactly because the test breaks the production
        // invariant ("floor <= oldest_active_tx_id"): the truncator
        // racing the writers means a writer might commit a low ID
        // *after* a truncate, leaving that ID in the file below the
        // current floor. In production `truncate_clog` captures the
        // floor from `oldest_active_tx_id`, so no concurrent commit
        // can land below it. Here we only check that truncation made
        // progress (file is smaller than it would be untrucnated).
        let untruncated_size = HEADER_SIZE + WRITERS * PER_WRITER * RECORD_SIZE as u64;
        assert!(
            clog_file_size(&db) < untruncated_size,
            "truncation should have shrunk the file"
        );

        // Survives reopen.
        drop(clog);
        let clog = Clog::open(&db).unwrap();
        for id in 100..=WRITERS * PER_WRITER {
            assert!(clog.is_committed(id));
        }
        assert_eq!(clog.min_tx_id(), 100);
        cleanup(&db);
    }
}
