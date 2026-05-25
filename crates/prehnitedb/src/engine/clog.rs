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
    file: Arc<Mutex<File>>,
    /// Followers park here while a leader is flushing; the leader signals
    /// `notify_all()` once `durable_lsn` advances.
    flush_done: Arc<Condvar>,
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
}

impl Clog {
    /// Open or create the clog file at `<path>-clog`, reading every existing
    /// record into the in-memory map. The file's records are append-only;
    /// the in-memory map mirrors them.
    pub fn open(db_path: &Path) -> Result<Clog> {
        let path = clog_path(db_path);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        let mut map = HashMap::new();
        let mut buf = [0u8; RECORD_SIZE];
        file.seek(SeekFrom::Start(0))?;
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
            })),
            file: Arc::new(Mutex::new(file)),
            flush_done: Arc::new(Condvar::new()),
        })
    }

    /// The status of `tx_id`. `None` means "not in the clog" — either still
    /// in flight (held by `TxState`), pending an in-progress group-commit
    /// fsync, or never assigned.
    pub fn status(&self, tx_id: u64) -> Option<Status> {
        let state = self.state.lock().expect("poisoned clog");
        state.map.get(&tx_id).copied()
    }

    /// Bulk lookup: status of `tx_id` for snapshot visibility. A TX ID below
    /// `oldest_active` (the watermark of "everything has resolved") that has
    /// no clog entry is treated as **rolled back** — this is the crash
    /// recovery rule. Above the watermark, "not in clog" means "still in
    /// flight" and the caller (the snapshot) is expected to know which.
    pub fn status_or_rolled_back(&self, tx_id: u64, oldest_active: u64) -> Option<Status> {
        let state = self.state.lock().expect("poisoned clog");
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
        let mut file = self.file.lock().expect("poisoned clog (file)");
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
}
