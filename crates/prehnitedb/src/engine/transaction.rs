//! Transaction state shared across `Database` handles on one file.
//!
//! Every row in the storage layer carries two MVCC timestamps —
//! `tx_min` (the transaction that created it) and `tx_max` (the
//! transaction that logically deleted it, `0` if it is still live).
//! A reader takes a [`Snapshot`] at statement start, and every row a
//! scan returns is checked against that snapshot before being emitted.
//!
//! v0.26 tracks **multiple** in-flight write transactions at once.
//! `TxState.in_flight` is now a `HashSet<u64>`; a transaction is
//! reserved at BEGIN (or the first write of an auto-commit), added to
//! the set, and removed at COMMIT/ROLLBACK with the outcome appended
//! to the persistent commit log ([`crate::engine::clog::Clog`]). A
//! snapshot captures the *whole* in-flight set at its start, so the
//! reader stays consistent against every concurrent writer.
//!
//! Visibility now consults the clog: a row is visible only if its
//! `tx_min` is recorded as committed AND committed *before* the
//! snapshot (`tx_min < snapshot.next_tx` and `tx_min` not in
//! `snapshot.in_flight`). A row whose `tx_min` is rolled back —
//! either explicitly via ROLLBACK or implicitly by crash recovery —
//! is invisible to every snapshot.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use crate::engine::clog::Clog;
use crate::error::Result;
use crate::storage::SharedMeta;

/// The visibility frame for one read. Captured at statement start; threaded
/// through `executor::execute` and applied to every row a scan returns.
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// Smallest TX ID *not* visible in this snapshot. A row's `tx_min` must
    /// be strictly less than this — anything `>= next_tx` started after
    /// our snapshot and is invisible.
    pub next_tx: u64,
    /// Every write transaction that was in flight when this snapshot was
    /// captured. Rows stamped with any of these are not visible — they
    /// belong to writers that hadn't committed yet at snapshot time.
    pub in_flight: HashSet<u64>,
    /// The reader's own write TX, if it is itself a writer. Own writes are
    /// visible to the writer even though `own_tx` is in `in_flight` from
    /// every other reader's view.
    pub own_tx: Option<u64>,
    /// The clog handle, used to check whether `tx_min`/`tx_max` IDs are
    /// committed, rolled back, or still in flight. Cloned at snapshot time
    /// so the snapshot keeps reading the same authoritative state even as
    /// concurrent writers commit.
    pub clog: Clog,
}

impl Snapshot {
    /// Capture a snapshot from `next_tx`, the in-flight write transactions
    /// at this instant, an optional `own_tx`, and a handle to the clog.
    pub fn new(next_tx: u64, in_flight: HashSet<u64>, own_tx: Option<u64>, clog: Clog) -> Snapshot {
        Snapshot {
            next_tx,
            in_flight,
            own_tx,
            clog,
        }
    }

    /// Whether a row with the given `(tx_min, tx_max)` MVCC header is
    /// visible to this snapshot. The rule:
    ///
    /// - **Created visible**: `tx_min` is committed (per the clog) AND
    ///   `tx_min < next_tx` AND `tx_min` not in `in_flight`, OR
    ///   `tx_min == own_tx` (own writes are always visible to the writer,
    ///   even though the clog hasn't recorded them yet).
    /// - **Not deleted to this snapshot**: `tx_max == 0` (never deleted),
    ///   OR `tx_max` not yet visible (uncommitted or future-to-us), BUT
    ///   NOT if `tx_max == own_tx` (our own delete hides the row from us).
    pub fn visible(&self, tx_min: u64, tx_max: u64) -> bool {
        // Created — visible iff committed per clog and committed-before-us.
        let created = if Some(tx_min) == self.own_tx {
            true
        } else if tx_min == 0 {
            // tx_min == 0 is a placeholder used in spilled rows; treat as
            // committed-from-time-0 for the scope of those callers.
            true
        } else if !self.clog.is_committed(tx_min) {
            // Rolled back or in flight per the clog. In-flight is the case
            // that overlaps with our `in_flight` set; rolled-back rows are
            // invisible to every snapshot.
            false
        } else {
            tx_min < self.next_tx && !self.in_flight.contains(&tx_min)
        };
        if !created {
            return false;
        }
        // Not deleted to this snapshot.
        if tx_max == 0 {
            return true;
        }
        if Some(tx_max) == self.own_tx {
            return false;
        }
        if !self.clog.is_committed(tx_max) {
            // The delete isn't committed yet — the row is still alive
            // from our point of view.
            return true;
        }
        // `tx_max` is committed per clog. Is it visible to our snapshot?
        // If yes, the delete applies and the row is gone.
        tx_max >= self.next_tx || self.in_flight.contains(&tx_max)
    }
}

/// Process-wide transaction coordinator. Holds the next unused TX ID, the
/// set of in-flight write transactions, the persistent commit log, the
/// shared database header (`SharedMeta`), and the runtime mutexes that
/// serialise writes:
///
/// - One **per-table** mutex per table name. Two writers touching
///   different tables run truly in parallel; two writers touching the
///   same table serialise. The map grows lazily on first lookup and
///   never shrinks.
/// - One **catalog** mutex for `CREATE TABLE` / `DROP TABLE` / catalog
///   reads — schema changes serialise against each other but not
///   against per-table data writes.
/// - One **commit** mutex held across `pager.commit()`'s WAL seal,
///   apply, and reset window so two writers' commits do not interleave
///   their physical I/O.
///
/// Shared by `Arc` across every `Database` open on one file.
#[derive(Clone)]
pub struct TxState {
    inner: Arc<Mutex<TxStateInner>>,
    /// The persistent commit log. Cloned into every snapshot for visibility
    /// checks.
    clog: Clog,
    /// The database header, shared across every pager on this file so
    /// allocations stay coherent across concurrent writers.
    shared_meta: SharedMeta,
    /// Per-table write mutexes, indexed by table name. Created lazily.
    table_locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
    /// One mutex for catalog mutations (CREATE/DROP TABLE/INDEX).
    catalog_lock: Arc<Mutex<()>>,
    /// One mutex held across the WAL seal+apply+reset window so two
    /// writers' commits don't tangle their physical I/O.
    commit_lock: Arc<Mutex<()>>,
}

struct TxStateInner {
    next_tx_id: u64,
    in_flight: HashSet<u64>,
}

impl TxState {
    /// A new coordinator initialised from `persisted_next_tx_id` — the value
    /// the pager last wrote to the database header — the open clog, and the
    /// shared meta the bootstrap pager created. Any TX ID
    /// `< persisted_next_tx_id` not in the clog is considered rolled
    /// back (crash-recovery rule).
    pub fn new(persisted_next_tx_id: u64, clog: Clog, shared_meta: SharedMeta) -> TxState {
        TxState {
            inner: Arc::new(Mutex::new(TxStateInner {
                next_tx_id: persisted_next_tx_id.max(1),
                in_flight: HashSet::new(),
            })),
            clog,
            shared_meta,
            table_locks: Arc::new(Mutex::new(HashMap::new())),
            catalog_lock: Arc::new(Mutex::new(())),
            commit_lock: Arc::new(Mutex::new(())),
        }
    }

    /// The shared header, cloned for peer pagers on the same file.
    pub fn shared_meta(&self) -> SharedMeta {
        self.shared_meta.clone()
    }

    /// The mutex for `table`, created on first request. Returned as an
    /// `Arc<Mutex<()>>` so the caller can `lock()` it for the duration
    /// of one write statement against the table.
    pub fn table_lock(&self, table: &str) -> Arc<Mutex<()>> {
        let mut locks = self.table_locks.lock().expect("poisoned table-locks map");
        Arc::clone(
            locks
                .entry(table.to_string())
                .or_insert_with(|| Arc::new(Mutex::new(()))),
        )
    }

    /// The catalog mutex — taken for `CREATE TABLE` / `DROP TABLE` and
    /// any other catalog mutation. Read-only catalog lookups (e.g.,
    /// planning a query) do not need this; they read through the
    /// pager's snapshot.
    pub fn catalog_lock(&self) -> Arc<Mutex<()>> {
        Arc::clone(&self.catalog_lock)
    }

    /// The commit mutex — held across the WAL seal+apply+reset window
    /// so two writers' commits don't interleave their physical I/O.
    pub fn commit_lock(&self) -> Arc<Mutex<()>> {
        Arc::clone(&self.commit_lock)
    }

    /// Capture a snapshot for a read statement. `own_tx` is the writer's own
    /// TX when the snapshot is taken inside a write statement — otherwise
    /// `None`. The snapshot captures the *whole* in-flight set at this
    /// instant, so readers stay consistent against every concurrent writer.
    pub fn snapshot(&self, own_tx: Option<u64>) -> Snapshot {
        let inner = self.inner.lock().expect("poisoned tx state");
        Snapshot::new(
            inner.next_tx_id,
            inner.in_flight.clone(),
            own_tx,
            self.clog.clone(),
        )
    }

    /// Reserve a TX ID for a new write transaction and mark it in-flight.
    pub fn begin_write(&self) -> u64 {
        let mut inner = self.inner.lock().expect("poisoned tx state");
        let id = inner.next_tx_id;
        inner.next_tx_id += 1;
        inner.in_flight.insert(id);
        id
    }

    /// Mark `tx_id` as committed: record in the clog and remove from
    /// in-flight. The clog write fsyncs, making the commit durable.
    pub fn commit_write(&self, tx_id: u64) -> Result<()> {
        self.clog.record_commit(tx_id)?;
        let mut inner = self.inner.lock().expect("poisoned tx state");
        inner.in_flight.remove(&tx_id);
        Ok(())
    }

    /// Mark `tx_id` as rolled back: record in the clog and remove from
    /// in-flight. Rows the writer stamped with this ID stay in the file
    /// but are now invisible to every snapshot.
    pub fn rollback_write(&self, tx_id: u64) -> Result<()> {
        self.clog.record_rollback(tx_id)?;
        let mut inner = self.inner.lock().expect("poisoned tx state");
        inner.in_flight.remove(&tx_id);
        Ok(())
    }

    /// The current next-TX value — used by `Database` to keep its pager
    /// metadata in step at commit time.
    pub fn next_tx_id(&self) -> u64 {
        self.inner.lock().expect("poisoned tx state").next_tx_id
    }

    /// Snapshot the in-flight set without taking a full [`Snapshot`].
    /// Diagnostic only.
    pub fn in_flight_count(&self) -> usize {
        self.inner
            .lock()
            .expect("poisoned tx state")
            .in_flight
            .len()
    }

    /// Direct access to the clog — used by the rest of the engine for
    /// status queries that don't need a full snapshot, and by VACUUM to
    /// reclaim rows whose `tx_min` is rolled back.
    pub fn clog(&self) -> Clog {
        self.clog.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::clog::clog_path;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    /// A scratch clog with the given TXs marked committed. Cleans up on drop.
    struct ScratchClog {
        path: PathBuf,
        clog: Clog,
    }

    impl ScratchClog {
        fn new(committed: &[u64]) -> ScratchClog {
            let n = COUNTER.fetch_add(1, Ordering::SeqCst);
            let path =
                std::env::temp_dir().join(format!("prehnite-tx-{}-{n}.db", std::process::id()));
            let _ = std::fs::remove_file(clog_path(&path));
            let clog = Clog::open(&path).unwrap();
            for &id in committed {
                clog.record_commit(id).unwrap();
            }
            ScratchClog { path, clog }
        }
    }

    impl Drop for ScratchClog {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(clog_path(&self.path));
        }
    }

    fn snap(
        next_tx: u64,
        in_flight: &[u64],
        own_tx: Option<u64>,
        committed: &[u64],
    ) -> (ScratchClog, Snapshot) {
        let scratch = ScratchClog::new(committed);
        let snapshot = Snapshot::new(
            next_tx,
            in_flight.iter().copied().collect(),
            own_tx,
            scratch.clog.clone(),
        );
        (scratch, snapshot)
    }

    #[test]
    fn rows_with_tx_min_at_or_above_next_tx_are_invisible() {
        let (_clog, snap) = snap(10, &[], None, &[3, 5, 9, 10, 11]);
        assert!(snap.visible(5, 0));
        assert!(snap.visible(9, 0));
        assert!(!snap.visible(10, 0));
        assert!(!snap.visible(11, 0));
    }

    #[test]
    fn in_flight_tx_is_invisible_to_other_readers() {
        // TX 15 was in flight at snapshot time, TX 14 was already committed.
        let (_clog, snap) = snap(20, &[15], None, &[14, 15]);
        assert!(snap.visible(14, 0));
        assert!(!snap.visible(15, 0));
    }

    #[test]
    fn own_writes_are_visible_to_self_via_override() {
        // Writer's TX 7 — clog doesn't have it yet (in-flight from
        // everyone else's view), but own_tx admits it for self.
        let (_clog, snap) = snap(8, &[7], Some(7), &[]);
        assert!(snap.visible(7, 0));
    }

    #[test]
    fn rolled_back_rows_are_invisible_even_to_their_own_descendants() {
        // TX 5 was rolled back. Any row stamped with tx_min=5 stays in
        // the file but is gone from every snapshot.
        let scratch = ScratchClog::new(&[]);
        scratch.clog.record_rollback(5).unwrap();
        let snap = Snapshot::new(10, HashSet::new(), None, scratch.clog.clone());
        assert!(!snap.visible(5, 0));
    }

    #[test]
    fn rows_deleted_by_an_older_committed_tx_are_invisible() {
        let (_clog, snap) = snap(10, &[], None, &[3, 7]);
        assert!(!snap.visible(3, 7));
    }

    #[test]
    fn rows_deleted_by_a_future_tx_are_still_visible() {
        let (_clog, snap) = snap(10, &[], None, &[3, 12]);
        assert!(snap.visible(3, 12));
    }

    #[test]
    fn own_deletes_hide_rows_from_self() {
        let (_clog, snap) = snap(8, &[7], Some(7), &[3]);
        assert!(!snap.visible(3, 7));
    }
}
