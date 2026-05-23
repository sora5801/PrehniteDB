//! Transaction state shared across `Database` handles on one file.
//!
//! Every row in the storage layer carries two MVCC timestamps —
//! `tx_min` (the transaction that created it) and `tx_max` (the
//! transaction that logically deleted it, `0` if it is still live).
//! A reader takes a [`Snapshot`] at statement start, and every row a
//! scan returns is checked against that snapshot before being emitted.
//!
//! The transaction counter and the single in-flight write transaction
//! (PrehniteDB v0.25 is still single-writer) live in [`TxState`], a
//! handle that `Clone`s by `Arc` so every `Database` open on one file
//! sees the same authoritative state. The server creates one `TxState`
//! at startup and hands a clone to each connection; embedded users get
//! a private one inside `Database::open`.

use std::sync::{Arc, Mutex};

/// The visibility frame for one read. Captured at statement start; threaded
/// through `executor::execute` and applied to every row a scan returns.
#[derive(Debug, Clone, Copy)]
pub struct Snapshot {
    /// Smallest TX ID *not* visible in this snapshot. A row is visible
    /// only if its `tx_min` is strictly less than this value (and not the
    /// in-flight writer's ID, see `in_flight`).
    pub next_tx: u64,
    /// The single in-flight write transaction, if any, as seen at snapshot
    /// time. With single-writer concurrency there is at most one. Rows
    /// stamped with this ID are not visible to the snapshot (the writer
    /// has not committed yet).
    pub in_flight: Option<u64>,
    /// The reader's own write TX, if it is a writer-statement or runs
    /// inside an explicit BEGIN..COMMIT that has done writes. Own writes
    /// are visible to the writer even though their `tx_min` equals the
    /// reader's `in_flight`-equivalent — the visibility check has an
    /// override.
    pub own_tx: Option<u64>,
}

impl Snapshot {
    /// A snapshot that admits every committed row up to (but not including)
    /// `next_tx`, treating `in_flight` (if any) as uncommitted. `own_tx`
    /// rows are admitted via the override even if they look in-flight.
    pub fn new(next_tx: u64, in_flight: Option<u64>, own_tx: Option<u64>) -> Snapshot {
        Snapshot {
            next_tx,
            in_flight,
            own_tx,
        }
    }

    /// Whether a row with the given `(tx_min, tx_max)` MVCC header is
    /// visible to this snapshot. The rule:
    ///
    /// - **Created visible**: `tx_min < next_tx` and `tx_min != in_flight`,
    ///   OR `tx_min == own_tx` (own writes are always visible to the
    ///   writer).
    /// - **Not deleted to this snapshot**: `tx_max == 0` (never deleted),
    ///   OR `tx_max >= next_tx` (the delete is future-to-us), OR
    ///   `tx_max == in_flight` (the delete is uncommitted), BUT NOT if
    ///   `tx_max == own_tx` (our own delete hides the row from us).
    pub fn visible(&self, tx_min: u64, tx_max: u64) -> bool {
        let created = (tx_min < self.next_tx && Some(tx_min) != self.in_flight)
            || Some(tx_min) == self.own_tx;
        if !created {
            return false;
        }
        if tx_max == 0 {
            return true;
        }
        if Some(tx_max) == self.own_tx {
            return false;
        }
        tx_max >= self.next_tx || Some(tx_max) == self.in_flight
    }
}

/// Process-wide transaction coordinator. Holds the next unused TX ID and
/// the single in-flight write transaction (if any), shared by `Arc` across
/// every `Database` open on one file.
#[derive(Clone)]
pub struct TxState {
    inner: Arc<Mutex<TxStateInner>>,
}

struct TxStateInner {
    next_tx_id: u64,
    in_flight: Option<u64>,
}

impl TxState {
    /// A new coordinator initialised from `persisted_next_tx_id` — the
    /// value the pager last wrote to the database header. Subsequent
    /// `begin_write`/`commit_write` calls take it from here.
    pub fn new(persisted_next_tx_id: u64) -> TxState {
        TxState {
            inner: Arc::new(Mutex::new(TxStateInner {
                next_tx_id: persisted_next_tx_id.max(1),
                in_flight: None,
            })),
        }
    }

    /// Capture a snapshot for a read statement. `own_tx` is the writer's
    /// own TX when the snapshot is taken inside a write statement —
    /// otherwise `None`.
    pub fn snapshot(&self, own_tx: Option<u64>) -> Snapshot {
        let inner = self.inner.lock().expect("poisoned tx state");
        Snapshot::new(inner.next_tx_id, inner.in_flight, own_tx)
    }

    /// Reserve a TX ID for a new write transaction and mark it in-flight.
    /// The reserved ID becomes the writer's `own_tx` for the duration of
    /// the transaction.
    pub fn begin_write(&self) -> u64 {
        let mut inner = self.inner.lock().expect("poisoned tx state");
        let id = inner.next_tx_id;
        inner.next_tx_id += 1;
        inner.in_flight = Some(id);
        id
    }

    /// End the in-flight write transaction. Called on COMMIT and ROLLBACK
    /// alike — the slot opens up either way. (On rollback the reserved ID
    /// is "wasted": no row in the file carries it, so concurrent readers
    /// will just see a gap.)
    pub fn end_write(&self) {
        let mut inner = self.inner.lock().expect("poisoned tx state");
        inner.in_flight = None;
    }

    /// The current next-TX value — used by `Database` to keep its pager
    /// metadata in step at commit time.
    pub fn next_tx_id(&self) -> u64 {
        self.inner.lock().expect("poisoned tx state").next_tx_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_with_tx_min_at_or_above_next_tx_are_invisible() {
        let snap = Snapshot::new(10, None, None);
        assert!(snap.visible(5, 0));
        assert!(snap.visible(9, 0));
        assert!(!snap.visible(10, 0));
        assert!(!snap.visible(11, 0));
    }

    #[test]
    fn in_flight_tx_is_invisible_to_other_readers() {
        let snap = Snapshot::new(20, Some(15), None);
        // 14 committed before in-flight TX began: visible.
        assert!(snap.visible(14, 0));
        // 15 is the in-flight TX: invisible, even though 15 < 20.
        assert!(!snap.visible(15, 0));
    }

    #[test]
    fn own_writes_are_visible_to_self_via_override() {
        // The writer's own TX is in-flight (tx == 7) and own_tx == 7. The
        // writer sees its own inserts.
        let snap = Snapshot::new(8, Some(7), Some(7));
        assert!(snap.visible(7, 0));
    }

    #[test]
    fn rows_deleted_by_an_older_tx_are_invisible() {
        let snap = Snapshot::new(10, None, None);
        // Created by TX 3, deleted by TX 7 — both committed: gone.
        assert!(!snap.visible(3, 7));
    }

    #[test]
    fn rows_deleted_by_a_future_tx_are_still_visible() {
        let snap = Snapshot::new(10, None, None);
        // Created by TX 3, deleted by TX 12 — the delete is "future".
        assert!(snap.visible(3, 12));
    }

    #[test]
    fn own_deletes_hide_rows_from_self() {
        // Writer's TX 7 deletes a row that existed already.
        let snap = Snapshot::new(8, Some(7), Some(7));
        assert!(!snap.visible(3, 7));
    }
}
