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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use crate::engine::clog::Clog;
use crate::error::{Error, Result};
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
    /// SSI bookkeeping shared with `TxState`. The executor calls
    /// [`Snapshot::record_read`] for every row it observes; if the
    /// snapshot has an `own_tx`, that read is added to the TX's
    /// read-set, and any rw-edge to an in-flight tombstoning peer is
    /// marked here too. `Arc` is cheap to clone with the snapshot.
    pub(crate) ssi: Arc<Mutex<HashMap<u64, SsiTxState>>>,
    /// Per-table atomic rowid counters, shared with `TxState`. The
    /// executor's INSERT/UPDATE path calls
    /// [`Snapshot::reserve_rowid`] instead of bumping the local
    /// schema's `next_rowid` directly — two writers on the same table
    /// must not get the same rowid back.
    pub(crate) rowid_counters: Arc<Mutex<HashMap<String, Arc<AtomicU64>>>>,
}

impl Snapshot {
    /// Capture a snapshot from `next_tx`, the in-flight write transactions
    /// at this instant, an optional `own_tx`, a handle to the clog, the
    /// shared SSI map, and the per-table rowid counters.
    pub(crate) fn new(
        next_tx: u64,
        in_flight: HashSet<u64>,
        own_tx: Option<u64>,
        clog: Clog,
        ssi: Arc<Mutex<HashMap<u64, SsiTxState>>>,
        rowid_counters: Arc<Mutex<HashMap<String, Arc<AtomicU64>>>>,
    ) -> Snapshot {
        Snapshot {
            next_tx,
            in_flight,
            own_tx,
            clog,
            ssi,
            rowid_counters,
        }
    }

    /// Reserve a fresh, unique rowid for `table`. `schema_next_rowid`
    /// is the floor from the persisted schema (used to initialise the
    /// counter the first time and to catch up to a peer's recent
    /// commit). Two concurrent writers each calling this get distinct
    /// rowids — the atomic `fetch_add` makes the bump atomic.
    pub fn reserve_rowid(&self, table: &str, schema_next_rowid: u64) -> u64 {
        let counter = {
            let mut map = self.rowid_counters.lock().expect("poisoned rowid counters");
            Arc::clone(
                map.entry(table.to_string())
                    .or_insert_with(|| Arc::new(AtomicU64::new(schema_next_rowid))),
            )
        };
        counter.fetch_max(schema_next_rowid, Ordering::SeqCst);
        counter.fetch_add(1, Ordering::SeqCst)
    }

    /// The next rowid the counter would hand out. Used when writing
    /// the schema back to the catalog so the persisted `next_rowid`
    /// reflects the latest reservation.
    pub fn current_next_rowid(&self, table: &str, schema_next_rowid: u64) -> u64 {
        let counter = {
            let mut map = self.rowid_counters.lock().expect("poisoned rowid counters");
            Arc::clone(
                map.entry(table.to_string())
                    .or_insert_with(|| Arc::new(AtomicU64::new(schema_next_rowid))),
            )
        };
        counter.fetch_max(schema_next_rowid, Ordering::SeqCst);
        counter.load(Ordering::SeqCst)
    }

    /// Record that this snapshot's `own_tx` (if any) has just observed
    /// the tuple at `(table_root, rowid_key)`, whose version's `tx_max`
    /// is `tombstone_by` (`None` if the version is live, `Some(peer)`
    /// if a peer writer is mid-tombstone).
    ///
    /// Adds a `Tuple` entry to `own_tx`'s SSI read-set, and marks an
    /// rw-edge `own_tx → peer` if `peer` is an in-flight writer.
    /// Index scans use this — they emit a bounded set of rows so a
    /// tuple-level lock is correct *and* cheap.
    ///
    /// A no-op when `own_tx` is `None` — autocommit reads don't need
    /// tracking because their transaction is over the moment the
    /// statement returns.
    pub fn record_read(&self, table_root: u32, rowid_key: &[u8], tombstone_by: Option<u64>) {
        let Some(tx) = self.own_tx else {
            return;
        };
        let mut ssi = self.ssi.lock().expect("poisoned ssi");
        if let Some(state) = ssi.get_mut(&tx) {
            state
                .read_set
                .insert(ReadLock::Tuple(table_root, rowid_key.to_vec()));
        }
        if let Some(peer) = tombstone_by {
            if peer != tx && ssi.contains_key(&peer) {
                if let Some(s) = ssi.get_mut(&tx) {
                    s.out_conflict = true;
                }
                if let Some(s) = ssi.get_mut(&peer) {
                    s.in_conflict = true;
                }
            }
        }
    }

    /// Record that this snapshot's `own_tx` has performed a *full
    /// table scan* over `table_root`. Adds a `Relation` entry to the
    /// read set — one lock for the whole table, regardless of how
    /// many tuples were emitted. The lock is what catches phantom
    /// inserts: a concurrent INSERT into the table marks an rw-edge
    /// even though the new row was never in our `read_set` (it
    /// didn't exist yet).
    pub fn record_relation_read(&self, table_root: u32) {
        let Some(tx) = self.own_tx else {
            return;
        };
        let mut ssi = self.ssi.lock().expect("poisoned ssi");
        if let Some(state) = ssi.get_mut(&tx) {
            state.read_set.insert(ReadLock::Relation(table_root));
        }
    }

    /// Record that this snapshot's `own_tx` is writing (tombstoning)
    /// the tuple at `(table_root, rowid_key)`. Walks every in-flight
    /// peer's SSI read-set; for each match — either a specific
    /// `Tuple` entry for this rowid *or* a `Relation` entry for the
    /// whole table — marks the rw-edge `peer → own_tx`.
    ///
    /// A no-op when `own_tx` is `None` (impossible in practice — writes
    /// always have a TX — but defensively).
    pub fn record_write(&self, table_root: u32, rowid_key: &[u8]) {
        let Some(writer_tx) = self.own_tx else {
            return;
        };
        let mut ssi = self.ssi.lock().expect("poisoned ssi");
        let tuple_lock = ReadLock::Tuple(table_root, rowid_key.to_vec());
        let relation_lock = ReadLock::Relation(table_root);
        let readers: Vec<u64> = ssi
            .iter()
            .filter(|(&t, _)| t != writer_tx)
            .filter(|(_, s)| {
                s.read_set.contains(&tuple_lock) || s.read_set.contains(&relation_lock)
            })
            .map(|(&t, _)| t)
            .collect();
        if readers.is_empty() {
            return;
        }
        if let Some(s) = ssi.get_mut(&writer_tx) {
            s.in_conflict = true;
        }
        for peer in readers {
            if let Some(s) = ssi.get_mut(&peer) {
                s.out_conflict = true;
            }
        }
    }

    /// Record that this snapshot's `own_tx` is **inserting** a new
    /// row into `table_root` — the phantom-insert case. The new row
    /// has a fresh rowid no peer's read set can name; predicate
    /// detection happens at the relation level. Walks peers' read
    /// sets for `Relation(table_root)` entries and marks
    /// `peer → own_tx` edges (peer would have seen this row had it
    /// scanned after our insert).
    pub fn record_insert(&self, table_root: u32) {
        let Some(writer_tx) = self.own_tx else {
            return;
        };
        let mut ssi = self.ssi.lock().expect("poisoned ssi");
        let relation_lock = ReadLock::Relation(table_root);
        let readers: Vec<u64> = ssi
            .iter()
            .filter(|(&t, _)| t != writer_tx)
            .filter(|(_, s)| s.read_set.contains(&relation_lock))
            .map(|(&t, _)| t)
            .collect();
        if readers.is_empty() {
            return;
        }
        if let Some(s) = ssi.get_mut(&writer_tx) {
            s.in_conflict = true;
        }
        for peer in readers {
            if let Some(s) = ssi.get_mut(&peer) {
                s.out_conflict = true;
            }
        }
    }

    /// v0.59: record that this snapshot's `own_tx` scanned `[lower,
    /// upper)` of the index rooted at `index_root`. Adds an
    /// [`ReadLock::IndexRange`] entry — coarser than the per-tuple
    /// `Tuple` lock the scan would also produce, but the only way
    /// to catch a phantom INSERT into the range. (Through v0.58
    /// index scans only recorded `Tuple` locks for the rows they
    /// returned, so a peer INSERT into the scanned range went
    /// undetected.)
    ///
    /// The `upper` bound is `None` for open-ended scans (e.g. `WHERE
    /// x >= 5` with no upper). A no-op when `own_tx` is `None`
    /// (autocommit reads need no SSI tracking — the read is over by
    /// the time the statement returns).
    pub fn record_index_range_read(
        &self,
        index_root: u32,
        lower: &[u8],
        upper: Option<&[u8]>,
    ) {
        let Some(tx) = self.own_tx else {
            return;
        };
        let mut ssi = self.ssi.lock().expect("poisoned ssi");
        if let Some(state) = ssi.get_mut(&tx) {
            state.read_set.insert(ReadLock::IndexRange {
                index_root,
                lower: lower.to_vec(),
                upper: upper.map(|u| u.to_vec()),
            });
        }
    }

    /// v0.59: record that this snapshot's `own_tx` is writing
    /// `encoded_key` into the index rooted at `index_root` — could
    /// be an INSERT, DELETE, or the new-side of an UPDATE that
    /// changes an indexed column. Walks every in-flight peer's
    /// read-set for [`ReadLock::IndexRange`] entries whose
    /// `index_root` matches AND whose `[lower, upper)` contains
    /// `encoded_key`; for each match, marks the rw-edge
    /// `peer → own_tx`.
    ///
    /// Range membership is byte-lexicographic: `lower <=
    /// encoded_key < upper` (with `upper = None` meaning
    /// open-ended).
    pub fn record_index_write(&self, index_root: u32, encoded_key: &[u8]) {
        let Some(writer_tx) = self.own_tx else {
            return;
        };
        let mut ssi = self.ssi.lock().expect("poisoned ssi");
        let readers: Vec<u64> = ssi
            .iter()
            .filter(|(&t, _)| t != writer_tx)
            .filter(|(_, s)| {
                s.read_set.iter().any(|lock| match lock {
                    ReadLock::IndexRange {
                        index_root: r,
                        lower,
                        upper,
                    } => {
                        *r == index_root
                            && encoded_key >= lower.as_slice()
                            && upper
                                .as_deref()
                                .map(|u| encoded_key < u)
                                .unwrap_or(true)
                    }
                    _ => false,
                })
            })
            .map(|(&t, _)| t)
            .collect();
        if readers.is_empty() {
            return;
        }
        if let Some(s) = ssi.get_mut(&writer_tx) {
            s.in_conflict = true;
        }
        for peer in readers {
            if let Some(s) = ssi.get_mut(&peer) {
                s.out_conflict = true;
            }
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

/// A single read lock entry in a transaction's SSI read set. v0.35
/// adds the [`ReadLock::Relation`] variant so a full table scan can
/// claim the whole relation in one entry rather than recording every
/// tuple it visited — bounded memory, and the new lock is also what
/// catches **phantom inserts**: a concurrent INSERT into a relation
/// we hold an `Relation` lock on marks an rw-edge from us to the
/// inserter, even though the new row was never in our `read_set` (it
/// didn't exist).
///
/// v0.59 adds the [`ReadLock::IndexRange`] variant for index range
/// scans. Through v0.58 an index range scan only recorded `Tuple`
/// locks for the rows it actually returned — so a phantom INSERT
/// into the scanned key range slipped through SSI's rw-conflict
/// graph and a write-skew anomaly survived. The `IndexRange` lock
/// claims the entire `[lower, upper)` byte range over an index's
/// encoded keys: any concurrent insert/update/delete that touches
/// a key in that range marks an rw-edge, exactly the way the
/// `Relation` lock does for full table scans.
///
/// Why a range instead of leaf-page pgnos: page splits would move
/// keys to new pages, breaking page-pgno locks without expensive
/// split-time lock propagation. Byte-lex ranges are independent of
/// physical tree shape and survive splits trivially.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub(crate) enum ReadLock {
    /// Specific tuple this transaction observed — keyed by the
    /// table's B+tree root and the rowid bytes. Produced by index
    /// scans, where the read set is already minimal.
    Tuple(u32, Vec<u8>),
    /// Whole relation this transaction observed — table B+tree root.
    /// Produced by a full table scan (`TableScan`), which would
    /// otherwise add every visible tuple. Catches phantoms.
    Relation(u32),
    /// v0.59: a half-open byte range over an index's encoded keys.
    /// `index_root` identifies the index B+tree; `lower` is inclusive
    /// and `upper` is exclusive (with `None` meaning open-ended). A
    /// concurrent write that touches a key in `[lower, upper)` on
    /// this index marks an rw-edge with the holder — catches phantom
    /// INSERTs into the scanned range.
    IndexRange {
        index_root: u32,
        lower: Vec<u8>,
        upper: Option<Vec<u8>>,
    },
}

/// One transaction's SSI bookkeeping — the read-set it has accumulated
/// since `BEGIN` and the two-bit "in/out conflict" Cahill flags. Lives
/// in `TxState`'s `ssi` map, keyed by the writer's TX ID; created on
/// first writing statement (or first read inside a `BEGIN..COMMIT`),
/// dropped at commit or rollback.
///
/// The dangerous-structure detection is the simplification Postgres
/// adopted: a transaction whose commit would close a cycle of rw-edges
/// in the precedence graph is detected by checking, at its commit, for
/// `in_conflict && out_conflict` — i.e. at least one peer read what we
/// wrote *and* we read what at least one peer wrote. Such a transaction
/// is the "pivot" of a dangerous structure and must abort to break the
/// cycle.
#[derive(Debug, Default)]
pub(crate) struct SsiTxState {
    /// Locks this transaction has acquired by reading. v0.35 supports
    /// both tuple-level (index scans) and relation-level (full table
    /// scans) entries.
    read_set: HashSet<ReadLock>,
    /// Some peer read a tuple we then wrote — we are the "from" side of
    /// at least one rw-edge.
    out_conflict: bool,
    /// We read a tuple a peer is concurrently writing — we are the "to"
    /// side of at least one rw-edge.
    in_conflict: bool,
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
    /// Per-table write locks, indexed by table name. Created lazily.
    /// v0.30 changes these from `Mutex` to `RwLock`: INSERT/UPDATE/DELETE
    /// take the shared (read) side so two writers on the same table can
    /// proceed in parallel — the B+tree's per-page latches serialise
    /// them at page granularity. Schema-changing operations like
    /// CREATE/DROP INDEX take the exclusive (write) side.
    table_locks: Arc<Mutex<HashMap<String, Arc<RwLock<()>>>>>,
    /// One mutex for catalog mutations (CREATE/DROP TABLE/INDEX).
    catalog_lock: Arc<Mutex<()>>,
    /// One mutex held across the WAL seal+apply+reset window so two
    /// writers' commits don't tangle their physical I/O.
    commit_lock: Arc<Mutex<()>>,
    /// SSI bookkeeping per in-flight write transaction. Keyed by TX ID;
    /// inserted when a writer takes its TX (`begin_write_ssi`), updated
    /// as the writer reads and writes, drained at commit / rollback.
    /// Wrapped in its own `Arc<Mutex<>>` so the rest of the engine can
    /// touch SSI state without holding the outer `inner` lock.
    ssi: Arc<Mutex<HashMap<u64, SsiTxState>>>,
    /// Per-table atomic rowid counters — the runtime source of truth
    /// for `next_rowid`, separate from the persisted schema field.
    /// Two writers inserting into the same table call
    /// [`TxState::next_rowid`] to atomically reserve a unique rowid;
    /// without it, both would read the same `schema.next_rowid` from
    /// their local schema copies and collide on the resulting rowid.
    /// Initialised lazily from the schema's persisted `next_rowid` on
    /// first request.
    rowid_counters: Arc<Mutex<HashMap<String, Arc<AtomicU64>>>>,
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
            ssi: Arc::new(Mutex::new(HashMap::new())),
            rowid_counters: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Atomically reserve a fresh rowid for `table`. `schema_next_rowid`
    /// is the value the schema currently advertises on disk — used as
    /// a floor in case this is the first call (initialises the counter)
    /// or in case the persisted value has been bumped by a peer's
    /// commit that this writer hasn't seen yet (`fetch_max` raises the
    /// counter, then `fetch_add` returns a unique value at or above).
    pub fn next_rowid(&self, table: &str, schema_next_rowid: u64) -> u64 {
        let counter = {
            let mut map = self.rowid_counters.lock().expect("poisoned rowid counters");
            Arc::clone(
                map.entry(table.to_string())
                    .or_insert_with(|| Arc::new(AtomicU64::new(schema_next_rowid))),
            )
        };
        counter.fetch_max(schema_next_rowid, Ordering::SeqCst);
        counter.fetch_add(1, Ordering::SeqCst)
    }

    /// The current value the rowid counter for `table` would hand out
    /// next — used at the end of an INSERT/UPDATE statement to set
    /// `schema.next_rowid` for the `catalog.put` that persists the
    /// counter. `schema_next_rowid` is the persisted floor (in case
    /// nothing has been reserved yet in this process).
    pub fn current_next_rowid(&self, table: &str, schema_next_rowid: u64) -> u64 {
        let counter = {
            let mut map = self.rowid_counters.lock().expect("poisoned rowid counters");
            Arc::clone(
                map.entry(table.to_string())
                    .or_insert_with(|| Arc::new(AtomicU64::new(schema_next_rowid))),
            )
        };
        counter.fetch_max(schema_next_rowid, Ordering::SeqCst);
        counter.load(Ordering::SeqCst)
    }

    /// The shared header, cloned for peer pagers on the same file.
    pub fn shared_meta(&self) -> SharedMeta {
        self.shared_meta.clone()
    }

    /// The RwLock for `table`, created on first request. Callers take
    /// `.read()` for INSERT/UPDATE/DELETE (which serialise at the
    /// page-latch level inside the B+tree) and `.write()` for schema
    /// changes that need whole-table exclusion (CREATE INDEX/DROP
    /// INDEX).
    pub fn table_lock(&self, table: &str) -> Arc<RwLock<()>> {
        let mut locks = self.table_locks.lock().expect("poisoned table-locks map");
        Arc::clone(
            locks
                .entry(table.to_string())
                .or_insert_with(|| Arc::new(RwLock::new(()))),
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
            Arc::clone(&self.ssi),
            Arc::clone(&self.rowid_counters),
        )
    }

    /// Reserve a TX ID for a new write transaction and mark it in-flight.
    /// Also opens an empty SSI bookkeeping slot under the new ID.
    pub fn begin_write(&self) -> u64 {
        let mut inner = self.inner.lock().expect("poisoned tx state");
        let id = inner.next_tx_id;
        inner.next_tx_id += 1;
        inner.in_flight.insert(id);
        drop(inner);
        self.ssi
            .lock()
            .expect("poisoned ssi")
            .insert(id, SsiTxState::default());
        id
    }

    /// Mark `tx_id` as committed: record in the clog and remove from
    /// in-flight. The clog write fsyncs, making the commit durable.
    /// Also drops the SSI bookkeeping for this TX.
    pub fn commit_write(&self, tx_id: u64) -> Result<()> {
        self.clog.record_commit(tx_id)?;
        let mut inner = self.inner.lock().expect("poisoned tx state");
        inner.in_flight.remove(&tx_id);
        drop(inner);
        self.ssi.lock().expect("poisoned ssi").remove(&tx_id);
        Ok(())
    }

    /// Mark `tx_id` as rolled back: record in the clog and remove from
    /// in-flight. Rows the writer stamped with this ID stay in the file
    /// but are now invisible to every snapshot. Also drops the SSI
    /// bookkeeping.
    pub fn rollback_write(&self, tx_id: u64) -> Result<()> {
        self.clog.record_rollback(tx_id)?;
        let mut inner = self.inner.lock().expect("poisoned tx state");
        inner.in_flight.remove(&tx_id);
        drop(inner);
        self.ssi.lock().expect("poisoned ssi").remove(&tx_id);
        Ok(())
    }

    /// Commit-time SSI check. Returns `Err(Serialization)` if `tx` is
    /// the pivot of a dangerous structure (`in_conflict && out_conflict`),
    /// `Ok(())` otherwise. Does not remove the SSI entry — the caller
    /// invokes `commit_write` or `rollback_write` after.
    pub fn ssi_check_commit(&self, tx: u64) -> Result<()> {
        let ssi = self.ssi.lock().expect("poisoned ssi");
        if let Some(state) = ssi.get(&tx) {
            if state.in_conflict && state.out_conflict {
                return Err(Error::serialization(format!(
                    "transaction {tx} would close a dangerous rw-dependency cycle"
                )));
            }
        }
        Ok(())
    }

    /// The smallest TX ID still in flight, or `next_tx_id` when no
    /// transaction is in flight. v0.36's background reclaimer uses
    /// this as the **safe-to-reclaim watermark**: any committed
    /// tombstone (`tx_max < oldest_active`) or rolled-back insert
    /// (`tx_min < oldest_active`) can be physically removed, because
    /// no active snapshot's `next_tx` is lower than `oldest_active`,
    /// so no future reader will ever try to see it as live.
    pub fn oldest_active_tx_id(&self) -> u64 {
        let inner = self.inner.lock().expect("poisoned tx state");
        inner
            .in_flight
            .iter()
            .min()
            .copied()
            .unwrap_or(inner.next_tx_id)
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
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashMap::new())),
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
        let snap = Snapshot::new(
            10,
            HashSet::new(),
            None,
            scratch.clog.clone(),
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashMap::new())),
        );
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
