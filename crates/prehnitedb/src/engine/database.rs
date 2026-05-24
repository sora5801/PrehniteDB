//! [`Database`] — the public face of PrehniteDB.
//!
//! A `Database` owns a [`Pager`] and a [`Catalog`] and turns SQL text into
//! results. Outside a transaction each [`Database::execute`] call is its own
//! transaction, committed on success and rolled back on failure. `BEGIN`
//! opens an explicit transaction: its statements stage together until
//! `COMMIT` makes them durable or `ROLLBACK` discards them.

use std::path::{Path, PathBuf};

use crate::engine::catalog::Catalog;
use crate::engine::executor::{self, Execution, QueryResult, RowStream};
use crate::engine::planner::{self, Plan};
use crate::engine::schema::{Index, Schema};
use crate::engine::transaction::{Snapshot, TxState};
use crate::engine::value::Value;
use crate::error::{Error, Result};
use crate::sql::ast::Statement;
use crate::storage::pager::wal_path;
use crate::storage::{BTree, Pager, SharedPool};

/// Where a `Database` stands with respect to an explicit transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TxnState {
    /// Auto-commit: each statement is committed on its own.
    None,
    /// An explicit transaction is open and accepting statements.
    Open,
    /// A statement inside the transaction failed; it accepts only `ROLLBACK`.
    Aborted,
}

/// An open PrehniteDB database.
pub struct Database {
    pager: Pager,
    catalog: Catalog,
    path: PathBuf,
    txn: TxnState,
    /// MVCC transaction coordinator — shared by every `Database` opened on
    /// this file when the caller passes a [`TxState`] in. When `open` is
    /// used directly each handle gets its own.
    tx_state: TxState,
    /// The TX ID this `Database` is currently writing under, if any.
    /// Assigned at the first writing statement of an auto-commit run or
    /// of an explicit BEGIN..COMMIT, and cleared at commit/rollback.
    current_tx: Option<u64>,
    /// The snapshot pinned for the duration of an explicit transaction.
    /// `BEGIN` captures it; every statement inside the transaction reads
    /// against it, so the transaction sees a single, stable view of the
    /// database — `SERIALIZABLE`-snapshot semantics, the substrate SSI
    /// runs on. Auto-commit statements still capture a fresh snapshot
    /// per statement; this stays `None` for them.
    transaction_snapshot: Option<Snapshot>,
}

impl Database {
    /// Open the database at `path`, creating it if it does not exist, with a
    /// private page cache and a private MVCC transaction coordinator.
    pub fn open(path: impl AsRef<Path>) -> Result<Database> {
        Database::open_with_pool(path, SharedPool::new())
    }

    /// Open the database at `path`, using `pool` as the page cache. The MVCC
    /// transaction coordinator is private to this `Database`. The server
    /// uses [`Database::open_shared`] instead so concurrent readers see the
    /// same in-flight write transaction.
    pub fn open_with_pool(path: impl AsRef<Path>, pool: SharedPool) -> Result<Database> {
        let path = path.as_ref().to_path_buf();
        let mut pager = Pager::open_with_pool(&path, pool)?;
        let clog = crate::engine::clog::Clog::open(&path)?;
        let tx_state = TxState::new(pager.next_tx_id(), clog, pager.shared_meta());
        // Build the catalog with the shared lock so every connection
        // serialises its read-modify-write of catalog leaf pages.
        let catalog = Catalog::open_with_lock(&mut pager, tx_state.catalog_lock())?;
        // Persist the catalog if it was just created. When it already
        // existed nothing is staged and this is a no-op.
        pager.commit()?;
        Ok(Database {
            pager,
            catalog,
            path,
            txn: TxnState::None,
            tx_state,
            current_tx: None,
            transaction_snapshot: None,
        })
    }

    /// Open the database at `path` using a shared page cache *and* a shared
    /// MVCC transaction coordinator. The server constructs one of each at
    /// startup and clones them into every connection: writers and readers
    /// then agree on the next-TX counter and on the single in-flight write
    /// transaction, so a reader's snapshot is consistent with what the
    /// writer is doing.
    pub fn open_shared(
        path: impl AsRef<Path>,
        pool: SharedPool,
        tx_state: TxState,
    ) -> Result<Database> {
        let path = path.as_ref().to_path_buf();
        let mut pager =
            Pager::open_shared_with_meta(&path, pool, tx_state.shared_meta())?;
        let catalog = Catalog::open_with_lock(&mut pager, tx_state.catalog_lock())?;
        pager.commit()?;
        Ok(Database {
            pager,
            catalog,
            path,
            txn: TxnState::None,
            tx_state,
            current_tx: None,
            transaction_snapshot: None,
        })
    }

    /// Whether `statement` reads only, used by the server to skip the
    /// write lock and the TX reservation. Statements that fail to parse
    /// or that mutate take the write path.
    pub fn is_read_only_sql(sql: &str) -> bool {
        matches!(crate::sql::parse(sql), Ok(Statement::Select { .. }))
    }

    /// The shared transaction coordinator, used by the server when opening
    /// peer `Database`s on the same file.
    pub fn tx_state(&self) -> TxState {
        self.tx_state.clone()
    }

    /// Pick up another writer's catalog changes before starting a write.
    ///
    /// The pager's metadata is now shared across every connection on the
    /// same file (`SharedMeta`), so page allocations stay coherent
    /// without a refresh step. The catalog *can* still need a re-open
    /// when the catalog B+tree's root has moved under a peer's split —
    /// the root page number lives in shared meta, but the in-memory
    /// `Catalog` wrapper is per-pager. This is a cheap header check.
    pub fn reload_for_write(&mut self) -> Result<()> {
        self.catalog =
            Catalog::open_with_lock(&mut self.pager, self.tx_state.catalog_lock())?;
        Ok(())
    }

    /// Parse and run one SQL statement.
    ///
    /// Outside a transaction the statement is its own unit — committed on
    /// success, rolled back on failure. Inside one (opened by `BEGIN`) its
    /// writes only stage: `COMMIT` makes the whole transaction durable,
    /// `ROLLBACK` discards it, and a statement that fails aborts it.
    pub fn execute(&mut self, sql: &str) -> Result<QueryResult> {
        let statement = crate::sql::parse(sql)?;
        let plan = match statement {
            Statement::Begin => return self.begin_transaction(),
            Statement::Commit => return self.commit_transaction(),
            Statement::Rollback => return self.rollback_transaction(),
            other => {
                if self.txn == TxnState::Aborted {
                    return Err(Error::exec("transaction is aborted — ROLLBACK to recover"));
                }
                planner::plan(other, &mut self.pager, &self.catalog)?
            }
        };
        if matches!(plan, Plan::Vacuum) {
            if self.txn == TxnState::Open {
                return Err(Error::exec("VACUUM cannot run inside a transaction"));
            }
            return self.vacuum();
        }
        self.run_plan(plan)
    }

    /// Run a planned data statement. v0.26's deferred-transaction model:
    /// every successful write physically commits via the pager immediately,
    /// stamped with the writer's TX ID, so the writer mutex can be released
    /// between statements of an explicit BEGIN..COMMIT. The TX itself is
    /// committed (added to the clog) only at the logical COMMIT; until
    /// then its in-flight ID keeps it invisible to other snapshots.
    fn run_plan(&mut self, plan: Plan) -> Result<QueryResult> {
        let writes = plan_writes(&plan);
        if writes {
            self.ensure_write_tx();
        }
        let snapshot = self.snapshot_for_statement();
        match executor::execute(&mut self.pager, &self.catalog, &snapshot, plan) {
            Ok(result) => {
                if writes {
                    // Physically commit this statement's writes. Inside an
                    // explicit BEGIN..COMMIT this happens per statement so
                    // the writer mutex can be released between statements;
                    // the TX stays in-flight (logical) and other snapshots
                    // still don't see the rows.
                    self.pager.commit()?;
                }
                if self.txn == TxnState::None && writes {
                    let id = self.current_tx.take().expect("write TX reserved above");
                    // SSI check for the autocommit transaction. If it
                    // would close a dangerous rw-cycle, abort instead of
                    // committing.
                    if let Err(e) = self.tx_state.ssi_check_commit(id) {
                        let _ = self.tx_state.rollback_write(id);
                        return Err(e);
                    }
                    self.tx_state.commit_write(id)?;
                }
                Ok(result)
            }
            Err(err) => {
                self.pager.rollback();
                if self.txn == TxnState::Open {
                    self.txn = TxnState::Aborted;
                }
                if self.txn != TxnState::Open && self.current_tx.is_some() {
                    let id = self.current_tx.take().unwrap();
                    let _ = self.tx_state.rollback_write(id);
                }
                Err(err)
            }
        }
    }

    /// Reserve a TX ID for the current writer (idempotent inside one
    /// transaction). For an explicit `BEGIN..COMMIT` the TX is already
    /// reserved at `BEGIN` (so the read-set tracking has somewhere to
    /// land); this method is mainly the autocommit lazy path.
    fn ensure_write_tx(&mut self) {
        if self.current_tx.is_none() {
            let id = self.tx_state.begin_write();
            self.pager.set_next_tx_id(id + 1);
            self.current_tx = Some(id);
        }
    }

    /// Pick the snapshot a statement should run under.
    ///
    /// Inside an explicit transaction it's the snapshot captured at
    /// `BEGIN` (so reads stay stable across statements — the substrate
    /// SSI runs on). The `own_tx` override is set from
    /// `current_tx` per call so the writer's own in-flight rows stay
    /// visible to itself.
    ///
    /// Outside an explicit transaction (auto-commit) each statement
    /// captures its own snapshot.
    fn snapshot_for_statement(&self) -> Snapshot {
        if let Some(base) = &self.transaction_snapshot {
            let mut snap = base.clone();
            snap.own_tx = self.current_tx;
            snap
        } else {
            self.tx_state.snapshot(self.current_tx)
        }
    }

    /// Parse and run one SQL statement, streaming.
    ///
    /// A `SELECT` returns an [`Execution::Rows`] whose rows are pulled, one at
    /// a time, with [`Database::stream_next`]; every other statement runs to
    /// completion and returns an [`Execution::Ack`]. The transaction rules are
    /// exactly those of [`Database::execute`].
    pub fn execute_streaming(&mut self, sql: &str) -> Result<Execution> {
        let statement = crate::sql::parse(sql)?;
        let plan = match statement {
            Statement::Begin => return self.begin_transaction().map(into_execution),
            Statement::Commit => return self.commit_transaction().map(into_execution),
            Statement::Rollback => return self.rollback_transaction().map(into_execution),
            other => {
                if self.txn == TxnState::Aborted {
                    return Err(Error::exec("transaction is aborted — ROLLBACK to recover"));
                }
                planner::plan(other, &mut self.pager, &self.catalog)?
            }
        };
        if matches!(plan, Plan::Vacuum) {
            if self.txn == TxnState::Open {
                return Err(Error::exec("VACUUM cannot run inside a transaction"));
            }
            return self.vacuum().map(into_execution);
        }
        self.run_plan_streaming(plan)
    }

    /// Like [`run_plan`](Self::run_plan), but streaming. A non-SELECT finishes
    /// here and is committed now (unless a transaction is open); a `SELECT`
    /// only *builds* its pipeline — it writes nothing, so there is nothing to
    /// commit — and its rows are pulled later by
    /// [`stream_next`](Self::stream_next).
    fn run_plan_streaming(&mut self, plan: Plan) -> Result<Execution> {
        let writes = plan_writes(&plan);
        if writes {
            self.ensure_write_tx();
        }
        let snapshot = self.snapshot_for_statement();
        match executor::execute_streaming(&mut self.pager, &self.catalog, &snapshot, plan) {
            Ok(execution) => {
                if matches!(execution, Execution::Ack(_)) {
                    if writes {
                        self.pager.commit()?;
                    }
                    if self.txn == TxnState::None && writes {
                        let id = self.current_tx.take().expect("write TX reserved above");
                        if let Err(e) = self.tx_state.ssi_check_commit(id) {
                            let _ = self.tx_state.rollback_write(id);
                            return Err(e);
                        }
                        self.tx_state.commit_write(id)?;
                    }
                }
                Ok(execution)
            }
            Err(err) => {
                self.pager.rollback();
                if self.txn == TxnState::Open {
                    self.txn = TxnState::Aborted;
                }
                if self.txn != TxnState::Open && self.current_tx.is_some() {
                    let id = self.current_tx.take().unwrap();
                    let _ = self.tx_state.rollback_write(id);
                }
                Err(err)
            }
        }
    }

    /// Pull the next row of a streaming `SELECT`. A fault here aborts an open
    /// transaction, exactly as a failed statement in `run_plan` would — a
    /// `SELECT` writes nothing, so the rollback only resets transaction state.
    pub fn stream_next(&mut self, stream: &mut RowStream) -> Result<Option<Vec<Value>>> {
        match stream.next(&mut self.pager) {
            Ok(row) => Ok(row),
            Err(err) => {
                self.pager.rollback();
                if self.txn == TxnState::Open {
                    self.txn = TxnState::Aborted;
                }
                Err(err)
            }
        }
    }

    /// Open an explicit transaction.
    ///
    /// v0.29 reserves the TX ID *at* `BEGIN` rather than lazily at first
    /// write, so SSI's read-set tracking has a TX to attribute reads to
    /// for any `SELECT` before the first write. The transaction's
    /// snapshot is captured here too (pinned for every statement inside
    /// the transaction) — that's the `SERIALIZABLE`-snapshot substrate
    /// SSI runs on. A read-only `BEGIN..COMMIT` therefore now writes one
    /// clog `committed` record at commit, the only durable cost of the
    /// reservation.
    fn begin_transaction(&mut self) -> Result<QueryResult> {
        if self.txn != TxnState::None {
            return Err(Error::exec("a transaction is already open"));
        }
        let id = self.tx_state.begin_write();
        self.pager.set_next_tx_id(id + 1);
        self.current_tx = Some(id);
        self.transaction_snapshot = Some(self.tx_state.snapshot(Some(id)));
        self.txn = TxnState::Open;
        Ok(QueryResult::Ack("transaction started".to_string()))
    }

    /// Logically commit the open transaction. v0.26: each statement's
    /// writes were already physically committed; this just appends a
    /// "committed" record to the clog. v0.29: also runs the SSI commit
    /// check — if our `in_conflict && out_conflict` flags say we're the
    /// pivot of a dangerous rw-cycle, abort with
    /// [`Error::Serialization`] instead.
    fn commit_transaction(&mut self) -> Result<QueryResult> {
        match self.txn {
            TxnState::None => Err(Error::exec("COMMIT without an open transaction")),
            TxnState::Open => {
                let id = self.current_tx.take();
                self.transaction_snapshot = None;
                self.txn = TxnState::None;
                if let Some(id) = id {
                    if let Err(e) = self.tx_state.ssi_check_commit(id) {
                        let _ = self.tx_state.rollback_write(id);
                        return Err(e);
                    }
                    self.tx_state.commit_write(id)?;
                }
                Ok(QueryResult::Ack("transaction committed".to_string()))
            }
            TxnState::Aborted => {
                // The aborting statement already rolled itself back. The
                // earlier successful statements physically committed — we
                // now mark the TX rolled-back in the clog so those rows
                // become invisible.
                self.transaction_snapshot = None;
                self.txn = TxnState::None;
                if let Some(id) = self.current_tx.take() {
                    self.tx_state.rollback_write(id)?;
                }
                Ok(QueryResult::Ack(
                    "transaction was aborted and has been rolled back".to_string(),
                ))
            }
        }
    }

    /// Logically roll back the open transaction. v0.26: earlier statements'
    /// writes are physically on disk; the clog rollback record renders
    /// them invisible to every future snapshot. VACUUM eventually reclaims
    /// the space.
    fn rollback_transaction(&mut self) -> Result<QueryResult> {
        if self.txn == TxnState::None {
            return Err(Error::exec("ROLLBACK without an open transaction"));
        }
        // Discard any work the current (in-flight) statement staged.
        self.pager.rollback();
        self.transaction_snapshot = None;
        self.txn = TxnState::None;
        if let Some(id) = self.current_tx.take() {
            self.tx_state.rollback_write(id)?;
        }
        Ok(QueryResult::Ack("transaction rolled back".to_string()))
    }

    /// Whether an explicit transaction is open (or aborted, awaiting
    /// `ROLLBACK`).
    pub fn in_transaction(&self) -> bool {
        self.txn != TxnState::None
    }

    /// Discard any open or aborted transaction — used when a client
    /// disconnects mid-transaction. Earlier statements' physical writes
    /// are left in the file but marked rolled-back via the clog.
    pub fn abort_transaction(&mut self) {
        if self.txn != TxnState::None {
            self.pager.rollback();
            self.transaction_snapshot = None;
            self.txn = TxnState::None;
            if let Some(id) = self.current_tx.take() {
                let _ = self.tx_state.rollback_write(id);
            }
        }
    }

    /// The names of all tables, in sorted order.
    pub fn table_names(&mut self) -> Result<Vec<String>> {
        self.catalog.table_names(&mut self.pager)
    }

    /// One **incremental in-place** garbage-collection pass over every
    /// table, deleting rows whose MVCC visibility says no live snapshot
    /// could ever see them again:
    ///
    /// - **Committed tombstones below the watermark**: `tx_max != 0`,
    ///   `tx_max < oldest_active`, and `clog.is_committed(tx_max)` — the
    ///   row's delete is durable and every snapshot's `next_tx` is
    ///   already past it, so no reader can see the row as live.
    /// - **Rolled-back inserts below the watermark**: `tx_min != 0`,
    ///   `tx_min < oldest_active`, and `clog.is_rolled_back(tx_min)` —
    ///   the row's insert was undone and no snapshot can ever
    ///   resurrect it.
    ///
    /// Unlike `VACUUM`, this runs **without exclusive access** to the
    /// engine: each table is reclaimed under its own per-table write
    /// lock (so foreground writers on the same table briefly block,
    /// but every other table and every reader proceeds). Returns the
    /// number of physical rows reclaimed across all tables.
    ///
    /// Designed to be called by a background thread on a timer. The
    /// server (`prehnited`) does so; library users can call it
    /// directly.
    pub fn reclaim_dead_rows(&mut self) -> Result<u64> {
        let oldest_active = self.tx_state.oldest_active_tx_id();
        let clog = self.tx_state.clog();
        let names = self.catalog.table_names(&mut self.pager)?;
        let mut reclaimed_total = 0u64;

        for name in names {
            // Hold the table's write lock for the duration of its pass.
            // Foreground writers on this table block briefly; readers
            // and writers on other tables are unaffected.
            let lock = self.tx_state.table_lock(&name);
            let _guard = lock.write().expect("poisoned table lock");

            let mut schema = match self.catalog.get(&mut self.pager, &name)? {
                Some(s) => s,
                None => continue,
            };
            let column_count = schema.columns.len();

            // First pass: collect the rowids of dead rows. Doing it in
            // two phases avoids mutating the B+tree while iterating.
            // Each entry also carries the decoded values so we can
            // delete its index entries by re-encoding the index key.
            let table = crate::storage::BTree::open(schema.root);
            let mut dead: Vec<(Vec<u8>, Vec<crate::engine::value::Value>, bool)> = Vec::new();
            for (rowid_key, encoded) in table.scan(&mut self.pager)? {
                let record = crate::engine::codec::decode_row(&encoded, column_count)?;
                // Committed tombstone past the watermark?
                let committed_tombstone = record.tx_max != 0
                    && record.tx_max < oldest_active
                    && clog.is_committed(record.tx_max);
                // Rolled-back insert past the watermark?
                let rolled_back_insert = record.tx_min != 0
                    && record.tx_min < oldest_active
                    && clog.is_rolled_back(record.tx_min);
                if committed_tombstone || rolled_back_insert {
                    dead.push((rowid_key, record.values, committed_tombstone));
                }
            }

            if dead.is_empty() {
                continue;
            }

            let mut reclaimed_in_table = 0u64;
            let mut tombstoned_reclaimed = 0u64;
            for (rowid_key, values, was_tombstone) in dead {
                // Delete every index entry that points at this rowid.
                // The index key is reconstructable from the row's
                // values + the indexed columns + the rowid suffix.
                for index in &schema.indexes {
                    let key = crate::engine::codec::encode_index_key(
                        &values,
                        &index.columns,
                        &rowid_key,
                    );
                    crate::storage::BTree::open(index.root).delete(&mut self.pager, &key)?;
                }
                table.delete(&mut self.pager, &rowid_key)?;
                if was_tombstone {
                    tombstoned_reclaimed += 1;
                }
                reclaimed_in_table += 1;
            }

            // `row_count` tracks live rows. Committed tombstones were
            // already deducted at DELETE time, so reclaiming them
            // doesn't change the count. Rolled-back inserts were
            // INCLUDED at INSERT time (the writer bumped `row_count`
            // optimistically) and never deducted — they need to come
            // off here. `reclaimed_in_table - tombstoned_reclaimed`
            // is the rolled-back-insert count.
            let rolled_back_reclaimed = reclaimed_in_table - tombstoned_reclaimed;
            if rolled_back_reclaimed > 0 {
                schema.row_count = schema.row_count.saturating_sub(rolled_back_reclaimed);
                self.catalog.put(&mut self.pager, &schema)?;
            }
            self.pager.commit()?;
            reclaimed_total += reclaimed_in_table;
        }

        Ok(reclaimed_total)
    }

    /// Rebuild the database compactly: every table and index is re-created in
    /// a fresh temp file with no free space, then that file's contents replace
    /// the live database in a single WAL-protected commit.
    fn vacuum(&mut self) -> Result<QueryResult> {
        let temp = vacuum_temp_path(&self.path);
        let _ = std::fs::remove_file(&temp);
        let _ = std::fs::remove_file(wal_path(&temp));

        // Build the compact copy in `temp`; its pager closes at scope end.
        {
            let mut dest = Pager::open(&temp)?;
            let dest_catalog = Catalog::open(&mut dest)?;
            for name in self.catalog.table_names(&mut self.pager)? {
                let schema = self
                    .catalog
                    .get(&mut self.pager, &name)?
                    .ok_or_else(|| Error::corruption("catalog lists a table it cannot return"))?;

                // Copy the live rows into a fresh, densely packed B+tree.
                // VACUUM reclaims two kinds of physical-but-dead row:
                //
                // - MVCC tombstones: rows whose `tx_max` is set and that
                //   tx_max is *committed* per the clog.
                // - Rolled-back inserts (v0.26): rows whose `tx_min` is
                //   recorded as rolled-back. Per v0.26's deferred-
                //   transaction model these were physically committed
                //   but their transaction never logically committed.
                //
                // Safe because VACUUM takes the exclusive write lock; no
                // other transaction is in flight, so every TX has a final
                // status in the clog.
                let clog = self.tx_state.clog();
                let table = BTree::create(&mut dest)?;
                let mut kept_rowids: std::collections::HashSet<Vec<u8>> =
                    std::collections::HashSet::new();
                for (key, value) in BTree::open(schema.root).scan(&mut self.pager)? {
                    let record = crate::engine::codec::decode_row(&value, schema.columns.len())?;
                    if record.tx_min != 0
                        && matches!(
                            clog.status(record.tx_min),
                            Some(crate::engine::clog::Status::RolledBack)
                        )
                    {
                        continue;
                    }
                    if record.tx_max != 0
                        && matches!(
                            clog.status(record.tx_max),
                            Some(crate::engine::clog::Status::Committed)
                        )
                    {
                        continue;
                    }
                    table.insert(&mut dest, &key, &value)?;
                    kept_rowids.insert(key);
                }

                // Rebuild each index, skipping entries whose rowid was
                // dropped above. An index entry's last 8 bytes are the
                // rowid — match against `kept_rowids` and copy only the
                // ones that still point at a live row.
                let mut indexes = Vec::with_capacity(schema.indexes.len());
                for index in &schema.indexes {
                    let rebuilt = BTree::create(&mut dest)?;
                    for (key, _) in BTree::open(index.root).scan(&mut self.pager)? {
                        if key.len() < 8 {
                            continue;
                        }
                        let rowid_key = key[key.len() - 8..].to_vec();
                        if kept_rowids.contains(&rowid_key) {
                            rebuilt.insert(&mut dest, &key, &[])?;
                        }
                    }
                    indexes.push(Index {
                        name: index.name.clone(),
                        columns: index.columns.clone(),
                        root: rebuilt.root(),
                    });
                }

                dest_catalog.put(
                    &mut dest,
                    &Schema {
                        name: schema.name,
                        columns: schema.columns,
                        root: table.root(),
                        next_rowid: schema.next_rowid,
                        row_count: schema.row_count,
                        indexes,
                    },
                )?;
            }
            dest.commit()?;
        }

        // Adopt the compact image as our own contents, then discard the temp.
        self.pager.replace_with(&temp)?;
        let _ = std::fs::remove_file(&temp);
        let _ = std::fs::remove_file(wal_path(&temp));
        // The catalog's root page has moved; reopen it.
        self.catalog = Catalog::open(&mut self.pager)?;
        Ok(QueryResult::Ack("database compacted".to_string()))
    }
}

/// Whether a plan writes (mutates state). Read-only plans skip the
/// TX-reservation path. CREATE/DROP/INSERT/UPDATE/DELETE all write; SELECT
/// reads; VACUUM is a special case handled outside this function.
fn plan_writes(plan: &Plan) -> bool {
    !matches!(plan, Plan::Select { .. })
}

/// The scratch file VACUUM builds its compact copy in, beside the database.
fn vacuum_temp_path(db: &Path) -> PathBuf {
    let mut name = db.as_os_str().to_os_string();
    name.push(".vacuum");
    PathBuf::from(name)
}

/// Lift a non-SELECT statement's [`QueryResult`] into an [`Execution`].
fn into_execution(result: QueryResult) -> Execution {
    match result {
        QueryResult::Ack(message) => Execution::Ack(message),
        QueryResult::Rows { .. } => {
            unreachable!("BEGIN / COMMIT / ROLLBACK / VACUUM never yield rows")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::value::Value;
    use crate::storage::pager::wal_path;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDb {
        path: PathBuf,
    }

    impl TempDb {
        fn new() -> TempDb {
            let n = COUNTER.fetch_add(1, Ordering::SeqCst);
            let path =
                std::env::temp_dir().join(format!("prehnite-db-{}-{n}.db", std::process::id()));
            let _ = std::fs::remove_file(&path);
            let _ = std::fs::remove_file(wal_path(&path));
            TempDb { path }
        }

        fn open(&self) -> Database {
            Database::open(&self.path).unwrap()
        }
    }

    impl Drop for TempDb {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
            let _ = std::fs::remove_file(wal_path(&self.path));
        }
    }

    fn rows(result: QueryResult) -> Vec<Vec<Value>> {
        match result {
            QueryResult::Rows { rows, .. } => rows,
            other => panic!("expected a result set, got {other:?}"),
        }
    }

    #[test]
    fn full_lifecycle() {
        let tmp = TempDb::new();
        let mut db = tmp.open();
        db.execute("CREATE TABLE users (id INT, name TEXT, active BOOL)")
            .unwrap();
        db.execute("INSERT INTO users VALUES (1, 'ada', true), (2, 'grace', false)")
            .unwrap();
        db.execute("INSERT INTO users (id, name) VALUES (3, 'edsger')")
            .unwrap();

        let all = rows(db.execute("SELECT * FROM users").unwrap());
        assert_eq!(all.len(), 3);
        assert_eq!(all[2][2], Value::Null); // unspecified column defaulted to NULL

        let active = rows(
            db.execute("SELECT name FROM users WHERE active = true")
                .unwrap(),
        );
        assert_eq!(active, vec![vec![Value::Text("ada".into())]]);

        db.execute("UPDATE users SET active = true WHERE id = 3")
            .unwrap();
        let active = rows(
            db.execute("SELECT id FROM users WHERE active = true")
                .unwrap(),
        );
        assert_eq!(active.len(), 2);

        db.execute("DELETE FROM users WHERE id = 2").unwrap();
        assert_eq!(rows(db.execute("SELECT * FROM users").unwrap()).len(), 2);

        db.execute("DROP TABLE users").unwrap();
        assert!(db.execute("SELECT * FROM users").is_err());
    }

    #[test]
    fn data_persists_across_reopen() {
        let tmp = TempDb::new();
        {
            let mut db = tmp.open();
            db.execute("CREATE TABLE t (n INT)").unwrap();
            for i in 0..50 {
                db.execute(&format!("INSERT INTO t VALUES ({i})")).unwrap();
            }
        }
        let mut db = tmp.open();
        let all = rows(db.execute("SELECT n FROM t").unwrap());
        assert_eq!(all.len(), 50);
        assert_eq!(all[0][0], Value::Int(0));
        assert_eq!(all[49][0], Value::Int(49));
    }

    #[test]
    fn failed_statement_rolls_back() {
        let tmp = TempDb::new();
        let mut db = tmp.open();
        db.execute("CREATE TABLE t (n INT)").unwrap();
        db.execute("INSERT INTO t VALUES (1)").unwrap();

        // The second row has the wrong type; the whole INSERT must fail
        // atomically, leaving only the one good row already present.
        assert!(db
            .execute("INSERT INTO t VALUES (2), ('not an int')")
            .is_err());
        let all = rows(db.execute("SELECT n FROM t").unwrap());
        assert_eq!(all, vec![vec![Value::Int(1)]]);

        // The database is still fully usable after the failure.
        db.execute("INSERT INTO t VALUES (3)").unwrap();
        assert_eq!(rows(db.execute("SELECT n FROM t").unwrap()).len(), 2);
    }

    #[test]
    fn rejects_semantic_errors() {
        let tmp = TempDb::new();
        let mut db = tmp.open();
        db.execute("CREATE TABLE t (n INT)").unwrap();
        assert!(db.execute("CREATE TABLE t (n INT)").is_err()); // table already exists
        assert!(db.execute("SELECT missing FROM t").is_err()); // unknown column
        assert!(db.execute("INSERT INTO t VALUES (1, 2)").is_err()); // wrong arity
        assert!(db.execute("SELECT * FROM ghost").is_err()); // unknown table
    }

    #[test]
    fn arithmetic_and_filters_in_queries() {
        let tmp = TempDb::new();
        let mut db = tmp.open();
        db.execute("CREATE TABLE nums (x INT)").unwrap();
        db.execute("INSERT INTO nums VALUES (1), (2), (3), (4), (5)")
            .unwrap();
        let big = rows(db.execute("SELECT x FROM nums WHERE x * 2 >= 6").unwrap());
        assert_eq!(big.len(), 3); // 3, 4, 5
        assert_eq!(db.table_names().unwrap(), vec!["nums".to_string()]);
    }

    #[test]
    fn commit_makes_a_transaction_durable() {
        let tmp = TempDb::new();
        {
            let mut db = tmp.open();
            db.execute("CREATE TABLE t (n INT)").unwrap();
            db.execute("BEGIN").unwrap();
            db.execute("INSERT INTO t VALUES (1)").unwrap();
            db.execute("INSERT INTO t VALUES (2)").unwrap();
            // Mid-transaction, the rows are visible to this connection.
            assert_eq!(rows(db.execute("SELECT n FROM t").unwrap()).len(), 2);
            db.execute("COMMIT").unwrap();
        }
        // After COMMIT and a reopen, both rows are durably on disk.
        let mut db = tmp.open();
        assert_eq!(rows(db.execute("SELECT n FROM t").unwrap()).len(), 2);
    }

    #[test]
    fn rollback_discards_a_transaction() {
        let tmp = TempDb::new();
        let mut db = tmp.open();
        db.execute("CREATE TABLE t (n INT)").unwrap();
        db.execute("INSERT INTO t VALUES (1)").unwrap(); // auto-committed
        db.execute("BEGIN").unwrap();
        db.execute("INSERT INTO t VALUES (2)").unwrap();
        db.execute("INSERT INTO t VALUES (3)").unwrap();
        db.execute("ROLLBACK").unwrap();
        // Only the auto-committed row survives the rollback.
        assert_eq!(
            rows(db.execute("SELECT n FROM t").unwrap()),
            vec![vec![Value::Int(1)]]
        );
    }

    #[test]
    fn an_uncommitted_transaction_does_not_survive_a_reopen() {
        let tmp = TempDb::new();
        {
            let mut db = tmp.open();
            db.execute("CREATE TABLE t (n INT)").unwrap();
            db.execute("BEGIN").unwrap();
            db.execute("INSERT INTO t VALUES (1)").unwrap();
            // `db` drops here — the transaction was never committed.
        }
        // The table exists (its CREATE auto-committed) but the row is gone.
        let mut db = tmp.open();
        assert!(rows(db.execute("SELECT n FROM t").unwrap()).is_empty());
    }

    #[test]
    fn transaction_control_errors() {
        let tmp = TempDb::new();
        let mut db = tmp.open();
        // COMMIT or ROLLBACK with no transaction open is an error.
        assert!(db.execute("COMMIT").is_err());
        assert!(db.execute("ROLLBACK").is_err());
        // A second BEGIN, with one already open, is an error.
        db.execute("BEGIN").unwrap();
        assert!(db.execute("BEGIN").is_err());
        // VACUUM cannot run inside a transaction.
        assert!(db.execute("VACUUM").is_err());
        db.execute("ROLLBACK").unwrap();
    }

    #[test]
    fn a_failed_statement_aborts_the_transaction() {
        let tmp = TempDb::new();
        let mut db = tmp.open();
        db.execute("CREATE TABLE t (n INT)").unwrap();
        db.execute("BEGIN").unwrap();
        db.execute("INSERT INTO t VALUES (1)").unwrap();
        // A bad statement inside the transaction aborts it.
        assert!(db.execute("INSERT INTO t VALUES ('not an int')").is_err());
        // The transaction is now aborted: further statements are refused...
        assert!(db.execute("INSERT INTO t VALUES (2)").is_err());
        // ...until ROLLBACK clears it, discarding the whole transaction.
        db.execute("ROLLBACK").unwrap();
        assert!(rows(db.execute("SELECT n FROM t").unwrap()).is_empty());
        // The database is fully usable again afterward.
        db.execute("INSERT INTO t VALUES (9)").unwrap();
        assert_eq!(
            rows(db.execute("SELECT n FROM t").unwrap()),
            vec![vec![Value::Int(9)]]
        );
    }
}
