//! [`Database`] — the public face of PrehniteDB.
//!
//! A `Database` owns a [`Pager`] and a [`Catalog`] and turns SQL text into
//! results. Outside a transaction each [`Database::execute`] call is its own
//! transaction, committed on success and rolled back on failure. `BEGIN`
//! opens an explicit transaction: its statements stage together until
//! `COMMIT` makes them durable or `ROLLBACK` discards them.

use std::collections::HashMap;
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
    /// v0.55: cache of prepared plans, keyed by the opaque handle returned
    /// from [`Database::prepare`]. A handle stays valid for the lifetime
    /// of this `Database`; the server keeps the cache per-connection so
    /// that one client's handles never collide with another's, matching
    /// Postgres session-level prepared-statement semantics.
    ///
    /// v0.56: each entry carries the schema version it was prepared
    /// against. At Execute, if the live schema version differs, the
    /// plan is stale — its access paths point at indexes/tables that
    /// may no longer exist or its statistics-driven decisions are
    /// outdated. The entry is dropped and the caller sees a clean
    /// "prepared statement is stale, re-prepare" error.
    prepared_statements: HashMap<u64, CachedPlan>,
    /// Monotonic counter for the next prepared-statement handle. Strictly
    /// increasing — handles are never reused, so a stale handle from a
    /// dropped prepared statement reliably errors instead of silently
    /// running a different cached plan.
    next_handle: u64,
}

/// v0.56: one entry in the prepared-statement cache. The Plan is the
/// fully validated, fully planned tree from [`planner::plan`]; the
/// `schema_version` is a snapshot of the global DDL counter at the
/// moment the plan was created.
///
/// At Execute, the live `SharedMeta::schema_version()` is compared to
/// `schema_version`. If they differ — meaning some DDL or ANALYZE has
/// run since this plan was prepared — the cached plan is dropped and
/// the caller is asked to re-prepare. This matches Postgres's
/// behaviour of invalidating prepared statements on relevant schema
/// changes (Postgres does it per-relation; v0.56 does it globally).
#[derive(Clone)]
struct CachedPlan {
    schema_version: u64,
    plan: Plan,
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
        // v0.58: sweep any `is_building` indexes left over from a
        // crashed CREATE INDEX. Safe here because `open_with_pool` is
        // the "first connection" path — no peer connection is mid-
        // CREATE-INDEX yet. (The server bootstraps through this path
        // and per-connection `open_shared` calls skip the sweep.)
        sweep_partial_indexes(&mut pager, &catalog)?;
        Ok(Database {
            pager,
            catalog,
            path,
            txn: TxnState::None,
            tx_state,
            current_tx: None,
            transaction_snapshot: None,
            prepared_statements: HashMap::new(),
            next_handle: 1,
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
            prepared_statements: HashMap::new(),
            next_handle: 1,
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
        self.execute_with_params(sql, &[])
    }

    /// Parse and run one SQL statement with bound parameters (v0.54).
    ///
    /// Wherever the SQL has a `?` placeholder, it is replaced with the
    /// matching `Value` from `params` (0-indexed by appearance). This
    /// is the secure way to pass user-supplied values into a query:
    ///
    /// ```no_run
    /// # use prehnitedb::{Database, Value};
    /// # let mut db = Database::open("scratch.db").unwrap();
    /// db.execute_with_params(
    ///     "SELECT name FROM users WHERE id = ? AND active = ?",
    ///     &[Value::Int(42), Value::Bool(true)],
    /// ).unwrap();
    /// ```
    ///
    /// String concatenating an `i64` into the SQL would work the same;
    /// concatenating a user-supplied string would be an injection
    /// vector. Bind parameters route the value to evaluation, never
    /// to the parser, closing that vector.
    ///
    /// Arity mismatch (too few params for the placeholders) is a
    /// plan-time error. Extra params are silently ignored.
    pub fn execute_with_params(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<QueryResult> {
        let statement = crate::sql::parse(sql)?;
        let mut plan = match statement {
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
        // v0.54: substitute every `?` in the planned tree with the
        // matching literal from `params`, before the executor sees
        // any Placeholder. A statement with no placeholders is a
        // no-op walk (params can be `&[]`).
        crate::engine::bind::bind_plan(&mut plan, params)?;
        if matches!(plan, Plan::Vacuum) {
            if self.txn == TxnState::Open {
                return Err(Error::exec("VACUUM cannot run inside a transaction"));
            }
            return self.vacuum();
        }
        // v0.58: CREATE INDEX is "online" — it runs in three phases
        // with mixed lock granularities and does its own per-phase
        // commits. Like VACUUM it doesn't compose with an open user
        // transaction, and it has to live outside the normal
        // single-statement TX wrapping done by `run_plan`.
        if let Plan::CreateIndex {
            name,
            table,
            columns,
        } = plan
        {
            if self.txn == TxnState::Open {
                return Err(Error::exec(
                    "CREATE INDEX cannot run inside a transaction",
                ));
            }
            return self.create_index_online(name, table, columns);
        }
        self.run_plan(plan)
    }

    /// Parse and plan `sql`, cache the plan, and return an opaque handle.
    /// The plan is kept in this `Database`'s in-memory cache and is
    /// reusable via [`Database::execute_prepared`] for the lifetime of
    /// the `Database`. This is v0.55's library face on top of v0.54's
    /// bind step.
    ///
    /// Why a separate prepare phase: parsing and planning are the
    /// non-trivial work (lexing, AST construction, validation, join
    /// reordering, access-path selection). With bind parameters, that
    /// work is independent of the parameter values, so one prepare can
    /// amortise across many executes. The wire protocol's
    /// Prepare/Execute frames (Postgres extended-query style) sit on
    /// top of this same API.
    ///
    /// Refuses transaction-control statements (`BEGIN` / `COMMIT` /
    /// `ROLLBACK`) and `VACUUM`: those have engine-side side effects
    /// the prepare path doesn't model. They're cheap to parse, so just
    /// run them through [`Database::execute`].
    pub fn prepare(&mut self, sql: &str) -> Result<u64> {
        let statement = crate::sql::parse(sql)?;
        match statement {
            Statement::Begin | Statement::Commit | Statement::Rollback => {
                return Err(Error::exec(
                    "transaction-control statements cannot be prepared; use execute()",
                ));
            }
            _ => {}
        }
        let plan = planner::plan(statement, &mut self.pager, &self.catalog)?;
        if matches!(plan, Plan::Vacuum) {
            return Err(Error::exec(
                "VACUUM cannot be prepared; use execute()",
            ));
        }
        // v0.58: CREATE INDEX online is a multi-phase orchestration
        // that does its own commits per phase — its "shape" depends
        // on table state at execution time (which rows exist), and
        // re-executing a cached CREATE INDEX would attempt to
        // recreate the same index and fail. Just call `execute`.
        if matches!(plan, Plan::CreateIndex { .. }) {
            return Err(Error::exec(
                "CREATE INDEX cannot be prepared; use execute()",
            ));
        }
        let handle = self.next_handle;
        self.next_handle = self.next_handle.checked_add(1).ok_or_else(|| {
            Error::exec("prepared-statement handle counter exhausted")
        })?;
        // v0.56: stamp the entry with the live schema version. Any
        // DDL/ANALYZE between now and the next Execute will bump
        // SharedMeta's counter past this snapshot, and the version
        // check at Execute will then reject the cached plan.
        let schema_version = self.pager.shared_meta().schema_version();
        self.prepared_statements.insert(
            handle,
            CachedPlan {
                schema_version,
                plan,
            },
        );
        Ok(handle)
    }

    /// Run a previously prepared plan with the given parameter values.
    /// Clones the cached plan, binds parameters into the clone, then
    /// runs it through the normal execution path. The cache entry is
    /// untouched — the same handle can be executed many times.
    ///
    /// Errors:
    /// - bad handle (no entry, or one already deallocated): `Error::Exec`
    /// - arity mismatch (too few params for placeholders): propagated
    ///   from [`bind_plan`](crate::engine::bind::bind_plan), which
    ///   names the missing placeholder index in its message.
    pub fn execute_prepared(
        &mut self,
        handle: u64,
        params: &[Value],
    ) -> Result<QueryResult> {
        if self.txn == TxnState::Aborted {
            return Err(Error::exec("transaction is aborted — ROLLBACK to recover"));
        }
        let mut plan = self.take_fresh_prepared_plan(handle)?;
        crate::engine::bind::bind_plan(&mut plan, params)?;
        self.run_plan(plan)
    }

    /// Drop a prepared statement from the cache, freeing its plan. A
    /// future [`Database::execute_prepared`] with this handle errors
    /// as "no prepared statement with handle N". Returns `true` if a
    /// plan was removed, `false` if the handle was already absent.
    pub fn deallocate_prepared(&mut self, handle: u64) -> bool {
        self.prepared_statements.remove(&handle).is_some()
    }

    /// Streaming counterpart to [`Database::execute_prepared`]. Same
    /// arity semantics, same error cases; returns an [`Execution`]
    /// whose rows are pulled by [`Database::stream_next`] for SELECT,
    /// or an [`Execution::Ack`] for everything else.
    ///
    /// This is the v0.55 server's hot path: the protocol's Execute
    /// frame routes straight through this so the row stream is
    /// emitted incrementally just like a plain Query.
    pub fn execute_prepared_streaming(
        &mut self,
        handle: u64,
        params: &[Value],
    ) -> Result<Execution> {
        if self.txn == TxnState::Aborted {
            return Err(Error::exec("transaction is aborted — ROLLBACK to recover"));
        }
        let mut plan = self.take_fresh_prepared_plan(handle)?;
        crate::engine::bind::bind_plan(&mut plan, params)?;
        if matches!(plan, Plan::Vacuum) {
            return Err(Error::exec(
                "VACUUM cannot be executed via a prepared statement",
            ));
        }
        self.run_plan_streaming(plan)
    }

    /// v0.56: look up a cached plan by handle, check it against the live
    /// schema version, and return a clone of the plan if fresh. On a
    /// version mismatch the entry is **dropped** from the cache (so a
    /// retry on the same handle gets the cleaner "no prepared statement"
    /// error rather than another stale-version error), and the caller
    /// sees a stale-statement error that tells them to re-prepare.
    ///
    /// Returning a clone keeps the caller free to call `bind_plan` (which
    /// mutates) without touching the cache entry — but here the cache
    /// entry is also dropped on the cold stale-path, so the clone is
    /// strictly for the hot fresh-path.
    fn take_fresh_prepared_plan(&mut self, handle: u64) -> Result<Plan> {
        let live = self.pager.shared_meta().schema_version();
        let cached = self
            .prepared_statements
            .get(&handle)
            .ok_or_else(|| Error::exec(format!("no prepared statement with handle {handle}")))?;
        if cached.schema_version != live {
            // Drop the stale entry. Future Executes on the same handle
            // get the "no prepared statement" error rather than this
            // staleness error every time — clearer signal that the
            // handle is dead.
            self.prepared_statements.remove(&handle);
            return Err(Error::exec(format!(
                "prepared statement {handle} is stale (schema changed); re-prepare"
            )));
        }
        Ok(cached.plan.clone())
    }

    /// The [`crate::WriteScope`] of the plan named by `handle`, used by
    /// the server to take the right table/catalog lock around an
    /// Execute. Errors if the handle is unknown — that error is then
    /// surfaced as the Execute reply.
    ///
    /// Read-only plans (SELECT, EXPLAIN) return [`crate::WriteScope::None`]
    /// just like their SQL counterparts in [`crate::write_scope`], so
    /// the server's existing dispatch in `run_write` Just Works for
    /// the prepared path too.
    pub fn prepared_write_scope(&self, handle: u64) -> Result<crate::WriteScope> {
        let cached = self
            .prepared_statements
            .get(&handle)
            .ok_or_else(|| Error::exec(format!("no prepared statement with handle {handle}")))?;
        // v0.56: don't validate the schema version here — the server uses
        // this to pick a lock, and locking against a stale-version handle
        // is harmless (the Execute that follows will see the same stale
        // version and error before touching the table). If we errored
        // here too, the server would have to do the same dance twice
        // for the same statement.
        Ok(crate::plan_write_scope(&cached.plan))
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
        // v0.58: same online-CREATE-INDEX intercept as in
        // execute_with_params — multi-phase, can't run inside an
        // open user transaction.
        if let Plan::CreateIndex {
            name,
            table,
            columns,
        } = plan
        {
            if self.txn == TxnState::Open {
                return Err(Error::exec(
                    "CREATE INDEX cannot run inside a transaction",
                ));
            }
            return self.create_index_online(name, table, columns).map(into_execution);
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

    /// v0.58: build a new index without holding the per-table
    /// exclusive lock for the duration of the scan. Three phases:
    ///
    /// **Phase 1 (brief exclusive).** Take the per-table write lock,
    /// allocate an empty B+tree for the index, add the `Index { …
    /// is_building: true }` entry to the schema, commit catalog.
    /// Release. From this point on, every connection's next write
    /// to this table reads the updated schema (via
    /// `reload_for_write`), sees the new index, and maintains it
    /// as a regular index.
    ///
    /// **Phase 2 (shared).** Take the per-table read lock.
    /// Concurrent INSERT/UPDATE/DELETE on this table run alongside
    /// us. Scan the table; for each row, idempotent-insert into
    /// the new index via [`crate::storage::BTree::insert_if_absent`]
    /// — peer writers may have inserted some entries already (rows
    /// they themselves just inserted), and we skip those silently.
    /// Release.
    ///
    /// **Phase 3 (brief exclusive).** Take the write lock (waits
    /// for any in-flight phase-2-era writer to release). Flip
    /// `is_building` to `false`, marking the index as usable for
    /// access-path selection. Commit catalog. Release.
    ///
    /// **Why peers maintain the building index too.** If peers
    /// skipped it, a row inserted at phase-2-time-T would be invisible
    /// to both peers' subsequent reads (the index entry would be
    /// missing) AND to our scan (which was a snapshot at phase 1).
    /// Letting peers maintain it during phase 2 — combined with the
    /// idempotent insert in our scan — converges the index to the
    /// post-build table state.
    ///
    /// **Crash safety.** A crash during phase 2 leaves an
    /// `is_building = true` entry in the catalog with a partially
    /// populated B+tree. `Database::open` sweeps these on startup
    /// (drops the partial index entries + frees their B+tree
    /// pages); the user re-issues CREATE INDEX.
    fn create_index_online(
        &mut self,
        name: String,
        table: String,
        column_names: Vec<String>,
    ) -> Result<QueryResult> {
        // --- Phase 1: brief exclusive lock; create empty B+tree;
        // add Index{is_building: true} to schema; commit. ---
        let (columns, index_root, table_root, column_count) = {
            let lock = self.tx_state.table_lock(&table);
            let _guard = lock.write().expect("poisoned table lock");
            // Refresh our catalog in case a peer changed the schema
            // since we last looked.
            self.reload_for_write()?;
            let (columns, index_root) = crate::engine::executor::create_index_phase1(
                &mut self.pager,
                &self.catalog,
                &name,
                &table,
                &column_names,
            )?;
            self.pager.commit()?;
            // Pull the table root + column count out from the now-
            // committed schema, so phase 2 (which holds only a
            // shared lock) doesn't need to reach for the catalog.
            let schema = self
                .catalog
                .get(&mut self.pager, &table)?
                .ok_or_else(|| Error::corruption("table vanished after phase 1"))?;
            (columns, index_root, schema.root, schema.columns.len())
        };

        // --- Phase 2: shared lock; populate the index. ---
        // The scan can take a while; this is the whole point of
        // the split — peer writers proceed concurrently here.
        {
            let lock = self.tx_state.table_lock(&table);
            let _guard = lock.read().expect("poisoned table lock");
            crate::engine::executor::create_index_phase2_populate(
                &mut self.pager,
                table_root,
                column_count,
                &columns,
                index_root,
            )?;
            self.pager.commit()?;
        }

        // --- Phase 3: brief exclusive lock; flip is_building = false. ---
        {
            let lock = self.tx_state.table_lock(&table);
            let _guard = lock.write().expect("poisoned table lock");
            self.reload_for_write()?;
            crate::engine::executor::create_index_phase3_finalize(
                &mut self.pager,
                &self.catalog,
                &name,
                &table,
            )?;
            self.pager.commit()?;
        }

        Ok(QueryResult::Ack(format!(
            "index '{name}' created on {table}({})",
            column_names.join(", ")
        )))
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
                        unique: index.unique,
                        // VACUUM rebuilds completed indexes only. A
                        // building-state index would have been swept
                        // by `Database::open` long before VACUUM runs.
                        is_building: false,
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
                        primary_key_column: schema.primary_key_column,
                        mutations_since_analyze: schema.mutations_since_analyze,
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
        // v0.56: every cached AccessPath::IndexScan / table root in a
        // cached Plan now points at an obsolete page number — the
        // post-VACUUM file has the same logical data at fresh page
        // numbers. Invalidate every prepared statement so the next
        // Execute re-plans against the new layout.
        self.pager.shared_meta().bump_schema_version();
        Ok(QueryResult::Ack("database compacted".to_string()))
    }

    /// v0.57: rewrite the commit log to drop records strictly below
    /// the current oldest-active TX. Bounds the clog file size on
    /// long-running workloads. Returns the new floor (= captured
    /// `oldest_active_tx_id` at call time), or `Ok(0)` if there was
    /// nothing to truncate (an empty database, or the floor hasn't
    /// advanced since the last truncation).
    ///
    /// **Ordering matters.** This calls
    /// [`Database::reclaim_dead_rows`] first, so every row with
    /// `tx_min` below the floor that was rolled back is physically
    /// gone before the clog forgets the rollback. Without that
    /// ordering, a below-floor rolled-back row would suddenly
    /// become "visible" to every snapshot, because the post-truncate
    /// default for forgotten TXs is `Committed`. See
    /// [`crate::engine::clog::Clog::truncate_below`] for the full
    /// safety contract.
    ///
    /// **Why we capture `oldest_active` BEFORE the reclaim pass.**
    /// The reclaim pass holds per-table write locks for short
    /// windows; between tables, new TXs come and go. If we captured
    /// the watermark *after* the pass, a TX that was in-flight
    /// during reclaim but completed before the capture would have a
    /// new `oldest_active` *higher* than the value the reclaim pass
    /// actually operated against, and we'd over-truncate. Capturing
    /// before means the floor is exactly the watermark the reclaim
    /// pass was working with.
    ///
    /// Designed for the v0.36 background reclaimer thread in
    /// `prehnited` to call once per tick; library users can call it
    /// directly. Cheap when there's nothing to truncate (a quick
    /// HashMap iteration + an in-memory check inside the clog).
    pub fn truncate_clog(&mut self) -> Result<u64> {
        // Capture the floor *before* reclaim — see method doc.
        let floor = self.tx_state.oldest_active_tx_id();
        if floor == 0 {
            // Empty database, nothing to do.
            return Ok(0);
        }
        // Force a full reclamation pass first so every rolled-back
        // row below the floor is physically removed. The reclaimer
        // is already safe under concurrent foreground writes (it
        // takes per-table write locks); calling it here just makes
        // sure it runs to completion before we forget what those
        // transactions were.
        let _reclaimed = self.reclaim_dead_rows()?;
        // Now safe to forget TXs below the floor.
        self.tx_state.clog().truncate_below(floor)?;
        Ok(floor)
    }

    /// One incremental auto-analyze pass (v0.49). Walks the catalog,
    /// finds the first table whose `mutations_since_analyze` exceeds
    /// the staleness threshold (`50 + 0.10 * row_count`, the Postgres
    /// default), and runs `ANALYZE <table>` on it. Returns the name
    /// of the analyzed table, or `None` if every table is fresh
    /// enough.
    ///
    /// At most one ANALYZE per call so the background reclaimer
    /// thread doesn't hammer the catalog when many tables are
    /// simultaneously stale — repeated calls walk through them one
    /// per tick. The `prehnited` server invokes this once per
    /// reclaimer interval.
    ///
    /// Catalog read + ANALYZE both go through the normal write
    /// path, so concurrent INSERT/UPDATE/DELETE serialise the
    /// table's RwLock with the analyzing pass — no special locking
    /// needed.
    pub fn auto_analyze_pass(&mut self) -> Result<Option<String>> {
        let names = self.catalog.table_names(&mut self.pager)?;
        for name in names {
            let Some(schema) = self.catalog.get(&mut self.pager, &name)? else {
                continue;
            };
            // Postgres's autovacuum_analyze_threshold formula.
            // For an empty table (row_count == 0), threshold is 50;
            // tables grow it proportionally.
            let threshold = 50 + (schema.row_count as f64 * 0.10) as u64;
            if schema.mutations_since_analyze > threshold {
                let sql = format!("ANALYZE {name}");
                self.execute(&sql)?;
                return Ok(Some(name));
            }
        }
        Ok(None)
    }
}

/// Whether a plan writes (mutates state). Read-only plans skip the
/// TX-reservation path. CREATE/DROP/INSERT/UPDATE/DELETE all write; SELECT
/// reads; VACUUM is a special case handled outside this function.
fn plan_writes(plan: &Plan) -> bool {
    !matches!(plan, Plan::Select { .. })
}

/// v0.58: drop any partially-built indexes left behind by a CREATE
/// INDEX that didn't complete (crashed phase 2, or aborted phase 3).
/// The catalog still lists them with `is_building = true`, and their
/// B+tree exists but is incomplete; we free the B+tree and remove
/// the catalog entry. The user is expected to re-issue CREATE INDEX.
///
/// Idempotent — second invocation on the same database is a no-op.
/// Safe to call from `Database::open_with_pool` (which is the
/// "first connection" path); `Database::open_shared` deliberately
/// skips this because a peer connection might be mid-CREATE-INDEX
/// in the same process.
fn sweep_partial_indexes(pager: &mut crate::storage::Pager, catalog: &Catalog) -> Result<()> {
    let table_names = catalog.table_names(pager)?;
    let mut swept_any = false;
    for name in table_names {
        let Some(mut schema) = catalog.get(pager, &name)? else {
            continue;
        };
        let before = schema.indexes.len();
        // Free B+trees for any building indexes, then drop them from
        // the schema vec. `BTree::free_all` releases every page the
        // tree owns back to the pager's free list.
        for index in schema.indexes.iter().filter(|i| i.is_building) {
            BTree::open(index.root).free_all(pager)?;
        }
        schema.indexes.retain(|i| !i.is_building);
        if schema.indexes.len() != before {
            catalog.put(pager, &schema)?;
            swept_any = true;
        }
    }
    if swept_any {
        pager.commit()?;
        // No need to bump schema_version — this runs at open, before
        // any cached prepared plan exists.
    }
    Ok(())
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

    // ------------------------------------------------------------------
    // v0.55: prepared statements at the library layer.

    #[test]
    fn prepare_then_execute_runs_with_bound_params() {
        let tmp = TempDb::new();
        let mut db = tmp.open();
        db.execute("CREATE TABLE t (id INT, name TEXT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'ada'), (2, 'grace'), (3, 'edsger')")
            .unwrap();

        let handle = db.prepare("SELECT name FROM t WHERE id = ?").unwrap();
        // Same plan reused with three different parameter values.
        let r1 = rows(db.execute_prepared(handle, &[Value::Int(1)]).unwrap());
        let r2 = rows(db.execute_prepared(handle, &[Value::Int(2)]).unwrap());
        let r3 = rows(db.execute_prepared(handle, &[Value::Int(3)]).unwrap());
        assert_eq!(r1, vec![vec![Value::Text("ada".into())]]);
        assert_eq!(r2, vec![vec![Value::Text("grace".into())]]);
        assert_eq!(r3, vec![vec![Value::Text("edsger".into())]]);
    }

    #[test]
    fn prepared_handles_are_unique_and_monotonic() {
        let tmp = TempDb::new();
        let mut db = tmp.open();
        db.execute("CREATE TABLE t (n INT)").unwrap();
        let h1 = db.prepare("SELECT n FROM t").unwrap();
        let h2 = db.prepare("SELECT n FROM t WHERE n > ?").unwrap();
        let h3 = db.prepare("INSERT INTO t VALUES (?)").unwrap();
        // Strictly increasing — handles never collide, even after a
        // deallocate frees one.
        assert!(h1 < h2);
        assert!(h2 < h3);
        assert!(db.deallocate_prepared(h2));
        let h4 = db.prepare("SELECT n FROM t").unwrap();
        assert!(h4 > h3, "handle counter doesn't recycle");
    }

    #[test]
    fn execute_prepared_rejects_bad_handle() {
        let tmp = TempDb::new();
        let mut db = tmp.open();
        db.execute("CREATE TABLE t (n INT)").unwrap();
        let err = db.execute_prepared(9999, &[]).unwrap_err();
        assert!(format!("{err}").contains("9999"));
    }

    #[test]
    fn execute_prepared_propagates_arity_mismatch() {
        let tmp = TempDb::new();
        let mut db = tmp.open();
        db.execute("CREATE TABLE t (n INT)").unwrap();
        let handle = db.prepare("SELECT n FROM t WHERE n = ?").unwrap();
        let err = db.execute_prepared(handle, &[]).unwrap_err();
        assert!(format!("{err}").contains("placeholder"));
    }

    #[test]
    fn prepare_refuses_transaction_control_and_vacuum() {
        let tmp = TempDb::new();
        let mut db = tmp.open();
        assert!(db.prepare("BEGIN").is_err());
        assert!(db.prepare("COMMIT").is_err());
        assert!(db.prepare("ROLLBACK").is_err());
        assert!(db.prepare("VACUUM").is_err());
    }

    #[test]
    fn prepared_insert_persists_and_round_trips_via_executes() {
        let tmp = TempDb::new();
        let mut db = tmp.open();
        db.execute("CREATE TABLE t (id INT, label TEXT)").unwrap();

        let ins = db.prepare("INSERT INTO t VALUES (?, ?)").unwrap();
        db.execute_prepared(ins, &[Value::Int(1), Value::Text("alpha".into())])
            .unwrap();
        db.execute_prepared(ins, &[Value::Int(2), Value::Text("beta".into())])
            .unwrap();
        db.execute_prepared(ins, &[Value::Int(3), Value::Text("gamma".into())])
            .unwrap();

        let all = rows(db.execute("SELECT id, label FROM t ORDER BY id").unwrap());
        assert_eq!(all.len(), 3);
        assert_eq!(all[0][1], Value::Text("alpha".into()));
        assert_eq!(all[2][1], Value::Text("gamma".into()));
    }

    // ------------------------------------------------------------------
    // v0.56: schema-change invalidation of cached prepared plans.

    #[test]
    fn drop_table_invalidates_a_prepared_select() {
        let tmp = TempDb::new();
        let mut db = tmp.open();
        db.execute("CREATE TABLE t (n INT)").unwrap();
        db.execute("INSERT INTO t VALUES (1), (2)").unwrap();
        let h = db.prepare("SELECT n FROM t").unwrap();
        // Fresh execute still works.
        let r = rows(db.execute_prepared(h, &[]).unwrap());
        assert_eq!(r.len(), 2);
        // DROP bumps the global schema version; the cached plan now
        // points at a vanished table root.
        db.execute("DROP TABLE t").unwrap();
        let err = db.execute_prepared(h, &[]).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("stale") && msg.contains(&h.to_string()),
            "stale error didn't name the handle: {msg}"
        );
        // The stale entry was removed from the cache, so a second
        // execute returns the cleaner "no prepared statement" error.
        let err2 = db.execute_prepared(h, &[]).unwrap_err();
        let msg2 = format!("{err2}");
        assert!(
            !msg2.contains("stale"),
            "second execute should not be a stale error: {msg2}"
        );
        assert!(
            msg2.contains("no prepared statement"),
            "expected unknown-handle error, got: {msg2}"
        );
    }

    #[test]
    fn create_table_invalidates_all_existing_prepared_plans() {
        // v0.56 uses a global schema version — DDL on table A
        // invalidates plans on table B too. Conservative but simple.
        let tmp = TempDb::new();
        let mut db = tmp.open();
        db.execute("CREATE TABLE t (n INT)").unwrap();
        db.execute("INSERT INTO t VALUES (1)").unwrap();
        let h = db.prepare("SELECT n FROM t").unwrap();
        // Create an unrelated table.
        db.execute("CREATE TABLE u (x INT)").unwrap();
        let err = db.execute_prepared(h, &[]).unwrap_err();
        assert!(format!("{err}").contains("stale"));
    }

    #[test]
    fn analyze_invalidates_cached_plans() {
        // ANALYZE doesn't change column or index layout, but it does
        // change planner stats — so cached plans planned against the
        // old stats are invalidated. Postgres convention.
        let tmp = TempDb::new();
        let mut db = tmp.open();
        db.execute("CREATE TABLE t (n INT)").unwrap();
        for i in 0..20 {
            db.execute(&format!("INSERT INTO t VALUES ({i})")).unwrap();
        }
        let h = db.prepare("SELECT n FROM t WHERE n > ?").unwrap();
        // Fresh.
        let r = rows(db.execute_prepared(h, &[Value::Int(15)]).unwrap());
        assert_eq!(r.len(), 4);
        // ANALYZE bumps the schema version.
        db.execute("ANALYZE t").unwrap();
        let err = db.execute_prepared(h, &[Value::Int(15)]).unwrap_err();
        assert!(format!("{err}").contains("stale"));
    }

    #[test]
    fn create_index_invalidates_cached_plans() {
        // Even a new index on the prepared statement's own table —
        // CREATE INDEX changes which access paths the planner could
        // have picked, so the cached plan may now be suboptimal.
        let tmp = TempDb::new();
        let mut db = tmp.open();
        db.execute("CREATE TABLE t (id INT, name TEXT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
            .unwrap();
        let h = db.prepare("SELECT name FROM t WHERE id = ?").unwrap();
        db.execute_prepared(h, &[Value::Int(1)]).unwrap();
        db.execute("CREATE INDEX t_id_idx ON t (id)").unwrap();
        let err = db.execute_prepared(h, &[Value::Int(1)]).unwrap_err();
        assert!(format!("{err}").contains("stale"));
    }

    #[test]
    fn dml_does_not_invalidate_prepared_plans() {
        // INSERT/UPDATE/DELETE don't touch the schema_version — only
        // DDL/ANALYZE do. Without this, every Execute after any data
        // mutation would have to re-prepare, defeating the cache.
        let tmp = TempDb::new();
        let mut db = tmp.open();
        db.execute("CREATE TABLE t (n INT)").unwrap();
        let h = db.prepare("SELECT n FROM t").unwrap();
        // A long stream of pure-data mutations between Executes.
        for i in 0..50 {
            db.execute(&format!("INSERT INTO t VALUES ({i})")).unwrap();
        }
        db.execute("UPDATE t SET n = 999 WHERE n = 1").unwrap();
        db.execute("DELETE FROM t WHERE n = 0").unwrap();
        let r = rows(db.execute_prepared(h, &[]).unwrap());
        assert_eq!(r.len(), 49);
    }

    #[test]
    fn re_prepare_after_invalidation_succeeds() {
        let tmp = TempDb::new();
        let mut db = tmp.open();
        db.execute("CREATE TABLE t (n INT)").unwrap();
        db.execute("INSERT INTO t VALUES (1), (2), (3)").unwrap();
        let h1 = db.prepare("SELECT n FROM t").unwrap();
        // Force a stale handle.
        db.execute("CREATE INDEX t_n_idx ON t (n)").unwrap();
        assert!(db.execute_prepared(h1, &[]).is_err());
        // Re-prepare against the now-current catalog: new handle, works.
        let h2 = db.prepare("SELECT n FROM t").unwrap();
        assert!(h2 != h1, "re-prepare returned the same handle");
        let r = rows(db.execute_prepared(h2, &[]).unwrap());
        assert_eq!(r.len(), 3);
    }

    // ------------------------------------------------------------------
    // v0.58: online CREATE INDEX.

    #[test]
    fn create_index_online_builds_a_usable_index() {
        let tmp = TempDb::new();
        let mut db = tmp.open();
        db.execute("CREATE TABLE t (id INT, name TEXT)").unwrap();
        for i in 0..200 {
            db.execute(&format!("INSERT INTO t VALUES ({i}, 'row{i}')"))
                .unwrap();
        }

        // Build the index.
        db.execute("CREATE INDEX t_id_idx ON t (id)").unwrap();

        // Lookups via the index match a full scan. The planner picks
        // the index for an `=` predicate on the indexed column.
        let r = rows(
            db.execute("SELECT name FROM t WHERE id = 42")
                .unwrap(),
        );
        assert_eq!(r, vec![vec![Value::Text("row42".into())]]);

        // ANALYZE works (it touches every index).
        db.execute("ANALYZE t").unwrap();
    }

    #[test]
    fn create_index_online_refuses_in_transaction() {
        let tmp = TempDb::new();
        let mut db = tmp.open();
        db.execute("CREATE TABLE t (id INT)").unwrap();
        db.execute("BEGIN").unwrap();
        let err = db
            .execute("CREATE INDEX t_id_idx ON t (id)")
            .unwrap_err();
        assert!(format!("{err}").contains("CREATE INDEX"));
        db.execute("ROLLBACK").unwrap();
        // The table is still usable.
        db.execute("INSERT INTO t VALUES (1)").unwrap();
    }

    #[test]
    fn create_index_online_invalidates_cached_plans_via_schema_version() {
        // v0.56 + v0.58 interaction: CREATE INDEX bumps the schema
        // version at phase 1 (when is_building=true is committed)
        // and at phase 3 (when is_building flips to false). Either
        // bump invalidates a prepared plan that was tagged before.
        let tmp = TempDb::new();
        let mut db = tmp.open();
        db.execute("CREATE TABLE t (id INT, name TEXT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
            .unwrap();
        let h = db.prepare("SELECT name FROM t WHERE id = ?").unwrap();
        db.execute("CREATE INDEX t_id_idx ON t (id)").unwrap();
        let err = db
            .execute_prepared(h, &[Value::Int(1)])
            .unwrap_err();
        assert!(format!("{err}").contains("stale"));
    }

    #[test]
    fn create_index_online_skipped_by_planner_while_building() {
        // The planner mustn't pick a building index as an access
        // path. We can't easily orchestrate "build is in flight"
        // from a single-connection test, but we can hand-craft a
        // schema with is_building=true and confirm the planner
        // skips it via EXPLAIN.
        //
        // Drive it via the public surface: insert a small table,
        // CREATE INDEX (which completes and sets is_building=false),
        // then forcibly mark the index as building via a fresh
        // schema put. EXPLAIN must then say FullScan, not IndexScan.
        let tmp = TempDb::new();
        let mut db = tmp.open();
        db.execute("CREATE TABLE t (id INT, name TEXT)").unwrap();
        for i in 0..10 {
            db.execute(&format!("INSERT INTO t VALUES ({i}, 'r')"))
                .unwrap();
        }
        db.execute("CREATE INDEX t_id_idx ON t (id)").unwrap();
        // Sanity: with the index built, EXPLAIN picks it.
        let plan_with_idx =
            rows(db.execute("EXPLAIN SELECT name FROM t WHERE id = 5").unwrap());
        let text = format!("{plan_with_idx:?}");
        assert!(
            text.contains("IndexScan") || text.contains("Index"),
            "should pick the index when not building: {text}"
        );

        // Force the index back into building state. (Real workloads
        // hit this naturally during phase 2 — this test just
        // simulates the planner-visible state.)
        let mut schema = db.catalog.get(&mut db.pager, "t").unwrap().unwrap();
        for index in &mut schema.indexes {
            if index.name == "t_id_idx" {
                index.is_building = true;
            }
        }
        db.catalog.put(&mut db.pager, &schema).unwrap();
        db.pager.commit().unwrap();
        db.pager.shared_meta().bump_schema_version();

        // Now EXPLAIN must fall back to FullScan — the building
        // index is invisible to access-path selection.
        let plan_building =
            rows(db.execute("EXPLAIN SELECT name FROM t WHERE id = 5").unwrap());
        let text = format!("{plan_building:?}");
        assert!(
            text.contains("FullScan") || !text.contains("IndexScan"),
            "should NOT pick a building index: {text}"
        );
    }

    #[test]
    fn open_sweeps_partial_indexes_left_by_a_crashed_build() {
        let tmp = TempDb::new();
        {
            let mut db = tmp.open();
            db.execute("CREATE TABLE t (id INT)").unwrap();
            db.execute("INSERT INTO t VALUES (1), (2), (3)").unwrap();
            db.execute("CREATE INDEX t_id_idx ON t (id)").unwrap();
            // Simulate a crashed mid-build by forcing the index
            // back into is_building=true and dropping the Database
            // without finishing.
            let mut schema = db.catalog.get(&mut db.pager, "t").unwrap().unwrap();
            for index in &mut schema.indexes {
                if index.name == "t_id_idx" {
                    index.is_building = true;
                }
            }
            db.catalog.put(&mut db.pager, &schema).unwrap();
            db.pager.commit().unwrap();
            // db drops here without a successful build.
        }
        // Reopen: the sweep at Database::open should have dropped
        // the building index. The user can now re-issue CREATE INDEX.
        let mut db = tmp.open();
        let schema = db.catalog.get(&mut db.pager, "t").unwrap().unwrap();
        assert!(
            schema.indexes.iter().all(|i| i.name != "t_id_idx"),
            "open should have swept the building index, found: {:?}",
            schema.indexes
        );
        // Re-issue: works.
        db.execute("CREATE INDEX t_id_idx ON t (id)").unwrap();
        let r = rows(
            db.execute("SELECT id FROM t WHERE id = 2")
                .unwrap(),
        );
        assert_eq!(r, vec![vec![Value::Int(2)]]);
    }

    #[test]
    fn deallocate_invalidates_the_handle() {
        let tmp = TempDb::new();
        let mut db = tmp.open();
        db.execute("CREATE TABLE t (n INT)").unwrap();
        let handle = db.prepare("SELECT n FROM t").unwrap();
        // First execute succeeds.
        rows(db.execute_prepared(handle, &[]).unwrap());
        // Deallocate returns true, repeats return false.
        assert!(db.deallocate_prepared(handle));
        assert!(!db.deallocate_prepared(handle));
        // Now the handle is stale.
        assert!(db.execute_prepared(handle, &[]).is_err());
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
