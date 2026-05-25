//! The executor — it runs a [`Plan`] against the storage engine.
//!
//! Rows are reached one of two ways, chosen by the planner: a full table scan,
//! or a bounded scan of a secondary index. Either way the statement's `WHERE`
//! clause is then applied in full, so an index only ever *narrows* the
//! candidate set; it never changes an answer.
//!
//! A `SELECT` runs as a *volcano* tree of pull-based operators: each `next`
//! call draws one row up from the operator below, so rows stream through the
//! pipeline a row at a time — nothing is materialized except where an operator
//! must buffer, as `Sort`, the grouped path, and a full-scan join's inner side
//! do. `INSERT` / `UPDATE` / `DELETE` instead gather their rows up front, which
//! in-place mutation needs.
//!
//! Expression evaluation follows SQL's three-valued logic: `NULL` propagates
//! through arithmetic and comparisons, and a `WHERE` clause keeps a row only
//! when its predicate evaluates to exactly `TRUE`.

use std::cmp::Ordering;
use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::hash::{BuildHasher, Hash, Hasher};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;

use crate::engine::batch::{Column as BatchColumn, ColumnBatch, NullMask, BATCH_SIZE};
use crate::engine::catalog::Catalog;
use crate::engine::codec;
use crate::engine::explain::{format_plan, format_plan_analyzed, AnalyzeStats};
use crate::engine::planner::{AccessPath, Plan};
use crate::engine::schema::{Column, Index, Schema};
use crate::engine::transaction::Snapshot;
use crate::engine::value::{coerce, Type, Value};
use crate::error::{Error, Result};
use crate::sql::ast::{
    Aggregate, AggregateArg, AggregateFunc, BinaryOp, ColumnRef, Expr, FromClause, JoinKind,
    OrderKey, Projection, SelectItem, Statement, UnaryOp,
};
use crate::storage::{BTree, Cursor, Pager};

/// The outcome of executing one statement.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryResult {
    /// A statement that changed state, with a human-readable summary.
    Ack(String),
    /// A result set produced by `SELECT`.
    Rows {
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
}

/// The outcome of executing one statement, streaming-friendly: an
/// acknowledgement, or a [`RowStream`] the caller pulls rows from.
pub enum Execution {
    /// A statement that changed state, with a human-readable summary.
    Ack(String),
    /// A result set, pulled a row at a time.
    Rows(RowStream),
}

/// A result set in the making — pulled one row at a time, so a `SELECT` of a
/// huge table need never be held whole in memory.
pub struct RowStream {
    columns: Vec<String>,
    source: RowSource,
}

/// Where a [`RowStream`]'s rows come from.
enum RowSource {
    /// A plain `SELECT`: pull the volcano operator tree directly.
    Volcano(Box<dyn Operator>),
    /// A grouped or aggregated `SELECT`: that pass is a pipeline breaker, so
    /// its rows are already materialized and handed out one at a time here.
    Buffered(std::vec::IntoIter<Vec<Value>>),
}

impl RowStream {
    /// The result set's column headers.
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// The next row, or `None` once the result set is exhausted.
    pub fn next(&mut self, pager: &mut Pager) -> Result<Option<Vec<Value>>> {
        match &mut self.source {
            RowSource::Volcano(op) => op.next(pager),
            RowSource::Buffered(rows) => Ok(rows.next()),
        }
    }
}

/// Run a planned statement, streaming: a `SELECT` returns a [`RowStream`] whose
/// rows are pulled on demand, so the executor never holds the whole result;
/// every other statement runs to completion and returns an `Ack`.
///
/// `snapshot` carries the visibility frame for the read side and the
/// writer's `own_tx` for the write side. Scans filter rows against the
/// snapshot; INSERT/DELETE/UPDATE stamp `tx_min`/`tx_max` with `own_tx`.
pub fn execute_streaming(
    pager: &mut Pager,
    catalog: &Catalog,
    snapshot: &Snapshot,
    plan: Plan,
) -> Result<Execution> {
    let ack = |result: Result<QueryResult>| match result? {
        QueryResult::Ack(message) => Ok(Execution::Ack(message)),
        QueryResult::Rows { .. } => unreachable!("only SELECT produces rows"),
    };
    match plan {
        Plan::CreateTable {
            name,
            columns,
            primary_key_column,
            unique_columns,
        } => ack(create_table(
            pager,
            catalog,
            name,
            columns,
            primary_key_column,
            unique_columns,
        )),
        Plan::DropTable { name } => ack(drop_table(pager, catalog, name)),
        Plan::CreateIndex {
            name,
            table,
            columns,
        } => ack(create_index(pager, catalog, name, table, columns, snapshot)),
        Plan::DropIndex { name } => ack(drop_index(pager, catalog, name)),
        Plan::Insert {
            table,
            columns,
            rows,
        } => ack(insert(pager, catalog, table, columns, rows, snapshot)),
        Plan::Select {
            from,
            projection,
            filter,
            access,
            group_by,
            having,
            order_by,
            presorted,
            limit,
            offset,
        } => Ok(Execution::Rows(select(
            pager, catalog, from, projection, filter, access, group_by, having, order_by,
            presorted, limit, offset, snapshot, None,
        )?)),
        Plan::Update {
            table,
            assignments,
            filter,
            access,
        } => ack(update(
            pager,
            catalog,
            table,
            assignments,
            filter,
            access,
            snapshot,
        )),
        Plan::Delete {
            table,
            filter,
            access,
        } => ack(delete(pager, catalog, table, filter, access, snapshot)),
        Plan::Vacuum => unreachable!("VACUUM is handled by Database::execute"),
        Plan::Analyze { table } => ack(analyze_table(pager, catalog, table)),
        Plan::Explain { inner, analyze } => Ok(Execution::Rows(explain(
            pager, catalog, snapshot, inner, analyze,
        )?)),
    }
}

/// Format an inner Plan as a `QueryResult::Rows` for `EXPLAIN`. One
/// column (`QUERY PLAN`), one row per line of the formatted tree.
///
/// When `analyze` is true, the inner Plan is **actually executed** —
/// the resulting row stream is drained to completion (timed with a
/// monotonic clock), and the formatted output picks up an
/// `actual: N` annotation on the root operator plus an
/// `Execution time: X.XXX ms` footer. Reads done by the inner
/// statement participate in the caller's snapshot exactly as a
/// normal `SELECT` does — SSI conflict edges and relation locks are
/// recorded normally — because EXPLAIN ANALYZE *is* a read, just one
/// the user asked the engine to also describe.
fn explain(
    pager: &mut Pager,
    catalog: &Catalog,
    snapshot: &Snapshot,
    inner: Box<Plan>,
    analyze: bool,
) -> Result<RowStream> {
    let text = if analyze {
        run_analyze(pager, catalog, snapshot, &inner)?
    } else {
        format_plan(pager, catalog, &inner)?
    };
    let rows: Vec<Vec<Value>> = text
        .lines()
        .map(|line| vec![Value::Text(line.to_string())])
        .collect();
    Ok(RowStream {
        columns: vec!["QUERY PLAN".to_string()],
        source: RowSource::Buffered(rows.into_iter()),
    })
}

/// Run the inner SELECT under [`OperatorCounters`] instrumentation,
/// time it with a monotonic clock, drain the row stream, snapshot the
/// counters, and hand the whole bundle to
/// [`crate::engine::explain::format_plan_analyzed`].
///
/// The parser restricts EXPLAIN's inner statement to SELECT, so the
/// inner Plan is always [`Plan::Select`] in v0.41. If a future version
/// loosens that restriction, the catch-all in this function falls back
/// to the v0.40 behaviour (execute via `execute_streaming`, report
/// total only) so the feature keeps working.
fn run_analyze(
    pager: &mut Pager,
    catalog: &Catalog,
    snapshot: &Snapshot,
    inner: &Plan,
) -> Result<String> {
    let start = std::time::Instant::now();

    // The fast path: inner is a SELECT we can instrument per operator.
    if let Plan::Select {
        from,
        projection,
        filter,
        access,
        group_by,
        having,
        order_by,
        presorted,
        limit,
        offset,
    } = inner
    {
        let mut counters = OperatorCounters::new();
        let mut stream = select(
            pager,
            catalog,
            from.clone(),
            projection.clone(),
            filter.clone(),
            access.clone(),
            group_by.clone(),
            having.clone(),
            order_by.clone(),
            *presorted,
            *limit,
            *offset,
            snapshot,
            Some(&mut counters),
        )?;
        let mut actual_rows: u64 = 0;
        while stream.next(pager)?.is_some() {
            actual_rows += 1;
        }
        let elapsed = start.elapsed();
        let actuals = counters.snapshot();
        return format_plan_analyzed(
            pager,
            catalog,
            inner,
            AnalyzeStats {
                actual_rows,
                elapsed,
            },
            Some(actuals),
        );
    }

    // Fallback: inner isn't a SELECT (impossible today per the parser
    // restriction, but kept as a safety net). Run through
    // `execute_streaming`, report only the total.
    let exec = execute_streaming(pager, catalog, snapshot, inner.clone())?;
    let actual_rows = match exec {
        Execution::Rows(mut stream) => {
            let mut count: u64 = 0;
            while stream.next(pager)?.is_some() {
                count += 1;
            }
            count
        }
        Execution::Ack(_) => {
            return Err(Error::corruption(
                "EXPLAIN ANALYZE inner statement did not return rows",
            ));
        }
    };
    let elapsed = start.elapsed();
    format_plan_analyzed(
        pager,
        catalog,
        inner,
        AnalyzeStats {
            actual_rows,
            elapsed,
        },
        None,
    )
}

/// Run a planned statement, materializing a `SELECT`'s rows into a
/// [`QueryResult`]. This is the embedding API; the server streams instead, via
/// [`execute_streaming`].
pub fn execute(
    pager: &mut Pager,
    catalog: &Catalog,
    snapshot: &Snapshot,
    plan: Plan,
) -> Result<QueryResult> {
    match execute_streaming(pager, catalog, snapshot, plan)? {
        Execution::Ack(message) => Ok(QueryResult::Ack(message)),
        Execution::Rows(mut stream) => {
            let columns = stream.columns().to_vec();
            let mut rows = Vec::new();
            while let Some(row) = stream.next(pager)? {
                rows.push(row);
            }
            Ok(QueryResult::Rows { columns, rows })
        }
    }
}

fn create_table(
    pager: &mut Pager,
    catalog: &Catalog,
    name: String,
    columns: Vec<Column>,
    primary_key_column: Option<usize>,
    unique_columns: Vec<usize>,
) -> Result<QueryResult> {
    if catalog.get(pager, &name)?.is_some() {
        return Err(Error::exec(format!("table '{name}' already exists")));
    }
    let tree = BTree::create(pager)?;
    // Auto-create the constraint-implied unique indexes. The PK
    // (if any) gets `_pk_<table>`; every other `UNIQUE` column gets
    // `_uq_<table>_<col>`. These indexes are real secondary indexes
    // — the planner and executor see them like any other — but with
    // `unique = true`, which makes the B+tree refuse duplicate keys.
    let mut indexes = Vec::new();
    if let Some(pk) = primary_key_column {
        let pk_index = BTree::create(pager)?;
        indexes.push(Index {
            name: format!("_pk_{name}"),
            columns: vec![pk],
            root: pk_index.root(),
            unique: true,
        });
    }
    for &col_idx in &unique_columns {
        let col_name = &columns[col_idx].name;
        let uq_index = BTree::create(pager)?;
        indexes.push(Index {
            name: format!("_uq_{name}_{col_name}"),
            columns: vec![col_idx],
            root: uq_index.root(),
            unique: true,
        });
    }
    let schema = Schema {
        name: name.clone(),
        columns,
        root: tree.root(),
        next_rowid: 1,
        row_count: 0,
        indexes,
        primary_key_column,
        mutations_since_analyze: 0,
    };
    catalog.put(pager, &schema)?;
    Ok(QueryResult::Ack(format!("table '{name}' created")))
}

fn drop_table(pager: &mut Pager, catalog: &Catalog, name: String) -> Result<QueryResult> {
    let schema = require_table(pager, catalog, &name)?;
    // v0.45: refuse DROP TABLE if any other table has a FOREIGN KEY
    // pointing at this one — dropping would leave orphan FK targets
    // in the catalog. The user must DROP the child first, or drop
    // the FK column itself (the latter is future work — ALTER TABLE).
    if let Some((child_table, child_column)) = child_referencing(pager, catalog, &name)? {
        return Err(Error::exec(format!(
            "cannot drop table '{name}': it is referenced by FOREIGN KEY '{child_table}.{child_column}'"
        )));
    }
    BTree::open(schema.root).free_all(pager)?;
    // Every secondary index has its own B+tree to reclaim.
    for index in &schema.indexes {
        BTree::open(index.root).free_all(pager)?;
    }
    catalog.remove(pager, &name)?;
    Ok(QueryResult::Ack(format!("table '{name}' dropped")))
}

fn create_index(
    pager: &mut Pager,
    catalog: &Catalog,
    index_name: String,
    table: String,
    column_names: Vec<String>,
    _snapshot: &Snapshot,
) -> Result<QueryResult> {
    let mut schema = require_table(pager, catalog, &table)?;
    let mut columns = Vec::with_capacity(column_names.len());
    for name in &column_names {
        let column = column_index(&schema, name)?;
        if columns.contains(&column) {
            return Err(Error::exec(format!(
                "index '{index_name}' names column '{name}' twice"
            )));
        }
        columns.push(column);
    }
    if catalog.table_with_index(pager, &index_name)?.is_some() {
        return Err(Error::exec(format!("index '{index_name}' already exists")));
    }

    // Populate the new index from the table's existing rows. Index entries
    // are added for every physical row, including those logically deleted
    // by some prior transaction — visibility is rechecked on the table side
    // after an index lookup, so a tombstoned row is harmless in the index.
    let index = BTree::create(pager)?;
    let table_tree = BTree::open(schema.root);
    for (rowid_key, encoded) in table_tree.scan(pager)? {
        let record = codec::decode_row(&encoded, schema.columns.len())?;
        let key = codec::encode_index_key(&record.values, &columns, &rowid_key);
        index.insert(pager, &key, &[])?;
    }

    schema.indexes.push(Index {
        name: index_name.clone(),
        columns,
        root: index.root(),
        // User-created indexes via `CREATE INDEX` are non-unique.
        // The unique flag is reserved for auto-created PK/UNIQUE
        // constraint indexes (v0.43). A future `CREATE UNIQUE INDEX`
        // syntax could change this.
        unique: false,
    });
    catalog.put(pager, &schema)?;
    Ok(QueryResult::Ack(format!(
        "index '{index_name}' created on {table}({})",
        column_names.join(", ")
    )))
}

fn drop_index(pager: &mut Pager, catalog: &Catalog, index_name: String) -> Result<QueryResult> {
    let (mut schema, position) = catalog
        .table_with_index(pager, &index_name)?
        .ok_or_else(|| Error::exec(format!("no such index: '{index_name}'")))?;
    BTree::open(schema.indexes[position].root).free_all(pager)?;
    schema.indexes.remove(position);
    catalog.put(pager, &schema)?;
    Ok(QueryResult::Ack(format!("index '{index_name}' dropped")))
}

/// `ANALYZE <table>` (v0.47) — scan every visible row, compute
/// per-column statistics, persist them on the schema. The next
/// `EXPLAIN` consults those stats via [`crate::engine::explain`]'s
/// selectivity estimator.
///
/// Strategy: one table scan, per-column collect (Value, count) of
/// the values seen. After the scan, for each column sort the
/// non-NULL values, compute `n_distinct`, then split into N=16
/// equi-depth buckets — each bucket holds roughly `non_null / N`
/// rows, with bucket widths varying so each holds the same count.
///
/// Bounded memory: O(table_rows × column_count) in the worst case
/// (one Value per cell). v0.47 accepts that — a future version
/// could swap to streaming reservoir-sample histograms. The full
/// scan is also the simplest way to capture an exact picture of
/// the table at one snapshot.
///
/// v0.47 ignores MVCC visibility at ANALYZE time — it counts every
/// row physically present in the B+tree, including tombstoned and
/// rolled-back rows. That's the same convention `Schema::row_count`
/// uses, and slightly imprecise but consistent with the existing
/// stats. A future version could pass through a Snapshot.
fn analyze_table(pager: &mut Pager, catalog: &Catalog, table: String) -> Result<QueryResult> {
    /// Number of equi-depth buckets per column. 16 is a good balance:
    /// fine-grained enough for range estimates to differentiate
    /// "small slice" from "most of the table", coarse enough that
    /// the per-column blob stays compact (16 buckets ≈ a few hundred
    /// bytes per column).
    const BUCKETS: usize = 16;

    let mut schema = require_table(pager, catalog, &table)?;
    let column_count = schema.columns.len();

    // Per-column value buffer. Values include NULL so we can count
    // null_count separately before sorting the non-NULLs.
    let mut per_column: Vec<Vec<Value>> = vec![Vec::new(); column_count];
    let tree = BTree::open(schema.root);
    let mut total_rows: u64 = 0;
    for (_, encoded) in tree.scan(pager)? {
        let record = codec::decode_row(&encoded, column_count)?;
        total_rows += 1;
        for (col_idx, value) in record.values.into_iter().enumerate() {
            per_column[col_idx].push(value);
        }
    }

    for (col_idx, values) in per_column.into_iter().enumerate() {
        let null_count: u64 = values
            .iter()
            .filter(|v| matches!(v, Value::Null))
            .count() as u64;
        let mut non_null: Vec<Value> = values
            .into_iter()
            .filter(|v| !matches!(v, Value::Null))
            .collect();
        // Sort using the order-preserving byte encoding the B+tree
        // uses for index keys — Value doesn't impl PartialOrd, and
        // every column has one declared type so values share a tag.
        // `encode_index_value` gives total order for any one type.
        non_null.sort_by(|a, b| {
            codec::encode_index_value(a).cmp(&codec::encode_index_value(b))
        });

        let n_distinct = count_distinct(&non_null);
        let histogram = build_equi_depth(&non_null, BUCKETS);

        schema.columns[col_idx].stats = Some(crate::engine::schema::ColumnStats {
            n_distinct,
            null_count,
            total_rows,
            histogram,
        });
    }

    // v0.49: ANALYZE refreshed the stats, so the mutation counter
    // resets — the auto-analyze trigger in the reclaimer thread will
    // wait until enough new mutations accumulate before re-firing.
    schema.mutations_since_analyze = 0;
    catalog.put(pager, &schema)?;
    Ok(QueryResult::Ack(format!(
        "table '{table}' analyzed ({total_rows} rows, {column_count} columns)"
    )))
}

/// Count distinct values in a sorted slice. O(n) — one pass,
/// counting transitions.
fn count_distinct(sorted: &[Value]) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let mut count: u64 = 1;
    for window in sorted.windows(2) {
        if window[0] != window[1] {
            count += 1;
        }
    }
    count
}

/// Build an equi-depth histogram of `buckets` buckets over `sorted`
/// (a sorted, non-NULL values slice). Each bucket holds roughly
/// `sorted.len() / buckets` rows; bucket widths vary so the counts
/// stay even. Returns one bucket per non-empty range — an empty
/// `sorted` returns an empty histogram.
fn build_equi_depth(
    sorted: &[Value],
    buckets: usize,
) -> Vec<crate::engine::schema::HistogramBucket> {
    use crate::engine::schema::HistogramBucket;
    if sorted.is_empty() {
        return Vec::new();
    }
    let total = sorted.len();
    let per_bucket = (total + buckets - 1) / buckets; // ceil
    let mut out = Vec::with_capacity(buckets.min(total));
    let mut start = 0;
    while start < total {
        let end = (start + per_bucket).min(total);
        out.push(HistogramBucket {
            lower: sorted[start].clone(),
            upper: sorted[end - 1].clone(),
            count: (end - start) as u64,
        });
        start = end;
    }
    out
}

fn insert(
    pager: &mut Pager,
    catalog: &Catalog,
    table: String,
    columns: Option<Vec<String>>,
    rows: Vec<Vec<Expr>>,
    snapshot: &Snapshot,
) -> Result<QueryResult> {
    let mut schema = require_table(pager, catalog, &table)?;
    let tx_min = snapshot
        .own_tx
        .ok_or_else(|| Error::corruption("INSERT reached executor without a write TX"))?;

    // Map each value position in a VALUES tuple to a schema column index.
    let targets: Vec<usize> = match &columns {
        Some(names) => {
            let mut indices = Vec::with_capacity(names.len());
            for name in names {
                indices.push(column_index(&schema, name)?);
            }
            indices
        }
        None => (0..schema.columns.len()).collect(),
    };

    let tree = BTree::open(schema.root);
    let mut inserted = 0u64;
    for row_exprs in &rows {
        if columns.is_none() && row_exprs.len() != schema.columns.len() {
            return Err(Error::exec(format!(
                "table '{table}' has {} column(s) but {} value(s) were given",
                schema.columns.len(),
                row_exprs.len()
            )));
        }
        let mut values = vec![Value::Null; schema.columns.len()];
        for (slot, expr) in row_exprs.iter().enumerate() {
            let column = targets[slot];
            let evaluated = eval(expr, None)?;
            values[column] = coerce(evaluated, schema.columns[column].ty)?;
        }
        // v0.43: NOT NULL constraint check. A column declared `NOT
        // NULL` (or `PRIMARY KEY`, which implies it) must have a
        // non-NULL value in every inserted row. When INSERT omits a
        // column from the explicit column list, `values[column]`
        // stays `Value::Null` from initialisation — which a NOT NULL
        // column refuses.
        for (col_idx, column) in schema.columns.iter().enumerate() {
            if column.not_null && matches!(values[col_idx], Value::Null) {
                return Err(Error::exec(format!(
                    "null value in column '{}' of '{}' violates NOT NULL constraint",
                    column.name, table
                )));
            }
        }
        // v0.45: FOREIGN KEY check. For every FK column with a
        // non-NULL value, the parent row must exist.
        check_foreign_keys(pager, catalog, &schema, &values)?;
        // Reserve a unique rowid through the shared atomic counter,
        // not the local schema. Two writers on the same table each
        // bumping `schema.next_rowid` locally would collide on rowids
        // and silently overwrite each other's inserts.
        let rowid = snapshot.reserve_rowid(&table, schema.next_rowid);
        let rowid_key = codec::rowid_key(rowid);
        tree.insert(pager, &rowid_key, &codec::encode_row(tx_min, 0, &values))?;
        index_insert_row(pager, &schema, &rowid_key, &values)?;
        // SSI v0.35: phantom detection. The new row's rowid was minted
        // here and is in no peer's read set as a `Tuple` — but any
        // peer that has scanned the table holds a `Relation` lock,
        // and this insert is the phantom they would have seen had
        // their scan come after us. `record_insert` walks peers'
        // read sets for `Relation(schema.root)` matches and marks
        // the rw-edges.
        snapshot.record_insert(schema.root);
        inserted += 1;
    }

    schema.next_rowid = snapshot.current_next_rowid(&table, schema.next_rowid);
    schema.row_count += inserted;
    // v0.49: auto-analyze counter. The reclaimer thread consults
    // this against the threshold `50 + 0.10 * row_count` and triggers
    // ANALYZE in the background when crossed.
    schema.mutations_since_analyze =
        schema.mutations_since_analyze.saturating_add(inserted);
    catalog.put(pager, &schema)?;
    Ok(QueryResult::Ack(format!("{inserted} row(s) inserted")))
}

/// The columns visible to a query's expressions: the concatenation of every
/// table in the `FROM` clause, each column tagged with its table's qualifier
/// (its alias, or its name) and its type. A single-table query has a one-table
/// scope; a join's scope spans all the joined tables.
#[derive(Clone)]
struct Scope {
    columns: Vec<ScopedColumn>,
}

#[derive(Clone)]
struct ScopedColumn {
    /// How a qualified reference must name this column's table.
    table: String,
    name: String,
    ty: Type,
}

impl Scope {
    /// A scope holding one table's columns, reached through `qualifier`.
    fn single(qualifier: &str, schema: &Schema) -> Scope {
        let mut scope = Scope {
            columns: Vec::new(),
        };
        scope.extend(qualifier, schema);
        scope
    }

    /// Append another table's columns — used as each join is added.
    fn extend(&mut self, qualifier: &str, schema: &Schema) {
        for column in &schema.columns {
            self.columns.push(ScopedColumn {
                table: qualifier.to_string(),
                name: column.name.clone(),
                ty: column.ty,
            });
        }
    }

    fn len(&self) -> usize {
        self.columns.len()
    }

    /// Whether some table already occupies this qualifier in the scope.
    fn has_qualifier(&self, qualifier: &str) -> bool {
        self.columns.iter().any(|c| c.table == qualifier)
    }

    /// Resolve a column reference to its position in a joined row. A qualified
    /// reference must match table *and* name; a bare one matches by name and
    /// is rejected as ambiguous if more than one table offers it.
    fn resolve(&self, colref: &ColumnRef) -> Result<usize> {
        let mut found = None;
        for (i, column) in self.columns.iter().enumerate() {
            let table_matches = match &colref.table {
                Some(qualifier) => qualifier == &column.table,
                None => true,
            };
            if table_matches && column.name == colref.name {
                if found.is_some() {
                    return Err(Error::exec(format!(
                        "column reference '{colref}' is ambiguous"
                    )));
                }
                found = Some(i);
            }
        }
        found.ok_or_else(|| Error::exec(format!("no such column: '{colref}'")))
    }

    fn column_type(&self, index: usize) -> Type {
        self.columns[index].ty
    }

    fn column_name(&self, index: usize) -> &str {
        &self.columns[index].name
    }

    /// Whether the scope spans more than one table.
    fn is_join(&self) -> bool {
        self.columns
            .iter()
            .any(|c| c.table != self.columns[0].table)
    }

    /// The output header for column `index` — qualified when the scope is a
    /// join, so a `SELECT *` over `a JOIN b` does not collapse two `id`s.
    fn header(&self, index: usize) -> String {
        let column = &self.columns[index];
        if self.is_join() {
            format!("{}.{}", column.table, column.name)
        } else {
            column.name.clone()
        }
    }
}

/// Resolve the `FROM` clause: build the operator subtree that produces joined
/// rows, paired with the [`Scope`] describing those rows' columns. The base
/// table uses `base_access` (possibly an index); every joined table is
/// full-scanned.
///
/// `instrument`, when `Some`, asks `build_from` to wrap each operator
/// it constructs (the base scan, every join, and every join's right
/// scan if it has one) with a [`Counting`] adapter, populating the
/// caller's [`OperatorCounters`] so EXPLAIN ANALYZE can report
/// per-node observed cardinalities.
fn build_from(
    pager: &mut Pager,
    catalog: &Catalog,
    from: &FromClause,
    base_access: &AccessPath,
    snapshot: &Snapshot,
    mut instrument: Option<&mut OperatorCounters>,
) -> Result<(Box<dyn Operator>, Scope)> {
    let base_schema = require_table(pager, catalog, &from.table.name)?;
    let mut scope = Scope::single(from.table.qualifier(), &base_schema);
    let mut op = scan_operator(pager, &base_schema, base_access, snapshot.clone())?;
    if let Some(counters) = instrument.as_deref_mut() {
        op = wrap_into(op, &mut counters.base_scan);
    }

    for join in &from.joins {
        let joined_schema = require_table(pager, catalog, &join.table.name)?;
        let qualifier = join.table.qualifier().to_string();
        if scope.has_qualifier(&qualifier) {
            return Err(Error::exec(format!(
                "table name or alias '{qualifier}' is used twice in FROM"
            )));
        }
        let right_width = joined_schema.columns.len();
        // The left scope (before this table joins) and the combined scope
        // (after) — the ON predicate is evaluated against the latter.
        // We keep a second `left_scope` clone for the
        // semi/anti-join reset below, since the index/equi branches
        // consume the first one.
        let left_scope = scope.clone();
        let left_scope_for_reset = scope.clone();
        scope.extend(&qualifier, &joined_schema);

        // Semi/anti-joins always go through `NestedLoopJoin` in v0.34 —
        // the hash and index-nested-loop variants don't yet teach
        // "emit left at most once per match / when no match found".
        // The rewrite is still a big win over per-outer-row plan-and-
        // execute even with a nested loop; specialised semi-hash paths
        // are a future refinement.
        let semi_or_anti = matches!(join.kind, JoinKind::Semi | JoinKind::Anti);

        // An equi-join onto an indexed leading column of the joined table lets
        // each left row look its matches up, sparing a full inner rescan.
        let index_join = if semi_or_anti {
            None
        } else {
            join.on
                .as_ref()
                .and_then(|on| find_index_join(on, left_scope.len(), &scope, &joined_schema))
        };
        // Failing an index, an equi-join on any inner column can still be
        // hashed — O(left + inner) instead of the nested loop's O(left × inner).
        let equi_join = if semi_or_anti {
            None
        } else {
            join.on
                .as_ref()
                .and_then(|on| find_equi_join(on, left_scope.len(), &scope))
        };

        // The per-join right-scan counter slot. v0.41 ANALYZE wraps a
        // streaming right scan with `Counting` when the join uses one
        // (NL or grace-hash); IndexNestedLoopJoin has no streaming
        // right scan (it does per-left-row index probes), so its slot
        // stays `None`.
        let mut right_scan_slot: Option<std::rc::Rc<std::cell::Cell<u64>>> = None;

        op = if let Some((key, index_root)) = index_join {
            Box::new(IndexNestedLoopJoin {
                left: op,
                left_scope,
                key,
                index: BTree::open(index_root),
                table: BTree::open(joined_schema.root),
                on: join.on.clone().expect("an index join has an ON predicate"),
                kind: join.kind,
                scope: scope.clone(),
                right_width,
                snapshot: snapshot.clone(),
                current_left: None,
                inner: Vec::new(),
                inner_pos: 0,
                matched_current: false,
            })
        } else if let Some((probe_col, build_col)) = equi_join {
            let mut right = scan_operator(
                pager,
                &joined_schema,
                &AccessPath::FullScan,
                snapshot.clone(),
            )?;
            if instrument.is_some() {
                right = wrap_into(right, &mut right_scan_slot);
            }
            Box::new(GraceHashJoin {
                left: Some(op),
                right_input: Some(right),
                probe_col,
                build_col,
                on: join.on.clone().expect("an equi-join has an ON predicate"),
                kind: join.kind,
                scope: scope.clone(),
                left_width: left_scope.len(),
                right_width,
                inner_spills: None,
                left_spills: None,
                partition: 0,
                current: None,
            })
        } else {
            let mut right = scan_operator(
                pager,
                &joined_schema,
                &AccessPath::FullScan,
                snapshot.clone(),
            )?;
            if instrument.is_some() {
                right = wrap_into(right, &mut right_scan_slot);
            }
            Box::new(NestedLoopJoin {
                left: op,
                right_input: Some(right),
                right_rows: None,
                on: join.on.clone(),
                kind: join.kind,
                scope: scope.clone(),
                right_width,
                current_left: None,
                right_pos: 0,
                matched_current: false,
            })
        };

        if let Some(counters) = instrument.as_deref_mut() {
            let mut output_slot: Option<std::rc::Rc<std::cell::Cell<u64>>> = None;
            op = wrap_into(op, &mut output_slot);
            counters
                .join_outputs
                .push(output_slot.expect("just populated"));
            counters.join_right_scans.push(right_scan_slot);
        }

        // A semi/anti-join's output is left columns only. The combined
        // scope captured above stays inside the operator for its ON
        // predicate evaluation; downstream operators see the original
        // (pre-join) scope so they don't try to reference the inner
        // table's columns.
        if matches!(join.kind, JoinKind::Semi | JoinKind::Anti) {
            scope = left_scope_for_reset;
        }
    }
    Ok((op, scope))
}

/// If `on` holds — at top level, possibly under `AND` — an equality between a
/// left-side column and the indexed leading column of the just-joined table,
/// return the left key expression and that index's root: the makings of an
/// index nested-loop join. `left_len` is the left scope's column count, so a
/// column resolving below it is a left column and at or above it an inner one.
fn find_index_join(
    on: &Expr,
    left_len: usize,
    scope: &Scope,
    joined: &Schema,
) -> Option<(Expr, u32)> {
    match on {
        Expr::Binary {
            op: BinaryOp::And,
            left,
            right,
        } => find_index_join(left, left_len, scope, joined)
            .or_else(|| find_index_join(right, left_len, scope, joined)),
        Expr::Binary {
            op: BinaryOp::Eq,
            left,
            right,
        } => equi_join_index(left, right, left_len, scope, joined),
        _ => None,
    }
}

/// For one equality `left = right`, decide whether it joins a left column to an
/// indexed leading column of the inner table — of a matching type, so the left
/// value encodes to the key the inner column was indexed under.
fn equi_join_index(
    left: &Expr,
    right: &Expr,
    left_len: usize,
    scope: &Scope,
    joined: &Schema,
) -> Option<(Expr, u32)> {
    let (Expr::Column(left_col), Expr::Column(right_col)) = (left, right) else {
        return None;
    };
    let left_at = scope.resolve(left_col).ok()?;
    let right_at = scope.resolve(right_col).ok()?;
    // One side must be a left column, the other a column of the inner table.
    let (key, key_at, inner_at) = if left_at < left_len && right_at >= left_len {
        (left.clone(), left_at, right_at)
    } else if right_at < left_len && left_at >= left_len {
        (right.clone(), right_at, left_at)
    } else {
        return None;
    };
    if scope.column_type(key_at) != scope.column_type(inner_at) {
        return None;
    }
    // The inner column must be the leading column of one of the table's indexes.
    let inner_column = inner_at - left_len;
    let root = joined
        .indexes
        .iter()
        .find(|index| index.columns.first() == Some(&inner_column))?
        .root;
    Some((key, root))
}

/// If `on` holds — at top level or under `AND` — an equality between a
/// left-side column and a column of the just-joined table, return the row
/// positions to hash on: the column index within a left row, and within an
/// inner row. The makings of a hash join. `left_len` is the left scope's width.
fn find_equi_join(on: &Expr, left_len: usize, scope: &Scope) -> Option<(usize, usize)> {
    match on {
        Expr::Binary {
            op: BinaryOp::And,
            left,
            right,
        } => {
            find_equi_join(left, left_len, scope).or_else(|| find_equi_join(right, left_len, scope))
        }
        Expr::Binary {
            op: BinaryOp::Eq,
            left,
            right,
        } => equi_join_columns(left, right, left_len, scope),
        _ => None,
    }
}

/// For one equality `left = right`, the `(left-row, inner-row)` column
/// positions when it joins a left column to an inner column of a matching
/// type — what a hash join hashes each side on.
fn equi_join_columns(
    left: &Expr,
    right: &Expr,
    left_len: usize,
    scope: &Scope,
) -> Option<(usize, usize)> {
    let (Expr::Column(left_col), Expr::Column(right_col)) = (left, right) else {
        return None;
    };
    let left_at = scope.resolve(left_col).ok()?;
    let right_at = scope.resolve(right_col).ok()?;
    // One side must be a left column, the other a column of the inner table.
    let (outer_at, inner_at) = if left_at < left_len && right_at >= left_len {
        (left_at, right_at)
    } else if right_at < left_len && left_at >= left_len {
        (right_at, left_at)
    } else {
        return None;
    };
    if scope.column_type(outer_at) != scope.column_type(inner_at) {
        return None;
    }
    // `outer_at` already indexes a left row; `inner_at` is a combined-scope
    // index, so the inner row's column sits at `inner_at - left_len`.
    Some((outer_at, inner_at - left_len))
}

#[allow(clippy::too_many_arguments)]
fn select(
    pager: &mut Pager,
    catalog: &Catalog,
    from: FromClause,
    projection: Projection,
    filter: Option<Expr>,
    access: AccessPath,
    group_by: Vec<ColumnRef>,
    having: Option<Expr>,
    order_by: Vec<OrderKey>,
    presorted: bool,
    limit: Option<u64>,
    offset: Option<u64>,
    snapshot: &Snapshot,
    mut instrument: Option<&mut OperatorCounters>,
) -> Result<RowStream> {
    // ----- vectorised fast path ---------------------------------------------
    //
    // A SELECT without GROUP BY/HAVING/aggregates/ORDER BY runs through the
    // batched operator tree. v0.23 extends the path to handle joins through
    // `BatchHashJoin` (equi-joins) and `BatchNestedLoopJoin` (everything
    // else); a join that would prefer an index nested-loop falls back to the
    // row-at-a-time pipeline so we keep that optimisation.
    let projection_has_aggregate = match &projection {
        Projection::All => false,
        Projection::Items(items) => items.iter().any(|item| match item {
            SelectItem::Aggregate(_) => true,
            SelectItem::Expr(e) => expr_contains_aggregate(e),
            SelectItem::Column(_) => false,
        }),
    };
    let joins_qualify = joins_vectorisable(pager, catalog, &from)?;
    let has_correlated = query_has_correlated_subquery(&projection, &filter, pager, catalog)?;
    // v0.33: aggregation no longer gates the vectorised path, *if* the
    // shape is simple — HAVING and projection-position Expr items keep
    // the row tree, because `BatchHashAggregate` only handles
    // `Column` and `Aggregate` items today. ORDER BY *with* aggregation
    // also keeps the row tree (the post-agg sort needs a synthetic
    // scope this v0.33 doesn't build).
    let needs_aggregation = !group_by.is_empty() || projection_has_aggregate;
    let aggregation_vectorisable = having.is_none()
        && (!needs_aggregation || order_by.is_empty())
        && match &projection {
            Projection::All => !needs_aggregation,
            Projection::Items(items) => items
                .iter()
                .all(|it| matches!(it, SelectItem::Column(_) | SelectItem::Aggregate(_))),
        };
    // EXPLAIN ANALYZE (v0.41) needs per-operator visibility, and the
    // batched path has its own (BatchOperator) types that aren't yet
    // instrumented. Force the row-at-a-time path whenever the caller
    // hands us counters; a plain SELECT (no ANALYZE) still takes the
    // fast path unchanged.
    if instrument.is_none() && joins_qualify && !has_correlated && aggregation_vectorisable {
        // v0.32: ORDER BY no longer gates the vectorised path —
        // `select_vectorised` inserts a `BatchSort` (external if it
        // outgrows memory) when the keys are non-empty. v0.33: GROUP
        // BY + aggregates flow through a `BatchHashAggregate` when
        // the projection is `Column`/`Aggregate` only. Correlated
        // subqueries still steer to the row pipeline.
        return select_vectorised(
            pager, catalog, from, projection, filter, access, group_by, order_by, limit,
            offset, snapshot,
        );
    }

    // ----- row-at-a-time pipeline -------------------------------------------
    //
    // The FROM pipeline — a scan, then a NestedLoopJoin per join — and the
    // scope spanning every column it produces.
    let (mut op, scope) = build_from(
        pager,
        catalog,
        &from,
        &access,
        snapshot,
        instrument.as_deref_mut(),
    )?;

    // A plain projection produces one output row per joined row; GROUP BY,
    // HAVING, or any aggregate falls through to the grouped path.
    let plain: Option<Vec<PlainItem>> = match &projection {
        Projection::All => {
            if !group_by.is_empty() || having.is_some() {
                return Err(Error::exec(
                    "SELECT * cannot be combined with GROUP BY or HAVING",
                ));
            }
            Some((0..scope.len()).map(PlainItem::Column).collect())
        }
        Projection::Items(items) => {
            let has_aggregate = items.iter().any(|item| match item {
                SelectItem::Aggregate(_) => true,
                SelectItem::Expr(e) => expr_contains_aggregate(e),
                SelectItem::Column(_) => false,
            });
            if group_by.is_empty() && !has_aggregate && having.is_none() {
                let mut resolved = Vec::with_capacity(items.len());
                for item in items {
                    resolved.push(match item {
                        SelectItem::Column(colref) => PlainItem::Column(scope.resolve(colref)?),
                        SelectItem::Aggregate(_) => unreachable!("guarded by has_aggregate"),
                        SelectItem::Expr(e) => {
                            let mut prepared = e.clone();
                            prepare_subqueries(&mut prepared, pager, catalog, snapshot)?;
                            PlainItem::Expr(prepared)
                        }
                    });
                }
                Some(resolved)
            } else {
                None
            }
        }
    };

    // The WHERE clause filters joined rows, downstream of every join. Resolve
    // any subqueries it carries before installing the operator.
    let mut filter = filter;
    if let Some(predicate) = filter.as_mut() {
        prepare_subqueries(predicate, pager, catalog, snapshot)?;
    }
    if let Some(predicate) = filter {
        let has_correlated = predicate_has_correlated(&predicate);
        op = Box::new(Filter {
            input: op,
            predicate,
            scope: scope.clone(),
            has_correlated,
            catalog: catalog.clone(),
            snapshot: snapshot.clone(),
        });
        if let Some(counters) = instrument.as_deref_mut() {
            op = wrap_into(op, &mut counters.filter);
        }
    }

    match plain {
        Some(projected) => {
            // scan/join -> filter -> sort -> project -> limit, pulled a row at
            // a time; only `Sort` buffers.
            if !order_by.is_empty() && !presorted {
                op = Box::new(Sort {
                    input: op,
                    keys: resolve_order_keys(&scope, &order_by)?,
                    buffered: None,
                });
                if let Some(counters) = instrument.as_deref_mut() {
                    op = wrap_into(op, &mut counters.sort);
                }
            }
            let columns = projection_headers(&projection, &scope);
            let project_scope = if projected
                .iter()
                .any(|item| matches!(item, PlainItem::Expr(_)))
            {
                Some(scope.clone())
            } else {
                None
            };
            let has_correlated = projected.iter().any(|item| match item {
                PlainItem::Expr(e) => predicate_has_correlated(e),
                _ => false,
            });
            op = Box::new(Project {
                input: op,
                items: projected,
                scope: project_scope,
                has_correlated,
                catalog: catalog.clone(),
                snapshot: snapshot.clone(),
            });
            if let Some(counters) = instrument.as_deref_mut() {
                op = wrap_into(op, &mut counters.project);
            }
            if limit.is_some() || offset.is_some() {
                op = Box::new(Limit {
                    input: op,
                    offset: offset.unwrap_or(0),
                    remaining: limit.unwrap_or(u64::MAX),
                });
                if let Some(counters) = instrument.as_deref_mut() {
                    op = wrap_into(op, &mut counters.limit);
                }
            }
            // The pipeline is handed back unrun — the caller pulls it a row at
            // a time, so nothing is materialized.
            Ok(RowStream {
                columns,
                source: RowSource::Volcano(op),
            })
        }
        None => {
            let Projection::Items(items) = projection else {
                unreachable!("`All` is always a plain projection");
            };
            // Prepare any subqueries riding inside HAVING before the grouped
            // pass runs. (GROUP BY columns are plain references — no
            // subqueries — and the projection's grouped path rejects
            // `SelectItem::Expr`, so we do not pre-walk it here.)
            let mut having = having;
            if let Some(predicate) = having.as_mut() {
                prepare_subqueries(predicate, pager, catalog, snapshot)?;
            }
            // GROUP BY / HAVING / aggregates are a pipeline breaker: the joined
            // and filtered rows are drained into one buffer, then grouped — so
            // this result is already materialized, and merely streamed out.
            let matched = drain(op, pager)?;
            let mut result = grouped_select(
                &scope,
                &items,
                &group_by,
                having.as_ref(),
                &order_by,
                matched,
            )?;
            apply_limit(&mut result, limit, offset);
            let QueryResult::Rows { columns, rows } = result else {
                unreachable!("a grouped SELECT always yields rows");
            };
            // v0.41 ANALYZE: the grouped path is materialised, so we
            // can record the final output count exactly once here.
            // Per-operator actuals for HashAggregate / Having / Sort /
            // Project / Limit collapse to this single observation —
            // see `OperatorCounters::grouped_output`.
            if let Some(counters) = instrument.as_deref_mut() {
                let cell = std::rc::Rc::new(std::cell::Cell::new(rows.len() as u64));
                counters.grouped_output = Some(cell);
            }
            Ok(RowStream {
                columns,
                source: RowSource::Buffered(rows.into_iter()),
            })
        }
    }
}

/// One item of a plain (non-grouped) projection: either a direct column copy
/// or an expression evaluated per row.
enum PlainItem {
    Column(usize),
    Expr(Expr),
}

/// The output column headers for a plain (non-grouped) projection. `SELECT *`
/// uses every column the scope holds; an explicit item list synthesises a
/// header per item (qualified column name, or `?column?` for an expression
/// since `AS` is not yet parsed).
fn projection_headers(projection: &Projection, scope: &Scope) -> Vec<String> {
    match projection {
        Projection::All => (0..scope.len()).map(|i| scope.header(i)).collect(),
        Projection::Items(items) => items
            .iter()
            .map(|item| match item {
                SelectItem::Column(colref) => colref.to_string(),
                SelectItem::Aggregate(agg) => aggregate_label(agg),
                SelectItem::Expr(_) => "?column?".to_string(),
            })
            .collect(),
    }
}

// --- the volcano operator tree --------------------------------------------
//
// A `SELECT` runs as a tree of operators, each a pull-based iterator: calling
// `next` on the root pulls one row, which pulls from its input, and so on down
// to a scan. Rows therefore stream through the pipeline one at a time, and an
// operator below a `LIMIT` is never asked for more rows than are wanted. Only
// a buffering operator — `Sort` here — must hold its whole input at once,
// because sorting inherently needs every row before it can yield the first.

/// One node of the operator tree. `next` yields the next row, or `None` once
/// the stream is exhausted.
trait Operator {
    fn next(&mut self, pager: &mut Pager) -> Result<Option<Vec<Value>>>;
}

/// A transparent operator wrapper for `EXPLAIN ANALYZE`. Forwards every
/// `next` to the inner operator and increments a shared counter on each
/// yielded row, so the EXPLAIN renderer can report observed cardinality
/// per node.
///
/// The counter is `Rc<Cell<u64>>` — execution is single-threaded per
/// statement, so an `Rc<Cell>` is enough; no atomics needed. The
/// wrapper is constructed only when `Option<&mut OperatorCounters>`
/// is `Some`, so a plain SELECT (no ANALYZE) pays nothing.
struct Counting {
    inner: Box<dyn Operator>,
    count: std::rc::Rc<std::cell::Cell<u64>>,
}

impl Operator for Counting {
    fn next(&mut self, pager: &mut Pager) -> Result<Option<Vec<Value>>> {
        let row = self.inner.next(pager)?;
        if row.is_some() {
            self.count.set(self.count.get() + 1);
        }
        Ok(row)
    }
}

/// Per-operator row counters threaded through `select()` and
/// `build_from()` when EXPLAIN ANALYZE is in flight. Each slot is
/// populated only if the corresponding operator actually appears in
/// the tree: a query without a `WHERE` has no `filter` counter, an
/// index nested-loop join has no `right_scan` counter (its right side
/// is per-left-row lookups, not a streaming scan), etc.
///
/// `join_outputs` and `join_right_scans` are indexed by the join's
/// position in `FromClause::joins` (build order, inner-to-outer
/// left-deep). A join always has an output counter; the right-scan
/// slot is `None` for `IndexNestedLoopJoin`.
pub(crate) struct OperatorCounters {
    pub base_scan: Option<std::rc::Rc<std::cell::Cell<u64>>>,
    pub join_outputs: Vec<std::rc::Rc<std::cell::Cell<u64>>>,
    pub join_right_scans: Vec<Option<std::rc::Rc<std::cell::Cell<u64>>>>,
    pub filter: Option<std::rc::Rc<std::cell::Cell<u64>>>,
    pub sort: Option<std::rc::Rc<std::cell::Cell<u64>>>,
    pub project: Option<std::rc::Rc<std::cell::Cell<u64>>>,
    pub limit: Option<std::rc::Rc<std::cell::Cell<u64>>>,
    /// For grouped queries: the post-grouping row count. The grouped
    /// path is materialised via `grouped_select`, so we cannot count
    /// Aggregate / Having / Sort / Project / Limit individually —
    /// they all collapse onto this single observation. The
    /// per-operator counters above still record the *pre-grouping*
    /// shape (base scan, joins, filter), which is the data flowing
    /// into the materialisation pass.
    pub grouped_output: Option<std::rc::Rc<std::cell::Cell<u64>>>,
}

impl OperatorCounters {
    pub fn new() -> OperatorCounters {
        OperatorCounters {
            base_scan: None,
            join_outputs: Vec::new(),
            join_right_scans: Vec::new(),
            filter: None,
            sort: None,
            project: None,
            limit: None,
            grouped_output: None,
        }
    }

    /// Read every populated counter into a plain-data
    /// [`crate::engine::explain::OperatorActuals`] for the renderer.
    pub fn snapshot(&self) -> crate::engine::explain::OperatorActuals {
        crate::engine::explain::OperatorActuals {
            base_scan: self.base_scan.as_ref().map(|c| c.get()),
            join_outputs: self.join_outputs.iter().map(|c| c.get()).collect(),
            join_right_scans: self
                .join_right_scans
                .iter()
                .map(|opt| opt.as_ref().map(|c| c.get()))
                .collect(),
            filter: self.filter.as_ref().map(|c| c.get()),
            sort: self.sort.as_ref().map(|c| c.get()),
            project: self.project.as_ref().map(|c| c.get()),
            limit: self.limit.as_ref().map(|c| c.get()),
            grouped_output: self.grouped_output.as_ref().map(|c| c.get()),
        }
    }
}

/// Wrap `op` with a fresh [`Counting`] adapter, store the counter in
/// `slot`, and return the wrapped operator. Lets every wrap site stay
/// a single line: `op = wrap_into(op, &mut counters.base_scan);`.
fn wrap_into(
    op: Box<dyn Operator>,
    slot: &mut Option<std::rc::Rc<std::cell::Cell<u64>>>,
) -> Box<dyn Operator> {
    let count = std::rc::Rc::new(std::cell::Cell::new(0u64));
    *slot = Some(count.clone());
    Box::new(Counting { inner: op, count })
}

/// Build the scan at the base of the tree — a full table walk or a bounded
/// index walk — as a streaming cursor wrapped in an operator. The scan
/// applies the MVCC visibility filter for `snapshot` per row.
fn scan_operator(
    pager: &mut Pager,
    schema: &Schema,
    access: &AccessPath,
    snapshot: Snapshot,
) -> Result<Box<dyn Operator>> {
    let column_count = schema.columns.len();
    let table_root = schema.root;
    let table = BTree::open(schema.root);
    match access {
        AccessPath::FullScan => {
            let cursor = table.cursor(pager, None, None)?;
            Ok(Box::new(TableScan {
                cursor,
                column_count,
                snapshot,
                table_root,
                relation_read_recorded: false,
            }))
        }
        AccessPath::IndexScan {
            index_root,
            lower,
            upper,
        } => {
            let cursor =
                BTree::open(*index_root).cursor(pager, Some(lower.as_slice()), upper.clone())?;
            Ok(Box::new(IndexScan {
                cursor,
                table,
                column_count,
                snapshot,
                seen_rowids: std::collections::HashSet::new(),
                table_root,
            }))
        }
    }
}

/// Pull every remaining row out of an operator.
fn drain(mut op: Box<dyn Operator>, pager: &mut Pager) -> Result<Vec<Vec<Value>>> {
    let mut rows = Vec::new();
    while let Some(row) = op.next(pager)? {
        rows.push(row);
    }
    Ok(rows)
}

/// Trim a finished result set to a `LIMIT` / `OFFSET` window. Used only by the
/// grouped path, whose group rows are already materialized; the plain path
/// streams through a `Limit` operator instead.
fn apply_limit(result: &mut QueryResult, limit: Option<u64>, offset: Option<u64>) {
    let QueryResult::Rows { rows, .. } = result else {
        return;
    };
    if let Some(skip) = offset {
        rows.drain(..(skip as usize).min(rows.len()));
    }
    if let Some(take) = limit {
        rows.truncate(take as usize);
    }
}

/// A full table walk: every row, in rowid order, filtered against the MVCC
/// snapshot — rows the snapshot can't see (tombstoned by a TX it sees, or
/// inserted by a TX it doesn't) are skipped.
struct TableScan {
    cursor: Cursor,
    column_count: usize,
    snapshot: Snapshot,
    /// The table's B+tree root. v0.35 uses this for an SSI
    /// relation-level read lock — recorded once when the scan starts,
    /// instead of one tuple lock per emitted row.
    table_root: u32,
    /// Whether `record_relation_read` has already been called for this
    /// scan. Cheap idempotency — saves a lock acquisition per `.next()`.
    relation_read_recorded: bool,
}

impl Operator for TableScan {
    fn next(&mut self, pager: &mut Pager) -> Result<Option<Vec<Value>>> {
        // v0.35: a full table scan takes a *relation-level* SSI lock
        // rather than one per emitted row. The lock is recorded once
        // and covers every row that might be visible — present
        // tuples, future inserts (the phantom case), tombstones.
        // Recording it lazily means an unbuilt cursor doesn't pay.
        if !self.relation_read_recorded {
            self.snapshot.record_relation_read(self.table_root);
            self.relation_read_recorded = true;
        }
        loop {
            match self.cursor.next(pager)? {
                Some((_rowid, encoded)) => {
                    let record = codec::decode_row(&encoded, self.column_count)?;
                    if self.snapshot.visible(record.tx_min, record.tx_max) {
                        return Ok(Some(record.values));
                    }
                }
                None => return Ok(None),
            }
        }
    }
}

/// A bounded index walk: each index entry's rowid is followed back to its row
/// in the table tree, then visibility-filtered. The same rowid can appear
/// in the index more than once (after an UPDATE that doesn't change the
/// indexed column the entry still points at the original rowid, while a
/// new entry points at the new version) — `seen_rowids` dedupes so each
/// physical row is decoded once per scan.
struct IndexScan {
    cursor: Cursor,
    table: BTree,
    column_count: usize,
    snapshot: Snapshot,
    seen_rowids: std::collections::HashSet<Vec<u8>>,
    table_root: u32,
}

impl Operator for IndexScan {
    fn next(&mut self, pager: &mut Pager) -> Result<Option<Vec<Value>>> {
        loop {
            let Some((index_key, _)) = self.cursor.next(pager)? else {
                return Ok(None);
            };
            if index_key.len() < 8 {
                return Err(Error::corruption("index key shorter than a rowid"));
            }
            let rowid_key = index_key[index_key.len() - 8..].to_vec();
            if !self.seen_rowids.insert(rowid_key.clone()) {
                continue;
            }
            match self.table.search(pager, &rowid_key)? {
                Some(encoded) => {
                    let record = codec::decode_row(&encoded, self.column_count)?;
                    if self.snapshot.visible(record.tx_min, record.tx_max) {
                        let tombstone_by = if record.tx_max != 0 {
                            Some(record.tx_max)
                        } else {
                            None
                        };
                        self.snapshot
                            .record_read(self.table_root, &rowid_key, tombstone_by);
                        return Ok(Some(record.values));
                    }
                }
                None => {
                    return Err(Error::corruption(
                        "index references a row that does not exist",
                    ))
                }
            }
        }
    }
}

/// Keep only rows for which the `WHERE` predicate is exactly `TRUE`.
struct Filter {
    input: Box<dyn Operator>,
    predicate: Expr,
    scope: Scope,
    /// `true` if `predicate` contains any `Correlated*` subquery node
    /// that must be resolved per outer row. Cached at construction so
    /// the hot path doesn't walk the tree on every row.
    has_correlated: bool,
    /// Catalog and snapshot, threaded into the operator so the per-row
    /// correlated-subquery resolver can execute the substituted
    /// subqueries.
    catalog: Catalog,
    snapshot: Snapshot,
}

impl Operator for Filter {
    fn next(&mut self, pager: &mut Pager) -> Result<Option<Vec<Value>>> {
        while let Some(row) = self.input.next(pager)? {
            if self.has_correlated {
                // Substitute outer column refs in the cloned subquery
                // statements with this row's values and execute them,
                // then evaluate the resolved predicate.
                let resolved = resolve_correlated(
                    &self.predicate,
                    &self.scope,
                    &row,
                    pager,
                    &self.catalog,
                    &self.snapshot,
                )?;
                if passes_filter(Some(&resolved), &self.scope, &row)? {
                    return Ok(Some(row));
                }
            } else if passes_filter(Some(&self.predicate), &self.scope, &row)? {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }
}

/// Sort the whole input by the `ORDER BY` keys. A pipeline breaker: the first
/// `next` drains the input and sorts it; later calls hand out the buffer.
struct Sort {
    input: Box<dyn Operator>,
    keys: Vec<(usize, bool)>,
    buffered: Option<std::vec::IntoIter<Vec<Value>>>,
}

impl Operator for Sort {
    fn next(&mut self, pager: &mut Pager) -> Result<Option<Vec<Value>>> {
        if self.buffered.is_none() {
            let mut rows = Vec::new();
            while let Some(row) = self.input.next(pager)? {
                rows.push(row);
            }
            sort_rows(&mut rows, &self.keys);
            self.buffered = Some(rows.into_iter());
        }
        Ok(self.buffered.as_mut().unwrap().next())
    }
}

/// Narrow each row to the selected columns or expressions. A pure-column
/// projection skips evaluation; an item that is an expression (a literal,
/// arithmetic, a scalar subquery result) is evaluated against the row.
struct Project {
    input: Box<dyn Operator>,
    items: Vec<PlainItem>,
    /// Only consulted when at least one item is an expression.
    scope: Option<Scope>,
    /// `true` if any expression item contains a `Correlated*` node and
    /// needs per-row resolution. Cached at construction.
    has_correlated: bool,
    /// Carried so `resolve_correlated` can execute substituted
    /// subqueries — only used when `has_correlated` is `true`.
    catalog: Catalog,
    snapshot: Snapshot,
}

impl Operator for Project {
    fn next(&mut self, pager: &mut Pager) -> Result<Option<Vec<Value>>> {
        let Some(row) = self.input.next(pager)? else {
            return Ok(None);
        };
        let mut out = Vec::with_capacity(self.items.len());
        for item in &self.items {
            out.push(match item {
                PlainItem::Column(i) => row[*i].clone(),
                PlainItem::Expr(expr) => {
                    let scope = self
                        .scope
                        .as_ref()
                        .expect("expression items require a scope");
                    let resolved;
                    let expr_ref: &Expr = if self.has_correlated {
                        resolved = resolve_correlated(
                            expr,
                            scope,
                            &row,
                            pager,
                            &self.catalog,
                            &self.snapshot,
                        )?;
                        &resolved
                    } else {
                        expr
                    };
                    eval(
                        expr_ref,
                        Some(&RowContext {
                            scope,
                            values: &row,
                        }),
                    )?
                }
            });
        }
        Ok(Some(out))
    }
}

/// Skip `offset` rows, then yield at most `remaining`. Once the quota is spent
/// it returns `None` without pulling its input again — the early stop that
/// lets a `LIMIT` query read only as far as it must.
struct Limit {
    input: Box<dyn Operator>,
    offset: u64,
    remaining: u64,
}

impl Operator for Limit {
    fn next(&mut self, pager: &mut Pager) -> Result<Option<Vec<Value>>> {
        while self.offset > 0 {
            if self.input.next(pager)?.is_none() {
                self.offset = 0;
                self.remaining = 0;
                return Ok(None);
            }
            self.offset -= 1;
        }
        if self.remaining == 0 {
            return Ok(None);
        }
        match self.input.next(pager)? {
            Some(row) => {
                self.remaining -= 1;
                Ok(Some(row))
            }
            None => Ok(None),
        }
    }
}

/// A nested-loop join. The left input streams; the right is buffered once, on
/// the first `next`, and rescanned for every left row. Each left/right pair
/// whose `ON` predicate holds is emitted as their concatenation; a `LEFT` join
/// also emits any left row that matched nothing, padded with `NULL`s.
struct NestedLoopJoin {
    left: Box<dyn Operator>,
    /// The right input, drained into `right_rows` on the first `next`.
    right_input: Option<Box<dyn Operator>>,
    right_rows: Option<Vec<Vec<Value>>>,
    /// The `ON` predicate; `None` for a `CROSS JOIN`, which pairs everything.
    on: Option<Expr>,
    kind: JoinKind,
    /// Scope spanning the left tables and the right — for evaluating `on`.
    scope: Scope,
    /// Columns the right side contributes, for `NULL`-padding a `LEFT` miss.
    right_width: usize,
    current_left: Option<Vec<Value>>,
    right_pos: usize,
    matched_current: bool,
}

impl Operator for NestedLoopJoin {
    fn next(&mut self, pager: &mut Pager) -> Result<Option<Vec<Value>>> {
        // Buffer the inner side once; thereafter it is rescanned from memory.
        if self.right_rows.is_none() {
            let input = self.right_input.take().expect("right input drained twice");
            self.right_rows = Some(drain(input, pager)?);
        }
        loop {
            // Pull the next left row when the current one is spent.
            if self.current_left.is_none() {
                match self.left.next(pager)? {
                    Some(row) => {
                        self.current_left = Some(row);
                        self.right_pos = 0;
                        self.matched_current = false;
                    }
                    None => return Ok(None),
                }
            }
            // Pair the current left row against the remaining right rows.
            let mut semi_emit: Option<Vec<Value>> = None;
            {
                let right_rows = self.right_rows.as_ref().unwrap();
                let left = self.current_left.as_ref().unwrap();
                while self.right_pos < right_rows.len() {
                    let mut combined = left.clone();
                    combined.extend_from_slice(&right_rows[self.right_pos]);
                    self.right_pos += 1;
                    let keep = match &self.on {
                        None => true, // CROSS JOIN pairs unconditionally
                        Some(predicate) => passes_filter(Some(predicate), &self.scope, &combined)?,
                    };
                    if keep {
                        self.matched_current = true;
                        match self.kind {
                            JoinKind::Semi => {
                                // Left columns only; stash to emit after we
                                // clear `current_left` so the next call
                                // starts on a fresh left row.
                                semi_emit = Some(left.clone());
                                break;
                            }
                            JoinKind::Anti => {
                                // Match found — don't emit, and skip the
                                // rest of right for this left.
                                self.right_pos = right_rows.len();
                                break;
                            }
                            JoinKind::Inner | JoinKind::Left | JoinKind::Cross => {
                                return Ok(Some(combined));
                            }
                        }
                    }
                }
            }
            if let Some(row) = semi_emit {
                self.current_left = None;
                return Ok(Some(row));
            }
            // The right side is exhausted for this left row.
            let left = self.current_left.take().expect("a current left row");
            match self.kind {
                JoinKind::Left if !self.matched_current => {
                    let mut combined = left;
                    combined.resize(combined.len() + self.right_width, Value::Null);
                    return Ok(Some(combined));
                }
                JoinKind::Anti if !self.matched_current => {
                    // No right row matched — anti-join emits the left row.
                    return Ok(Some(left));
                }
                _ => {
                    // Inner/Cross/Semi(no-match)/Anti(matched): advance.
                }
            }
        }
    }
}

/// An index nested-loop join. For each left row it evaluates the join key and
/// looks it up in an index on the inner table, fetching just the matching
/// rows — sparing the full rescan a plain `NestedLoopJoin` pays. As with that
/// join, a `LEFT` variant pads an unmatched left row with `NULL`s.
struct IndexNestedLoopJoin {
    left: Box<dyn Operator>,
    /// Scope of the left rows — for evaluating `key`.
    left_scope: Scope,
    /// The left-side column of the equi-join; its value is the lookup key.
    key: Expr,
    /// The inner table's index, keyed on the join column.
    index: BTree,
    /// The inner table's data tree, for fetching matched rows by rowid.
    table: BTree,
    /// The full `ON` predicate — still applied, since the index only narrows.
    on: Expr,
    kind: JoinKind,
    /// Combined left + inner scope, for evaluating `on`.
    scope: Scope,
    /// Columns in an inner row — to decode one, and to `NULL`-pad a `LEFT` miss.
    right_width: usize,
    /// MVCC snapshot for visibility on inner rows. The index entry may point
    /// at a tombstoned row; we filter on the table side after the lookup.
    snapshot: Snapshot,
    current_left: Option<Vec<Value>>,
    /// Inner rows matched for the current left row.
    inner: Vec<Vec<Value>>,
    inner_pos: usize,
    matched_current: bool,
}

impl IndexNestedLoopJoin {
    /// The inner rows whose join column equals `key_value`, reached through the
    /// index. A `NULL` key matches nothing — `NULL = anything` is never `TRUE`.
    /// Inner rows are visibility-filtered against the join's snapshot; an
    /// index entry can point at a tombstoned row, which the filter drops.
    fn lookup(&self, pager: &mut Pager, key_value: &Value) -> Result<Vec<Vec<Value>>> {
        if key_value.is_null() {
            return Ok(Vec::new());
        }
        let lower = codec::encode_index_value(key_value);
        let upper = codec::prefix_upper_bound(&lower);
        let mut rows = Vec::new();
        let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
        for (index_key, _) in self.index.scan_range(pager, &lower, upper.as_deref())? {
            if index_key.len() < 8 {
                return Err(Error::corruption("index key shorter than a rowid"));
            }
            let rowid_key = index_key[index_key.len() - 8..].to_vec();
            if !seen.insert(rowid_key.clone()) {
                continue;
            }
            match self.table.search(pager, &rowid_key)? {
                Some(encoded) => {
                    let record = codec::decode_row(&encoded, self.right_width)?;
                    if self.snapshot.visible(record.tx_min, record.tx_max) {
                        rows.push(record.values);
                    }
                }
                None => {
                    return Err(Error::corruption(
                        "index references a row that does not exist",
                    ))
                }
            }
        }
        Ok(rows)
    }
}

impl Operator for IndexNestedLoopJoin {
    fn next(&mut self, pager: &mut Pager) -> Result<Option<Vec<Value>>> {
        loop {
            // Pull the next left row, and look its join key up in the index.
            if self.current_left.is_none() {
                let Some(row) = self.left.next(pager)? else {
                    return Ok(None);
                };
                let key_value = eval(
                    &self.key,
                    Some(&RowContext {
                        scope: &self.left_scope,
                        values: &row,
                    }),
                )?;
                self.inner = self.lookup(pager, &key_value)?;
                self.inner_pos = 0;
                self.matched_current = false;
                self.current_left = Some(row);
            }
            // Pair the left row against the inner rows the lookup returned.
            {
                let left = self.current_left.as_ref().unwrap();
                while self.inner_pos < self.inner.len() {
                    let mut combined = left.clone();
                    combined.extend_from_slice(&self.inner[self.inner_pos]);
                    self.inner_pos += 1;
                    if passes_filter(Some(&self.on), &self.scope, &combined)? {
                        self.matched_current = true;
                        return Ok(Some(combined));
                    }
                }
            }
            // The lookup is exhausted for this left row.
            let left = self.current_left.take().expect("a current left row");
            if self.kind == JoinKind::Left && !self.matched_current {
                let mut combined = left;
                combined.resize(combined.len() + self.right_width, Value::Null);
                return Ok(Some(combined));
            }
        }
    }
}

/// A hash join. The inner side is drained once into a hash table keyed on the
/// join column; each left row then probes that table for its matches, sparing
/// the full inner rescan a plain `NestedLoopJoin` pays — O(left + inner) rather
/// than O(left × inner). The full `ON` predicate is still applied to each pair,
/// and a `LEFT` variant pads an unmatched left row with `NULL`s.
struct HashJoin {
    left: Box<dyn Operator>,
    /// The inner input, drained and hashed on the first `next`.
    right_input: Option<Box<dyn Operator>>,
    /// Encoded inner join-key -> the inner rows carrying it. Built once.
    table: Option<HashMap<Vec<u8>, Vec<Vec<Value>>>>,
    /// Index of the join column within a left row, and within an inner row.
    probe_col: usize,
    build_col: usize,
    /// The full `ON` predicate — still applied, since the hash only narrows.
    on: Expr,
    kind: JoinKind,
    /// Combined left + inner scope, for evaluating `on`.
    scope: Scope,
    /// Columns in an inner row — to `NULL`-pad a `LEFT` miss.
    right_width: usize,
    current_left: Option<Vec<Value>>,
    /// The current left row's encoded join key, or `None` when that key is
    /// `NULL` — a `NULL` key matches nothing, so it probes no bucket.
    probe_key: Option<Vec<u8>>,
    /// Position within the current left row's bucket of inner matches.
    match_pos: usize,
    matched_current: bool,
}

impl HashJoin {
    /// Drain the inner input and hash every row by its join key. An inner row
    /// whose key is `NULL` is dropped — `NULL = anything` is never `TRUE`, so
    /// it can never be an equi-join match.
    fn build(&mut self, pager: &mut Pager) -> Result<HashMap<Vec<u8>, Vec<Vec<Value>>>> {
        let input = self.right_input.take().expect("inner input drained twice");
        let mut table: HashMap<Vec<u8>, Vec<Vec<Value>>> = HashMap::new();
        for row in drain(input, pager)? {
            if row[self.build_col].is_null() {
                continue;
            }
            let key = codec::encode_index_value(&row[self.build_col]);
            table.entry(key).or_default().push(row);
        }
        Ok(table)
    }
}

impl Operator for HashJoin {
    fn next(&mut self, pager: &mut Pager) -> Result<Option<Vec<Value>>> {
        // Build the inner side into a hash table once.
        if self.table.is_none() {
            self.table = Some(self.build(pager)?);
        }
        loop {
            // Pull the next left row and hash its join key.
            if self.current_left.is_none() {
                let Some(row) = self.left.next(pager)? else {
                    return Ok(None);
                };
                self.probe_key = if row[self.probe_col].is_null() {
                    None // a NULL probe key matches nothing
                } else {
                    Some(codec::encode_index_value(&row[self.probe_col]))
                };
                self.match_pos = 0;
                self.matched_current = false;
                self.current_left = Some(row);
            }
            // Pair the left row against the inner rows that share its key.
            {
                let table = self.table.as_ref().expect("table built above");
                let bucket = self
                    .probe_key
                    .as_ref()
                    .and_then(|key| table.get(key))
                    .map(|rows| rows.as_slice())
                    .unwrap_or(&[]);
                let left = self.current_left.as_ref().unwrap();
                while self.match_pos < bucket.len() {
                    let mut combined = left.clone();
                    combined.extend_from_slice(&bucket[self.match_pos]);
                    self.match_pos += 1;
                    if passes_filter(Some(&self.on), &self.scope, &combined)? {
                        self.matched_current = true;
                        return Ok(Some(combined));
                    }
                }
            }
            // The bucket is exhausted for this left row.
            let left = self.current_left.take().expect("a current left row");
            if self.kind == JoinKind::Left && !self.matched_current {
                let mut combined = left;
                combined.resize(combined.len() + self.right_width, Value::Null);
                return Ok(Some(combined));
            }
        }
    }
}

/// How many ways a grace hash join partitions its inputs. Equal join keys
/// hash to the same partition, so matches are confined to one partition pair
/// and memory stays bounded by the partition rather than the inner table.
const HASH_PARTITIONS: usize = 16;

/// A grace hash join. Both inputs are partitioned to disk by the hash of their
/// join key, into a fixed number of partition files, so matching rows land in
/// the same partition pair. Each pair is then joined in memory by an ordinary
/// [`HashJoin`]. Memory is bounded by the largest partition rather than the
/// inner table, so a join scales to inner sides that do not fit in memory.
struct GraceHashJoin {
    left: Option<Box<dyn Operator>>,
    right_input: Option<Box<dyn Operator>>,
    probe_col: usize,
    build_col: usize,
    on: Expr,
    kind: JoinKind,
    scope: Scope,
    left_width: usize,
    right_width: usize,
    /// One spill file per partition, per side. Set after the partition phase;
    /// drained one slot at a time as the join phase advances.
    inner_spills: Option<Vec<Option<SpillFile>>>,
    left_spills: Option<Vec<Option<SpillFile>>>,
    /// Index of the next partition pair to join.
    partition: usize,
    /// The current partition pair's in-memory join, if one is open.
    current: Option<HashJoin>,
}

impl GraceHashJoin {
    /// Drain both inputs into per-partition spill files. Equal join keys hash
    /// to the same partition, so matching pairs always land in the same pair.
    fn partition_phase(&mut self, pager: &mut Pager) -> Result<()> {
        let hasher_state = RandomState::new();
        let mut inner_spills = Vec::with_capacity(HASH_PARTITIONS);
        let mut left_spills = Vec::with_capacity(HASH_PARTITIONS);
        for _ in 0..HASH_PARTITIONS {
            inner_spills.push(Some(SpillFile::create()?));
            left_spills.push(Some(SpillFile::create()?));
        }
        // The inner side first — `codec::encode_row`'d, written to the
        // partition its build key hashes to.
        let mut inner = self.right_input.take().expect("inner drained twice");
        while let Some(row) = inner.next(pager)? {
            let partition = partition_for(&row[self.build_col], &hasher_state);
            // Spilled rows have already been visibility-filtered upstream;
            // stamp placeholder TX bytes so the codec is happy on read-back.
            let encoded = codec::encode_row(0, 0, &row);
            inner_spills[partition]
                .as_mut()
                .expect("partition file present")
                .write_row(&encoded)?;
        }
        // Then the left side, partitioned the same way.
        let mut left = self.left.take().expect("left drained twice");
        while let Some(row) = left.next(pager)? {
            let partition = partition_for(&row[self.probe_col], &hasher_state);
            // Spilled rows have already been visibility-filtered upstream;
            // stamp placeholder TX bytes so the codec is happy on read-back.
            let encoded = codec::encode_row(0, 0, &row);
            left_spills[partition]
                .as_mut()
                .expect("partition file present")
                .write_row(&encoded)?;
        }
        self.inner_spills = Some(inner_spills);
        self.left_spills = Some(left_spills);
        Ok(())
    }
}

impl Operator for GraceHashJoin {
    fn next(&mut self, pager: &mut Pager) -> Result<Option<Vec<Value>>> {
        if self.inner_spills.is_none() {
            self.partition_phase(pager)?;
        }
        loop {
            // Pull from the current partition's in-memory join until it ends.
            if let Some(current) = self.current.as_mut() {
                match current.next(pager)? {
                    Some(row) => return Ok(Some(row)),
                    None => self.current = None,
                }
            }
            // Open the next partition pair.
            if self.partition >= HASH_PARTITIONS {
                return Ok(None);
            }
            let i = self.partition;
            self.partition += 1;
            let mut inner_spill = self.inner_spills.as_mut().expect("partitioned")[i]
                .take()
                .expect("partition taken twice");
            let mut left_spill = self.left_spills.as_mut().expect("partitioned")[i]
                .take()
                .expect("partition taken twice");
            inner_spill.rewind()?;
            left_spill.rewind()?;
            let inner_reader = SpillReader {
                spill: inner_spill,
                column_count: self.right_width,
            };
            let left_reader = SpillReader {
                spill: left_spill,
                column_count: self.left_width,
            };
            // An ordinary in-memory hash join over the partition pair — it
            // will re-apply the full `ON` predicate and `NULL`-pad LEFT misses.
            self.current = Some(HashJoin {
                left: Box::new(left_reader),
                right_input: Some(Box::new(inner_reader)),
                table: None,
                probe_col: self.probe_col,
                build_col: self.build_col,
                on: self.on.clone(),
                kind: self.kind,
                scope: self.scope.clone(),
                right_width: self.right_width,
                current_left: None,
                probe_key: None,
                match_pos: 0,
                matched_current: false,
            });
        }
    }
}

/// Which partition the encoded `value` hashes to. A `NULL` value is sent to
/// partition 0 unconditionally — it can never match anything, so any one
/// partition is fine, and putting all `NULL`s in one keeps the rule simple.
fn partition_for(value: &Value, hasher_state: &RandomState) -> usize {
    if value.is_null() {
        return 0;
    }
    let encoded = codec::encode_index_value(value);
    let mut hasher = hasher_state.build_hasher();
    hasher.write(&encoded);
    (hasher.finish() % HASH_PARTITIONS as u64) as usize
}

/// A temp file holding length-prefixed encoded rows — the body of a grace-hash
/// partition. Removed on drop, so a panic or early return cleans up after
/// itself; living in the OS temp dir means the OS will sweep up anything that
/// somehow escapes.
struct SpillFile {
    path: PathBuf,
    file: File,
}

impl SpillFile {
    fn create() -> Result<SpillFile> {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("prehnite-spill-{}-{}", std::process::id(), n));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;
        Ok(SpillFile { path, file })
    }

    /// Append one length-prefixed row.
    fn write_row(&mut self, encoded: &[u8]) -> Result<()> {
        self.file.write_all(&(encoded.len() as u32).to_be_bytes())?;
        self.file.write_all(encoded)?;
        Ok(())
    }

    /// Reposition the read cursor at the start, between the partition phase
    /// and the join phase that reads back what it wrote.
    fn rewind(&mut self) -> Result<()> {
        self.file.seek(SeekFrom::Start(0))?;
        Ok(())
    }

    /// Read the next row's bytes, or `None` at end of file.
    fn read_row(&mut self) -> Result<Option<Vec<u8>>> {
        let mut len_buf = [0u8; 4];
        match self.file.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(Error::Io(e)),
        }
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut bytes = vec![0u8; len];
        self.file.read_exact(&mut bytes)?;
        Ok(Some(bytes))
    }
}

impl Drop for SpillFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// An [`Operator`] that hands rows back out of a [`SpillFile`] — one of the
/// two inputs to a per-partition [`HashJoin`] inside a [`GraceHashJoin`].
struct SpillReader {
    spill: SpillFile,
    column_count: usize,
}

impl Operator for SpillReader {
    fn next(&mut self, _pager: &mut Pager) -> Result<Option<Vec<Value>>> {
        match self.spill.read_row()? {
            Some(bytes) => Ok(Some(codec::decode_row(&bytes, self.column_count)?.values)),
            None => Ok(None),
        }
    }
}

/// Stable-sort rows by `(column, descending)` keys; `NULL`s sort first.
fn sort_rows(rows: &mut [Vec<Value>], keys: &[(usize, bool)]) {
    rows.sort_by(|a, b| {
        for &(column, descending) in keys {
            let ordering = order_values(&a[column], &b[column]);
            let ordering = if descending {
                ordering.reverse()
            } else {
                ordering
            };
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        Ordering::Equal
    });
}

/// Run a grouped or whole-table aggregate query: partition the matched rows
/// into groups by the `GROUP BY` columns (no `GROUP BY` ⇒ a single group of
/// everything) and emit one output row per group.
fn grouped_select(
    scope: &Scope,
    items: &[SelectItem],
    group_by: &[ColumnRef],
    having: Option<&Expr>,
    order_by: &[OrderKey],
    matched: Vec<Vec<Value>>,
) -> Result<QueryResult> {
    let group_cols: Vec<usize> = group_by
        .iter()
        .map(|colref| scope.resolve(colref))
        .collect::<Result<_>>()?;

    // A bare column in the SELECT list must be one of the GROUP BY columns —
    // otherwise its value is not well-defined for the group.
    for item in items {
        if let SelectItem::Column(colref) = item {
            let column = scope.resolve(colref)?;
            if !group_cols.contains(&column) {
                return Err(Error::exec(format!(
                    "column '{colref}' must appear in GROUP BY or inside an aggregate"
                )));
            }
        }
    }

    // Collect every distinct aggregate the projection and HAVING mention; each
    // gets one slot and is updated exactly once per input row.
    let registry = AggregateRegistry::build(items, having, scope)?;
    let template: Vec<AggregateState> = registry
        .slots
        .iter()
        .map(|slot| AggregateState::for_slot(slot, scope))
        .collect::<Result<_>>()?;

    // Hash pass: one bucket per distinct grouping-column tuple, holding the
    // running aggregate state. Insertion order is tracked separately so the
    // output is deterministic when there is no ORDER BY.
    use std::collections::hash_map::Entry;
    let mut buckets: HashMap<GroupKey, Vec<AggregateState>> = HashMap::new();
    let mut insertion_order: Vec<GroupKey> = Vec::new();
    for row in &matched {
        let key = GroupKey {
            values: group_cols.iter().map(|&i| row[i].clone()).collect(),
        };
        let states = match buckets.entry(key) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => {
                insertion_order.push(e.key().clone());
                e.insert(template.clone())
            }
        };
        for (state, slot) in states.iter_mut().zip(&registry.slots) {
            state.update(slot, row)?;
        }
    }
    // Whole-table aggregate (no GROUP BY) over zero input rows still yields
    // one result row, with the aggregates at their initial values — `COUNT`
    // = 0, everything else `NULL`.
    if group_cols.is_empty() && buckets.is_empty() {
        let key = GroupKey { values: Vec::new() };
        insertion_order.push(key.clone());
        buckets.insert(key, template.clone());
    }

    // Finalise each bucket: aggregate states collapse to their result values.
    let mut groups: Vec<(GroupKey, Vec<Value>)> = Vec::with_capacity(insertion_order.len());
    for key in insertion_order {
        let states = buckets.remove(&key).expect("inserted above");
        let aggregates: Vec<Value> = states.into_iter().map(|s| s.finalize()).collect();
        groups.push((key, aggregates));
    }

    // HAVING discards whole groups, judged by their aggregates.
    if let Some(predicate) = having {
        let mut kept = Vec::with_capacity(groups.len());
        for (key, aggregates) in groups {
            let verdict =
                eval_group_expr(predicate, &group_cols, &key, &aggregates, &registry, scope)?;
            if matches!(verdict, Value::Bool(true)) {
                kept.push((key, aggregates));
            }
        }
        groups = kept;
    }

    // ORDER BY on a grouped query orders the groups and may name only GROUP BY
    // columns. With no GROUP BY there is a single group, so nothing to order.
    if !group_cols.is_empty() && !order_by.is_empty() {
        let mut keys = Vec::with_capacity(order_by.len());
        for key in order_by {
            let column = scope.resolve(&key.column)?;
            let pos = group_cols
                .iter()
                .position(|&c| c == column)
                .ok_or_else(|| {
                    Error::exec(format!(
                        "ORDER BY column '{}' must be a GROUP BY column here",
                        key.column
                    ))
                })?;
            keys.push((pos, key.descending));
        }
        groups.sort_by(|a, b| {
            for &(pos, descending) in &keys {
                let ordering = order_values(&a.0.values[pos], &b.0.values[pos]);
                let ordering = if descending {
                    ordering.reverse()
                } else {
                    ordering
                };
                if ordering != Ordering::Equal {
                    return ordering;
                }
            }
            Ordering::Equal
        });
    }

    let columns: Vec<String> = items
        .iter()
        .map(|item| match item {
            SelectItem::Column(colref) => colref.to_string(),
            SelectItem::Aggregate(aggregate) => aggregate_label(aggregate),
            SelectItem::Expr(_) => "?column?".to_string(),
        })
        .collect();

    let mut rows = Vec::with_capacity(groups.len());
    for (key, aggregates) in &groups {
        let mut row = Vec::with_capacity(items.len());
        for item in items {
            row.push(match item {
                SelectItem::Column(colref) => {
                    let column = scope.resolve(colref)?;
                    let pos = group_cols
                        .iter()
                        .position(|&c| c == column)
                        .expect("validated above");
                    key.values[pos].clone()
                }
                SelectItem::Aggregate(aggregate) => {
                    let idx = registry
                        .lookup(aggregate)
                        .expect("aggregate registered above");
                    aggregates[idx].clone()
                }
                SelectItem::Expr(expr) => {
                    eval_group_expr(expr, &group_cols, key, aggregates, &registry, scope)?
                }
            });
        }
        rows.push(row);
    }
    Ok(QueryResult::Rows { columns, rows })
}

/// The grouping-column tuple of one bucket. A custom `Eq`/`Hash` over
/// [`Value`] is needed because `f64` is not `Eq` and not `Hash` — we hash
/// `Real` by `to_bits` and compare by bit-equality, which puts every NaN in
/// one bucket and keeps -0 and 0 distinct (consistent with `to_bits`).
#[derive(Clone)]
struct GroupKey {
    values: Vec<Value>,
}

impl PartialEq for GroupKey {
    fn eq(&self, other: &Self) -> bool {
        self.values.len() == other.values.len()
            && self
                .values
                .iter()
                .zip(&other.values)
                .all(|(a, b)| value_eq(a, b))
    }
}

impl Eq for GroupKey {}

impl Hash for GroupKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for v in &self.values {
            hash_value(v, state);
        }
    }
}

fn value_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Null, Value::Null) => true,
        (Value::Int(x), Value::Int(y)) => x == y,
        (Value::Real(x), Value::Real(y)) => x.to_bits() == y.to_bits(),
        (Value::Text(x), Value::Text(y)) => x == y,
        (Value::Bool(x), Value::Bool(y)) => x == y,
        _ => false,
    }
}

fn hash_value<H: Hasher>(v: &Value, state: &mut H) {
    // A discriminant byte first, so a `Bool(false)` and a `Null` cannot hash
    // to the same word by accident.
    match v {
        Value::Null => 0u8.hash(state),
        Value::Int(n) => {
            1u8.hash(state);
            n.hash(state);
        }
        Value::Real(r) => {
            2u8.hash(state);
            r.to_bits().hash(state);
        }
        Value::Text(s) => {
            3u8.hash(state);
            s.hash(state);
        }
        Value::Bool(b) => {
            4u8.hash(state);
            b.hash(state);
        }
    }
}

/// One distinct aggregate call in the query, paired with its resolved column
/// index. Two textually identical aggregates share one slot.
#[derive(Clone)]
struct AggregateSlot {
    func: AggregateFunc,
    /// Column index of the argument, or `None` for `COUNT(*)`.
    column: Option<usize>,
}

/// The set of distinct aggregates in a query's projection and HAVING clause,
/// each with a stable index that the per-row update loop and the per-group
/// eval pass both refer to.
struct AggregateRegistry {
    slots: Vec<AggregateSlot>,
    by_aggregate: HashMap<Aggregate, usize>,
}

impl AggregateRegistry {
    fn build(
        items: &[SelectItem],
        having: Option<&Expr>,
        scope: &Scope,
    ) -> Result<AggregateRegistry> {
        let mut reg = AggregateRegistry {
            slots: Vec::new(),
            by_aggregate: HashMap::new(),
        };
        for item in items {
            match item {
                SelectItem::Aggregate(a) => reg.intern(a, scope)?,
                SelectItem::Expr(e) => reg.collect_in_expr(e, scope)?,
                SelectItem::Column(_) => {}
            }
        }
        if let Some(h) = having {
            reg.collect_in_expr(h, scope)?;
        }
        Ok(reg)
    }

    fn intern(&mut self, aggregate: &Aggregate, scope: &Scope) -> Result<()> {
        if self.by_aggregate.contains_key(aggregate) {
            return Ok(());
        }
        let column = match &aggregate.arg {
            AggregateArg::Star => None,
            AggregateArg::Column(colref) => Some(scope.resolve(colref)?),
        };
        if column.is_none() && !matches!(aggregate.func, AggregateFunc::Count) {
            return Err(Error::exec(format!(
                "{}(*) is not allowed — {} needs a column",
                func_name(aggregate.func),
                func_name(aggregate.func)
            )));
        }
        let idx = self.slots.len();
        self.by_aggregate.insert(aggregate.clone(), idx);
        self.slots.push(AggregateSlot {
            func: aggregate.func,
            column,
        });
        Ok(())
    }

    fn collect_in_expr(&mut self, expr: &Expr, scope: &Scope) -> Result<()> {
        match expr {
            Expr::Aggregate(a) => self.intern(a, scope)?,
            Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } => {
                self.collect_in_expr(expr, scope)?;
            }
            Expr::Binary { left, right, .. } => {
                self.collect_in_expr(left, scope)?;
                self.collect_in_expr(right, scope)?;
            }
            Expr::InSubquery { expr, .. } | Expr::InList { expr, .. } => {
                self.collect_in_expr(expr, scope)?;
            }
            _ => {}
        }
        Ok(())
    }

    fn lookup(&self, aggregate: &Aggregate) -> Option<usize> {
        self.by_aggregate.get(aggregate).copied()
    }
}

/// One aggregate's running state, updated in place per input row. A bucket
/// holds `Vec<AggregateState>` parallel to the [`AggregateRegistry`]'s slots.
#[derive(Clone)]
enum AggregateState {
    /// `COUNT(*)` or `COUNT(col)` — non-null row count.
    Count(u64),
    /// `SUM` over an `INT` column — kept in `i64` with overflow checking.
    SumInt { total: i64, seen: bool },
    /// `SUM` over a `REAL` column.
    SumReal { total: f64, seen: bool },
    /// `AVG` over either numeric type — sum tracked in `f64` always.
    AvgReal { total: f64, count: u64 },
    /// `MIN` (`want = Less`) or `MAX` (`want = Greater`) — `None` until the
    /// first non-null row arrives.
    Extreme { best: Option<Value>, want: Ordering },
}

impl AggregateState {
    fn for_slot(slot: &AggregateSlot, scope: &Scope) -> Result<AggregateState> {
        match slot.func {
            AggregateFunc::Count => Ok(AggregateState::Count(0)),
            AggregateFunc::Sum => {
                let column = slot.column.expect("SUM has a column");
                match scope.column_type(column) {
                    Type::Int => Ok(AggregateState::SumInt {
                        total: 0,
                        seen: false,
                    }),
                    Type::Real => Ok(AggregateState::SumReal {
                        total: 0.0,
                        seen: false,
                    }),
                    other => Err(Error::exec(format!(
                        "SUM requires a numeric column, but '{}' is {other}",
                        scope.column_name(column)
                    ))),
                }
            }
            AggregateFunc::Avg => {
                let column = slot.column.expect("AVG has a column");
                match scope.column_type(column) {
                    Type::Int | Type::Real => Ok(AggregateState::AvgReal {
                        total: 0.0,
                        count: 0,
                    }),
                    other => Err(Error::exec(format!(
                        "AVG requires a numeric column, but '{}' is {other}",
                        scope.column_name(column)
                    ))),
                }
            }
            AggregateFunc::Min => Ok(AggregateState::Extreme {
                best: None,
                want: Ordering::Less,
            }),
            AggregateFunc::Max => Ok(AggregateState::Extreme {
                best: None,
                want: Ordering::Greater,
            }),
        }
    }

    fn update(&mut self, slot: &AggregateSlot, row: &[Value]) -> Result<()> {
        match (slot.func, slot.column, self) {
            (AggregateFunc::Count, None, AggregateState::Count(c)) => *c += 1,
            (AggregateFunc::Count, Some(col), AggregateState::Count(c)) => {
                if !row[col].is_null() {
                    *c += 1;
                }
            }
            (AggregateFunc::Sum, Some(col), AggregateState::SumInt { total, seen }) => {
                if let Value::Int(n) = &row[col] {
                    *seen = true;
                    *total = total
                        .checked_add(*n)
                        .ok_or_else(|| Error::exec("SUM overflowed a 64-bit integer"))?;
                }
            }
            (AggregateFunc::Sum, Some(col), AggregateState::SumReal { total, seen }) => {
                if let Value::Real(x) = &row[col] {
                    *seen = true;
                    *total += x;
                }
            }
            (AggregateFunc::Avg, Some(col), AggregateState::AvgReal { total, count }) => {
                match &row[col] {
                    Value::Int(n) => {
                        *total += *n as f64;
                        *count += 1;
                    }
                    Value::Real(x) => {
                        *total += x;
                        *count += 1;
                    }
                    _ => {}
                }
            }
            (
                AggregateFunc::Min | AggregateFunc::Max,
                Some(col),
                AggregateState::Extreme { best, want },
            ) => {
                let value = &row[col];
                if value.is_null() {
                    return Ok(());
                }
                let replace = match best {
                    None => true,
                    Some(current) => order_values(value, current) == *want,
                };
                if replace {
                    *best = Some(value.clone());
                }
            }
            _ => unreachable!("aggregate state and slot are out of sync"),
        }
        Ok(())
    }

    fn finalize(self) -> Value {
        match self {
            AggregateState::Count(c) => Value::Int(c as i64),
            AggregateState::SumInt { total, seen } => {
                if seen {
                    Value::Int(total)
                } else {
                    Value::Null
                }
            }
            AggregateState::SumReal { total, seen } => {
                if seen {
                    Value::Real(total)
                } else {
                    Value::Null
                }
            }
            AggregateState::AvgReal { total, count } => {
                if count == 0 {
                    Value::Null
                } else {
                    Value::Real(total / count as f64)
                }
            }
            AggregateState::Extreme { best, .. } => best.unwrap_or(Value::Null),
        }
    }
}

/// Evaluate an expression in the context of one finalised group: column
/// references resolve to the group's value for that grouping column, and
/// aggregate calls look up the precomputed value from the registry. Replaces
/// the old `eval_having` *and* powers projection-time expression evaluation,
/// so an aggregate used in both runs exactly once across the whole query.
fn eval_group_expr(
    expr: &Expr,
    group_cols: &[usize],
    key: &GroupKey,
    aggregates: &[Value],
    registry: &AggregateRegistry,
    scope: &Scope,
) -> Result<Value> {
    match expr {
        Expr::Null => Ok(Value::Null),
        Expr::Integer(n) => Ok(Value::Int(*n)),
        Expr::Real(r) => Ok(Value::Real(*r)),
        Expr::Str(s) => Ok(Value::Text(s.clone())),
        Expr::Bool(b) => Ok(Value::Bool(*b)),
        Expr::Aggregate(aggregate) => {
            let idx = registry
                .lookup(aggregate)
                .expect("aggregate registered during build");
            Ok(aggregates[idx].clone())
        }
        Expr::Column(colref) => {
            let column = scope.resolve(colref)?;
            let pos = group_cols
                .iter()
                .position(|&c| c == column)
                .ok_or_else(|| {
                    Error::exec(format!(
                        "column '{colref}' must be a GROUP BY column or wrapped in an aggregate"
                    ))
                })?;
            Ok(key.values[pos].clone())
        }
        Expr::Unary { op, expr } => eval_unary(
            *op,
            eval_group_expr(expr, group_cols, key, aggregates, registry, scope)?,
        ),
        Expr::Binary { op, left, right } => eval_binary(
            *op,
            eval_group_expr(left, group_cols, key, aggregates, registry, scope)?,
            eval_group_expr(right, group_cols, key, aggregates, registry, scope)?,
        ),
        Expr::IsNull { expr, negated } => {
            let value = eval_group_expr(expr, group_cols, key, aggregates, registry, scope)?;
            Ok(Value::Bool(value.is_null() != *negated))
        }
        Expr::InList {
            expr,
            values,
            has_null,
            negated,
        } => {
            let probe = eval_group_expr(expr, group_cols, key, aggregates, registry, scope)?;
            let result = eval_in_list(probe, values, *has_null)?;
            if *negated {
                Ok(negate_bool(result))
            } else {
                Ok(result)
            }
        }
        Expr::InSubquery { .. }
        | Expr::Exists(_)
        | Expr::ScalarSubquery(_)
        | Expr::CorrelatedExists(_)
        | Expr::CorrelatedScalarSubquery(_)
        | Expr::CorrelatedInSubquery { .. } => Err(Error::corruption(
            "subquery in grouped expression was not pre-evaluated",
        )),
    }
}

/// Resolve each `ORDER BY` key's column reference to its index.
fn resolve_order_keys(scope: &Scope, order_by: &[OrderKey]) -> Result<Vec<(usize, bool)>> {
    order_by
        .iter()
        .map(|key| Ok((scope.resolve(&key.column)?, key.descending)))
        .collect()
}

/// A total order over values, used for `ORDER BY` and `MIN`/`MAX`. `NULL` sorts
/// before every non-null value.
fn order_values(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::Int(x), Value::Int(y)) => x.cmp(y),
        (Value::Real(x), Value::Real(y)) => x.total_cmp(y),
        (Value::Int(x), Value::Real(y)) => (*x as f64).total_cmp(y),
        (Value::Real(x), Value::Int(y)) => x.total_cmp(&(*y as f64)),
        (Value::Text(x), Value::Text(y)) => x.cmp(y),
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        // Mismatched types never arise within one column; rank for totality.
        _ => value_rank(a).cmp(&value_rank(b)),
    }
}

fn value_rank(value: &Value) -> u8 {
    match value {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Int(_) | Value::Real(_) => 2,
        Value::Text(_) => 3,
    }
}

/// The output `Type` of an aggregate over `scope`'s columns.
///
/// `COUNT` is always `Int`. `SUM` keeps the input's numeric type
/// (`Int → Int`, `Real → Real`). `AVG` is always `Real`. `MIN` and
/// `MAX` keep the input column's type. Used by
/// [`BatchHashAggregate`] to type its output columns upfront.
fn infer_aggregate_type(aggregate: &Aggregate, scope: &Scope) -> Result<Type> {
    match aggregate.func {
        AggregateFunc::Count => Ok(Type::Int),
        AggregateFunc::Sum => {
            let column = match &aggregate.arg {
                AggregateArg::Column(colref) => scope.resolve(colref)?,
                AggregateArg::Star => {
                    return Err(Error::exec("SUM(*) is not allowed — SUM needs a column"))
                }
            };
            match scope.column_type(column) {
                Type::Int => Ok(Type::Int),
                Type::Real => Ok(Type::Real),
                other => Err(Error::exec(format!(
                    "SUM requires a numeric column, but '{}' is {other}",
                    scope.column_name(column)
                ))),
            }
        }
        AggregateFunc::Avg => {
            let column = match &aggregate.arg {
                AggregateArg::Column(colref) => scope.resolve(colref)?,
                AggregateArg::Star => {
                    return Err(Error::exec("AVG(*) is not allowed — AVG needs a column"))
                }
            };
            match scope.column_type(column) {
                Type::Int | Type::Real => Ok(Type::Real),
                other => Err(Error::exec(format!(
                    "AVG requires a numeric column, but '{}' is {other}",
                    scope.column_name(column)
                ))),
            }
        }
        AggregateFunc::Min | AggregateFunc::Max => {
            let column = match &aggregate.arg {
                AggregateArg::Column(colref) => scope.resolve(colref)?,
                AggregateArg::Star => {
                    return Err(Error::exec(format!(
                        "{}(*) is not allowed — {} needs a column",
                        func_name(aggregate.func),
                        func_name(aggregate.func)
                    )))
                }
            };
            Ok(scope.column_type(column))
        }
    }
}

/// Infer the output types of the vectorised grouped projection. v0.33
/// supports only `SelectItem::Column` (which must be a `GROUP BY`
/// column) and `SelectItem::Aggregate`. Returns `Err` on any
/// `SelectItem::Expr` — the dispatch gate steers those to the row
/// pipeline.
fn infer_grouped_output_types(items: &[SelectItem], scope: &Scope) -> Result<Vec<Type>> {
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        out.push(match item {
            SelectItem::Column(colref) => scope.column_type(scope.resolve(colref)?),
            SelectItem::Aggregate(agg) => infer_aggregate_type(agg, scope)?,
            SelectItem::Expr(_) => {
                return Err(Error::corruption(
                    "vectorised aggregation reached Expr-item projection",
                ))
            }
        });
    }
    Ok(out)
}

fn aggregate_label(aggregate: &Aggregate) -> String {
    let func = func_name(aggregate.func);
    match &aggregate.arg {
        AggregateArg::Star => format!("{func}(*)"),
        AggregateArg::Column(colref) => format!("{func}({colref})"),
    }
}

fn func_name(func: AggregateFunc) -> &'static str {
    match func {
        AggregateFunc::Count => "COUNT",
        AggregateFunc::Sum => "SUM",
        AggregateFunc::Avg => "AVG",
        AggregateFunc::Min => "MIN",
        AggregateFunc::Max => "MAX",
    }
}

fn update(
    pager: &mut Pager,
    catalog: &Catalog,
    table: String,
    mut assignments: Vec<(String, Expr)>,
    filter: Option<Expr>,
    access: AccessPath,
    snapshot: &Snapshot,
) -> Result<QueryResult> {
    let mut schema = require_table(pager, catalog, &table)?;
    let tx_id = snapshot
        .own_tx
        .ok_or_else(|| Error::corruption("UPDATE reached executor without a write TX"))?;
    let scope = Scope::single(&table, &schema);

    for (_, expr) in assignments.iter_mut() {
        prepare_subqueries(expr, pager, catalog, snapshot)?;
    }
    let mut filter = filter;
    if let Some(predicate) = filter.as_mut() {
        prepare_subqueries(predicate, pager, catalog, snapshot)?;
    }

    let mut resolved = Vec::with_capacity(assignments.len());
    for (name, expr) in &assignments {
        resolved.push((column_index(&schema, name)?, expr));
    }

    let table_tree = BTree::open(schema.root);
    let mut updated = 0u64;
    for (rowid_key, record) in collect_candidates(pager, &schema, &access, snapshot)? {
        if !passes_filter(filter.as_ref(), &scope, &record.values)? {
            continue;
        }
        // FUW after WHERE — see `delete` for the rationale.
        check_write_write_conflict(&record, snapshot)?;
        let old = record.values;
        let mut new = old.clone();
        for (column, expr) in &resolved {
            let evaluated = eval(
                expr,
                Some(&RowContext {
                    scope: &scope,
                    values: &old,
                }),
            )?;
            new[*column] = coerce(evaluated, schema.columns[*column].ty)?;
        }
        // v0.43: NOT NULL check on every column the SET touches (and
        // technically on every NOT NULL column, since UPDATE could in
        // principle assign NULL to one via a subquery — we check all
        // for safety). The UNIQUE constraint is enforced inside
        // `index_insert_row` below: the new row's index entry collides
        // with any existing one on a unique index.
        for (col_idx, column) in schema.columns.iter().enumerate() {
            if column.not_null && matches!(new[col_idx], Value::Null) {
                return Err(Error::exec(format!(
                    "null value in column '{}' of '{}' violates NOT NULL constraint",
                    column.name, table
                )));
            }
        }
        // v0.45: FOREIGN KEY check on the child side. If this UPDATE
        // changed an FK column's value to something new and non-NULL,
        // the new value must resolve to an existing parent row.
        // `check_foreign_keys` walks every FK column unconditionally
        // — slightly more work than strictly needed, but it's a
        // bounded number of lookups and we'd otherwise have to
        // diff old vs new column by column.
        check_foreign_keys(pager, catalog, &schema, &new)?;
        // v0.45: FOREIGN KEY check on the parent side. If this UPDATE
        // changes a column that some other table's FK points at (PK
        // or UNIQUE), RESTRICT: refuse the update when any child
        // references the old value.
        let referenced = schema
            .indexes
            .iter()
            .filter(|i| i.unique && i.columns.len() == 1)
            .map(|i| i.columns[0])
            .collect::<Vec<_>>();
        let mut parent_changed = false;
        for &col_idx in &referenced {
            if old[col_idx] != new[col_idx] {
                parent_changed = true;
                break;
            }
        }
        if parent_changed {
            check_no_child_references(pager, catalog, &schema, &old)?;
        }
        // Logical update: tombstone the old version in place (tx_max = our
        // TX) and write a new version with a fresh rowid. The old row stays
        // visible to readers whose snapshot predates this TX.
        table_tree.insert(
            pager,
            &rowid_key,
            &codec::encode_row(record.tx_min, tx_id, &old),
        )?;
        // SSI: record the write — peers reading this rowid get an
        // rw-edge to us.
        snapshot.record_write(schema.root, &rowid_key);
        // Reserve the new version's rowid through the shared atomic
        // counter; see `insert` for why.
        let new_rowid = snapshot.reserve_rowid(&table, schema.next_rowid);
        let new_rowid_key = codec::rowid_key(new_rowid);
        table_tree.insert(pager, &new_rowid_key, &codec::encode_row(tx_id, 0, &new))?;
        // v0.48: pass the old values so the unique check skips any
        // index whose column values didn't change — otherwise the
        // existing (about-to-tombstone) index entry trips a false
        // positive. The unique check still fires when an UPDATE
        // *does* change a unique column (the legitimate conflict
        // case).
        index_insert_row_with_old(pager, &schema, &new_rowid_key, &new, Some(&old))?;
        updated += 1;
    }
    // The row count is "live row count" — net change zero for an update.
    schema.next_rowid = snapshot.current_next_rowid(&table, schema.next_rowid);
    // v0.49: auto-analyze counter — each updated row counts as one
    // mutation. UPDATE rewrites the row in place (logically), so the
    // shape of the data shifts even though `row_count` doesn't.
    schema.mutations_since_analyze =
        schema.mutations_since_analyze.saturating_add(updated);
    catalog.put(pager, &schema)?;
    Ok(QueryResult::Ack(format!("{updated} row(s) updated")))
}

fn delete(
    pager: &mut Pager,
    catalog: &Catalog,
    table: String,
    filter: Option<Expr>,
    access: AccessPath,
    snapshot: &Snapshot,
) -> Result<QueryResult> {
    let mut schema = require_table(pager, catalog, &table)?;
    let tx_id = snapshot
        .own_tx
        .ok_or_else(|| Error::corruption("DELETE reached executor without a write TX"))?;
    let scope = Scope::single(&table, &schema);
    let table_tree = BTree::open(schema.root);

    let mut filter = filter;
    if let Some(predicate) = filter.as_mut() {
        prepare_subqueries(predicate, pager, catalog, snapshot)?;
    }

    let mut deleted = 0u64;
    for (rowid_key, record) in collect_candidates(pager, &schema, &access, snapshot)? {
        if !passes_filter(filter.as_ref(), &scope, &record.values)? {
            continue;
        }
        // FUW conflict check: only fires on rows we actually intend to
        // write. Doing it here (not in `collect_candidates`) keeps
        // disjoint-row writers from spuriously conflicting just because
        // their scans touch the same in-flight tombstones.
        check_write_write_conflict(&record, snapshot)?;
        // v0.48: dispatch on each child FK's ON DELETE action.
        // RESTRICT (the v0.45 default) → refuse on any match;
        // CASCADE → delete matching child rows (recursively, via
        // the engine's own delete path so each cascaded delete
        // applies *its* own actions); SET NULL → UPDATE matching
        // children to NULL (or error if child column is NOT NULL).
        apply_parent_delete_actions(pager, catalog, snapshot, &schema, &record.values)?;
        // Logical delete: rewrite the row in place with tx_max set. Index
        // entries are left alone — the row is still in the tree, just
        // tombstoned, and visibility on the table side filters it.
        table_tree.insert(
            pager,
            &rowid_key,
            &codec::encode_row(record.tx_min, tx_id, &record.values),
        )?;
        // SSI: record the write — peers that read this row in their
        // active transactions get an rw-edge pointing at us.
        snapshot.record_write(schema.root, &rowid_key);
        deleted += 1;
    }
    if deleted > 0 {
        schema.row_count = schema.row_count.saturating_sub(deleted);
        // v0.49: auto-analyze counter.
        schema.mutations_since_analyze =
            schema.mutations_since_analyze.saturating_add(deleted);
        catalog.put(pager, &schema)?;
    }
    Ok(QueryResult::Ack(format!("{deleted} row(s) deleted")))
}

/// FUW (first-updater-wins) write-write conflict check. Called by
/// `update` and `delete` after the `WHERE` filter has decided we
/// actually want to write `record`. If `record.tx_max` is in flight by
/// another writer, the row is mid-tombstoning by a peer — we abort
/// with [`Error::conflict`] so our transaction can roll back and
/// retry. A rolled-back peer is harmless (we can overwrite); a
/// committed peer was filtered out by visibility and shouldn't reach
/// here (defensive `Ok`).
fn check_write_write_conflict(record: &codec::RowRecord, snapshot: &Snapshot) -> Result<()> {
    if record.tx_max == 0 || Some(record.tx_max) == snapshot.own_tx {
        return Ok(());
    }
    match snapshot.clog.status(record.tx_max) {
        Some(crate::engine::clog::Status::RolledBack) | Some(crate::engine::clog::Status::Committed) => {
            Ok(())
        }
        None => Err(Error::conflict(format!(
            "write-write conflict on a row stamped by in-flight transaction {}",
            record.tx_max
        ))),
    }
}

/// Gather the rows a query should consider, as `(rowid key, RowRecord)`
/// pairs, via the access path the planner chose. Visibility is applied —
/// rows the snapshot can't see are not returned. Index lookups may yield
/// duplicate rowids (an UPDATE inserts a new version with its own rowid
/// but leaves old index entries); we dedupe by rowid.
///
/// v0.29: write-write conflict detection is deferred to the caller
/// (`update` / `delete`) so it runs only on rows the `WHERE` filter
/// keeps. Calling it inside the scan would conflict on tombstones that
/// the WHERE would have discarded anyway, falsely aborting disjoint
/// writers. Reads recorded into the SSI read-set happen here.
fn collect_candidates(
    pager: &mut Pager,
    schema: &Schema,
    access: &AccessPath,
    snapshot: &Snapshot,
) -> Result<Vec<(Vec<u8>, codec::RowRecord)>> {
    let table = BTree::open(schema.root);
    let table_root = schema.root;
    let is_full_scan = matches!(access, AccessPath::FullScan);
    // v0.35: a full-scan candidate collection takes a relation-level
    // SSI lock — the entire table is in this writer's read set, so
    // any concurrent insert into the table is a phantom that marks an
    // rw-edge. An index-scan path keeps per-tuple locks (bounded set,
    // already cheap).
    if is_full_scan {
        snapshot.record_relation_read(table_root);
    }
    let admit = |out: &mut Vec<(Vec<u8>, codec::RowRecord)>,
                 rowid_key: Vec<u8>,
                 record: codec::RowRecord|
     -> Result<()> {
        if !snapshot.visible(record.tx_min, record.tx_max) {
            return Ok(());
        }
        // For index scans, record the specific tuple — the full-scan
        // path already took a relation lock above. The FUW write-write
        // conflict check is deferred to the caller — once the
        // `WHERE` filter has decided whether we actually intend to
        // *write* this row.
        if !is_full_scan {
            let tombstone_by = if record.tx_max != 0 {
                Some(record.tx_max)
            } else {
                None
            };
            snapshot.record_read(table_root, &rowid_key, tombstone_by);
        }
        out.push((rowid_key, record));
        Ok(())
    };

    let mut out: Vec<(Vec<u8>, codec::RowRecord)> = Vec::new();
    match access {
        AccessPath::FullScan => {
            for (rowid_key, encoded) in table.scan(pager)? {
                let record = codec::decode_row(&encoded, schema.columns.len())?;
                admit(&mut out, rowid_key, record)?;
            }
            Ok(out)
        }
        AccessPath::IndexScan {
            index_root,
            lower,
            upper,
        } => {
            let index = BTree::open(*index_root);
            let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
            for (index_key, _) in index.scan_range(pager, lower, upper.as_deref())? {
                if index_key.len() < 8 {
                    return Err(Error::corruption("index key shorter than a rowid"));
                }
                let rowid_key = index_key[index_key.len() - 8..].to_vec();
                if !seen.insert(rowid_key.clone()) {
                    continue;
                }
                match table.search(pager, &rowid_key)? {
                    Some(encoded) => {
                        let record = codec::decode_row(&encoded, schema.columns.len())?;
                        admit(&mut out, rowid_key, record)?;
                    }
                    None => {
                        return Err(Error::corruption(
                            "index references a row that does not exist",
                        ))
                    }
                }
            }
            Ok(out)
        }
    }
}

/// Add this row to every index on the table.
///
/// v0.43: a unique index (`PRIMARY KEY` or `UNIQUE` column) checks
/// for an existing entry with the same value prefix before inserting,
/// and returns an `exec` error on conflict — "duplicate key value
/// violates UNIQUE constraint '<idx>' on '<table>'". `NULL` values
/// are exempt from the check (SQL standard: multiple NULLs are
/// permitted in a UNIQUE column).
///
/// Known limitation: a row that was INSERTed under a transaction
/// that later ROLLBACKed still leaves an index entry behind (MVCC:
/// the index doesn't track `tx_min`/`tx_max`). A future re-INSERT
/// with the same value spuriously rejects until VACUUM reclaims the
/// rolled-back row. Single-statement workloads (no explicit
/// transactions) never hit this.
fn index_insert_row(
    pager: &mut Pager,
    schema: &Schema,
    rowid_key: &[u8],
    values: &[Value],
) -> Result<()> {
    index_insert_row_with_old(pager, schema, rowid_key, values, None)
}

/// Like [`index_insert_row`], but for the UPDATE path, where the
/// existing index entry for the old row is still present and would
/// otherwise spuriously trip the unique check. v0.48: when
/// `old_values` is `Some`, skip the unique check for any index
/// whose columns' values are unchanged — the "duplicate" the check
/// would find is the row we're about to tombstone.
fn index_insert_row_with_old(
    pager: &mut Pager,
    schema: &Schema,
    rowid_key: &[u8],
    values: &[Value],
    old_values: Option<&[Value]>,
) -> Result<()> {
    for index in &schema.indexes {
        if index.unique {
            // SQL standard: NULL values are not considered equal to
            // each other for UNIQUE purposes, so any NULL in the
            // indexed columns skips the duplicate check.
            let any_null = index
                .columns
                .iter()
                .any(|&c| matches!(values[c], Value::Null));
            // v0.48: if this is an UPDATE and the unique column(s)
            // values didn't change, the existing index entry is the
            // row we're tombstoning — not a real duplicate. Skip the
            // check. (The old row's MVCC tombstone makes its index
            // entry invisible to future readers, and VACUUM reclaims
            // it. The check would be a false positive.)
            let unchanged = old_values.is_some_and(|old| {
                index
                    .columns
                    .iter()
                    .all(|&c| values[c] == old[c])
            });
            if !any_null && !unchanged {
                let value_prefix = encode_index_value_prefix(values, &index.columns);
                if index_has_value(pager, index.root, &value_prefix)? {
                    return Err(Error::exec(format!(
                        "duplicate key value violates UNIQUE constraint '{}' on '{}'",
                        index.name, schema.name
                    )));
                }
            }
        }
        let key = codec::encode_index_key(values, &index.columns, rowid_key);
        BTree::open(index.root).insert(pager, &key, &[])?;
    }
    Ok(())
}

/// Encode just the value-prefix portion of an index key — the
/// concatenated value encodings for `columns`, without the trailing
/// rowid bytes. Used by the UNIQUE duplicate check, which scans the
/// index range that all keys for this value share.
fn encode_index_value_prefix(values: &[Value], columns: &[usize]) -> Vec<u8> {
    let mut key = Vec::new();
    for &column in columns {
        key.extend_from_slice(&codec::encode_index_value(&values[column]));
    }
    key
}

/// Whether any key in `index_root` starts with `value_prefix` — i.e.
/// whether some row already in the table has the same indexed
/// column value(s). Bounded scan from `value_prefix` up to its
/// `prefix_upper_bound`; we only need to know if a single entry
/// exists, so we stop at the first hit.
fn index_has_value(pager: &mut Pager, index_root: u32, value_prefix: &[u8]) -> Result<bool> {
    let upper = codec::prefix_upper_bound(value_prefix);
    let tree = BTree::open(index_root);
    let mut cursor = tree.cursor(pager, Some(value_prefix), upper)?;
    Ok(cursor.next(pager)?.is_some())
}

/// v0.45: enforce every `FOREIGN KEY` constraint on `schema` for the
/// row in `values`. For each FK column with a non-NULL value, look
/// up the parent's unique index for the matching key; if absent,
/// return a constraint-violation error. NULL FK values are exempt
/// (NULL means "no parent").
///
/// The lookup uses the parent's PK or UNIQUE index — which the
/// CREATE TABLE planner already validated exists for any column
/// referenced by an FK — so this is one B+tree prefix-range scan
/// per FK column per row, the same shape as the UNIQUE check.
fn check_foreign_keys(
    pager: &mut Pager,
    catalog: &Catalog,
    schema: &Schema,
    values: &[Value],
) -> Result<()> {
    for (idx, column) in schema.columns.iter().enumerate() {
        let Some(fk) = &column.foreign_key else {
            continue;
        };
        if matches!(values[idx], Value::Null) {
            continue;
        }
        // Look up the parent table and its referenced column. The
        // parent schema is read fresh — concurrent writers could in
        // principle change it, but DROP TABLE on a parent already
        // refuses while FKs point at it (see drop_table), and the
        // catalog read sees a consistent snapshot via the catalog
        // tree's per-page latches.
        let parent_schema = catalog.get(pager, &fk.table)?.ok_or_else(|| {
            Error::corruption(format!(
                "FOREIGN KEY parent table '{}' is gone (catalog inconsistent)",
                fk.table
            ))
        })?;
        let parent_idx = parent_schema.column_index(&fk.column).ok_or_else(|| {
            Error::corruption(format!(
                "FOREIGN KEY parent column '{}.{}' is gone",
                fk.table, fk.column
            ))
        })?;
        // Find the parent's unique index over that column. Planner
        // guaranteed one exists at CREATE TABLE time.
        let parent_index_root = parent_schema
            .indexes
            .iter()
            .find(|i| i.unique && i.columns == vec![parent_idx])
            .map(|i| i.root)
            .ok_or_else(|| {
                Error::corruption(format!(
                    "FOREIGN KEY parent column '{}.{}' has no unique index",
                    fk.table, fk.column
                ))
            })?;
        let key = codec::encode_index_value(&values[idx]);
        if !index_has_value(pager, parent_index_root, &key)? {
            return Err(Error::exec(format!(
                "FOREIGN KEY violation: value in column '{}' of '{}' has no matching parent in '{}.{}'",
                column.name, schema.name, fk.table, fk.column
            )));
        }
    }
    Ok(())
}

/// v0.45: enforce DELETE/UPDATE-parent RESTRICT semantics. For each
/// table in the catalog with an FK pointing at `parent_schema`,
/// scan for any row whose FK value equals `affected_values` (the
/// values of `parent_schema`'s PK/UNIQUE-referenced columns about
/// to be deleted or updated). If any child reference exists, return
/// a constraint-violation error.
///
/// `parent_visible_values` maps each PK/UNIQUE column index in the
/// parent to the value that's about to change. Today v0.45 only
/// covers the single-column FK case, so this is at most one entry
/// per FK target column.
///
/// The catalog scan is `O(tables * FKs-per-table)`; for v0.45 we
/// accept the linear cost and rely on small catalogs. A future
/// version could maintain a reverse-FK map keyed by parent table.
/// v0.45: parent UPDATE of a referenced column always RESTRICTs —
/// any child reference refuses the update. v0.48 keeps this for
/// UPDATEs since `ON UPDATE` actions aren't supported.
fn check_no_child_references(
    pager: &mut Pager,
    catalog: &Catalog,
    parent_schema: &Schema,
    parent_values: &[Value],
) -> Result<()> {
    let parent_name = &parent_schema.name;
    for (child_name, child_schema, child_idx, child_col, parent_col_idx) in
        children_referencing(pager, catalog, parent_schema)?
    {
        let key = codec::encode_index_value(&parent_values[parent_col_idx]);
        if scan_child_for_fk_value(pager, &child_schema, child_idx, &key, &parent_values[parent_col_idx])? {
            return Err(Error::exec(format!(
                "FOREIGN KEY violation: cannot modify '{}.{}' — row is referenced by '{}.{}'",
                parent_name, child_col.name, child_name, child_col.name
            )));
        }
    }
    Ok(())
}

/// v0.48: parent DELETE dispatches on each child FK's `on_delete`
/// action: RESTRICT (refuse), CASCADE (delete child rows), or SET
/// NULL (UPDATE child rows to NULL). Returns the list of CASCADE
/// child deletes to recurse into — the caller (`delete()`)
/// processes the recursion through the same code path so each
/// recursive delete checks its own FK actions in turn.
///
/// The SET NULL path errors at runtime if the child column is
/// NOT NULL — SQL standard catches this at CREATE TABLE, but
/// v0.48 leaves the check to runtime so users can declare schemas
/// in any order. The runtime error reads:
/// `cannot SET NULL on column 'x' of 't' — column is NOT NULL`.
fn apply_parent_delete_actions(
    pager: &mut Pager,
    catalog: &Catalog,
    snapshot: &Snapshot,
    parent_schema: &Schema,
    parent_values: &[Value],
) -> Result<()> {
    use crate::engine::schema::ForeignKeyAction;
    let parent_name = parent_schema.name.clone();
    let referencing = children_referencing(pager, catalog, parent_schema)?;
    for (child_name, child_schema, child_idx, child_col, parent_col_idx) in referencing {
        let action = child_col.foreign_key.as_ref().unwrap().on_delete;
        let parent_value = &parent_values[parent_col_idx];
        let key = codec::encode_index_value(parent_value);

        match action {
            ForeignKeyAction::Restrict => {
                if scan_child_for_fk_value(pager, &child_schema, child_idx, &key, parent_value)? {
                    return Err(Error::exec(format!(
                        "FOREIGN KEY violation: cannot delete from '{}' — row is referenced by '{}.{}'",
                        parent_name, child_name, child_col.name
                    )));
                }
            }
            ForeignKeyAction::Cascade => {
                // Delete every child row whose FK column matches the
                // parent's value. Run through the engine's `delete()`
                // so each cascaded delete checks ITS own FK actions
                // (recursion through one mechanism, not two).
                let value_sql = sql_literal(parent_value);
                let sql = format!(
                    "DELETE FROM {child_name} WHERE {col} = {value_sql}",
                    col = child_col.name
                );
                let inner_plan = crate::engine::planner::plan(
                    crate::sql::parse(&sql)?,
                    pager,
                    catalog,
                )?;
                let exec = execute_streaming(pager, catalog, snapshot, inner_plan)?;
                // DELETE returns an Ack; drain it.
                if let Execution::Rows(_) = exec {
                    // Shouldn't happen for DELETE.
                }
            }
            ForeignKeyAction::SetNull => {
                if child_col.not_null {
                    // If the child column is NOT NULL, surface the
                    // conflict only when an actual matching row
                    // exists — otherwise SET NULL is a no-op and
                    // doesn't trigger.
                    if scan_child_for_fk_value(pager, &child_schema, child_idx, &key, parent_value)? {
                        return Err(Error::exec(format!(
                            "ON DELETE SET NULL on '{}.{}' violates NOT NULL constraint",
                            child_name, child_col.name
                        )));
                    }
                    continue;
                }
                let value_sql = sql_literal(parent_value);
                let sql = format!(
                    "UPDATE {child_name} SET {col} = NULL WHERE {col} = {value_sql}",
                    col = child_col.name
                );
                let inner_plan = crate::engine::planner::plan(
                    crate::sql::parse(&sql)?,
                    pager,
                    catalog,
                )?;
                let _ = execute_streaming(pager, catalog, snapshot, inner_plan)?;
            }
        }
    }
    Ok(())
}

/// Walk every other table for an FK pointing at `parent_schema` and
/// return one tuple per FK: `(child_table, child_schema, child_col_idx,
/// child_column, parent_col_idx)`. The four enforcement points
/// (`check_no_child_references`, `apply_parent_delete_actions`,
/// `child_referencing`) share this catalog walk so the per-table-FK
/// iteration logic lives in one place.
fn children_referencing(
    pager: &mut Pager,
    catalog: &Catalog,
    parent_schema: &Schema,
) -> Result<Vec<(String, Schema, usize, Column, usize)>> {
    let mut out = Vec::new();
    let parent_name = &parent_schema.name;
    let table_names = catalog.table_names(pager)?;
    for child_name in table_names {
        if child_name == *parent_name {
            // v0.45 doesn't support self-references, but be defensive.
            continue;
        }
        let Some(child_schema) = catalog.get(pager, &child_name)? else {
            continue;
        };
        for (child_idx, child_col) in child_schema.columns.iter().enumerate() {
            let Some(fk) = &child_col.foreign_key else {
                continue;
            };
            if fk.table != *parent_name {
                continue;
            }
            let Some(parent_col_idx) = parent_schema.column_index(&fk.column) else {
                continue;
            };
            out.push((
                child_name.clone(),
                child_schema.clone(),
                child_idx,
                child_col.clone(),
                parent_col_idx,
            ));
        }
    }
    Ok(out)
}

/// Whether any row in `child_schema` has `parent_value` in
/// column position `child_idx`. Uses the FK column's index if one
/// exists, else a full table scan.
fn scan_child_for_fk_value(
    pager: &mut Pager,
    child_schema: &Schema,
    child_idx: usize,
    key: &[u8],
    parent_value: &Value,
) -> Result<bool> {
    if let Some(child_idx_root) = child_schema
        .indexes
        .iter()
        .find(|i| i.columns == vec![child_idx])
        .map(|i| i.root)
    {
        return index_has_value(pager, child_idx_root, key);
    }
    for (_rowid, encoded) in BTree::open(child_schema.root).scan(pager)? {
        let record = codec::decode_row(&encoded, child_schema.columns.len())?;
        if record.values[child_idx] == *parent_value {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Render a `Value` as a SQL literal for use in dynamically-built
/// CASCADE/SET NULL queries. v0.48 covers the four scalar types
/// FK columns can hold; NULL is excluded (NULL FK values don't
/// reference a parent and never trigger an action).
fn sql_literal(value: &Value) -> String {
    match value {
        Value::Int(n) => n.to_string(),
        Value::Real(r) => r.to_string(),
        Value::Text(s) => format!("'{}'", s.replace('\'', "''")),
        Value::Bool(true) => "TRUE".to_string(),
        Value::Bool(false) => "FALSE".to_string(),
        Value::Null => "NULL".to_string(),
    }
}

/// v0.45: whether any other table has a FOREIGN KEY pointing at
/// `parent_table`. DROP TABLE refuses if `Some(..)` is returned —
/// we surface the offending child table/column for the error.
fn child_referencing(
    pager: &mut Pager,
    catalog: &Catalog,
    parent_table: &str,
) -> Result<Option<(String, String)>> {
    let table_names = catalog.table_names(pager)?;
    for child_name in table_names {
        if child_name == parent_table {
            continue;
        }
        let Some(child_schema) = catalog.get(pager, &child_name)? else {
            continue;
        };
        for col in &child_schema.columns {
            if let Some(fk) = &col.foreign_key {
                if fk.table == parent_table {
                    return Ok(Some((child_name, col.name.clone())));
                }
            }
        }
    }
    Ok(None)
}

// Note: index entries are no longer physically deleted on row delete or
// update. The MVCC visibility check at scan time filters out tombstoned
// rows, and VACUUM reclaims the index entries together with the rows
// they point at. The old `index_delete_row` helper went with that change.

fn require_table(pager: &mut Pager, catalog: &Catalog, name: &str) -> Result<Schema> {
    catalog
        .get(pager, name)?
        .ok_or_else(|| Error::exec(format!("no such table: '{name}'")))
}

fn column_index(schema: &Schema, name: &str) -> Result<usize> {
    schema
        .column_index(name)
        .ok_or_else(|| Error::exec(format!("table '{}' has no column '{name}'", schema.name)))
}

/// Whether a row satisfies an optional `WHERE` clause. A predicate must
/// evaluate to exactly `TRUE`; `FALSE` and `NULL` both reject the row.
fn passes_filter(filter: Option<&Expr>, scope: &Scope, values: &[Value]) -> Result<bool> {
    match filter {
        None => Ok(true),
        Some(expr) => {
            let verdict = eval(expr, Some(&RowContext { scope, values }))?;
            Ok(matches!(verdict, Value::Bool(true)))
        }
    }
}

/// The row a column reference resolves against during evaluation, and the
/// scope that maps a reference to a position within it.
struct RowContext<'a> {
    scope: &'a Scope,
    values: &'a [Value],
}

/// Evaluate an expression. `context` is `None` where column references are not
/// allowed (the `VALUES` list of an `INSERT`).
fn eval(expr: &Expr, context: Option<&RowContext>) -> Result<Value> {
    match expr {
        Expr::Null => Ok(Value::Null),
        Expr::Integer(n) => Ok(Value::Int(*n)),
        Expr::Real(r) => Ok(Value::Real(*r)),
        Expr::Str(s) => Ok(Value::Text(s.clone())),
        Expr::Bool(b) => Ok(Value::Bool(*b)),
        Expr::Column(colref) => {
            let ctx = context.ok_or_else(|| {
                Error::exec(format!("column '{colref}' cannot be referenced here"))
            })?;
            let index = ctx.scope.resolve(colref)?;
            Ok(ctx.values[index].clone())
        }
        Expr::Aggregate(_) => Err(Error::exec(
            "aggregate functions are only allowed in a SELECT list or a HAVING clause",
        )),
        Expr::Unary { op, expr } => eval_unary(*op, eval(expr, context)?),
        Expr::Binary { op, left, right } => {
            eval_binary(*op, eval(left, context)?, eval(right, context)?)
        }
        Expr::IsNull { expr, negated } => {
            let value = eval(expr, context)?;
            Ok(Value::Bool(value.is_null() != *negated))
        }
        Expr::InList {
            expr,
            values,
            has_null,
            negated,
        } => {
            let probe = eval(expr, context)?;
            let result = eval_in_list(probe, values, *has_null)?;
            if *negated {
                Ok(negate_bool(result))
            } else {
                Ok(result)
            }
        }
        // The parser-only variants should have been pre-resolved by
        // `prepare_subqueries` before any expression evaluation.
        Expr::InSubquery { .. } => Err(Error::corruption(
            "IN subquery was not pre-evaluated before filter execution",
        )),
        Expr::Exists(_) => Err(Error::corruption(
            "EXISTS subquery was not pre-evaluated before filter execution",
        )),
        Expr::ScalarSubquery(_) => Err(Error::corruption(
            "scalar subquery was not pre-evaluated before filter execution",
        )),
        // Correlated subqueries should have been resolved per-row by the
        // Filter operator's `resolve_correlated` pass before reaching here.
        Expr::CorrelatedExists(_) => Err(Error::corruption(
            "correlated EXISTS subquery was not resolved before filter execution",
        )),
        Expr::CorrelatedScalarSubquery(_) => Err(Error::corruption(
            "correlated scalar subquery was not resolved before filter execution",
        )),
        Expr::CorrelatedInSubquery { .. } => Err(Error::corruption(
            "correlated IN subquery was not resolved before filter execution",
        )),
    }
}

/// Walk an expression and execute every uncorrelated subquery it contains,
/// rewriting each subquery node in place with the materialised result:
///
/// - `EXISTS (...)` collapses to `Expr::Bool(any_rows)`.
/// - `(SELECT ...)` (scalar) collapses to a literal `Expr` of the returned
///   value, raising an error if more than one row or column is produced.
/// - `expr IN (...)` becomes `Expr::InList`, carrying the distinct values
///   and a `has_null` flag for three-valued logic.
///
/// Pre-evaluating happens once, before the per-row evaluation loop, so the
/// subquery cost is paid once per outer query — not per outer row.
fn prepare_subqueries(
    expr: &mut Expr,
    pager: &mut Pager,
    catalog: &Catalog,
    snapshot: &Snapshot,
) -> Result<()> {
    // Recurse first, so nested subqueries (a subquery whose WHERE contains
    // another subquery) are resolved bottom-up.
    match expr {
        Expr::Null
        | Expr::Integer(_)
        | Expr::Real(_)
        | Expr::Str(_)
        | Expr::Bool(_)
        | Expr::Column(_)
        | Expr::Aggregate(_) => {}
        Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } => {
            prepare_subqueries(expr, pager, catalog, snapshot)?;
        }
        Expr::Binary { left, right, .. } => {
            prepare_subqueries(left, pager, catalog, snapshot)?;
            prepare_subqueries(right, pager, catalog, snapshot)?;
        }
        Expr::InSubquery { expr, .. } | Expr::InList { expr, .. } => {
            prepare_subqueries(expr, pager, catalog, snapshot)?;
        }
        Expr::Exists(_) | Expr::ScalarSubquery(_) => {}
        // Already detected as correlated on an earlier pass.
        Expr::CorrelatedExists(_)
        | Expr::CorrelatedScalarSubquery(_)
        | Expr::CorrelatedInSubquery { .. } => {}
    }
    match expr {
        Expr::InSubquery {
            expr: inner,
            subquery,
            negated,
        } => {
            if subquery_is_correlated(subquery, pager, catalog)? {
                // Defer to per-row resolution at the Filter operator.
                let inner = std::mem::replace(inner, Box::new(Expr::Null));
                let stmt = std::mem::replace(subquery, Box::new(Statement::Vacuum));
                let neg = *negated;
                *expr = Expr::CorrelatedInSubquery {
                    expr: inner,
                    subquery: stmt,
                    negated: neg,
                };
            } else {
                let (values, has_null) = execute_in_subquery(subquery, pager, catalog, snapshot)?;
                let inner = std::mem::replace(inner, Box::new(Expr::Null));
                let neg = *negated;
                *expr = Expr::InList {
                    expr: inner,
                    values,
                    has_null,
                    negated: neg,
                };
            }
        }
        Expr::Exists(subquery) => {
            if subquery_is_correlated(subquery, pager, catalog)? {
                let stmt = std::mem::replace(subquery, Box::new(Statement::Vacuum));
                *expr = Expr::CorrelatedExists(stmt);
            } else {
                let any = execute_exists_subquery(subquery, pager, catalog, snapshot)?;
                *expr = Expr::Bool(any);
            }
        }
        Expr::ScalarSubquery(subquery) => {
            if subquery_is_correlated(subquery, pager, catalog)? {
                let stmt = std::mem::replace(subquery, Box::new(Statement::Vacuum));
                *expr = Expr::CorrelatedScalarSubquery(stmt);
            } else {
                let value = execute_scalar_subquery(subquery, pager, catalog, snapshot)?;
                *expr = value_to_literal(value);
            }
        }
        _ => {}
    }
    Ok(())
}

/// Whether `statement` references any column its own FROM scope can't
/// resolve — i.e. a column that must come from an enclosing query's
/// scope. Such a subquery is *correlated* and must be evaluated per
/// outer row, not pre-computed.
///
/// v0.31 checks the `WHERE` clause only — the most common correlated
/// position. A reference in `SELECT` items, `HAVING`, or `ORDER BY`
/// inside the subquery is not detected by this pass; the subquery's
/// own planner will reject the unresolved column and the user will
/// see an error.
fn subquery_is_correlated(
    statement: &Statement,
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<bool> {
    let Statement::Select { from, filter, .. } = statement else {
        return Ok(false);
    };
    let Some(filter) = filter else {
        return Ok(false);
    };
    // Build the subquery's own FROM scope.
    let Some(base_schema) = catalog.get(pager, &from.table.name)? else {
        // The subquery's own table doesn't exist; treat as non-correlated
        // so the subquery's planner can surface the real error.
        return Ok(false);
    };
    let mut inner_scope = Scope::single(from.table.qualifier(), &base_schema);
    for join in &from.joins {
        let Some(joined_schema) = catalog.get(pager, &join.table.name)? else {
            return Ok(false);
        };
        inner_scope.extend(join.table.qualifier(), &joined_schema);
    }
    // Walk the filter looking for column refs the inner scope can't see.
    let mut found = false;
    has_outer_ref(filter, &inner_scope, &mut found);
    Ok(found)
}

/// Whether the predicate carries any [`Expr::CorrelatedExists`],
/// [`Expr::CorrelatedScalarSubquery`], or [`Expr::CorrelatedInSubquery`]
/// node — used by `Filter` to skip the per-row `resolve_correlated`
/// pass for the common case of no correlation.
fn predicate_has_correlated(expr: &Expr) -> bool {
    match expr {
        Expr::CorrelatedExists(_)
        | Expr::CorrelatedScalarSubquery(_)
        | Expr::CorrelatedInSubquery { .. } => true,
        Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } => predicate_has_correlated(expr),
        Expr::Binary { left, right, .. } => {
            predicate_has_correlated(left) || predicate_has_correlated(right)
        }
        Expr::InList { expr, .. } | Expr::InSubquery { expr, .. } => {
            predicate_has_correlated(expr)
        }
        _ => false,
    }
}

/// Walk `expr` returning a copy where every `Correlated*` subquery
/// node has been replaced by its per-row result for the outer row
/// described by (`outer_scope`, `outer_values`). The result is a fully
/// resolved expression `eval` can run.
///
/// For each correlated node we clone the subquery's `Statement`,
/// substitute every outer column reference inside it with the literal
/// value from the outer row, plan + execute the substituted (now
/// uncorrelated) statement, and lift the result into a literal or
/// `InList`.
fn resolve_correlated(
    expr: &Expr,
    outer_scope: &Scope,
    outer_values: &[Value],
    pager: &mut Pager,
    catalog: &Catalog,
    snapshot: &Snapshot,
) -> Result<Expr> {
    match expr {
        Expr::CorrelatedExists(statement) => {
            let substituted =
                substitute_outer_refs(statement, outer_scope, outer_values, pager, catalog)?;
            let any = execute_exists_subquery(&substituted, pager, catalog, snapshot)?;
            Ok(Expr::Bool(any))
        }
        Expr::CorrelatedScalarSubquery(statement) => {
            let substituted =
                substitute_outer_refs(statement, outer_scope, outer_values, pager, catalog)?;
            let value = execute_scalar_subquery(&substituted, pager, catalog, snapshot)?;
            Ok(value_to_literal(value))
        }
        Expr::CorrelatedInSubquery {
            expr: inner,
            subquery,
            negated,
        } => {
            let substituted =
                substitute_outer_refs(subquery, outer_scope, outer_values, pager, catalog)?;
            let (values, has_null) =
                execute_in_subquery(&substituted, pager, catalog, snapshot)?;
            // The inner expression of the IN may itself reference
            // correlated subqueries; resolve recursively.
            let inner_resolved = resolve_correlated(
                inner,
                outer_scope,
                outer_values,
                pager,
                catalog,
                snapshot,
            )?;
            Ok(Expr::InList {
                expr: Box::new(inner_resolved),
                values,
                has_null,
                negated: *negated,
            })
        }
        Expr::Unary { op, expr } => Ok(Expr::Unary {
            op: *op,
            expr: Box::new(resolve_correlated(
                expr,
                outer_scope,
                outer_values,
                pager,
                catalog,
                snapshot,
            )?),
        }),
        Expr::Binary { op, left, right } => Ok(Expr::Binary {
            op: *op,
            left: Box::new(resolve_correlated(
                left,
                outer_scope,
                outer_values,
                pager,
                catalog,
                snapshot,
            )?),
            right: Box::new(resolve_correlated(
                right,
                outer_scope,
                outer_values,
                pager,
                catalog,
                snapshot,
            )?),
        }),
        Expr::IsNull { expr, negated } => Ok(Expr::IsNull {
            expr: Box::new(resolve_correlated(
                expr,
                outer_scope,
                outer_values,
                pager,
                catalog,
                snapshot,
            )?),
            negated: *negated,
        }),
        Expr::InList {
            expr: inner,
            values,
            has_null,
            negated,
        } => Ok(Expr::InList {
            expr: Box::new(resolve_correlated(
                inner,
                outer_scope,
                outer_values,
                pager,
                catalog,
                snapshot,
            )?),
            values: values.clone(),
            has_null: *has_null,
            negated: *negated,
        }),
        // Leaves and the uncorrelated subquery nodes are returned as-is.
        // Uncorrelated subqueries should already have been resolved by
        // `prepare_subqueries`; if one shows up here we leave it and
        // `eval` will surface a corruption error.
        _ => Ok(expr.clone()),
    }
}

/// Deep-clone `statement` and replace every outer-scope column
/// reference inside it with the literal value from `outer_values`. The
/// result is an uncorrelated `Statement` we can plan and execute.
fn substitute_outer_refs(
    statement: &Statement,
    outer_scope: &Scope,
    outer_values: &[Value],
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<Statement> {
    let mut cloned = statement.clone();
    // Build the subquery's own scope so we can tell which Column refs
    // are inner (leave alone) and which are outer (substitute).
    let inner_scope = subquery_inner_scope(&cloned, pager, catalog)?;
    substitute_in_statement(&mut cloned, &inner_scope, outer_scope, outer_values)?;
    Ok(cloned)
}

/// Build the scope a subquery's expressions see from its own FROM
/// clause. Returns an empty scope for non-SELECT statements (which
/// shouldn't appear inside a subquery anyway).
fn subquery_inner_scope(
    statement: &Statement,
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<Scope> {
    let Statement::Select { from, .. } = statement else {
        return Ok(Scope { columns: Vec::new() });
    };
    let base_schema = catalog
        .get(pager, &from.table.name)?
        .ok_or_else(|| Error::exec(format!("no such table: {}", from.table.name)))?;
    let mut scope = Scope::single(from.table.qualifier(), &base_schema);
    for join in &from.joins {
        let joined_schema = catalog
            .get(pager, &join.table.name)?
            .ok_or_else(|| Error::exec(format!("no such table: {}", join.table.name)))?;
        scope.extend(join.table.qualifier(), &joined_schema);
    }
    Ok(scope)
}

fn substitute_in_statement(
    statement: &mut Statement,
    inner_scope: &Scope,
    outer_scope: &Scope,
    outer_values: &[Value],
) -> Result<()> {
    let Statement::Select {
        filter,
        having,
        projection,
        order_by: _,
        ..
    } = statement
    else {
        return Ok(());
    };
    if let Some(expr) = filter {
        substitute_in_expr(expr, inner_scope, outer_scope, outer_values)?;
    }
    if let Some(expr) = having {
        substitute_in_expr(expr, inner_scope, outer_scope, outer_values)?;
    }
    // Projection items: walk each item's expression. Aggregates and
    // bare columns from inner scope are left as-is; outer-scope refs
    // get substituted.
    if let crate::sql::ast::Projection::Items(items) = projection {
        for item in items.iter_mut() {
            if let crate::sql::ast::SelectItem::Expr(expr) = item {
                substitute_in_expr(expr, inner_scope, outer_scope, outer_values)?;
            }
        }
    }
    Ok(())
}

fn substitute_in_expr(
    expr: &mut Expr,
    inner_scope: &Scope,
    outer_scope: &Scope,
    outer_values: &[Value],
) -> Result<()> {
    #[allow(clippy::collapsible_match)]
    match expr {
        Expr::Column(colref) => {
            if inner_scope.resolve(colref).is_err() {
                // Not in inner scope — must be outer. If outer can't
                // resolve it either, surface the error.
                let outer_idx = outer_scope.resolve(colref)?;
                let value = outer_values[outer_idx].clone();
                *expr = value_to_literal(value);
            }
        }
        Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } => {
            substitute_in_expr(expr, inner_scope, outer_scope, outer_values)?
        }
        Expr::Binary { left, right, .. } => {
            substitute_in_expr(left, inner_scope, outer_scope, outer_values)?;
            substitute_in_expr(right, inner_scope, outer_scope, outer_values)?;
        }
        Expr::InList { expr, .. }
        | Expr::InSubquery { expr, .. }
        | Expr::CorrelatedInSubquery { expr, .. } => {
            substitute_in_expr(expr, inner_scope, outer_scope, outer_values)?
        }
        // Nested subqueries: don't descend — each has its own scope
        // and its own correlation analysis. v0.31 doesn't substitute
        // through nesting.
        _ => {}
    }
    Ok(())
}

/// Walk `expr` looking for a `Column` reference that `scope.resolve`
/// rejects — the marker of a reference into an enclosing query's scope.
/// Does not recurse into nested subqueries; each has its own scope and
/// its own correlation analysis.
fn has_outer_ref(expr: &Expr, scope: &Scope, found: &mut bool) {
    if *found {
        return;
    }
    #[allow(clippy::collapsible_match)]
    match expr {
        Expr::Column(colref) => {
            if scope.resolve(colref).is_err() {
                *found = true;
            }
        }
        Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } => {
            has_outer_ref(expr, scope, found)
        }
        Expr::Binary { left, right, .. } => {
            has_outer_ref(left, scope, found);
            has_outer_ref(right, scope, found);
        }
        Expr::InList { expr, .. }
        | Expr::InSubquery { expr, .. }
        | Expr::CorrelatedInSubquery { expr, .. } => {
            has_outer_ref(expr, scope, found);
        }
        _ => {}
    }
}

/// Run a subquery in `expr IN (subquery)` position. The subquery must yield
/// exactly one column; rows are collected as candidate values, with `NULL`s
/// split out into a separate flag so IN's three-valued logic stays exact.
fn execute_in_subquery(
    statement: &Statement,
    pager: &mut Pager,
    catalog: &Catalog,
    snapshot: &Snapshot,
) -> Result<(Vec<Expr>, bool)> {
    let rows = run_subquery_for_rows(statement, pager, catalog, snapshot, "IN", Some(1))?;
    let mut values = Vec::with_capacity(rows.len());
    let mut has_null = false;
    for row in rows {
        let mut iter = row.into_iter();
        let value = iter.next().expect("one-column subquery yields one value");
        if value.is_null() {
            has_null = true;
        } else {
            values.push(value_to_literal(value));
        }
    }
    Ok((values, has_null))
}

/// Run an `EXISTS (subquery)`. Only existence matters; the columns and values
/// are ignored. The current materialising executor pays for every row of the
/// subquery — a short-circuit at the first row is a worthwhile future
/// improvement but is not required for correctness.
fn execute_exists_subquery(
    statement: &Statement,
    pager: &mut Pager,
    catalog: &Catalog,
    snapshot: &Snapshot,
) -> Result<bool> {
    let plan = crate::engine::planner::plan(statement.clone(), pager, catalog)?;
    let result = execute(pager, catalog, snapshot, plan)?;
    match result {
        QueryResult::Rows { rows, .. } => Ok(!rows.is_empty()),
        QueryResult::Ack(_) => Err(Error::exec("EXISTS argument must be a SELECT")),
    }
}

/// Run a `(SELECT ...)` used as a scalar value. The subquery must yield at
/// most one row and exactly one column; zero rows is `NULL`, multiple rows
/// is an error.
fn execute_scalar_subquery(
    statement: &Statement,
    pager: &mut Pager,
    catalog: &Catalog,
    snapshot: &Snapshot,
) -> Result<Value> {
    let rows = run_subquery_for_rows(statement, pager, catalog, snapshot, "scalar", Some(1))?;
    match rows.len() {
        0 => Ok(Value::Null),
        1 => {
            let mut iter = rows.into_iter().next().unwrap().into_iter();
            Ok(iter.next().expect("one-column subquery yields one value"))
        }
        n => Err(Error::exec(format!(
            "scalar subquery returned {n} rows; at most one was expected"
        ))),
    }
}

/// Plan + execute a subquery, returning its rows. Enforces a column-count
/// requirement when one is given.
fn run_subquery_for_rows(
    statement: &Statement,
    pager: &mut Pager,
    catalog: &Catalog,
    snapshot: &Snapshot,
    kind: &str,
    expected_columns: Option<usize>,
) -> Result<Vec<Vec<Value>>> {
    let plan = crate::engine::planner::plan(statement.clone(), pager, catalog)?;
    let result = execute(pager, catalog, snapshot, plan)?;
    let QueryResult::Rows { columns, rows } = result else {
        return Err(Error::exec(format!(
            "{kind} subquery argument must be a SELECT"
        )));
    };
    if let Some(want) = expected_columns {
        if columns.len() != want {
            return Err(Error::exec(format!(
                "{kind} subquery must return {want} column(s); got {}",
                columns.len()
            )));
        }
    }
    Ok(rows)
}

/// A runtime [`Value`] as the matching literal [`Expr`]. The mapping is
/// bijective so the executor can reinsert a subquery result into the AST
/// without losing typing.
fn value_to_literal(value: Value) -> Expr {
    match value {
        Value::Null => Expr::Null,
        Value::Int(n) => Expr::Integer(n),
        Value::Real(r) => Expr::Real(r),
        Value::Text(s) => Expr::Str(s),
        Value::Bool(b) => Expr::Bool(b),
    }
}

/// Whether an expression mentions an aggregate function call anywhere — used
/// when classifying a SELECT projection. Subquery interiors live in their own
/// scope and do not count.
fn expr_contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Aggregate(_) => true,
        Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } => expr_contains_aggregate(expr),
        Expr::Binary { left, right, .. } => {
            expr_contains_aggregate(left) || expr_contains_aggregate(right)
        }
        Expr::InSubquery { expr, .. }
        | Expr::CorrelatedInSubquery { expr, .. }
        | Expr::InList { expr, .. } => expr_contains_aggregate(expr),
        _ => false,
    }
}

/// Standard SQL three-valued IN: `TRUE` if the probe matches a value,
/// `FALSE` if it matches none and no value was `NULL`, and `NULL` if it
/// matches none but a value was `NULL` (or the probe itself is `NULL`).
fn eval_in_list(probe: Value, values: &[Expr], has_null: bool) -> Result<Value> {
    if probe.is_null() {
        return Ok(Value::Null);
    }
    for candidate in values {
        let value = eval(candidate, None)?;
        if let Value::Bool(true) = compare_op(BinaryOp::Eq, probe.clone(), value)? {
            return Ok(Value::Bool(true));
        }
    }
    Ok(if has_null {
        Value::Null
    } else {
        Value::Bool(false)
    })
}

/// Three-valued logical negation: `NOT TRUE = FALSE`, `NOT FALSE = TRUE`,
/// `NOT NULL = NULL`. Anything else is unreachable since IN always yields
/// `Bool` or `Null`.
fn negate_bool(value: Value) -> Value {
    match value {
        Value::Bool(b) => Value::Bool(!b),
        Value::Null => Value::Null,
        other => unreachable!("IN-list result is bool or null, got {other:?}"),
    }
}

/// Evaluate a `HAVING` predicate against one group: column references resolve
/// to the group's (constant) value for that grouping column, and aggregate
/// calls are computed over the group's rows.
fn eval_unary(op: UnaryOp, value: Value) -> Result<Value> {
    match op {
        UnaryOp::Neg => match value {
            Value::Null => Ok(Value::Null),
            Value::Int(n) => n
                .checked_neg()
                .map(Value::Int)
                .ok_or_else(|| Error::exec("integer overflow while negating")),
            Value::Real(r) => Ok(Value::Real(-r)),
            other => Err(Error::exec(format!("cannot negate {}", other.type_name()))),
        },
        UnaryOp::Not => match value {
            Value::Null => Ok(Value::Null),
            Value::Bool(b) => Ok(Value::Bool(!b)),
            other => Err(Error::exec(format!(
                "NOT expects a boolean, found {}",
                other.type_name()
            ))),
        },
    }
}

fn eval_binary(op: BinaryOp, left: Value, right: Value) -> Result<Value> {
    use BinaryOp::*;
    match op {
        Add | Sub | Mul | Div => arithmetic(op, left, right),
        Eq | NotEq | Lt | LtEq | Gt | GtEq => compare_op(op, left, right),
        And | Or => logic(op, left, right),
    }
}

fn arithmetic(op: BinaryOp, left: Value, right: Value) -> Result<Value> {
    if left.is_null() || right.is_null() {
        return Ok(Value::Null);
    }
    if let (Value::Int(a), Value::Int(b)) = (&left, &right) {
        let (a, b) = (*a, *b);
        let result = match op {
            BinaryOp::Add => a.checked_add(b),
            BinaryOp::Sub => a.checked_sub(b),
            BinaryOp::Mul => a.checked_mul(b),
            BinaryOp::Div => {
                if b == 0 {
                    return Err(Error::exec("division by zero"));
                }
                a.checked_div(b)
            }
            _ => unreachable!("arithmetic() only handles + - * /"),
        };
        return result
            .map(Value::Int)
            .ok_or_else(|| Error::exec("integer overflow"));
    }
    // Any mix of INT and REAL is computed in floating point.
    let (a, b) = (as_number(&left)?, as_number(&right)?);
    let result = match op {
        BinaryOp::Add => a + b,
        BinaryOp::Sub => a - b,
        BinaryOp::Mul => a * b,
        BinaryOp::Div => {
            if b == 0.0 {
                return Err(Error::exec("division by zero"));
            }
            a / b
        }
        _ => unreachable!("arithmetic() only handles + - * /"),
    };
    Ok(Value::Real(result))
}

fn compare_op(op: BinaryOp, left: Value, right: Value) -> Result<Value> {
    if left.is_null() || right.is_null() {
        return Ok(Value::Null);
    }
    let ordering = compare(&left, &right)?;
    let verdict = match op {
        BinaryOp::Eq => ordering == Ordering::Equal,
        BinaryOp::NotEq => ordering != Ordering::Equal,
        BinaryOp::Lt => ordering == Ordering::Less,
        BinaryOp::LtEq => ordering != Ordering::Greater,
        BinaryOp::Gt => ordering == Ordering::Greater,
        BinaryOp::GtEq => ordering != Ordering::Less,
        _ => unreachable!("compare_op() only handles comparisons"),
    };
    Ok(Value::Bool(verdict))
}

fn compare(left: &Value, right: &Value) -> Result<Ordering> {
    match (left, right) {
        (Value::Int(a), Value::Int(b)) => Ok(a.cmp(b)),
        (Value::Text(a), Value::Text(b)) => Ok(a.cmp(b)),
        (Value::Bool(a), Value::Bool(b)) => Ok(a.cmp(b)),
        (a, b) if is_number(a) && is_number(b) => as_number(a)?
            .partial_cmp(&as_number(b)?)
            .ok_or_else(|| Error::exec("cannot compare NaN")),
        (a, b) => Err(Error::exec(format!(
            "cannot compare {} with {}",
            a.type_name(),
            b.type_name()
        ))),
    }
}

fn logic(op: BinaryOp, left: Value, right: Value) -> Result<Value> {
    let left = as_bool(&left)?;
    let right = as_bool(&right)?;
    let value = match op {
        // SQL three-valued logic: a definite FALSE/TRUE wins even against NULL.
        BinaryOp::And => match (left, right) {
            (Some(false), _) | (_, Some(false)) => Value::Bool(false),
            (Some(true), Some(true)) => Value::Bool(true),
            _ => Value::Null,
        },
        BinaryOp::Or => match (left, right) {
            (Some(true), _) | (_, Some(true)) => Value::Bool(true),
            (Some(false), Some(false)) => Value::Bool(false),
            _ => Value::Null,
        },
        _ => unreachable!("logic() only handles AND/OR"),
    };
    Ok(value)
}

fn is_number(value: &Value) -> bool {
    matches!(value, Value::Int(_) | Value::Real(_))
}

fn as_number(value: &Value) -> Result<f64> {
    match value {
        Value::Int(n) => Ok(*n as f64),
        Value::Real(r) => Ok(*r),
        other => Err(Error::exec(format!(
            "expected a number, found {}",
            other.type_name()
        ))),
    }
}

fn as_bool(value: &Value) -> Result<Option<bool>> {
    match value {
        Value::Null => Ok(None),
        Value::Bool(b) => Ok(Some(*b)),
        other => Err(Error::exec(format!(
            "expected a boolean, found {}",
            other.type_name()
        ))),
    }
}

impl fmt::Display for QueryResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryResult::Ack(message) => f.write_str(message),
            QueryResult::Rows { columns, rows } => {
                let mut widths: Vec<usize> = columns.iter().map(|c| c.chars().count()).collect();
                let rendered: Vec<Vec<String>> = rows
                    .iter()
                    .map(|row| row.iter().map(|v| v.to_string()).collect())
                    .collect();
                for row in &rendered {
                    for (i, cell) in row.iter().enumerate() {
                        widths[i] = widths[i].max(cell.chars().count());
                    }
                }

                for (i, name) in columns.iter().enumerate() {
                    if i > 0 {
                        f.write_str(" | ")?;
                    }
                    write!(f, "{name:<width$}", width = widths[i])?;
                }
                writeln!(f)?;
                for (i, width) in widths.iter().enumerate() {
                    if i > 0 {
                        f.write_str("-+-")?;
                    }
                    write!(f, "{}", "-".repeat(*width))?;
                }
                writeln!(f)?;
                for row in &rendered {
                    for (i, cell) in row.iter().enumerate() {
                        if i > 0 {
                            f.write_str(" | ")?;
                        }
                        write!(f, "{cell:<width$}", width = widths[i])?;
                    }
                    writeln!(f)?;
                }
                write!(
                    f,
                    "({} row{})",
                    rows.len(),
                    if rows.len() == 1 { "" } else { "s" }
                )
            }
        }
    }
}

// === Vectorised pipeline ====================================================
//
// A parallel operator tree that moves whole [`ColumnBatch`]es through the
// pipeline at once, with one tight loop per output column instead of one loop
// per row through every operator. The bottom decodes the table or index into
// batches; each layer above evaluates its expressions columnwise where it can
// and falls back to per-row scalar eval where it cannot. The tree always ends
// with a `BatchToRow` adapter, so the rest of the executor sees the existing
// row-at-a-time `Operator` interface.
//
// Used only for "scan-shape" SELECTs: no joins, no GROUP BY / HAVING /
// aggregates, no ORDER BY. The planner has already pre-resolved any
// uncorrelated subqueries before this path runs.

/// Whether every join in `from` can be handled by the batched operator tree.
/// Any join whose ON predicate would prefer an index nested-loop (the inner
/// side is indexed on the join column) keeps the existing row pipeline, so
/// that optimisation is not lost. Resolution errors fall back to the row
/// path too — its error messages are more polished.
fn joins_vectorisable(pager: &mut Pager, catalog: &Catalog, from: &FromClause) -> Result<bool> {
    if from.joins.is_empty() {
        return Ok(true);
    }
    let Some(base_schema) = catalog.get(pager, &from.table.name)? else {
        return Ok(false);
    };
    let mut scope = Scope::single(from.table.qualifier(), &base_schema);
    for join in &from.joins {
        // Semi/anti-joins (planner rewrite of EXISTS / NOT EXISTS) only
        // exist in the row pipeline today — `BatchHashJoin` /
        // `BatchNestedLoopJoin` would emit combined rows. Route the
        // whole query to the row tree when any join is Semi/Anti.
        if matches!(join.kind, JoinKind::Semi | JoinKind::Anti) {
            return Ok(false);
        }
        let Some(joined_schema) = catalog.get(pager, &join.table.name)? else {
            return Ok(false);
        };
        let qualifier = join.table.qualifier();
        if scope.has_qualifier(qualifier) {
            return Ok(false);
        }
        let left_len = scope.len();
        scope.extend(qualifier, &joined_schema);
        if let Some(on) = &join.on {
            if find_index_join(on, left_len, &scope, &joined_schema).is_some() {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

/// Whether any subquery in the query's predicate or projection items
/// is correlated (references an outer column). v0.32 steers such
/// queries back to the row pipeline — the vectorised operators don't
/// carry the per-row `resolve_correlated` substitution machinery the
/// row pipeline's `Filter` and `Project` gained in v0.31.
fn query_has_correlated_subquery(
    projection: &Projection,
    filter: &Option<Expr>,
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<bool> {
    if let Some(predicate) = filter {
        if expr_has_correlated_subquery(predicate, pager, catalog)? {
            return Ok(true);
        }
    }
    if let Projection::Items(items) = projection {
        for item in items {
            if let SelectItem::Expr(e) = item {
                if expr_has_correlated_subquery(e, pager, catalog)? {
                    return Ok(true);
                }
            }
        }
    }
    Ok(false)
}

/// Recurse through `expr` looking for any subquery whose `WHERE`
/// references an outer column.
fn expr_has_correlated_subquery(
    expr: &Expr,
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<bool> {
    match expr {
        Expr::Exists(stmt) | Expr::ScalarSubquery(stmt) => {
            subquery_is_correlated(stmt, pager, catalog)
        }
        Expr::InSubquery {
            expr: inner,
            subquery,
            ..
        } => {
            if subquery_is_correlated(subquery, pager, catalog)? {
                return Ok(true);
            }
            expr_has_correlated_subquery(inner, pager, catalog)
        }
        Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } => {
            expr_has_correlated_subquery(expr, pager, catalog)
        }
        Expr::Binary { left, right, .. } => {
            if expr_has_correlated_subquery(left, pager, catalog)? {
                return Ok(true);
            }
            expr_has_correlated_subquery(right, pager, catalog)
        }
        Expr::InList { expr, .. } => expr_has_correlated_subquery(expr, pager, catalog),
        // Already-detected correlated nodes (from a prior pass) say yes.
        Expr::CorrelatedExists(_)
        | Expr::CorrelatedScalarSubquery(_)
        | Expr::CorrelatedInSubquery { .. } => Ok(true),
        _ => Ok(false),
    }
}

/// Build a [`BatchScan`] over a table or one of its secondary indexes,
/// according to `access`. The base table of `select_vectorised` and the
/// inner side of every join both go through this — the inner side always
/// with [`AccessPath::FullScan`].
fn build_batched_scan(
    pager: &mut Pager,
    schema: &Schema,
    access: &AccessPath,
    snapshot: Snapshot,
) -> Result<Box<dyn BatchOperator>> {
    let column_types: Vec<Type> = schema.columns.iter().map(|c| c.ty).collect();
    let column_count = schema.columns.len();
    let table_root = schema.root;
    Ok(match access {
        AccessPath::FullScan => {
            let table = BTree::open(schema.root);
            let cursor = table.cursor(pager, None, None)?;
            Box::new(BatchScan {
                cursor,
                column_types,
                column_count,
                table_for_index: None,
                snapshot,
                seen_rowids: std::collections::HashSet::new(),
                table_root,
                relation_read_recorded: false,
            })
        }
        AccessPath::IndexScan {
            index_root,
            lower,
            upper,
        } => {
            let index = BTree::open(*index_root);
            let table = BTree::open(schema.root);
            let cursor = index.cursor(pager, Some(lower.as_slice()), upper.clone())?;
            Box::new(BatchScan {
                cursor,
                column_types,
                column_count,
                table_for_index: Some(table),
                snapshot,
                seen_rowids: std::collections::HashSet::new(),
                table_root,
                relation_read_recorded: false,
            })
        }
    })
}

/// Plan a SELECT through the batched operator tree. Called by `select` once
/// it has decided the query qualifies; everything below this point is allowed
/// to assume single-table, scalar-projection shape.
#[allow(clippy::too_many_arguments)]
fn select_vectorised(
    pager: &mut Pager,
    catalog: &Catalog,
    from: FromClause,
    projection: Projection,
    filter: Option<Expr>,
    access: AccessPath,
    group_by: Vec<crate::sql::ast::ColumnRef>,
    order_by: Vec<OrderKey>,
    limit: Option<u64>,
    offset: Option<u64>,
    snapshot: &Snapshot,
) -> Result<RowStream> {
    // Build the base scan from the chosen access path.
    let base_schema = require_table(pager, catalog, &from.table.name)?;
    let mut scope = Scope::single(from.table.qualifier(), &base_schema);
    let mut op: Box<dyn BatchOperator> =
        build_batched_scan(pager, &base_schema, &access, snapshot.clone())?;

    // Each join: pull the inner side as its own BatchScan, pick equi-join
    // (BatchHashJoin) or general (BatchNestedLoopJoin) based on the ON
    // predicate's shape — same per-join algorithm choice the row pipeline
    // makes in `build_from`, minus the index nested-loop case which the
    // pre-check has already routed to the row path.
    for join in &from.joins {
        let joined_schema = require_table(pager, catalog, &join.table.name)?;
        let qualifier = join.table.qualifier().to_string();
        if scope.has_qualifier(&qualifier) {
            return Err(Error::exec(format!(
                "table name or alias '{qualifier}' is used twice in FROM"
            )));
        }
        let left_scope = scope.clone();
        scope.extend(&qualifier, &joined_schema);
        let right_width = joined_schema.columns.len();
        let output_types: Vec<Type> = (0..scope.len()).map(|i| scope.column_type(i)).collect();

        let right_input = build_batched_scan(
            pager,
            &joined_schema,
            &AccessPath::FullScan,
            snapshot.clone(),
        )?;
        let equi_join = join
            .on
            .as_ref()
            .and_then(|on| find_equi_join(on, left_scope.len(), &scope));

        op = if let Some((probe_col, build_col)) = equi_join {
            Box::new(BatchHashJoin {
                left: op,
                right_input: Some(right_input),
                table: None,
                probe_col,
                build_col,
                on: join.on.clone().expect("equi-join carries an ON predicate"),
                kind: join.kind,
                scope: scope.clone(),
                output_types,
                right_width,
                current_left: None,
                left_pos: 0,
                probe_key: None,
                row_started: false,
                match_pos: 0,
                matched_current: false,
            })
        } else {
            Box::new(BatchNestedLoopJoin {
                left: op,
                right_input: Some(right_input),
                right_rows: None,
                output_types,
                on: join.on.clone(),
                kind: join.kind,
                scope: scope.clone(),
                right_width,
                current_left: None,
                left_pos: 0,
                right_pos: 0,
                matched_current: false,
            })
        };
    }

    // WHERE — every subquery is resolved up front, then the predicate rides
    // into a single `BatchFilter`.
    let mut filter = filter;
    if let Some(predicate) = filter.as_mut() {
        prepare_subqueries(predicate, pager, catalog, snapshot)?;
    }
    if let Some(predicate) = filter {
        op = Box::new(BatchFilter {
            input: op,
            predicate,
            scope: scope.clone(),
        });
    }

    // Detect whether this query needs a grouped aggregation step. With
    // `Projection::All` we can never aggregate (no aggregate items);
    // with `Projection::Items` we aggregate when GROUP BY is present
    // or any item is an aggregate. The dispatch in `select()` has
    // already enforced that aggregation queries reaching here have
    // only `Column` / `Aggregate` items and no HAVING.
    let needs_aggregation = !group_by.is_empty()
        || match &projection {
            Projection::Items(items) => {
                items.iter().any(|it| matches!(it, SelectItem::Aggregate(_)))
            }
            Projection::All => false,
        };

    let columns = projection_headers(&projection, &scope);

    // ORDER BY (non-aggregation case) — slot a `BatchSort` *before*
    // projection so the keys can reference the pre-projection (joined)
    // scope, mirroring the row pipeline's
    // `scan/join -> filter -> sort -> project -> limit` order.
    // `BatchSort` spills runs to temp files once the in-memory buffer
    // crosses [`SORT_SPILL_THRESHOLD`] rows, then k-way merges.
    //
    // For aggregation queries v0.33 routes ORDER BY back to the row
    // pipeline — sorting after `BatchHashAggregate` would need a
    // post-agg scope; left for a future session.
    if !order_by.is_empty() && !needs_aggregation {
        let sort_keys = resolve_order_keys(&scope, &order_by)?;
        let joined_types: Vec<Type> =
            (0..scope.len()).map(|i| scope.column_type(i)).collect();
        op = Box::new(BatchSort::new(op, sort_keys, joined_types));
    }

    if needs_aggregation {
        // `BatchHashAggregate` consumes the joined-scope rows, produces
        // one batch row per group with the projection items in output
        // order. No `BatchProject` follows — aggregation already
        // projected.
        let items = match projection {
            Projection::Items(items) => items,
            Projection::All => {
                return Err(Error::exec(
                    "SELECT * is not allowed with GROUP BY or an aggregate",
                ))
            }
        };
        op = Box::new(BatchHashAggregate::new(op, scope.clone(), items, &group_by)?);
    } else {
        // No aggregation: classic vectorised projection.
        let mut needs_project = false;
        let projected: Vec<PlainItem> = match projection {
            Projection::All => (0..scope.len()).map(PlainItem::Column).collect(),
            Projection::Items(items) => {
                needs_project = true;
                let mut resolved = Vec::with_capacity(items.len());
                for item in items {
                    resolved.push(match item {
                        SelectItem::Column(colref) => PlainItem::Column(scope.resolve(&colref)?),
                        SelectItem::Aggregate(_) => unreachable!(
                            "aggregation steered to BatchHashAggregate above"
                        ),
                        SelectItem::Expr(e) => {
                            let mut prepared = e;
                            prepare_subqueries(&mut prepared, pager, catalog, snapshot)?;
                            PlainItem::Expr(prepared)
                        }
                    });
                }
                resolved
            }
        };
        if needs_project {
            let project_scope = if projected
                .iter()
                .any(|item| matches!(item, PlainItem::Expr(_)))
            {
                Some(scope.clone())
            } else {
                None
            };
            op = Box::new(BatchProject {
                input: op,
                items: projected,
                scope: project_scope,
            });
        }
    }

    // LIMIT / OFFSET — bounded entirely within the batch stream, so the scan
    // stops the instant the quota is filled.
    if limit.is_some() || offset.is_some() {
        op = Box::new(BatchLimit {
            input: op,
            offset: offset.unwrap_or(0),
            remaining: limit.unwrap_or(u64::MAX),
        });
    }

    // The adapter back to the row-at-a-time interface the rest of the
    // executor consumes.
    let adapter: Box<dyn Operator> = Box::new(BatchToRow {
        input: op,
        current: None,
        cursor: 0,
    });
    Ok(RowStream {
        columns,
        source: RowSource::Volcano(adapter),
    })
}

/// Pull-based operator that emits one [`ColumnBatch`] per `next_batch` call.
/// `None` signals end of stream.
trait BatchOperator {
    fn next_batch(&mut self, pager: &mut Pager) -> Result<Option<ColumnBatch>>;
}

/// A vectorised table or index scan. Decodes up to [`BATCH_SIZE`] rows per
/// pull into one `ColumnBatch`, typed per the schema's columns. Rows are
/// visibility-filtered against the snapshot; tombstoned rows or rows from
/// uncommitted writes never reach the batch.
struct BatchScan {
    cursor: Cursor,
    column_types: Vec<Type>,
    column_count: usize,
    /// `Some(table_tree)` when scanning an index: each index entry's rowid
    /// suffix is chased back to the table tree before the row is decoded.
    table_for_index: Option<BTree>,
    snapshot: Snapshot,
    /// Index scans can yield the same rowid more than once (post-UPDATE).
    /// `seen_rowids` dedupes so we decode each physical row at most once
    /// per scan.
    seen_rowids: std::collections::HashSet<Vec<u8>>,
    /// The table's B+tree root — used for SSI read locking.
    table_root: u32,
    /// One-shot guard for `record_relation_read` on the full-scan path.
    relation_read_recorded: bool,
}

impl BatchOperator for BatchScan {
    fn next_batch(&mut self, pager: &mut Pager) -> Result<Option<ColumnBatch>> {
        // v0.35 SSI: a full table scan takes a relation-level read
        // lock once; an index scan records per-tuple as rows are
        // emitted (handled below per row).
        if self.table_for_index.is_none() && !self.relation_read_recorded {
            self.snapshot.record_relation_read(self.table_root);
            self.relation_read_recorded = true;
        }
        let mut batch = ColumnBatch::with_types(&self.column_types);
        while batch.n_rows < BATCH_SIZE {
            let Some((key, encoded)) = self.cursor.next(pager)? else {
                break;
            };
            match &self.table_for_index {
                Some(table) => {
                    if key.len() < 8 {
                        return Err(Error::corruption("index key shorter than a rowid"));
                    }
                    let rowid_key = key[key.len() - 8..].to_vec();
                    if !self.seen_rowids.insert(rowid_key.clone()) {
                        continue;
                    }
                    match table.search(pager, &rowid_key)? {
                        Some(row_bytes) => {
                            let record = codec::decode_row(&row_bytes, self.column_count)?;
                            if self.snapshot.visible(record.tx_min, record.tx_max) {
                                // Per-tuple SSI lock for the index path.
                                let tombstone_by = if record.tx_max != 0 {
                                    Some(record.tx_max)
                                } else {
                                    None
                                };
                                self.snapshot.record_read(
                                    self.table_root,
                                    &rowid_key,
                                    tombstone_by,
                                );
                                batch.push_row(&record.values)?;
                            }
                        }
                        None => {
                            return Err(Error::corruption(
                                "index references a row that does not exist",
                            ));
                        }
                    }
                }
                None => {
                    let record = codec::decode_row(&encoded, self.column_count)?;
                    if self.snapshot.visible(record.tx_min, record.tx_max) {
                        batch.push_row(&record.values)?;
                    }
                }
            }
        }
        Ok(if batch.is_empty() { None } else { Some(batch) })
    }
}

/// A vectorised filter: evaluate the predicate columnwise to produce a Bool
/// column, then materialise the rows where the mask is exactly `Bool(true)` —
/// `NULL` and `FALSE` are both dropped, matching the row-at-a-time semantics.
struct BatchFilter {
    input: Box<dyn BatchOperator>,
    predicate: Expr,
    scope: Scope,
}

impl BatchOperator for BatchFilter {
    fn next_batch(&mut self, pager: &mut Pager) -> Result<Option<ColumnBatch>> {
        // A batch that filters down to zero rows is invisible above us; pull
        // again until something survives or the input is exhausted.
        loop {
            let Some(input) = self.input.next_batch(pager)? else {
                return Ok(None);
            };
            let mask = eval_batch(&self.predicate, &input, &self.scope)?;
            let selection = build_selection(&input, &mask)?;
            if !selection.is_empty() {
                // Reuse the input's column data unchanged — the new selection
                // points into the same `Vec`s. No per-row copy.
                let n_rows = selection.len();
                return Ok(Some(ColumnBatch {
                    columns: input.columns,
                    n_rows,
                    selection: Some(selection),
                }));
            }
        }
    }
}

/// A vectorised projection: every output column is the result of evaluating
/// one expression over the input batch.
struct BatchProject {
    input: Box<dyn BatchOperator>,
    items: Vec<PlainItem>,
    scope: Option<Scope>,
}

impl BatchOperator for BatchProject {
    fn next_batch(&mut self, pager: &mut Pager) -> Result<Option<ColumnBatch>> {
        let Some(input) = self.input.next_batch(pager)? else {
            return Ok(None);
        };
        let mut columns = Vec::with_capacity(self.items.len());
        for item in &self.items {
            columns.push(match item {
                // A column reference re-lays the input's column at logical
                // row order — a clone when the input is materialised, a
                // gather through the selection otherwise.
                PlainItem::Column(i) => {
                    materialise_column(&input.columns[*i], input.selection.as_deref())
                }
                PlainItem::Expr(expr) => {
                    let scope = self
                        .scope
                        .as_ref()
                        .expect("expression items require a scope");
                    eval_batch(expr, &input, scope)?
                }
            });
        }
        // `BatchProject` materialises: its output columns are either the
        // input's columns re-laid through the input's selection or fresh
        // ones from `eval_batch`. Either way, no selection rides on the
        // output.
        Ok(Some(ColumnBatch {
            columns,
            n_rows: input.n_rows,
            selection: None,
        }))
    }
}

/// A vectorised `LIMIT` / `OFFSET`. Skips `offset` rows across batches, then
/// yields at most `remaining`. Stops pulling its input the instant the quota
/// runs out, so the scan ends early on a small limit.
struct BatchLimit {
    input: Box<dyn BatchOperator>,
    offset: u64,
    remaining: u64,
}

impl BatchOperator for BatchLimit {
    fn next_batch(&mut self, pager: &mut Pager) -> Result<Option<ColumnBatch>> {
        // Consume offset rows. A batch shorter than what is still skipped is
        // dropped whole; the last partial batch is sliced.
        while self.offset > 0 {
            let Some(batch) = self.input.next_batch(pager)? else {
                return Ok(None);
            };
            let n = batch.n_rows as u64;
            if n <= self.offset {
                self.offset -= n;
                continue;
            }
            let skip = self.offset as usize;
            self.offset = 0;
            let kept = slice_logical_rows(batch, skip..);
            return self.apply_remaining(kept);
        }
        if self.remaining == 0 {
            return Ok(None);
        }
        let Some(batch) = self.input.next_batch(pager)? else {
            return Ok(None);
        };
        self.apply_remaining(batch)
    }
}

impl BatchLimit {
    fn apply_remaining(&mut self, batch: ColumnBatch) -> Result<Option<ColumnBatch>> {
        let n = batch.n_rows as u64;
        if n <= self.remaining {
            self.remaining -= n;
            Ok(Some(batch))
        } else {
            let take = self.remaining as usize;
            self.remaining = 0;
            let kept = slice_logical_rows(batch, ..take);
            Ok(Some(kept))
        }
    }
}

/// Return a batch holding only the logical rows in `range` of the input.
/// Reuses the input's column data — selection vectors mean no per-cell copy.
/// If the input already had a selection, the new one is a sub-slice of it;
/// otherwise a fresh selection covering the kept range is built.
fn slice_logical_rows<R>(batch: ColumnBatch, range: R) -> ColumnBatch
where
    R: std::ops::RangeBounds<usize>,
{
    use std::ops::Bound;
    let start = match range.start_bound() {
        Bound::Included(&n) => n,
        Bound::Excluded(&n) => n + 1,
        Bound::Unbounded => 0,
    };
    let end = match range.end_bound() {
        Bound::Included(&n) => n + 1,
        Bound::Excluded(&n) => n,
        Bound::Unbounded => batch.n_rows,
    };
    let n = end - start;
    let new_selection: Vec<u32> = match &batch.selection {
        Some(sel) => sel[start..end].to_vec(),
        None => (start..end).map(|i| i as u32).collect(),
    };
    ColumnBatch {
        columns: batch.columns,
        n_rows: n,
        selection: Some(new_selection),
    }
}

/// Vectorised hash aggregation. Streams every input batch into a
/// per-group bucket of [`AggregateState`]s (the same row-pipeline
/// helper, fed one row at a time via [`ColumnBatch::row_at`]), then
/// emits one output row per group as a [`ColumnBatch`] typed up front
/// by [`infer_grouped_output_types`].
///
/// v0.33's vectorised aggregator handles:
///
/// - bare aggregates over the whole table (no `GROUP BY` ⇒ one bucket),
/// - `GROUP BY` with `Column` and `Aggregate` projection items,
/// - downstream `ORDER BY` via the generic `BatchSort` (sort runs over
///   the grouped output rows, naming output column positions).
///
/// `HAVING` and projection-position `Expr` items keep the row tree;
/// the dispatch gate steers them away from this operator.
struct BatchHashAggregate {
    input: Option<Box<dyn BatchOperator>>,
    /// Scope of the input batches — the joined-table scope, used for
    /// `Column` items and the registry's column lookups.
    scope: Scope,
    /// Resolved indices of the `GROUP BY` columns within `scope`.
    group_cols: Vec<usize>,
    /// Distinct aggregates from the projection, each with a stable slot.
    registry: AggregateRegistry,
    /// One fresh per-slot state, cloned into each new bucket.
    template: Vec<AggregateState>,
    /// Projection items, in output order — what each output position
    /// holds (`Column` references resolve through `group_cols`,
    /// aggregates look up the registry).
    items: Vec<SelectItem>,
    /// Pre-computed output column types, used to build typed
    /// [`ColumnBatch`]es when draining.
    output_types: Vec<Type>,
    /// State machine: `Building` while pulling input, `Draining` once
    /// the input is exhausted and the buckets have been finalised.
    state: AggregateOpState,
}

enum AggregateOpState {
    Building {
        buckets: HashMap<GroupKey, Vec<AggregateState>>,
        order: Vec<GroupKey>,
    },
    Draining(std::vec::IntoIter<Vec<Value>>),
}

impl BatchHashAggregate {
    fn new(
        input: Box<dyn BatchOperator>,
        scope: Scope,
        items: Vec<SelectItem>,
        group_by: &[crate::sql::ast::ColumnRef],
    ) -> Result<BatchHashAggregate> {
        let group_cols: Vec<usize> = group_by
            .iter()
            .map(|colref| scope.resolve(colref))
            .collect::<Result<_>>()?;
        // A bare column in the SELECT list must be a GROUP BY column,
        // exactly the row pipeline's rule.
        for item in &items {
            if let SelectItem::Column(colref) = item {
                let column = scope.resolve(colref)?;
                if !group_cols.contains(&column) {
                    return Err(Error::exec(format!(
                        "column '{colref}' must appear in GROUP BY or inside an aggregate"
                    )));
                }
            }
        }
        let registry = AggregateRegistry::build(&items, None, &scope)?;
        let template: Vec<AggregateState> = registry
            .slots
            .iter()
            .map(|slot| AggregateState::for_slot(slot, &scope))
            .collect::<Result<_>>()?;
        let output_types = infer_grouped_output_types(&items, &scope)?;
        Ok(BatchHashAggregate {
            input: Some(input),
            scope,
            group_cols,
            registry,
            template,
            items,
            output_types,
            state: AggregateOpState::Building {
                buckets: HashMap::new(),
                order: Vec::new(),
            },
        })
    }

    /// Drain the input: walk every batch's rows, hash to a bucket,
    /// update the bucket's per-slot states. Transitions `state` to
    /// `Draining` with an iterator of finalised output rows.
    fn drain_input(&mut self, pager: &mut Pager) -> Result<()> {
        use std::collections::hash_map::Entry;
        let mut input = self
            .input
            .take()
            .expect("drain_input called twice — input already drained");
        let AggregateOpState::Building { buckets, order } = &mut self.state else {
            unreachable!("drain_input called in non-Building state");
        };
        while let Some(batch) = input.next_batch(pager)? {
            for i in 0..batch.n_rows {
                let row = batch.row_at(i);
                let key = GroupKey {
                    values: self.group_cols.iter().map(|&c| row[c].clone()).collect(),
                };
                let states = match buckets.entry(key) {
                    Entry::Occupied(e) => e.into_mut(),
                    Entry::Vacant(e) => {
                        order.push(e.key().clone());
                        e.insert(self.template.clone())
                    }
                };
                for (state, slot) in states.iter_mut().zip(&self.registry.slots) {
                    state.update(slot, &row)?;
                }
            }
        }
        // Whole-table aggregate over zero input rows: still emit one
        // row with the aggregate identities. Group-by aggregations emit
        // zero rows from zero input.
        if self.group_cols.is_empty() && buckets.is_empty() {
            let key = GroupKey { values: Vec::new() };
            order.push(key.clone());
            buckets.insert(key, self.template.clone());
        }
        // Finalise: build the output rows now, in insertion order, so
        // `next_batch` can stream them out.
        let mut output_rows: Vec<Vec<Value>> = Vec::with_capacity(order.len());
        for key in order.drain(..) {
            let states = buckets.remove(&key).expect("inserted above");
            let aggregates: Vec<Value> = states.into_iter().map(|s| s.finalize()).collect();
            let mut row = Vec::with_capacity(self.items.len());
            for item in &self.items {
                row.push(match item {
                    SelectItem::Column(colref) => {
                        let column = self.scope.resolve(colref)?;
                        let pos = self
                            .group_cols
                            .iter()
                            .position(|&c| c == column)
                            .expect("validated in `new`");
                        key.values[pos].clone()
                    }
                    SelectItem::Aggregate(agg) => {
                        let idx = self
                            .registry
                            .lookup(agg)
                            .expect("registered in `new`");
                        aggregates[idx].clone()
                    }
                    SelectItem::Expr(_) => unreachable!("Expr items route to row pipeline"),
                });
            }
            output_rows.push(row);
        }
        self.state = AggregateOpState::Draining(output_rows.into_iter());
        Ok(())
    }
}

impl BatchOperator for BatchHashAggregate {
    fn next_batch(&mut self, pager: &mut Pager) -> Result<Option<ColumnBatch>> {
        if self.input.is_some() {
            self.drain_input(pager)?;
        }
        let AggregateOpState::Draining(iter) = &mut self.state else {
            unreachable!("draining state required after drain_input");
        };
        let mut batch = ColumnBatch::with_types(&self.output_types);
        for _ in 0..crate::engine::batch::BATCH_SIZE {
            match iter.next() {
                Some(row) => batch.push_row(&row)?,
                None => break,
            }
        }
        if batch.is_empty() {
            Ok(None)
        } else {
            Ok(Some(batch))
        }
    }
}

/// Maximum in-memory rows held by [`BatchSort`] before it sorts the
/// run and spills it to a temporary file. 8 KiB rows at, say, 50 bytes
/// each ≈ 400 KiB per run — fits comfortably alongside everything
/// else in the executor.
const SORT_SPILL_THRESHOLD: usize = 8 * 1024;

static SORT_SPILL_COUNTER: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

fn unique_spill_path() -> std::path::PathBuf {
    let n = SORT_SPILL_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "prehnite-sort-{}-{n}",
        std::process::id()
    ))
}

/// One spilled run on disk: rows in sorted order, each as a `u32` LE
/// length prefix followed by [`codec::encode_values`] payload. Reads
/// streamingly through a [`BufReader`]; the temp file is removed on
/// drop, so a panic or early abort cleans up.
struct SpilledRun {
    path: std::path::PathBuf,
    reader: std::io::BufReader<std::fs::File>,
    column_count: usize,
}

impl SpilledRun {
    /// Pull the next row from the run, or `None` at end-of-file.
    fn next_row(&mut self) -> Result<Option<Vec<Value>>> {
        use std::io::Read;
        let mut len_buf = [0u8; 4];
        match self.reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut payload = vec![0u8; len];
        self.reader.read_exact(&mut payload)?;
        Ok(Some(codec::decode_values(&payload, self.column_count)?))
    }
}

impl Drop for SpilledRun {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Sort `buffer` in place per `keys`, then write it to a fresh temp
/// file as a run. Returns the `SpilledRun` handle (with the temp file
/// already open for reading).
fn spill_sorted_run(
    buffer: &mut [Vec<Value>],
    keys: &[(usize, bool)],
    column_count: usize,
) -> Result<SpilledRun> {
    use std::io::Write;
    sort_rows(buffer, keys);
    let path = unique_spill_path();
    {
        let mut writer = std::io::BufWriter::new(std::fs::File::create(&path)?);
        for row in buffer.iter() {
            let encoded = codec::encode_values(row);
            writer.write_all(&(encoded.len() as u32).to_le_bytes())?;
            writer.write_all(&encoded)?;
        }
        writer.flush()?;
    }
    let reader = std::io::BufReader::new(std::fs::File::open(&path)?);
    Ok(SpilledRun {
        path,
        reader,
        column_count,
    })
}

/// An entry in the k-way merge heap: one row, the run it came from,
/// and a shared handle to the sort keys (so `Ord` can compare without
/// external context).
struct MergeEntry {
    row: Vec<Value>,
    run_id: usize,
    keys: std::sync::Arc<[(usize, bool)]>,
}

impl MergeEntry {
    fn cmp_rows(&self, other: &MergeEntry) -> std::cmp::Ordering {
        for &(col, desc) in self.keys.iter() {
            let ordering = order_values(&self.row[col], &other.row[col]);
            let ordering = if desc {
                ordering.reverse()
            } else {
                ordering
            };
            if ordering != std::cmp::Ordering::Equal {
                return ordering;
            }
        }
        std::cmp::Ordering::Equal
    }
}

impl Eq for MergeEntry {}
impl PartialEq for MergeEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}
impl PartialOrd for MergeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for MergeEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // `BinaryHeap` is a max-heap; we want the smallest sorted row
        // to pop first, so reverse the natural ordering.
        let row_ord = self.cmp_rows(other).reverse();
        // Break ties by run id for deterministic output.
        row_ord.then(other.run_id.cmp(&self.run_id))
    }
}

/// In-flight state of [`BatchSort`].
enum SortState {
    /// Still pulling input. `buffer` accumulates rows; when it crosses
    /// `SORT_SPILL_THRESHOLD` we sort + spill it to `runs` and start a
    /// fresh buffer.
    Building {
        buffer: Vec<Vec<Value>>,
        runs: Vec<SpilledRun>,
    },
    /// Input drained, no spills happened — sort the in-memory buffer
    /// and stream from it.
    DrainingMemory(std::vec::IntoIter<Vec<Value>>),
    /// Input drained, at least one spill — k-way merge across the
    /// runs (and the final in-memory buffer, also spilled).
    DrainingMerge {
        runs: Vec<SpilledRun>,
        heap: std::collections::BinaryHeap<MergeEntry>,
        keys: std::sync::Arc<[(usize, bool)]>,
    },
}

/// External-sort `ORDER BY` for the vectorised pipeline.
///
/// Buffers input rows up to [`SORT_SPILL_THRESHOLD`], sorts the run,
/// and spills it to a temp file. After the input is drained, performs
/// a k-way merge across the spilled runs (plus the final in-memory
/// run, also spilled, for a uniform code path). Output rows are
/// gathered into [`ColumnBatch`]es for downstream operators.
///
/// Memory bound: at most one run's worth of rows in `buffer` plus one
/// row per spilled run in the merge heap — `O(SORT_SPILL_THRESHOLD +
/// number_of_runs)` rows, regardless of input size.
struct BatchSort {
    input: Option<Box<dyn BatchOperator>>,
    keys: std::sync::Arc<[(usize, bool)]>,
    column_types: Vec<Type>,
    state: SortState,
}

impl BatchSort {
    fn new(
        input: Box<dyn BatchOperator>,
        keys: Vec<(usize, bool)>,
        column_types: Vec<Type>,
    ) -> BatchSort {
        BatchSort {
            input: Some(input),
            keys: keys.into(),
            column_types,
            state: SortState::Building {
                buffer: Vec::new(),
                runs: Vec::new(),
            },
        }
    }

    /// Pull every input batch, materialise rows into `buffer`, spilling
    /// whenever the threshold is crossed. Transitions `state` from
    /// `Building` to either `DrainingMemory` (no spills) or
    /// `DrainingMerge` (one or more spills).
    fn drain_input(&mut self, pager: &mut Pager) -> Result<()> {
        let mut input = self
            .input
            .take()
            .expect("drain_input called twice — input already drained");
        let column_count = self.column_types.len();
        let SortState::Building { buffer, runs } = &mut self.state else {
            unreachable!("drain_input called in non-Building state");
        };
        while let Some(batch) = input.next_batch(pager)? {
            for i in 0..batch.n_rows {
                buffer.push(batch.row_at(i));
                if buffer.len() >= SORT_SPILL_THRESHOLD {
                    let run = spill_sorted_run(buffer, &self.keys, column_count)?;
                    buffer.clear();
                    runs.push(run);
                }
            }
        }
        if runs.is_empty() {
            // Pure in-memory case: sort the buffer once and stream it.
            sort_rows(buffer, &self.keys);
            let drained = std::mem::take(buffer);
            self.state = SortState::DrainingMemory(drained.into_iter());
        } else {
            // Spill the tail too, for a uniform merge code path.
            if !buffer.is_empty() {
                let run = spill_sorted_run(buffer, &self.keys, column_count)?;
                buffer.clear();
                runs.push(run);
            }
            let mut runs = std::mem::take(runs);
            let mut heap = std::collections::BinaryHeap::with_capacity(runs.len());
            for (id, run) in runs.iter_mut().enumerate() {
                if let Some(row) = run.next_row()? {
                    heap.push(MergeEntry {
                        row,
                        run_id: id,
                        keys: std::sync::Arc::clone(&self.keys),
                    });
                }
            }
            self.state = SortState::DrainingMerge {
                runs,
                heap,
                keys: std::sync::Arc::clone(&self.keys),
            };
        }
        Ok(())
    }

    /// Pull the next sorted row from whichever draining state we're in.
    fn next_sorted_row(&mut self) -> Result<Option<Vec<Value>>> {
        match &mut self.state {
            SortState::Building { .. } => {
                unreachable!("next_sorted_row called before drain_input")
            }
            SortState::DrainingMemory(iter) => Ok(iter.next()),
            SortState::DrainingMerge { runs, heap, keys } => {
                let Some(entry) = heap.pop() else {
                    return Ok(None);
                };
                let run_id = entry.run_id;
                // Refill from the run we just consumed.
                if let Some(next) = runs[run_id].next_row()? {
                    heap.push(MergeEntry {
                        row: next,
                        run_id,
                        keys: std::sync::Arc::clone(keys),
                    });
                }
                Ok(Some(entry.row))
            }
        }
    }
}

impl BatchOperator for BatchSort {
    fn next_batch(&mut self, pager: &mut Pager) -> Result<Option<ColumnBatch>> {
        if self.input.is_some() {
            self.drain_input(pager)?;
        }
        let mut batch = ColumnBatch::with_types(&self.column_types);
        for _ in 0..crate::engine::batch::BATCH_SIZE {
            match self.next_sorted_row()? {
                Some(row) => batch.push_row(&row)?,
                None => break,
            }
        }
        if batch.is_empty() {
            Ok(None)
        } else {
            Ok(Some(batch))
        }
    }
}

/// Convert a [`BatchOperator`] tree into the row-at-a-time [`Operator`]
/// interface the rest of the executor consumes. Keeps a cursor into the
/// current batch and pulls a new one when exhausted.
struct BatchToRow {
    input: Box<dyn BatchOperator>,
    current: Option<ColumnBatch>,
    cursor: usize,
}

impl Operator for BatchToRow {
    fn next(&mut self, pager: &mut Pager) -> Result<Option<Vec<Value>>> {
        loop {
            if let Some(batch) = &self.current {
                if self.cursor < batch.n_rows {
                    let row = batch.row_at(self.cursor);
                    self.cursor += 1;
                    return Ok(Some(row));
                }
            }
            self.current = self.input.next_batch(pager)?;
            self.cursor = 0;
            if self.current.is_none() {
                return Ok(None);
            }
        }
    }
}

/// A vectorised nested-loop join: the left side streams as batches; the right
/// is drained once into a `Vec<Vec<Value>>` and rescanned per left row. Same
/// shape as the row-at-a-time [`NestedLoopJoin`], but with output assembled
/// into batches up to [`BATCH_SIZE`] rows. Per-row predicate eval still uses
/// the scalar evaluator over the combined row — vectorising the predicate
/// over the cross product is a future optimisation.
struct BatchNestedLoopJoin {
    left: Box<dyn BatchOperator>,
    /// Right input, drained into `right_rows` on first use.
    right_input: Option<Box<dyn BatchOperator>>,
    right_rows: Option<Vec<Vec<Value>>>,
    /// Combined column types for assembling the output batch.
    output_types: Vec<Type>,
    /// The `ON` predicate; `None` for a `CROSS JOIN`.
    on: Option<Expr>,
    kind: JoinKind,
    /// Scope spanning left + right, for evaluating `on`.
    scope: Scope,
    /// Right-side column count, for `NULL`-padding a `LEFT` miss.
    right_width: usize,
    /// Iteration state across the (left × right) cross product. Persists
    /// between `next_batch` calls so an output batch can split a left batch.
    current_left: Option<ColumnBatch>,
    left_pos: usize,
    right_pos: usize,
    matched_current: bool,
}

impl BatchOperator for BatchNestedLoopJoin {
    fn next_batch(&mut self, pager: &mut Pager) -> Result<Option<ColumnBatch>> {
        if self.right_rows.is_none() {
            self.right_rows = Some(drain_batches_to_rows(
                self.right_input.take().expect("right input drained twice"),
                pager,
            )?);
        }
        let mut out = ColumnBatch::with_types(&self.output_types);
        loop {
            if self.current_left.is_none() {
                match self.left.next_batch(pager)? {
                    Some(batch) => {
                        self.current_left = Some(batch);
                        self.left_pos = 0;
                        self.right_pos = 0;
                        self.matched_current = false;
                    }
                    None => return Ok(if out.is_empty() { None } else { Some(out) }),
                }
            }
            let left_batch = self.current_left.as_ref().unwrap();
            while self.left_pos < left_batch.n_rows {
                let left_row = left_batch.row_at(self.left_pos);
                let right_rows = self.right_rows.as_ref().unwrap();
                while self.right_pos < right_rows.len() {
                    let mut combined = left_row.clone();
                    combined.extend_from_slice(&right_rows[self.right_pos]);
                    self.right_pos += 1;
                    let keep = match &self.on {
                        None => true,
                        Some(predicate) => passes_filter(Some(predicate), &self.scope, &combined)?,
                    };
                    if keep {
                        self.matched_current = true;
                        out.push_row(&combined)?;
                        if out.n_rows >= BATCH_SIZE {
                            return Ok(Some(out));
                        }
                    }
                }
                // Right side exhausted for this left row.
                if self.kind == JoinKind::Left && !self.matched_current {
                    let mut padded = left_row;
                    padded.resize(padded.len() + self.right_width, Value::Null);
                    out.push_row(&padded)?;
                    if out.n_rows >= BATCH_SIZE {
                        self.left_pos += 1;
                        self.right_pos = 0;
                        self.matched_current = false;
                        return Ok(Some(out));
                    }
                }
                self.left_pos += 1;
                self.right_pos = 0;
                self.matched_current = false;
            }
            // Current left batch is spent; pull the next.
            self.current_left = None;
        }
    }
}

/// A vectorised hash join. Build phase: drain the inner side once, hash each
/// non-null row by its build-column value into `table`. Probe phase: for each
/// left batch row, encode the probe-column value, look up the bucket, reapply
/// the full `ON` predicate to each pair, emit a row per match. `LEFT` keeps
/// unmatched left rows padded with `NULL`s. Mirrors the row-at-a-time
/// [`HashJoin`]; output is assembled into batches up to [`BATCH_SIZE`] rows.
struct BatchHashJoin {
    left: Box<dyn BatchOperator>,
    right_input: Option<Box<dyn BatchOperator>>,
    table: Option<HashMap<Vec<u8>, Vec<Vec<Value>>>>,
    /// Column position within a left row; column position within an inner row.
    probe_col: usize,
    build_col: usize,
    /// The full `ON` predicate — still applied since the hash key only narrows.
    on: Expr,
    kind: JoinKind,
    scope: Scope,
    output_types: Vec<Type>,
    right_width: usize,
    /// Iteration state, kept across `next_batch` calls.
    current_left: Option<ColumnBatch>,
    left_pos: usize,
    /// Set per left row when we cross into it: the encoded probe key, or
    /// `None` if the probe column was `NULL` (no match possible).
    probe_key: Option<Vec<u8>>,
    /// Whether `probe_key` has been initialised for `left_pos` yet.
    row_started: bool,
    /// Cursor into the matching bucket for the current left row.
    match_pos: usize,
    matched_current: bool,
}

impl BatchOperator for BatchHashJoin {
    fn next_batch(&mut self, pager: &mut Pager) -> Result<Option<ColumnBatch>> {
        if self.table.is_none() {
            // Build phase: drain the inner side and hash by build column.
            let input = self.right_input.take().expect("inner input drained twice");
            let inner = drain_batches_to_rows(input, pager)?;
            let mut table: HashMap<Vec<u8>, Vec<Vec<Value>>> = HashMap::new();
            for row in inner {
                // NULL keys never match: NULL = anything is never TRUE.
                if row[self.build_col].is_null() {
                    continue;
                }
                let key = codec::encode_index_value(&row[self.build_col]);
                table.entry(key).or_default().push(row);
            }
            self.table = Some(table);
        }
        let mut out = ColumnBatch::with_types(&self.output_types);
        loop {
            if self.current_left.is_none() {
                match self.left.next_batch(pager)? {
                    Some(batch) => {
                        self.current_left = Some(batch);
                        self.left_pos = 0;
                        self.row_started = false;
                    }
                    None => return Ok(if out.is_empty() { None } else { Some(out) }),
                }
            }
            let left_batch = self.current_left.as_ref().unwrap();
            while self.left_pos < left_batch.n_rows {
                if !self.row_started {
                    let left_row = left_batch.row_at(self.left_pos);
                    self.probe_key = if left_row[self.probe_col].is_null() {
                        None
                    } else {
                        Some(codec::encode_index_value(&left_row[self.probe_col]))
                    };
                    self.match_pos = 0;
                    self.matched_current = false;
                    self.row_started = true;
                }
                let left_row = left_batch.row_at(self.left_pos);
                let bucket: &[Vec<Value>] = match &self.probe_key {
                    None => &[],
                    Some(key) => self
                        .table
                        .as_ref()
                        .unwrap()
                        .get(key)
                        .map(|v| v.as_slice())
                        .unwrap_or(&[]),
                };
                while self.match_pos < bucket.len() {
                    let mut combined = left_row.clone();
                    combined.extend_from_slice(&bucket[self.match_pos]);
                    self.match_pos += 1;
                    if passes_filter(Some(&self.on), &self.scope, &combined)? {
                        self.matched_current = true;
                        out.push_row(&combined)?;
                        if out.n_rows >= BATCH_SIZE {
                            return Ok(Some(out));
                        }
                    }
                }
                // Bucket spent for this left row.
                if self.kind == JoinKind::Left && !self.matched_current {
                    let mut padded = left_row;
                    padded.resize(padded.len() + self.right_width, Value::Null);
                    out.push_row(&padded)?;
                    if out.n_rows >= BATCH_SIZE {
                        self.left_pos += 1;
                        self.row_started = false;
                        return Ok(Some(out));
                    }
                }
                self.left_pos += 1;
                self.row_started = false;
            }
            // Current left batch is spent; pull the next.
            self.current_left = None;
        }
    }
}

/// Drain a batched operator into a flat `Vec` of decoded rows. Used by the
/// nested-loop and hash joins to buffer the right side once.
fn drain_batches_to_rows(
    mut op: Box<dyn BatchOperator>,
    pager: &mut Pager,
) -> Result<Vec<Vec<Value>>> {
    let mut rows = Vec::new();
    while let Some(batch) = op.next_batch(pager)? {
        for i in 0..batch.n_rows {
            rows.push(batch.row_at(i));
        }
    }
    Ok(rows)
}

/// Columnwise expression evaluator: every recursive call returns a `Column`
/// of exactly `batch.n_rows`, so the result can compose with sibling columns.
/// Literals broadcast to a full column; column refs clone the matching input
/// column; arithmetic and comparisons run a tight per-element loop; logical
/// ops follow SQL's three-valued logic. Anything the columnar paths cannot
/// handle (a mistyped operand, an unsupported `Aggregate`, an unresolved
/// subquery) errors out — the vectorised pipeline is only entered when the
/// expression shape is in scope.
fn eval_batch(expr: &Expr, batch: &ColumnBatch, scope: &Scope) -> Result<BatchColumn> {
    let n = batch.n_rows;
    match expr {
        Expr::Null => Ok(broadcast_null(n)),
        Expr::Integer(v) => Ok(broadcast_int(*v, n)),
        Expr::Real(v) => Ok(broadcast_real(*v, n)),
        Expr::Str(v) => Ok(broadcast_text(v.clone(), n)),
        Expr::Bool(v) => Ok(broadcast_bool(*v, n)),
        Expr::Column(colref) => {
            let idx = scope.resolve(colref)?;
            // Materialise the column at logical-row order: when the input
            // batch carries a selection vector, we gather the selected
            // physical rows so the rest of the columnar eval can run over a
            // contiguous slice aligned to `batch.n_rows`.
            Ok(materialise_column(
                &batch.columns[idx],
                batch.selection.as_deref(),
            ))
        }
        Expr::Unary { op, expr } => {
            let inner = eval_batch(expr, batch, scope)?;
            unary_columnar(*op, inner)
        }
        Expr::Binary { op, left, right } => {
            let l = eval_batch(left, batch, scope)?;
            let r = eval_batch(right, batch, scope)?;
            binary_columnar(*op, l, r)
        }
        Expr::IsNull { expr, negated } => {
            let inner = eval_batch(expr, batch, scope)?;
            Ok(is_null_columnar(&inner, *negated))
        }
        Expr::InList {
            expr,
            values,
            has_null,
            negated,
        } => {
            // IN's per-row check against a (small) value list does not yet
            // have a columnar fast path; reuse the scalar evaluator on each
            // row of the probe column.
            let probes = eval_batch(expr, batch, scope)?;
            in_list_columnar(&probes, values, *has_null, *negated)
        }
        Expr::Aggregate(_) => Err(Error::exec(
            "aggregate functions are only allowed in a SELECT list or a HAVING clause",
        )),
        Expr::InSubquery { .. }
        | Expr::Exists(_)
        | Expr::ScalarSubquery(_)
        | Expr::CorrelatedExists(_)
        | Expr::CorrelatedScalarSubquery(_)
        | Expr::CorrelatedInSubquery { .. } => Err(Error::corruption(
            "subquery reached vectorised eval before being resolved",
        )),
    }
}

fn broadcast_int(v: i64, n: usize) -> BatchColumn {
    BatchColumn::Int {
        values: vec![v; n],
        nulls: NullMask::all_valid(n),
    }
}

fn broadcast_real(v: f64, n: usize) -> BatchColumn {
    BatchColumn::Real {
        values: vec![v; n],
        nulls: NullMask::all_valid(n),
    }
}

fn broadcast_text(v: String, n: usize) -> BatchColumn {
    BatchColumn::Text {
        values: vec![v; n],
        nulls: NullMask::all_valid(n),
    }
}

fn broadcast_bool(v: bool, n: usize) -> BatchColumn {
    BatchColumn::Bool {
        values: vec![v; n],
        nulls: NullMask::all_valid(n),
    }
}

/// An all-`NULL` column. Picks `Bool` arbitrarily — the underlying type does
/// not matter when every position is null, and a `Bool` representation
/// composes with logical ops and filters.
fn broadcast_null(n: usize) -> BatchColumn {
    let mut nulls = NullMask::with_capacity(n);
    for _ in 0..n {
        nulls.push(false);
    }
    BatchColumn::Bool {
        values: vec![false; n],
        nulls,
    }
}

fn unary_columnar(op: UnaryOp, col: BatchColumn) -> Result<BatchColumn> {
    match op {
        UnaryOp::Neg => match col {
            BatchColumn::Int { values, nulls } => {
                let mut out = Vec::with_capacity(values.len());
                for (i, &v) in values.iter().enumerate() {
                    if nulls.is_valid(i) {
                        out.push(
                            v.checked_neg()
                                .ok_or_else(|| Error::exec("integer overflow while negating"))?,
                        );
                    } else {
                        out.push(0);
                    }
                }
                Ok(BatchColumn::Int { values: out, nulls })
            }
            BatchColumn::Real { values, nulls } => {
                let out: Vec<f64> = values.iter().map(|&v| -v).collect();
                Ok(BatchColumn::Real { values: out, nulls })
            }
            other => Err(Error::exec(format!("cannot negate {}", other.ty()))),
        },
        UnaryOp::Not => match col {
            BatchColumn::Bool { values, nulls } => {
                // NOT NULL = NULL: the null mask is preserved. NOT TRUE/FALSE
                // flips the value bit.
                let out: Vec<bool> = values.iter().map(|&v| !v).collect();
                Ok(BatchColumn::Bool { values: out, nulls })
            }
            other => Err(Error::exec(format!(
                "NOT expects a boolean, found {}",
                other.ty()
            ))),
        },
    }
}

fn binary_columnar(op: BinaryOp, left: BatchColumn, right: BatchColumn) -> Result<BatchColumn> {
    use BinaryOp::*;
    match op {
        Add | Sub | Mul | Div => arithmetic_columnar(op, left, right),
        Eq | NotEq | Lt | LtEq | Gt | GtEq => compare_columnar(op, &left, &right),
        And | Or => logic_columnar(op, left, right),
    }
}

fn arithmetic_columnar(op: BinaryOp, left: BatchColumn, right: BatchColumn) -> Result<BatchColumn> {
    match (&left, &right) {
        (BatchColumn::Int { .. }, BatchColumn::Int { .. }) => arith_int_int(op, left, right),
        (
            BatchColumn::Int { .. } | BatchColumn::Real { .. },
            BatchColumn::Int { .. } | BatchColumn::Real { .. },
        ) => arith_real_real(op, left, right),
        (l, r) => Err(Error::exec(format!(
            "cannot {} {} and {}",
            op_symbol(op),
            l.ty(),
            r.ty()
        ))),
    }
}

fn arith_int_int(op: BinaryOp, left: BatchColumn, right: BatchColumn) -> Result<BatchColumn> {
    let (
        BatchColumn::Int {
            values: lv,
            nulls: ln,
        },
        BatchColumn::Int {
            values: rv,
            nulls: rn,
        },
    ) = (left, right)
    else {
        unreachable!("arith_int_int called with non-Int columns");
    };
    let n = lv.len();
    let mut out = Vec::with_capacity(n);
    let mut out_nulls = NullMask::with_capacity(n);
    for i in 0..n {
        if ln.is_valid(i) && rn.is_valid(i) {
            let v = match op {
                BinaryOp::Add => lv[i].checked_add(rv[i]),
                BinaryOp::Sub => lv[i].checked_sub(rv[i]),
                BinaryOp::Mul => lv[i].checked_mul(rv[i]),
                BinaryOp::Div => {
                    if rv[i] == 0 {
                        return Err(Error::exec("division by zero"));
                    }
                    lv[i].checked_div(rv[i])
                }
                _ => unreachable!(),
            }
            .ok_or_else(|| Error::exec("integer overflow"))?;
            out.push(v);
            out_nulls.push(true);
        } else {
            out.push(0);
            out_nulls.push(false);
        }
    }
    Ok(BatchColumn::Int {
        values: out,
        nulls: out_nulls,
    })
}

fn arith_real_real(op: BinaryOp, left: BatchColumn, right: BatchColumn) -> Result<BatchColumn> {
    let n = left.len();
    let mut out = Vec::with_capacity(n);
    let mut out_nulls = NullMask::with_capacity(n);
    for i in 0..n {
        let lv = column_as_real(&left, i);
        let rv = column_as_real(&right, i);
        match (lv, rv) {
            (Some(a), Some(b)) => {
                let v = match op {
                    BinaryOp::Add => a + b,
                    BinaryOp::Sub => a - b,
                    BinaryOp::Mul => a * b,
                    BinaryOp::Div => {
                        if b == 0.0 {
                            return Err(Error::exec("division by zero"));
                        }
                        a / b
                    }
                    _ => unreachable!(),
                };
                out.push(v);
                out_nulls.push(true);
            }
            _ => {
                out.push(0.0);
                out_nulls.push(false);
            }
        }
    }
    Ok(BatchColumn::Real {
        values: out,
        nulls: out_nulls,
    })
}

fn column_as_real(col: &BatchColumn, i: usize) -> Option<f64> {
    match col {
        BatchColumn::Int { values, nulls } if nulls.is_valid(i) => Some(values[i] as f64),
        BatchColumn::Real { values, nulls } if nulls.is_valid(i) => Some(values[i]),
        _ => None,
    }
}

fn compare_columnar(op: BinaryOp, left: &BatchColumn, right: &BatchColumn) -> Result<BatchColumn> {
    let n = left.len();
    let mut out = Vec::with_capacity(n);
    let mut out_nulls = NullMask::with_capacity(n);
    for i in 0..n {
        let l = left.value_at(i);
        let r = right.value_at(i);
        match compare_op(op, l, r)? {
            Value::Bool(b) => {
                out.push(b);
                out_nulls.push(true);
            }
            Value::Null => {
                out.push(false);
                out_nulls.push(false);
            }
            other => unreachable!("compare_op returned {other:?}"),
        }
    }
    Ok(BatchColumn::Bool {
        values: out,
        nulls: out_nulls,
    })
}

fn logic_columnar(op: BinaryOp, left: BatchColumn, right: BatchColumn) -> Result<BatchColumn> {
    let (
        BatchColumn::Bool {
            values: lv,
            nulls: ln,
        },
        BatchColumn::Bool {
            values: rv,
            nulls: rn,
        },
    ) = (&left, &right)
    else {
        return Err(Error::exec(format!(
            "{} expects boolean operands, got {} and {}",
            op_symbol(op),
            left.ty(),
            right.ty()
        )));
    };
    let n = lv.len();
    let mut out = Vec::with_capacity(n);
    let mut out_nulls = NullMask::with_capacity(n);
    for i in 0..n {
        let l_valid = ln.is_valid(i);
        let r_valid = rn.is_valid(i);
        // SQL three-valued logic: a definite FALSE/TRUE can dominate a NULL.
        let (val, valid) = match op {
            BinaryOp::And => match (l_valid, r_valid) {
                (true, true) => (lv[i] && rv[i], true),
                (true, false) if !lv[i] => (false, true),
                (false, true) if !rv[i] => (false, true),
                _ => (false, false),
            },
            BinaryOp::Or => match (l_valid, r_valid) {
                (true, true) => (lv[i] || rv[i], true),
                (true, false) if lv[i] => (true, true),
                (false, true) if rv[i] => (true, true),
                _ => (false, false),
            },
            _ => unreachable!(),
        };
        out.push(val);
        out_nulls.push(valid);
    }
    Ok(BatchColumn::Bool {
        values: out,
        nulls: out_nulls,
    })
}

fn is_null_columnar(col: &BatchColumn, negated: bool) -> BatchColumn {
    let nulls = col.nulls();
    let n = nulls.len();
    let mut out = Vec::with_capacity(n);
    let mut out_nulls = NullMask::with_capacity(n);
    for i in 0..n {
        let is_null = !nulls.is_valid(i);
        // `IS NULL` and `IS NOT NULL` always produce a definite boolean — they
        // are exactly the predicates SQL uses to probe nullability.
        out.push(is_null != negated);
        out_nulls.push(true);
    }
    BatchColumn::Bool {
        values: out,
        nulls: out_nulls,
    }
}

fn in_list_columnar(
    probes: &BatchColumn,
    values: &[Expr],
    has_null: bool,
    negated: bool,
) -> Result<BatchColumn> {
    let n = probes.len();
    let mut out = Vec::with_capacity(n);
    let mut out_nulls = NullMask::with_capacity(n);
    for i in 0..n {
        let probe = probes.value_at(i);
        let result = eval_in_list(probe, values, has_null)?;
        let final_value = if negated { negate_bool(result) } else { result };
        match final_value {
            Value::Bool(b) => {
                out.push(b);
                out_nulls.push(true);
            }
            Value::Null => {
                out.push(false);
                out_nulls.push(false);
            }
            other => unreachable!("IN-list yields bool or null, got {other:?}"),
        }
    }
    Ok(BatchColumn::Bool {
        values: out,
        nulls: out_nulls,
    })
}

/// Build a selection vector listing the physical column indices of the rows
/// that pass `mask`. The mask is in logical-row order (its `i`-th value is
/// the predicate's verdict on logical row `i`); the input batch's selection,
/// if any, maps each logical row back to its physical position in the
/// underlying [`Column`]s. The returned vector is what a fresh
/// [`ColumnBatch`] points into, reusing the input's column data unchanged.
///
/// `NULL` and `FALSE` both drop the row — only an exact `Bool(true)` keeps
/// it, matching the row-at-a-time `passes_filter` rule.
fn build_selection(batch: &ColumnBatch, mask: &BatchColumn) -> Result<Vec<u32>> {
    let (mvals, mnulls) = match mask {
        BatchColumn::Bool { values, nulls } => (values, nulls),
        other => {
            return Err(Error::exec(format!(
                "WHERE clause must produce a boolean, got {}",
                other.ty()
            )));
        }
    };
    let mut selection = Vec::with_capacity(batch.n_rows);
    for (logical, &v) in mvals.iter().enumerate().take(batch.n_rows) {
        if mnulls.is_valid(logical) && v {
            selection.push(batch.physical_for(logical) as u32);
        }
    }
    Ok(selection)
}

/// Build a column of exactly `selection.len()` rows, holding the values at
/// the given physical indices of `col`. Used by projection to materialise
/// column-ref output when the input carries a selection — and by `eval_batch`
/// in the same situation, so arithmetic and comparisons walk a contiguous
/// `Vec<T>` aligned to the batch's logical rows.
fn gather_column(col: &BatchColumn, selection: &[u32]) -> BatchColumn {
    let n = selection.len();
    match col {
        BatchColumn::Int { values, nulls } => {
            let mut out = Vec::with_capacity(n);
            let mut out_nulls = NullMask::with_capacity(n);
            for &i in selection {
                let i = i as usize;
                out.push(values[i]);
                out_nulls.push(nulls.is_valid(i));
            }
            BatchColumn::Int {
                values: out,
                nulls: out_nulls,
            }
        }
        BatchColumn::Real { values, nulls } => {
            let mut out = Vec::with_capacity(n);
            let mut out_nulls = NullMask::with_capacity(n);
            for &i in selection {
                let i = i as usize;
                out.push(values[i]);
                out_nulls.push(nulls.is_valid(i));
            }
            BatchColumn::Real {
                values: out,
                nulls: out_nulls,
            }
        }
        BatchColumn::Text { values, nulls } => {
            let mut out = Vec::with_capacity(n);
            let mut out_nulls = NullMask::with_capacity(n);
            for &i in selection {
                let i = i as usize;
                out.push(values[i].clone());
                out_nulls.push(nulls.is_valid(i));
            }
            BatchColumn::Text {
                values: out,
                nulls: out_nulls,
            }
        }
        BatchColumn::Bool { values, nulls } => {
            let mut out = Vec::with_capacity(n);
            let mut out_nulls = NullMask::with_capacity(n);
            for &i in selection {
                let i = i as usize;
                out.push(values[i]);
                out_nulls.push(nulls.is_valid(i));
            }
            BatchColumn::Bool {
                values: out,
                nulls: out_nulls,
            }
        }
    }
}

/// Materialise `col` at the batch's logical row order. If the batch has no
/// selection, the column is returned unchanged (cloned); otherwise the
/// physical indices in the selection are gathered into a fresh column.
fn materialise_column(col: &BatchColumn, selection: Option<&[u32]>) -> BatchColumn {
    match selection {
        None => col.clone(),
        Some(sel) => gather_column(col, sel),
    }
}

fn op_symbol(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Add => "+",
        BinaryOp::Sub => "-",
        BinaryOp::Mul => "*",
        BinaryOp::Div => "/",
        BinaryOp::Eq => "=",
        BinaryOp::NotEq => "<>",
        BinaryOp::Lt => "<",
        BinaryOp::LtEq => "<=",
        BinaryOp::Gt => ">",
        BinaryOp::GtEq => ">=",
        BinaryOp::And => "AND",
        BinaryOp::Or => "OR",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lit(n: i64) -> Box<Expr> {
        Box::new(Expr::Integer(n))
    }

    #[test]
    fn arithmetic_respects_precedence_results() {
        let expr = Expr::Binary {
            op: BinaryOp::Add,
            left: lit(1),
            right: Box::new(Expr::Binary {
                op: BinaryOp::Mul,
                left: lit(2),
                right: lit(3),
            }),
        };
        assert_eq!(eval(&expr, None).unwrap(), Value::Int(7));
    }

    #[test]
    fn integer_overflow_is_an_error() {
        let expr = Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Integer(i64::MAX)),
            right: lit(1),
        };
        assert!(eval(&expr, None).is_err());
    }

    #[test]
    fn division_by_zero_is_an_error() {
        let expr = Expr::Binary {
            op: BinaryOp::Div,
            left: lit(1),
            right: lit(0),
        };
        assert!(eval(&expr, None).is_err());
    }

    #[test]
    fn null_propagates_through_arithmetic_and_comparison() {
        let add = Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::Null),
            right: lit(1),
        };
        assert_eq!(eval(&add, None).unwrap(), Value::Null);

        let cmp = Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Null),
            right: Box::new(Expr::Null),
        };
        assert_eq!(eval(&cmp, None).unwrap(), Value::Null);
    }

    #[test]
    fn three_valued_logic() {
        // FALSE AND NULL is FALSE; TRUE AND NULL is NULL.
        let false_and_null = Expr::Binary {
            op: BinaryOp::And,
            left: Box::new(Expr::Bool(false)),
            right: Box::new(Expr::Null),
        };
        assert_eq!(eval(&false_and_null, None).unwrap(), Value::Bool(false));

        let true_and_null = Expr::Binary {
            op: BinaryOp::And,
            left: Box::new(Expr::Bool(true)),
            right: Box::new(Expr::Null),
        };
        assert_eq!(eval(&true_and_null, None).unwrap(), Value::Null);
    }

    #[test]
    fn int_and_real_compare_across_types() {
        let expr = Expr::Binary {
            op: BinaryOp::Lt,
            left: lit(3),
            right: Box::new(Expr::Real(3.5)),
        };
        assert_eq!(eval(&expr, None).unwrap(), Value::Bool(true));
    }

    #[test]
    fn comparing_incompatible_types_errors() {
        let expr = Expr::Binary {
            op: BinaryOp::Eq,
            left: lit(1),
            right: Box::new(Expr::Str("one".into())),
        };
        assert!(eval(&expr, None).is_err());
    }

    #[test]
    fn index_join_is_recognized() {
        // Left table a(x INT); inner table b(y INT) carrying an index on y.
        let a = Schema {
            name: "a".into(),
            columns: vec![Column {
                name: "x".into(),
                ty: Type::Int,
                not_null: false,
                foreign_key: None,
                stats: None,
            }],
            root: 1,
            next_rowid: 1,
            row_count: 0,
            indexes: vec![],
            primary_key_column: None,
            mutations_since_analyze: 0,
        };
        let b = Schema {
            name: "b".into(),
            columns: vec![Column {
                name: "y".into(),
                ty: Type::Int,
                not_null: false,
                foreign_key: None,
                stats: None,
            }],
            root: 2,
            next_rowid: 1,
            row_count: 0,
            indexes: vec![Index {
                name: "by_y".into(),
                columns: vec![0],
                root: 99,
                unique: false,
            }],
            primary_key_column: None,
            mutations_since_analyze: 0,
        };
        let mut scope = Scope::single("a", &a);
        let left_len = scope.len();
        scope.extend("b", &b);

        let eq = |left: ColumnRef, right: ColumnRef| Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Column(left)),
            right: Box::new(Expr::Column(right)),
        };
        let a_x = || ColumnRef {
            table: Some("a".into()),
            name: "x".into(),
        };
        let b_y = || ColumnRef {
            table: Some("b".into()),
            name: "y".into(),
        };

        // `a.x = b.y` drives the index on b(y); the key is the left column a.x.
        assert_eq!(
            find_index_join(&eq(a_x(), b_y()), left_len, &scope, &b),
            Some((Expr::Column(a_x()), 99))
        );
        // Orientation does not matter — `b.y = a.x` works the same.
        assert_eq!(
            find_index_join(&eq(b_y(), a_x()), left_len, &scope, &b),
            Some((Expr::Column(a_x()), 99))
        );
        // A compound ON still finds the usable equi-join conjunct.
        let compound = Expr::Binary {
            op: BinaryOp::And,
            left: Box::new(Expr::Bool(true)),
            right: Box::new(eq(a_x(), b_y())),
        };
        assert_eq!(
            find_index_join(&compound, left_len, &scope, &b),
            Some((Expr::Column(a_x()), 99))
        );

        // With no index on the inner column there is nothing to drive.
        let b_unindexed = Schema {
            indexes: vec![],
            ..b.clone()
        };
        assert_eq!(
            find_index_join(&eq(a_x(), b_y()), left_len, &scope, &b_unindexed),
            None
        );
    }
}
