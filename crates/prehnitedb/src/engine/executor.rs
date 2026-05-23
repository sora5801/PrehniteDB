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
use std::hash::{BuildHasher, Hasher};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;

use crate::engine::catalog::Catalog;
use crate::engine::codec;
use crate::engine::planner::{AccessPath, Plan};
use crate::engine::schema::{Column, Index, Schema};
use crate::engine::value::{coerce, Type, Value};
use crate::error::{Error, Result};
use crate::sql::ast::{
    Aggregate, AggregateArg, AggregateFunc, BinaryOp, ColumnRef, Expr, FromClause, JoinKind,
    OrderKey, Projection, SelectItem, UnaryOp,
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
pub fn execute_streaming(pager: &mut Pager, catalog: &Catalog, plan: Plan) -> Result<Execution> {
    // Wrap a non-SELECT statement's `Ack` outcome; only a `SELECT` has rows.
    let ack = |result: Result<QueryResult>| match result? {
        QueryResult::Ack(message) => Ok(Execution::Ack(message)),
        QueryResult::Rows { .. } => unreachable!("only SELECT produces rows"),
    };
    match plan {
        Plan::CreateTable { name, columns } => ack(create_table(pager, catalog, name, columns)),
        Plan::DropTable { name } => ack(drop_table(pager, catalog, name)),
        Plan::CreateIndex {
            name,
            table,
            columns,
        } => ack(create_index(pager, catalog, name, table, columns)),
        Plan::DropIndex { name } => ack(drop_index(pager, catalog, name)),
        Plan::Insert {
            table,
            columns,
            rows,
        } => ack(insert(pager, catalog, table, columns, rows)),
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
            presorted, limit, offset,
        )?)),
        Plan::Update {
            table,
            assignments,
            filter,
            access,
        } => ack(update(pager, catalog, table, assignments, filter, access)),
        Plan::Delete {
            table,
            filter,
            access,
        } => ack(delete(pager, catalog, table, filter, access)),
        // VACUUM must replace the pager's contents, which the executor cannot
        // do; `Database::execute` intercepts it before reaching here.
        Plan::Vacuum => unreachable!("VACUUM is handled by Database::execute"),
    }
}

/// Run a planned statement, materializing a `SELECT`'s rows into a
/// [`QueryResult`]. This is the embedding API; the server streams instead, via
/// [`execute_streaming`].
pub fn execute(pager: &mut Pager, catalog: &Catalog, plan: Plan) -> Result<QueryResult> {
    match execute_streaming(pager, catalog, plan)? {
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
) -> Result<QueryResult> {
    if catalog.get(pager, &name)?.is_some() {
        return Err(Error::exec(format!("table '{name}' already exists")));
    }
    let tree = BTree::create(pager)?;
    let schema = Schema {
        name: name.clone(),
        columns,
        root: tree.root(),
        next_rowid: 1,
        row_count: 0,
        indexes: Vec::new(),
    };
    catalog.put(pager, &schema)?;
    Ok(QueryResult::Ack(format!("table '{name}' created")))
}

fn drop_table(pager: &mut Pager, catalog: &Catalog, name: String) -> Result<QueryResult> {
    let schema = require_table(pager, catalog, &name)?;
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
) -> Result<QueryResult> {
    let mut schema = require_table(pager, catalog, &table)?;
    // Resolve every named column, rejecting a repeat within one index.
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

    // Populate the new index from the table's existing rows.
    let index = BTree::create(pager)?;
    let table_tree = BTree::open(schema.root);
    for (rowid_key, encoded) in table_tree.scan(pager)? {
        let values = codec::decode_row(&encoded, schema.columns.len())?;
        let key = codec::encode_index_key(&values, &columns, &rowid_key);
        index.insert(pager, &key, &[])?;
    }

    schema.indexes.push(Index {
        name: index_name.clone(),
        columns,
        root: index.root(),
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

fn insert(
    pager: &mut Pager,
    catalog: &Catalog,
    table: String,
    columns: Option<Vec<String>>,
    rows: Vec<Vec<Expr>>,
) -> Result<QueryResult> {
    let mut schema = require_table(pager, catalog, &table)?;

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
        // Columns not named by the INSERT default to NULL.
        let mut values = vec![Value::Null; schema.columns.len()];
        for (slot, expr) in row_exprs.iter().enumerate() {
            let column = targets[slot];
            let evaluated = eval(expr, None)?;
            values[column] = coerce(evaluated, schema.columns[column].ty)?;
        }
        let rowid = schema.next_rowid;
        schema.next_rowid += 1;
        let rowid_key = codec::rowid_key(rowid);
        tree.insert(pager, &rowid_key, &codec::encode_row(&values))?;
        index_insert_row(pager, &schema, &rowid_key, &values)?;
        inserted += 1;
    }

    // Persist the advanced rowid counter and the new row count.
    schema.row_count += inserted;
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
fn build_from(
    pager: &mut Pager,
    catalog: &Catalog,
    from: &FromClause,
    base_access: &AccessPath,
) -> Result<(Box<dyn Operator>, Scope)> {
    let base_schema = require_table(pager, catalog, &from.table.name)?;
    let mut scope = Scope::single(from.table.qualifier(), &base_schema);
    let mut op = scan_operator(pager, &base_schema, base_access)?;

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
        let left_scope = scope.clone();
        scope.extend(&qualifier, &joined_schema);

        // An equi-join onto an indexed leading column of the joined table lets
        // each left row look its matches up, sparing a full inner rescan.
        let index_join = join
            .on
            .as_ref()
            .and_then(|on| find_index_join(on, left_scope.len(), &scope, &joined_schema));
        // Failing an index, an equi-join on any inner column can still be
        // hashed — O(left + inner) instead of the nested loop's O(left × inner).
        let equi_join = join
            .on
            .as_ref()
            .and_then(|on| find_equi_join(on, left_scope.len(), &scope));

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
                current_left: None,
                inner: Vec::new(),
                inner_pos: 0,
                matched_current: false,
            })
        } else if let Some((probe_col, build_col)) = equi_join {
            let right = scan_operator(pager, &joined_schema, &AccessPath::FullScan)?;
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
            let right = scan_operator(pager, &joined_schema, &AccessPath::FullScan)?;
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
) -> Result<RowStream> {
    // The FROM pipeline — a scan, then a NestedLoopJoin per join — and the
    // scope spanning every column it produces.
    let (mut op, scope) = build_from(pager, catalog, &from, &access)?;

    // A plain projection (`Some(cols)`) returns one output row per joined row;
    // GROUP BY, HAVING, or any aggregate falls through to the grouped path.
    let plain: Option<Vec<usize>> = match &projection {
        Projection::All => {
            if !group_by.is_empty() || having.is_some() {
                return Err(Error::exec(
                    "SELECT * cannot be combined with GROUP BY or HAVING",
                ));
            }
            Some((0..scope.len()).collect())
        }
        Projection::Items(items) => {
            let has_aggregate = items
                .iter()
                .any(|item| matches!(item, SelectItem::Aggregate(_)));
            if group_by.is_empty() && !has_aggregate && having.is_none() {
                let mut columns = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        SelectItem::Column(colref) => columns.push(scope.resolve(colref)?),
                        SelectItem::Aggregate(_) => unreachable!("guarded by has_aggregate"),
                    }
                }
                Some(columns)
            } else {
                None
            }
        }
    };

    // The WHERE clause filters joined rows, downstream of every join.
    if let Some(predicate) = filter {
        op = Box::new(Filter {
            input: op,
            predicate,
            scope: scope.clone(),
        });
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
            }
            let columns = projection_headers(&projection, &projected, &scope);
            op = Box::new(Project {
                input: op,
                columns: projected,
            });
            if limit.is_some() || offset.is_some() {
                op = Box::new(Limit {
                    input: op,
                    offset: offset.unwrap_or(0),
                    remaining: limit.unwrap_or(u64::MAX),
                });
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
            Ok(RowStream {
                columns,
                source: RowSource::Buffered(rows.into_iter()),
            })
        }
    }
}

/// The output column headers for a plain (non-grouped) projection.
fn projection_headers(projection: &Projection, projected: &[usize], scope: &Scope) -> Vec<String> {
    match projection {
        Projection::All => projected.iter().map(|&i| scope.header(i)).collect(),
        Projection::Items(items) => items
            .iter()
            .map(|item| match item {
                SelectItem::Column(colref) => colref.to_string(),
                SelectItem::Aggregate(_) => unreachable!("a plain projection has no aggregates"),
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

/// Build the scan at the base of the tree — a full table walk or a bounded
/// index walk — as a streaming cursor wrapped in an operator.
fn scan_operator(
    pager: &mut Pager,
    schema: &Schema,
    access: &AccessPath,
) -> Result<Box<dyn Operator>> {
    let column_count = schema.columns.len();
    let table = BTree::open(schema.root);
    match access {
        AccessPath::FullScan => {
            let cursor = table.cursor(pager, None, None)?;
            Ok(Box::new(TableScan {
                cursor,
                column_count,
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

/// A full table walk: every row, in rowid order.
struct TableScan {
    cursor: Cursor,
    column_count: usize,
}

impl Operator for TableScan {
    fn next(&mut self, pager: &mut Pager) -> Result<Option<Vec<Value>>> {
        match self.cursor.next(pager)? {
            Some((_rowid, encoded)) => Ok(Some(codec::decode_row(&encoded, self.column_count)?)),
            None => Ok(None),
        }
    }
}

/// A bounded index walk: each index entry's rowid is followed back to its row
/// in the table tree.
struct IndexScan {
    cursor: Cursor,
    table: BTree,
    column_count: usize,
}

impl Operator for IndexScan {
    fn next(&mut self, pager: &mut Pager) -> Result<Option<Vec<Value>>> {
        let Some((index_key, _)) = self.cursor.next(pager)? else {
            return Ok(None);
        };
        if index_key.len() < 8 {
            return Err(Error::corruption("index key shorter than a rowid"));
        }
        let rowid_key = &index_key[index_key.len() - 8..];
        match self.table.search(pager, rowid_key)? {
            Some(encoded) => Ok(Some(codec::decode_row(&encoded, self.column_count)?)),
            None => Err(Error::corruption(
                "index references a row that does not exist",
            )),
        }
    }
}

/// Keep only rows for which the `WHERE` predicate is exactly `TRUE`.
struct Filter {
    input: Box<dyn Operator>,
    predicate: Expr,
    scope: Scope,
}

impl Operator for Filter {
    fn next(&mut self, pager: &mut Pager) -> Result<Option<Vec<Value>>> {
        while let Some(row) = self.input.next(pager)? {
            if passes_filter(Some(&self.predicate), &self.scope, &row)? {
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

/// Narrow each row to the selected columns.
struct Project {
    input: Box<dyn Operator>,
    columns: Vec<usize>,
}

impl Operator for Project {
    fn next(&mut self, pager: &mut Pager) -> Result<Option<Vec<Value>>> {
        match self.input.next(pager)? {
            Some(row) => Ok(Some(self.columns.iter().map(|&i| row[i].clone()).collect())),
            None => Ok(None),
        }
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
                        return Ok(Some(combined));
                    }
                }
            }
            // The right side is exhausted for this left row.
            let left = self.current_left.take().expect("a current left row");
            if self.kind == JoinKind::Left && !self.matched_current {
                let mut combined = left;
                combined.resize(combined.len() + self.right_width, Value::Null);
                return Ok(Some(combined));
            }
            // Otherwise advance to the next left row.
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
    current_left: Option<Vec<Value>>,
    /// Inner rows matched for the current left row.
    inner: Vec<Vec<Value>>,
    inner_pos: usize,
    matched_current: bool,
}

impl IndexNestedLoopJoin {
    /// The inner rows whose join column equals `key_value`, reached through the
    /// index. A `NULL` key matches nothing — `NULL = anything` is never `TRUE`.
    fn lookup(&self, pager: &mut Pager, key_value: &Value) -> Result<Vec<Vec<Value>>> {
        if key_value.is_null() {
            return Ok(Vec::new());
        }
        let lower = codec::encode_index_value(key_value);
        let upper = codec::prefix_upper_bound(&lower);
        let mut rows = Vec::new();
        for (index_key, _) in self.index.scan_range(pager, &lower, upper.as_deref())? {
            if index_key.len() < 8 {
                return Err(Error::corruption("index key shorter than a rowid"));
            }
            let rowid_key = &index_key[index_key.len() - 8..];
            match self.table.search(pager, rowid_key)? {
                Some(encoded) => rows.push(codec::decode_row(&encoded, self.right_width)?),
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
            let encoded = codec::encode_row(&row);
            inner_spills[partition]
                .as_mut()
                .expect("partition file present")
                .write_row(&encoded)?;
        }
        // Then the left side, partitioned the same way.
        let mut left = self.left.take().expect("left drained twice");
        while let Some(row) = left.next(pager)? {
            let partition = partition_for(&row[self.probe_col], &hasher_state);
            let encoded = codec::encode_row(&row);
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
            Some(bytes) => Ok(Some(codec::decode_row(&bytes, self.column_count)?)),
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

    let mut groups = partition(matched, &group_cols);

    // HAVING discards whole groups, judged by their aggregates.
    if let Some(predicate) = having {
        let mut kept = Vec::with_capacity(groups.len());
        for group in groups {
            let verdict = eval_having(predicate, scope, &group, &group_cols)?;
            if matches!(verdict, Value::Bool(true)) {
                kept.push(group);
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
            if !group_cols.contains(&column) {
                return Err(Error::exec(format!(
                    "ORDER BY column '{}' must be a GROUP BY column here",
                    key.column
                )));
            }
            keys.push((column, key.descending));
        }
        groups.sort_by(|a, b| {
            for &(column, descending) in &keys {
                // Each group is non-empty and constant on its grouping columns.
                let ordering = order_values(&a[0][column], &b[0][column]);
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
        })
        .collect();

    let mut rows = Vec::with_capacity(groups.len());
    for group in &groups {
        let mut row = Vec::with_capacity(items.len());
        for item in items {
            row.push(match item {
                SelectItem::Column(colref) => group[0][scope.resolve(colref)?].clone(),
                SelectItem::Aggregate(aggregate) => compute_aggregate(scope, aggregate, group)?,
            });
        }
        rows.push(row);
    }
    Ok(QueryResult::Rows { columns, rows })
}

/// Partition rows into groups by the `group_cols` tuple. With no grouping
/// columns the whole set is one group — kept even when empty, so a whole-table
/// aggregate over zero rows still yields one result row.
fn partition(mut rows: Vec<Vec<Value>>, group_cols: &[usize]) -> Vec<Vec<Vec<Value>>> {
    if group_cols.is_empty() {
        return vec![rows];
    }
    rows.sort_by(|a, b| {
        for &column in group_cols {
            let ordering = order_values(&a[column], &b[column]);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        Ordering::Equal
    });
    let mut groups: Vec<Vec<Vec<Value>>> = Vec::new();
    for row in rows {
        match groups.last_mut() {
            Some(group)
                if group_cols
                    .iter()
                    .all(|&c| order_values(&row[c], &group[0][c]) == Ordering::Equal) =>
            {
                group.push(row);
            }
            _ => groups.push(vec![row]),
        }
    }
    groups
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

fn compute_aggregate(scope: &Scope, aggregate: &Aggregate, rows: &[Vec<Value>]) -> Result<Value> {
    match (aggregate.func, &aggregate.arg) {
        (AggregateFunc::Count, AggregateArg::Star) => Ok(Value::Int(rows.len() as i64)),
        (AggregateFunc::Count, AggregateArg::Column(colref)) => {
            let column = scope.resolve(colref)?;
            let present = rows.iter().filter(|row| !row[column].is_null()).count();
            Ok(Value::Int(present as i64))
        }
        (func, AggregateArg::Star) => Err(Error::exec(format!(
            "{}(*) is not allowed — {} needs a column",
            func_name(func),
            func_name(func)
        ))),
        (AggregateFunc::Sum, AggregateArg::Column(colref)) => {
            sum_values(scope, scope.resolve(colref)?, rows)
        }
        (AggregateFunc::Avg, AggregateArg::Column(colref)) => {
            avg_values(scope, scope.resolve(colref)?, rows)
        }
        (AggregateFunc::Min, AggregateArg::Column(colref)) => {
            Ok(extreme(scope.resolve(colref)?, rows, Ordering::Less))
        }
        (AggregateFunc::Max, AggregateArg::Column(colref)) => {
            Ok(extreme(scope.resolve(colref)?, rows, Ordering::Greater))
        }
    }
}

/// `SUM` over non-null values. Empty or all-null input sums to `NULL`.
fn sum_values(scope: &Scope, column: usize, rows: &[Vec<Value>]) -> Result<Value> {
    match scope.column_type(column) {
        Type::Int => {
            let mut total: i64 = 0;
            let mut seen = false;
            for row in rows {
                if let Value::Int(n) = &row[column] {
                    seen = true;
                    total = total
                        .checked_add(*n)
                        .ok_or_else(|| Error::exec("SUM overflowed a 64-bit integer"))?;
                }
            }
            Ok(if seen { Value::Int(total) } else { Value::Null })
        }
        Type::Real => {
            let mut total = 0.0f64;
            let mut seen = false;
            for row in rows {
                if let Value::Real(x) = &row[column] {
                    seen = true;
                    total += *x;
                }
            }
            Ok(if seen {
                Value::Real(total)
            } else {
                Value::Null
            })
        }
        other => Err(Error::exec(format!(
            "SUM requires a numeric column, but '{}' is {other}",
            scope.column_name(column)
        ))),
    }
}

/// `AVG` over non-null values, always a `REAL`. Empty input averages to `NULL`.
fn avg_values(scope: &Scope, column: usize, rows: &[Vec<Value>]) -> Result<Value> {
    match scope.column_type(column) {
        Type::Int | Type::Real => {
            let mut total = 0.0f64;
            let mut count = 0u64;
            for row in rows {
                match &row[column] {
                    Value::Int(n) => {
                        total += *n as f64;
                        count += 1;
                    }
                    Value::Real(x) => {
                        total += *x;
                        count += 1;
                    }
                    _ => {}
                }
            }
            Ok(if count == 0 {
                Value::Null
            } else {
                Value::Real(total / count as f64)
            })
        }
        other => Err(Error::exec(format!(
            "AVG requires a numeric column, but '{}' is {other}",
            scope.column_name(column)
        ))),
    }
}

/// `MIN` (`want` = `Less`) or `MAX` (`want` = `Greater`) over non-null values.
fn extreme(column: usize, rows: &[Vec<Value>], want: Ordering) -> Value {
    let mut best: Option<&Value> = None;
    for row in rows {
        let value = &row[column];
        if value.is_null() {
            continue;
        }
        match best {
            None => best = Some(value),
            Some(current) if order_values(value, current) == want => best = Some(value),
            Some(_) => {}
        }
    }
    best.cloned().unwrap_or(Value::Null)
}

fn update(
    pager: &mut Pager,
    catalog: &Catalog,
    table: String,
    assignments: Vec<(String, Expr)>,
    filter: Option<Expr>,
    access: AccessPath,
) -> Result<QueryResult> {
    let schema = require_table(pager, catalog, &table)?;
    // A WHERE clause and the assignment expressions resolve against this one
    // table — `UPDATE` does not join.
    let scope = Scope::single(&table, &schema);

    // Resolve every assignment target up front so an unknown column fails
    // before any row is touched.
    let mut resolved = Vec::with_capacity(assignments.len());
    for (name, expr) in &assignments {
        resolved.push((column_index(&schema, name)?, expr));
    }

    let table_tree = BTree::open(schema.root);
    let mut updated = 0u64;
    for (rowid_key, old) in collect_candidates(pager, &schema, &access)? {
        if !passes_filter(filter.as_ref(), &scope, &old)? {
            continue;
        }
        let mut new = old.clone();
        for (column, expr) in &resolved {
            // Assignment expressions see the row's pre-update values.
            let evaluated = eval(
                expr,
                Some(&RowContext {
                    scope: &scope,
                    values: &old,
                }),
            )?;
            new[*column] = coerce(evaluated, schema.columns[*column].ty)?;
        }
        // Keep every index in step with the row it points at.
        index_delete_row(pager, &schema, &rowid_key, &old)?;
        index_insert_row(pager, &schema, &rowid_key, &new)?;
        table_tree.insert(pager, &rowid_key, &codec::encode_row(&new))?;
        updated += 1;
    }
    Ok(QueryResult::Ack(format!("{updated} row(s) updated")))
}

fn delete(
    pager: &mut Pager,
    catalog: &Catalog,
    table: String,
    filter: Option<Expr>,
    access: AccessPath,
) -> Result<QueryResult> {
    let mut schema = require_table(pager, catalog, &table)?;
    // A WHERE clause resolves against this one table — `DELETE` does not join.
    let scope = Scope::single(&table, &schema);
    let table_tree = BTree::open(schema.root);

    let mut deleted = 0u64;
    for (rowid_key, values) in collect_candidates(pager, &schema, &access)? {
        if !passes_filter(filter.as_ref(), &scope, &values)? {
            continue;
        }
        index_delete_row(pager, &schema, &rowid_key, &values)?;
        table_tree.delete(pager, &rowid_key)?;
        deleted += 1;
    }
    if deleted > 0 {
        // saturating_sub: belt-and-braces against a miscount corrupting stats.
        schema.row_count = schema.row_count.saturating_sub(deleted);
        catalog.put(pager, &schema)?;
    }
    Ok(QueryResult::Ack(format!("{deleted} row(s) deleted")))
}

/// Gather the rows a query should consider, as `(rowid key, decoded row)`
/// pairs, via the access path the planner chose.
fn collect_candidates(
    pager: &mut Pager,
    schema: &Schema,
    access: &AccessPath,
) -> Result<Vec<(Vec<u8>, Vec<Value>)>> {
    let table = BTree::open(schema.root);
    match access {
        AccessPath::FullScan => {
            let mut out = Vec::new();
            for (rowid_key, encoded) in table.scan(pager)? {
                let row = codec::decode_row(&encoded, schema.columns.len())?;
                out.push((rowid_key, row));
            }
            Ok(out)
        }
        AccessPath::IndexScan {
            index_root,
            lower,
            upper,
        } => {
            let index = BTree::open(*index_root);
            let mut out = Vec::new();
            for (index_key, _) in index.scan_range(pager, lower, upper.as_deref())? {
                if index_key.len() < 8 {
                    return Err(Error::corruption("index key shorter than a rowid"));
                }
                let rowid_key = index_key[index_key.len() - 8..].to_vec();
                match table.search(pager, &rowid_key)? {
                    Some(encoded) => {
                        let row = codec::decode_row(&encoded, schema.columns.len())?;
                        out.push((rowid_key, row));
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
fn index_insert_row(
    pager: &mut Pager,
    schema: &Schema,
    rowid_key: &[u8],
    values: &[Value],
) -> Result<()> {
    for index in &schema.indexes {
        let key = codec::encode_index_key(values, &index.columns, rowid_key);
        BTree::open(index.root).insert(pager, &key, &[])?;
    }
    Ok(())
}

/// Remove this row from every index on the table.
fn index_delete_row(
    pager: &mut Pager,
    schema: &Schema,
    rowid_key: &[u8],
    values: &[Value],
) -> Result<()> {
    for index in &schema.indexes {
        let key = codec::encode_index_key(values, &index.columns, rowid_key);
        BTree::open(index.root).delete(pager, &key)?;
    }
    Ok(())
}

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
    }
}

/// Evaluate a `HAVING` predicate against one group: column references resolve
/// to the group's (constant) value for that grouping column, and aggregate
/// calls are computed over the group's rows.
fn eval_having(
    expr: &Expr,
    scope: &Scope,
    group: &[Vec<Value>],
    group_cols: &[usize],
) -> Result<Value> {
    match expr {
        Expr::Null => Ok(Value::Null),
        Expr::Integer(n) => Ok(Value::Int(*n)),
        Expr::Real(r) => Ok(Value::Real(*r)),
        Expr::Str(s) => Ok(Value::Text(s.clone())),
        Expr::Bool(b) => Ok(Value::Bool(*b)),
        Expr::Aggregate(aggregate) => compute_aggregate(scope, aggregate, group),
        Expr::Column(colref) => {
            let column = scope.resolve(colref)?;
            if !group_cols.contains(&column) {
                return Err(Error::exec(format!(
                    "HAVING column '{colref}' must be a GROUP BY column or wrapped in an aggregate"
                )));
            }
            Ok(group[0][column].clone())
        }
        Expr::Unary { op, expr } => eval_unary(*op, eval_having(expr, scope, group, group_cols)?),
        Expr::Binary { op, left, right } => eval_binary(
            *op,
            eval_having(left, scope, group, group_cols)?,
            eval_having(right, scope, group, group_cols)?,
        ),
        Expr::IsNull { expr, negated } => {
            let value = eval_having(expr, scope, group, group_cols)?;
            Ok(Value::Bool(value.is_null() != *negated))
        }
    }
}

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
            }],
            root: 1,
            next_rowid: 1,
            row_count: 0,
            indexes: vec![],
        };
        let b = Schema {
            name: "b".into(),
            columns: vec![Column {
                name: "y".into(),
                ty: Type::Int,
            }],
            root: 2,
            next_rowid: 1,
            row_count: 0,
            indexes: vec![Index {
                name: "by_y".into(),
                columns: vec![0],
                root: 99,
            }],
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
