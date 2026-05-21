//! The executor — it runs a [`Plan`] against the storage engine.
//!
//! Rows are reached one of two ways, chosen by the planner: a full table scan,
//! or a bounded scan of a secondary index. Either way the statement's `WHERE`
//! clause is then applied in full, so an index only ever *narrows* the
//! candidate set; it never changes an answer.
//!
//! Expression evaluation follows SQL's three-valued logic: `NULL` propagates
//! through arithmetic and comparisons, and a `WHERE` clause keeps a row only
//! when its predicate evaluates to exactly `TRUE`.

use std::cmp::Ordering;
use std::fmt;

use crate::engine::catalog::Catalog;
use crate::engine::codec;
use crate::engine::planner::{AccessPath, Plan};
use crate::engine::schema::{Column, Index, Schema};
use crate::engine::value::{coerce, Type, Value};
use crate::error::{Error, Result};
use crate::sql::ast::{
    Aggregate, AggregateArg, AggregateFunc, BinaryOp, Expr, OrderKey, Projection, UnaryOp,
};
use crate::storage::{BTree, Pager};

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

/// Run a planned statement.
pub fn execute(pager: &mut Pager, catalog: &Catalog, plan: Plan) -> Result<QueryResult> {
    match plan {
        Plan::CreateTable { name, columns } => create_table(pager, catalog, name, columns),
        Plan::DropTable { name } => drop_table(pager, catalog, name),
        Plan::CreateIndex {
            name,
            table,
            columns,
        } => create_index(pager, catalog, name, table, columns),
        Plan::DropIndex { name } => drop_index(pager, catalog, name),
        Plan::Insert {
            table,
            columns,
            rows,
        } => insert(pager, catalog, table, columns, rows),
        Plan::Select {
            table,
            projection,
            filter,
            access,
            order_by,
            presorted,
        } => select(
            pager, catalog, table, projection, filter, access, order_by, presorted,
        ),
        Plan::Update {
            table,
            assignments,
            filter,
            access,
        } => update(pager, catalog, table, assignments, filter, access),
        Plan::Delete {
            table,
            filter,
            access,
        } => delete(pager, catalog, table, filter, access),
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

    // Persist the advanced rowid counter.
    catalog.put(pager, &schema)?;
    Ok(QueryResult::Ack(format!("{inserted} row(s) inserted")))
}

#[allow(clippy::too_many_arguments)]
fn select(
    pager: &mut Pager,
    catalog: &Catalog,
    table: String,
    projection: Projection,
    filter: Option<Expr>,
    access: AccessPath,
    order_by: Vec<OrderKey>,
    presorted: bool,
) -> Result<QueryResult> {
    let schema = require_table(pager, catalog, &table)?;

    // Gather every row that satisfies the WHERE clause, as full rows.
    let mut matched: Vec<Vec<Value>> = Vec::new();
    for (_rowid, values) in collect_candidates(pager, &schema, &access)? {
        if passes_filter(filter.as_ref(), &schema, &values)? {
            matched.push(values);
        }
    }

    // Aggregates collapse the whole set into a single labelled row.
    if let Projection::Aggregates(aggregates) = &projection {
        return aggregate_result(&schema, aggregates, &matched);
    }

    // Sort, unless the access path already produced rows in the wanted order.
    if !order_by.is_empty() && !presorted {
        let keys = resolve_order_keys(&schema, &order_by)?;
        matched.sort_by(|a, b| {
            for &(column, descending) in &keys {
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

    let projected: Vec<usize> = match &projection {
        Projection::All => (0..schema.columns.len()).collect(),
        Projection::Columns(names) => {
            let mut indices = Vec::with_capacity(names.len());
            for name in names {
                indices.push(column_index(&schema, name)?);
            }
            indices
        }
        Projection::Aggregates(_) => unreachable!("aggregate projections return above"),
    };
    let columns: Vec<String> = projected
        .iter()
        .map(|&i| schema.columns[i].name.clone())
        .collect();
    let rows = matched
        .iter()
        .map(|row| projected.iter().map(|&i| row[i].clone()).collect())
        .collect();
    Ok(QueryResult::Rows { columns, rows })
}

/// Resolve each `ORDER BY` key's column name to its index.
fn resolve_order_keys(schema: &Schema, order_by: &[OrderKey]) -> Result<Vec<(usize, bool)>> {
    order_by
        .iter()
        .map(|key| Ok((column_index(schema, &key.column)?, key.descending)))
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

/// Compute every aggregate over `rows`, producing a single result row.
fn aggregate_result(
    schema: &Schema,
    aggregates: &[Aggregate],
    rows: &[Vec<Value>],
) -> Result<QueryResult> {
    let mut columns = Vec::with_capacity(aggregates.len());
    let mut result = Vec::with_capacity(aggregates.len());
    for aggregate in aggregates {
        columns.push(aggregate_label(aggregate));
        result.push(compute_aggregate(schema, aggregate, rows)?);
    }
    Ok(QueryResult::Rows {
        columns,
        rows: vec![result],
    })
}

fn aggregate_label(aggregate: &Aggregate) -> String {
    let arg = match &aggregate.arg {
        AggregateArg::Star => "*",
        AggregateArg::Column(name) => name,
    };
    format!("{}({arg})", func_name(aggregate.func))
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

fn compute_aggregate(schema: &Schema, aggregate: &Aggregate, rows: &[Vec<Value>]) -> Result<Value> {
    match (aggregate.func, &aggregate.arg) {
        (AggregateFunc::Count, AggregateArg::Star) => Ok(Value::Int(rows.len() as i64)),
        (AggregateFunc::Count, AggregateArg::Column(name)) => {
            let column = column_index(schema, name)?;
            let present = rows.iter().filter(|row| !row[column].is_null()).count();
            Ok(Value::Int(present as i64))
        }
        (func, AggregateArg::Star) => Err(Error::exec(format!(
            "{}(*) is not allowed — {} needs a column",
            func_name(func),
            func_name(func)
        ))),
        (AggregateFunc::Sum, AggregateArg::Column(name)) => {
            sum_values(schema, column_index(schema, name)?, rows)
        }
        (AggregateFunc::Avg, AggregateArg::Column(name)) => {
            avg_values(schema, column_index(schema, name)?, rows)
        }
        (AggregateFunc::Min, AggregateArg::Column(name)) => {
            Ok(extreme(column_index(schema, name)?, rows, Ordering::Less))
        }
        (AggregateFunc::Max, AggregateArg::Column(name)) => Ok(extreme(
            column_index(schema, name)?,
            rows,
            Ordering::Greater,
        )),
    }
}

/// `SUM` over non-null values. Empty or all-null input sums to `NULL`.
fn sum_values(schema: &Schema, column: usize, rows: &[Vec<Value>]) -> Result<Value> {
    match schema.columns[column].ty {
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
            schema.columns[column].name
        ))),
    }
}

/// `AVG` over non-null values, always a `REAL`. Empty input averages to `NULL`.
fn avg_values(schema: &Schema, column: usize, rows: &[Vec<Value>]) -> Result<Value> {
    match schema.columns[column].ty {
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
            schema.columns[column].name
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

    // Resolve every assignment target up front so an unknown column fails
    // before any row is touched.
    let mut resolved = Vec::with_capacity(assignments.len());
    for (name, expr) in &assignments {
        resolved.push((column_index(&schema, name)?, expr));
    }

    let table_tree = BTree::open(schema.root);
    let mut updated = 0u64;
    for (rowid_key, old) in collect_candidates(pager, &schema, &access)? {
        if !passes_filter(filter.as_ref(), &schema, &old)? {
            continue;
        }
        let mut new = old.clone();
        for (column, expr) in &resolved {
            // Assignment expressions see the row's pre-update values.
            let evaluated = eval(
                expr,
                Some(&RowContext {
                    schema: &schema,
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
    let schema = require_table(pager, catalog, &table)?;
    let table_tree = BTree::open(schema.root);

    let mut deleted = 0u64;
    for (rowid_key, values) in collect_candidates(pager, &schema, &access)? {
        if !passes_filter(filter.as_ref(), &schema, &values)? {
            continue;
        }
        index_delete_row(pager, &schema, &rowid_key, &values)?;
        table_tree.delete(pager, &rowid_key)?;
        deleted += 1;
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
fn passes_filter(filter: Option<&Expr>, schema: &Schema, values: &[Value]) -> Result<bool> {
    match filter {
        None => Ok(true),
        Some(expr) => {
            let verdict = eval(expr, Some(&RowContext { schema, values }))?;
            Ok(matches!(verdict, Value::Bool(true)))
        }
    }
}

/// The row a column reference resolves against during evaluation.
struct RowContext<'a> {
    schema: &'a Schema,
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
        Expr::Column(name) => {
            let ctx = context
                .ok_or_else(|| Error::exec(format!("column '{name}' cannot be referenced here")))?;
            let index = column_index(ctx.schema, name)?;
            Ok(ctx.values[index].clone())
        }
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
}
