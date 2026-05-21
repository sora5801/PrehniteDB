//! The executor — it runs a [`Plan`] against the storage engine.
//!
//! Every table is a full scan: v0.1 has no secondary indexes. Expression
//! evaluation follows SQL's three-valued logic, where `NULL` propagates through
//! arithmetic and comparisons and a `WHERE` clause keeps a row only when its
//! predicate evaluates to exactly `TRUE`.

use std::cmp::Ordering;
use std::fmt;

use crate::engine::catalog::Catalog;
use crate::engine::codec;
use crate::engine::planner::Plan;
use crate::engine::schema::{Column, Schema};
use crate::engine::value::{Type, Value};
use crate::error::{Error, Result};
use crate::sql::ast::{BinaryOp, Expr, Projection, UnaryOp};
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
        Plan::Insert {
            table,
            columns,
            rows,
        } => insert(pager, catalog, table, columns, rows),
        Plan::Select {
            table,
            projection,
            filter,
        } => select(pager, catalog, table, projection, filter),
        Plan::Update {
            table,
            assignments,
            filter,
        } => update(pager, catalog, table, assignments, filter),
        Plan::Delete { table, filter } => delete(pager, catalog, table, filter),
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
    };
    catalog.put(pager, &schema)?;
    Ok(QueryResult::Ack(format!("table '{name}' created")))
}

fn drop_table(pager: &mut Pager, catalog: &Catalog, name: String) -> Result<QueryResult> {
    let schema = require_table(pager, catalog, &name)?;
    BTree::open(schema.root).free_all(pager)?;
    catalog.remove(pager, &name)?;
    Ok(QueryResult::Ack(format!("table '{name}' dropped")))
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
        tree.insert(pager, &codec::rowid_key(rowid), &codec::encode_row(&values))?;
        inserted += 1;
    }

    // Persist the advanced rowid counter.
    catalog.put(pager, &schema)?;
    Ok(QueryResult::Ack(format!("{inserted} row(s) inserted")))
}

fn select(
    pager: &mut Pager,
    catalog: &Catalog,
    table: String,
    projection: Projection,
    filter: Option<Expr>,
) -> Result<QueryResult> {
    let schema = require_table(pager, catalog, &table)?;
    let projected: Vec<usize> = match &projection {
        Projection::All => (0..schema.columns.len()).collect(),
        Projection::Columns(names) => {
            let mut indices = Vec::with_capacity(names.len());
            for name in names {
                indices.push(column_index(&schema, name)?);
            }
            indices
        }
    };
    let columns: Vec<String> = projected
        .iter()
        .map(|&i| schema.columns[i].name.clone())
        .collect();

    let tree = BTree::open(schema.root);
    let mut rows = Vec::new();
    for (_rowid, encoded) in tree.scan(pager)? {
        let values = codec::decode_row(&encoded, schema.columns.len())?;
        if !passes_filter(filter.as_ref(), &schema, &values)? {
            continue;
        }
        rows.push(projected.iter().map(|&i| values[i].clone()).collect());
    }
    Ok(QueryResult::Rows { columns, rows })
}

fn update(
    pager: &mut Pager,
    catalog: &Catalog,
    table: String,
    assignments: Vec<(String, Expr)>,
    filter: Option<Expr>,
) -> Result<QueryResult> {
    let schema = require_table(pager, catalog, &table)?;

    // Resolve every assignment target up front so an unknown column fails
    // before any row is touched.
    let mut resolved = Vec::with_capacity(assignments.len());
    for (name, expr) in &assignments {
        resolved.push((column_index(&schema, name)?, expr));
    }

    let tree = BTree::open(schema.root);
    let mut updated = 0u64;
    for (key, encoded) in tree.scan(pager)? {
        let old = codec::decode_row(&encoded, schema.columns.len())?;
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
        tree.insert(pager, &key, &codec::encode_row(&new))?;
        updated += 1;
    }
    Ok(QueryResult::Ack(format!("{updated} row(s) updated")))
}

fn delete(
    pager: &mut Pager,
    catalog: &Catalog,
    table: String,
    filter: Option<Expr>,
) -> Result<QueryResult> {
    let schema = require_table(pager, catalog, &table)?;
    let tree = BTree::open(schema.root);

    let mut doomed = Vec::new();
    for (key, encoded) in tree.scan(pager)? {
        let values = codec::decode_row(&encoded, schema.columns.len())?;
        if passes_filter(filter.as_ref(), &schema, &values)? {
            doomed.push(key);
        }
    }
    for key in &doomed {
        tree.delete(pager, key)?;
    }
    Ok(QueryResult::Ack(format!("{} row(s) deleted", doomed.len())))
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

/// Adapt a value to the type of the column it is about to be stored in. `NULL`
/// fits anywhere, and an integer widens into a `REAL` column; nothing else is
/// converted implicitly.
fn coerce(value: Value, target: Type) -> Result<Value> {
    match (value, target) {
        (Value::Null, _) => Ok(Value::Null),
        (Value::Int(n), Type::Int) => Ok(Value::Int(n)),
        (Value::Int(n), Type::Real) => Ok(Value::Real(n as f64)),
        (Value::Real(r), Type::Real) => Ok(Value::Real(r)),
        (Value::Text(s), Type::Text) => Ok(Value::Text(s)),
        (Value::Bool(b), Type::Bool) => Ok(Value::Bool(b)),
        (other, target) => Err(Error::exec(format!(
            "cannot store {} in a {} column",
            other.type_name(),
            target
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
    fn coercion_rules() {
        assert_eq!(coerce(Value::Int(4), Type::Real).unwrap(), Value::Real(4.0));
        assert!(coerce(Value::Real(4.0), Type::Int).is_err());
        assert_eq!(coerce(Value::Null, Type::Bool).unwrap(), Value::Null);
    }
}
