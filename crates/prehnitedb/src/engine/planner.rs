//! The planner — the bind-and-plan pass between the parser and the executor.
//!
//! It has three jobs. First, **static validation**: catalog-free checks like
//! duplicate column names and mismatched `INSERT` arity, plus lowering parser
//! [`TypeName`](crate::sql::ast::TypeName)s into engine [`Type`]s. Second,
//! **access-path selection**: for a `SELECT`, `UPDATE`, or `DELETE` carrying a
//! `WHERE` clause, it consults the catalog and tries to turn the predicate into
//! a bounded scan over a secondary index. Third, **join reordering**: for a
//! `SELECT` whose `FROM` is a chain of `INNER JOIN`s, it picks a left-deep
//! ordering that minimises a coarse cost estimate over the catalog's row-count
//! statistics.
//!
//! The access-path search classifies each top-level `AND` conjunct as an
//! equality or a range on a single column, then for each index walks its
//! columns left to right: equality predicates extend a pinned key prefix, and
//! the first non-equality column may contribute a single range bound — the
//! standard "leftmost prefix" rule. The planner also notices when the chosen
//! index scan already yields rows in `ORDER BY` order, letting the executor
//! skip the sort. Everything is best-effort: anything unclear falls back to
//! [`AccessPath::FullScan`], and the executor still has the final word.

use std::collections::{HashMap, HashSet};

use crate::engine::catalog::Catalog;
use crate::engine::codec;
use crate::engine::schema::{Column, Index, Schema};
use crate::engine::value::{coerce, Type, Value};
use crate::error::{Error, Result};
use crate::sql::ast::{
    BinaryOp, ColumnRef, Expr, FromClause, Join, JoinKind, OrderKey, Projection, SelectItem,
    Statement,
};
use crate::storage::Pager;

/// How the executor should find the rows a statement operates on.
#[derive(Debug, Clone, PartialEq)]
pub enum AccessPath {
    /// Walk every row of the table.
    FullScan,
    /// Walk a `[lower, upper)` key range of a secondary index. `upper` is
    /// `None` for a scan that runs to the end of the index.
    IndexScan {
        index_root: u32,
        lower: Vec<u8>,
        upper: Option<Vec<u8>>,
    },
}

/// A validated, lowered statement ready for the executor.
#[derive(Debug, Clone, PartialEq)]
pub enum Plan {
    CreateTable {
        name: String,
        columns: Vec<Column>,
    },
    DropTable {
        name: String,
    },
    CreateIndex {
        name: String,
        table: String,
        columns: Vec<String>,
    },
    DropIndex {
        name: String,
    },
    Insert {
        table: String,
        /// Target columns, or `None` for "every column in order".
        columns: Option<Vec<String>>,
        rows: Vec<Vec<Expr>>,
    },
    Select {
        /// The `FROM` clause — the base table and any joins.
        from: FromClause,
        projection: Projection,
        filter: Option<Expr>,
        /// Access path for the base table. Joined tables are always full-scanned.
        access: AccessPath,
        group_by: Vec<ColumnRef>,
        having: Option<Expr>,
        order_by: Vec<OrderKey>,
        /// True when `access` already yields rows in `order_by` order, so the
        /// executor need not sort. Always false for a grouped or joined query.
        presorted: bool,
        /// `LIMIT` / `OFFSET` row bounds, carried through from the statement.
        limit: Option<u64>,
        offset: Option<u64>,
    },
    Update {
        table: String,
        assignments: Vec<(String, Expr)>,
        filter: Option<Expr>,
        access: AccessPath,
    },
    Delete {
        table: String,
        filter: Option<Expr>,
        access: AccessPath,
    },
    /// `VACUUM` — rebuild the database file compactly. Handled by `Database`
    /// itself, since it must replace the pager's contents wholesale.
    Vacuum,
    /// `EXPLAIN <select>` — describe the plan tree without executing
    /// it. The executor's EXPLAIN path walks the inner Plan and
    /// produces a text result instead of running it.
    Explain(Box<Plan>),
}

/// Lower, validate, and plan one statement.
pub fn plan(statement: Statement, pager: &mut Pager, catalog: &Catalog) -> Result<Plan> {
    match statement {
        Statement::CreateTable { name, columns } => {
            let mut seen = HashSet::new();
            let mut lowered = Vec::with_capacity(columns.len());
            for column in columns {
                if !seen.insert(column.name.clone()) {
                    return Err(Error::exec(format!(
                        "table '{name}' declares column '{}' twice",
                        column.name
                    )));
                }
                lowered.push(Column {
                    name: column.name,
                    ty: Type::from(column.ty),
                });
            }
            Ok(Plan::CreateTable {
                name,
                columns: lowered,
            })
        }

        Statement::DropTable { name } => Ok(Plan::DropTable { name }),

        Statement::CreateIndex {
            name,
            table,
            columns,
        } => Ok(Plan::CreateIndex {
            name,
            table,
            columns,
        }),

        Statement::DropIndex { name } => Ok(Plan::DropIndex { name }),

        Statement::Insert {
            table,
            columns,
            rows,
        } => {
            if let Some(list) = &columns {
                let mut seen = HashSet::new();
                for column in list {
                    if !seen.insert(column) {
                        return Err(Error::exec(format!("INSERT lists column '{column}' twice")));
                    }
                }
                for (i, row) in rows.iter().enumerate() {
                    if row.len() != list.len() {
                        return Err(Error::exec(format!(
                            "INSERT row {} supplies {} value(s) but {} column(s) were listed",
                            i + 1,
                            row.len(),
                            list.len()
                        )));
                    }
                }
            }
            Ok(Plan::Insert {
                table,
                columns,
                rows,
            })
        }

        Statement::Select {
            from,
            projection,
            filter,
            group_by,
            having,
            order_by,
            limit,
            offset,
        } => {
            // v0.34/v0.37 — rewrite top-level `EXISTS (...)` /
            // `NOT EXISTS (...)` and `expr IN (...)` patterns in the
            // WHERE clause into semi-joins on `from`. The remaining
            // (non-rewritten) conjuncts stay in `filter`. Done before
            // join reordering so the new joins participate in cost
            // estimation.
            let (from, filter) = rewrite_subquery_joins(from, filter, pager, catalog)?;
            // Cost-based reorder of an INNER-join chain, when one is present.
            // Leaves single-table and LEFT/CROSS-bearing FROMs untouched.
            let from = reorder_inner_chain(from, pager, catalog)?;
            // Index access-path selection is single-table only; a joined query
            // full-scans every table.
            let (access, presorted) = if from.joins.is_empty() {
                choose_access(pager, catalog, &from.table.name, filter.as_ref(), &order_by)?
            } else {
                (AccessPath::FullScan, false)
            };
            // A grouped query's rows are groups, not table rows, so an index's
            // row order cannot satisfy ORDER BY.
            let presorted = presorted && group_by.is_empty();
            Ok(Plan::Select {
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
            })
        }

        Statement::Update {
            table,
            assignments,
            filter,
        } => {
            let (access, _) = choose_access(pager, catalog, &table, filter.as_ref(), &[])?;
            Ok(Plan::Update {
                table,
                assignments,
                filter,
                access,
            })
        }

        Statement::Delete { table, filter } => {
            let (access, _) = choose_access(pager, catalog, &table, filter.as_ref(), &[])?;
            Ok(Plan::Delete {
                table,
                filter,
                access,
            })
        }

        Statement::Vacuum => Ok(Plan::Vacuum),

        Statement::Explain(inner) => {
            // Plan the inner statement so EXPLAIN reports exactly
            // what the planner *would* hand to the executor. The
            // executor's EXPLAIN path then walks the Plan and
            // produces a text description instead of running it.
            let inner_plan = plan(*inner, pager, catalog)?;
            Ok(Plan::Explain(Box::new(inner_plan)))
        }

        Statement::Begin | Statement::Commit | Statement::Rollback => {
            unreachable!("transaction control is handled before planning")
        }
    }
}

/// Pick an access path and report whether it already satisfies `order_by`.
fn choose_access(
    pager: &mut Pager,
    catalog: &Catalog,
    table: &str,
    filter: Option<&Expr>,
    order_by: &[OrderKey],
) -> Result<(AccessPath, bool)> {
    let Some(filter) = filter else {
        return Ok((AccessPath::FullScan, false));
    };
    let Some(schema) = catalog.get(pager, table)? else {
        // Unknown table — leave the real error to the executor.
        return Ok((AccessPath::FullScan, false));
    };
    if schema.indexes.is_empty() {
        return Ok((AccessPath::FullScan, false));
    }

    let mut conjuncts = Vec::new();
    collect_conjuncts(filter, &mut conjuncts);
    let predicates: Vec<ColumnPredicate> = conjuncts
        .iter()
        .filter_map(|conjunct| classify(conjunct, &schema))
        .collect();

    // Among usable indexes, prefer the one pinning the most columns; break a
    // tie by whether it also contributes a range bound.
    let mut best: Option<(&Index, IndexPlan)> = None;
    for index in &schema.indexes {
        let Some(plan) = build_index_scan(index, &predicates) else {
            continue;
        };
        let improves = match &best {
            None => true,
            Some((_, current)) => {
                (plan.pinned, plan.has_range) > (current.pinned, current.has_range)
            }
        };
        if improves {
            best = Some((index, plan));
        }
    }

    match best {
        Some((index, plan)) => {
            let presorted = order_matches(index, plan.pinned, &schema, order_by);
            let access = AccessPath::IndexScan {
                index_root: index.root,
                lower: plan.lower,
                upper: plan.upper,
            };
            Ok((access, presorted))
        }
        None => Ok((AccessPath::FullScan, false)),
    }
}

/// Whether scanning `index` in key order already yields rows in `order_by`
/// order. The leading `pinned` columns are equality-constrained — hence
/// constant across the results — so the effective sort is by the columns after
/// them; the (all-ascending) `ORDER BY` keys must form a prefix of those.
fn order_matches(index: &Index, pinned: usize, schema: &Schema, order_by: &[OrderKey]) -> bool {
    if order_by.is_empty() || order_by.iter().any(|key| key.descending) {
        return false;
    }
    let effective = &index.columns[pinned.min(index.columns.len())..];
    order_by.len() <= effective.len()
        && order_by.iter().zip(effective).all(|(key, &column)| {
            schema
                .columns
                .get(column)
                .is_some_and(|c| c.name == key.column.name)
        })
}

/// A single-column predicate that can be matched against an index column.
struct ColumnPredicate {
    column: usize,
    kind: PredKind,
}

enum PredKind {
    Eq(Value),
    /// `col > value` (`inclusive` false) or `col >= value` (`inclusive` true).
    Lower {
        value: Value,
        inclusive: bool,
    },
    /// `col < value` (`inclusive` false) or `col <= value` (`inclusive` true).
    Upper {
        value: Value,
        inclusive: bool,
    },
}

/// A candidate index scan, before it is turned into an [`AccessPath`].
struct IndexPlan {
    /// Equality-pinned leading columns — the primary selectivity signal.
    pinned: usize,
    /// Whether a trailing range bound was applied.
    has_range: bool,
    lower: Vec<u8>,
    upper: Option<Vec<u8>>,
}

/// Classify one conjunct as a single-column equality or range predicate.
fn classify(expr: &Expr, schema: &Schema) -> Option<ColumnPredicate> {
    let Expr::Binary { op, left, right } = expr else {
        return None;
    };
    // Orient the comparison so the column sits on the left.
    let (colref, literal, op) = match (left.as_ref(), right.as_ref()) {
        (Expr::Column(colref), other) => (colref, literal_value(other)?, *op),
        (other, Expr::Column(colref)) => (colref, literal_value(other)?, flip_op(*op)),
        _ => return None,
    };
    let column = schema.column_index(&colref.name)?;
    // Index keys hold values coerced to the column type; coerce the literal the
    // same way. A NULL never matches via a comparison.
    let value = coerce(literal, schema.columns[column].ty).ok()?;
    if value.is_null() {
        return None;
    }
    let kind = match op {
        BinaryOp::Eq => PredKind::Eq(value),
        BinaryOp::Gt => PredKind::Lower {
            value,
            inclusive: false,
        },
        BinaryOp::GtEq => PredKind::Lower {
            value,
            inclusive: true,
        },
        BinaryOp::Lt => PredKind::Upper {
            value,
            inclusive: false,
        },
        BinaryOp::LtEq => PredKind::Upper {
            value,
            inclusive: true,
        },
        _ => return None,
    };
    Some(ColumnPredicate { column, kind })
}

/// Mirror a comparison operator, for when the column is on the right.
fn flip_op(op: BinaryOp) -> BinaryOp {
    match op {
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::LtEq => BinaryOp::GtEq,
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::GtEq => BinaryOp::LtEq,
        other => other,
    }
}

/// Try to build a bounded scan of `index` from the available predicates, by
/// the leftmost-prefix rule.
fn build_index_scan(index: &Index, predicates: &[ColumnPredicate]) -> Option<IndexPlan> {
    let mut prefix = Vec::new(); // concatenated encodings of equality-pinned columns
    let mut pinned = 0usize;
    let mut lower_bound: Option<(Value, bool)> = None;
    let mut upper_bound: Option<(Value, bool)> = None;
    let mut has_range = false;

    for &column in &index.columns {
        // An equality on this column extends the pinned prefix.
        if let Some(value) = predicates.iter().find_map(|p| match &p.kind {
            PredKind::Eq(value) if p.column == column => Some(value),
            _ => None,
        }) {
            prefix.extend_from_slice(&codec::encode_index_value(value));
            pinned += 1;
            continue;
        }
        // Otherwise this column ends the prefix; it may carry a range bound.
        lower_bound = predicates.iter().find_map(|p| match &p.kind {
            PredKind::Lower { value, inclusive } if p.column == column => {
                Some((value.clone(), *inclusive))
            }
            _ => None,
        });
        upper_bound = predicates.iter().find_map(|p| match &p.kind {
            PredKind::Upper { value, inclusive } if p.column == column => {
                Some((value.clone(), *inclusive))
            }
            _ => None,
        });
        has_range = lower_bound.is_some() || upper_bound.is_some();
        break;
    }

    if pinned == 0 && !has_range {
        return None;
    }

    // Lower key: the pinned prefix, then the trailing range's lower bound.
    let mut lower = prefix.clone();
    if let Some((value, inclusive)) = lower_bound {
        let encoded = codec::encode_index_value(&value);
        if inclusive {
            lower.extend_from_slice(&encoded);
        } else {
            // `col > v` — step past every key whose column equals v.
            lower.extend_from_slice(&codec::prefix_upper_bound(&encoded)?);
        }
    }

    // Upper key.
    let upper = if let Some((value, inclusive)) = upper_bound {
        let encoded = codec::encode_index_value(&value);
        let mut bound = prefix.clone();
        if inclusive {
            bound.extend_from_slice(&codec::prefix_upper_bound(&encoded)?);
        } else {
            bound.extend_from_slice(&encoded);
        }
        Some(bound)
    } else if pinned > 0 {
        // A pure equality prefix: bound the group of keys sharing it.
        codec::prefix_upper_bound(&prefix)
    } else {
        // A lower-only range on the leading column: run to the end.
        None
    };

    Some(IndexPlan {
        pinned,
        has_range,
        lower,
        upper,
    })
}

/// Flatten the top-level `AND` conjuncts of a predicate. A predicate joined by
/// `OR` is opaque — the whole thing becomes a single conjunct.
fn collect_conjuncts<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    match expr {
        Expr::Binary {
            op: BinaryOp::And,
            left,
            right,
        } => {
            collect_conjuncts(left, out);
            collect_conjuncts(right, out);
        }
        other => out.push(other),
    }
}

/// A literal expression as a [`Value`]. Compound and `NULL` expressions return
/// `None`: neither can serve as an index bound.
fn literal_value(expr: &Expr) -> Option<Value> {
    match expr {
        Expr::Integer(n) => Some(Value::Int(*n)),
        Expr::Real(r) => Some(Value::Real(*r)),
        Expr::Str(s) => Some(Value::Text(s.clone())),
        Expr::Bool(b) => Some(Value::Bool(*b)),
        _ => None,
    }
}

// --- INNER-join reorder ----------------------------------------------------
//
// SQL's `INNER JOIN` is commutative and associative, so the order in which a
// chain is evaluated is the planner's call. The executor always builds joins
// left-deep (the running result feeds the left side of the next join), so the
// question is which table to start with and how to add the others.
//
// The heuristic is intentionally simple: enumerate every ordering (at most 8!
// = 40320), score each by the sum of intermediate row-count estimates, and
// pick the cheapest. The intermediate for a step where the predicates connect
// the new table to the existing set is `max(prev, new_table_rows)`; a step
// with no connecting predicate is a cross product, scored `prev * new`.
//
// ON predicates ride along: each one re-attaches to the earliest join step
// where every table it mentions is in the joined set. A step that no
// predicate lands on becomes `ON TRUE` — semantically a cross product, but
// kept INNER so the executor's join planner sees it.

/// Rewrite the top-level `WHERE` clause: every conjunct that is a
/// correlated `EXISTS (simple subquery)` or `NOT EXISTS (simple
/// subquery)` becomes a semi-join (or anti-join) appended to `from`,
/// and that conjunct drops out of the filter. The subquery's own
/// `WHERE` becomes the join's `ON` predicate, so an outer reference
/// inside it now resolves against the combined scope at execution.
///
/// "Simple" here means: the subquery is a `SELECT` over a single
/// table, with a non-empty `WHERE`, no `GROUP BY` / `HAVING` /
/// aggregate / `ORDER BY` / `LIMIT`. Anything more elaborate keeps
/// the per-row evaluation path v0.31 built — correctness is the same,
/// the throughput just isn't.
fn rewrite_subquery_joins(
    mut from: FromClause,
    filter: Option<Expr>,
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<(FromClause, Option<Expr>)> {
    let Some(predicate) = filter else {
        return Ok((from, None));
    };
    // Flatten the top-level AND chain so we can inspect each conjunct.
    let mut conjuncts: Vec<Expr> = Vec::new();
    flatten_and(predicate, &mut conjuncts);
    let mut leftover: Vec<Expr> = Vec::with_capacity(conjuncts.len());
    for conjunct in conjuncts {
        // Try the v0.34 EXISTS extractor, then the v0.37 IN extractor.
        // First match wins; nothing else takes that conjunct.
        if let Some(join) = try_extract_exists_join(&conjunct, &from, pager, catalog)? {
            from.joins.push(join);
        } else if let Some(join) = try_extract_in_join(&conjunct, &from, pager, catalog)? {
            from.joins.push(join);
        } else {
            leftover.push(conjunct);
        }
    }
    let new_filter = and_chain_opt(leftover);
    Ok((from, new_filter))
}

/// Walk an AND-chained expression into its leaves. `x AND y AND z`
/// returns `[x, y, z]`.
fn flatten_and(expr: Expr, out: &mut Vec<Expr>) {
    match expr {
        Expr::Binary {
            op: BinaryOp::And,
            left,
            right,
        } => {
            flatten_and(*left, out);
            flatten_and(*right, out);
        }
        other => out.push(other),
    }
}

/// Rebuild an AND chain from `parts`, left-associatively. Returns
/// `None` for an empty input (no remaining filter), `Some(expr)`
/// otherwise.
fn and_chain_opt(mut parts: Vec<Expr>) -> Option<Expr> {
    if parts.is_empty() {
        return None;
    }
    let mut acc = parts.remove(0);
    for next in parts {
        acc = Expr::Binary {
            op: BinaryOp::And,
            left: Box::new(acc),
            right: Box::new(next),
        };
    }
    Some(acc)
}

/// If `expr` is a top-level `EXISTS (subquery)` or `NOT EXISTS
/// (subquery)` whose subquery has the simple shape described on
/// [`rewrite_exists_to_semi_joins`], return the corresponding
/// semi/anti-join. Otherwise `None`.
fn try_extract_exists_join(
    expr: &Expr,
    outer_from: &FromClause,
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<Option<Join>> {
    let (negated, subquery) = match expr {
        Expr::Exists(stmt) => (false, stmt.as_ref()),
        Expr::Unary {
            op: crate::sql::ast::UnaryOp::Not,
            expr,
        } => match expr.as_ref() {
            Expr::Exists(stmt) => (true, stmt.as_ref()),
            _ => return Ok(None),
        },
        _ => return Ok(None),
    };
    let Statement::Select {
        from: inner_from,
        projection: _,
        filter: Some(inner_filter),
        group_by,
        having,
        order_by,
        limit,
        offset,
    } = subquery
    else {
        return Ok(None);
    };
    // The subquery must be a plain SELECT over one table with no
    // grouping / sorting / paging.
    if !inner_from.joins.is_empty()
        || !group_by.is_empty()
        || having.is_some()
        || !order_by.is_empty()
        || limit.is_some()
        || offset.is_some()
    {
        return Ok(None);
    }
    // The inner table must exist (the executor would otherwise have
    // surfaced the error; safer to skip the rewrite than mislead).
    if catalog.get(pager, &inner_from.table.name)?.is_none() {
        return Ok(None);
    }
    // The outer FROM must not already reuse the inner table's
    // qualifier — otherwise the new join would collide on scope
    // names.
    let inner_qualifier = inner_from.table.qualifier();
    if outer_uses_qualifier(outer_from, inner_qualifier) {
        return Ok(None);
    }
    Ok(Some(Join {
        kind: if negated {
            JoinKind::Anti
        } else {
            JoinKind::Semi
        },
        table: inner_from.table.clone(),
        on: Some(inner_filter.clone()),
    }))
}

/// If `expr` is a top-level `outer_expr IN (subquery)` whose subquery
/// has the simple shape (single table, single column projection,
/// non-empty `WHERE`, no `GROUP BY` / `HAVING` / `ORDER BY` /
/// `LIMIT` / sub-joins), return a `Semi` join whose `ON` clause is
/// the subquery's `WHERE` AND-ed with `outer_expr = inner_projection`.
///
/// Otherwise `None`. `NOT IN` is intentionally skipped — SQL's
/// three-valued `NOT IN` is `NULL` (not `TRUE`) when the set
/// contains a `NULL`, so an anti-join rewrite would be wrong unless
/// the inner projection is provably non-nullable. v0.37 leaves that
/// case to v0.31's per-row evaluation.
fn try_extract_in_join(
    expr: &Expr,
    outer_from: &FromClause,
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<Option<Join>> {
    let Expr::InSubquery {
        expr: outer_expr,
        subquery,
        negated,
    } = expr
    else {
        return Ok(None);
    };
    if *negated {
        // NOT IN — needs NULL-safety analysis we don't have.
        return Ok(None);
    }
    let Statement::Select {
        from: inner_from,
        projection,
        filter: Some(inner_filter),
        group_by,
        having,
        order_by,
        limit,
        offset,
    } = subquery.as_ref()
    else {
        return Ok(None);
    };
    // The subquery must be a plain SELECT over one table, with no
    // grouping / sorting / paging.
    if !inner_from.joins.is_empty()
        || !group_by.is_empty()
        || having.is_some()
        || !order_by.is_empty()
        || limit.is_some()
        || offset.is_some()
    {
        return Ok(None);
    }
    // The IN subquery returns exactly one column. Extract a simple
    // column reference for that projection — an expression-shaped
    // item (`SELECT a + 1 FROM t`) isn't worth rewriting because we'd
    // have to plumb the expression through the join's ON; future
    // work could lift this.
    let inner_colref = match projection {
        Projection::Items(items) if items.len() == 1 => match &items[0] {
            SelectItem::Column(colref) => colref.clone(),
            _ => return Ok(None),
        },
        _ => return Ok(None),
    };
    if catalog.get(pager, &inner_from.table.name)?.is_none() {
        return Ok(None);
    }
    let inner_qualifier = inner_from.table.qualifier();
    if outer_uses_qualifier(outer_from, inner_qualifier) {
        return Ok(None);
    }
    // Qualify the inner column with the inner table's qualifier if
    // the subquery wrote it bare — the join's combined scope needs
    // the qualifier to resolve the reference unambiguously when the
    // outer query also has a column of the same name.
    let qualified_inner = ColumnRef {
        table: Some(
            inner_colref
                .table
                .unwrap_or_else(|| inner_qualifier.to_string()),
        ),
        name: inner_colref.name,
    };
    // The outer expression is evaluated against the outer scope today;
    // after the rewrite it lives inside the join's ON clause, which
    // sees both outer *and* inner columns. A bare column reference on
    // the outer side that happens to share a name with an inner
    // column is suddenly ambiguous. v0.37 handles the common case —
    // outer expression is a bare or already-qualified column ref —
    // by re-qualifying with the *first outer table* (the base of
    // outer_from). More elaborate outer expressions (arithmetic,
    // calls, sub-expressions) stay on the per-row path.
    let outer_expr_qualified = match outer_expr.as_ref() {
        Expr::Column(colref) => {
            let outer_qualifier = outer_from.table.qualifier().to_string();
            let table = colref.table.clone().unwrap_or(outer_qualifier);
            Box::new(Expr::Column(ColumnRef {
                table: Some(table),
                name: colref.name.clone(),
            }))
        }
        // Anything more complex than a bare column — skip the rewrite
        // rather than risk a wrong qualification. The v0.31 per-row
        // path still produces the right answer.
        _ => return Ok(None),
    };
    // ON = (subquery's WHERE) AND (outer_expr = inner_projection).
    let equi = Expr::Binary {
        op: BinaryOp::Eq,
        left: outer_expr_qualified,
        right: Box::new(Expr::Column(qualified_inner)),
    };
    let combined_on = Expr::Binary {
        op: BinaryOp::And,
        left: Box::new(inner_filter.clone()),
        right: Box::new(equi),
    };
    Ok(Some(Join {
        kind: JoinKind::Semi,
        table: inner_from.table.clone(),
        on: Some(combined_on),
    }))
}

/// Whether any table in `from` (base or join) uses `qualifier` for
/// its name or alias.
fn outer_uses_qualifier(from: &FromClause, qualifier: &str) -> bool {
    if from.table.qualifier() == qualifier {
        return true;
    }
    from.joins.iter().any(|j| j.table.qualifier() == qualifier)
}

/// Cap on tables in an enumerated chain. 8! permutations is ~40k — cheap
/// enough at plan time; beyond that the user's order is left alone.
const REORDER_CAP: usize = 8;

/// If `from` is an INNER-only chain of at most [`REORDER_CAP`] tables, return a
/// reordered equivalent that the cost heuristic prefers. Otherwise return
/// `from` unchanged. The fallback is silent on purpose: a chain whose
/// predicates use unresolvable column names, or whose tables are missing from
/// the catalog, just keeps the order the user wrote.
fn reorder_inner_chain(
    from: FromClause,
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<FromClause> {
    if from.joins.is_empty() {
        return Ok(from);
    }
    if from.joins.iter().any(|j| j.kind != JoinKind::Inner) {
        return Ok(from);
    }
    let n = 1 + from.joins.len();
    if n > REORDER_CAP {
        return Ok(from);
    }

    // Gather one entry per chained table.
    let mut tables: Vec<crate::sql::ast::TableRef> = Vec::with_capacity(n);
    let mut row_counts: Vec<u64> = Vec::with_capacity(n);
    let mut qual_to_idx: HashMap<String, usize> = HashMap::new();
    // column name → indices of tables holding it. A multi-table entry marks
    // ambiguity, which forces the analyzer to bail on bare references.
    let mut col_to_tables: HashMap<String, Vec<usize>> = HashMap::new();

    let mut push_table = |table_ref: &crate::sql::ast::TableRef,
                          tables: &mut Vec<crate::sql::ast::TableRef>,
                          row_counts: &mut Vec<u64>,
                          qual_to_idx: &mut HashMap<String, usize>,
                          col_to_tables: &mut HashMap<String, Vec<usize>>|
     -> Result<bool> {
        let idx = tables.len();
        let qualifier = table_ref.qualifier().to_string();
        if qual_to_idx.insert(qualifier, idx).is_some() {
            // Duplicate qualifier — let the executor produce the real error.
            return Ok(false);
        }
        let schema = catalog.get(pager, &table_ref.name)?;
        let rows = schema.as_ref().map(|s| s.row_count).unwrap_or(0);
        row_counts.push(rows);
        if let Some(schema) = &schema {
            for column in &schema.columns {
                col_to_tables
                    .entry(column.name.clone())
                    .or_default()
                    .push(idx);
            }
        }
        tables.push(table_ref.clone());
        Ok(true)
    };

    if !push_table(
        &from.table,
        &mut tables,
        &mut row_counts,
        &mut qual_to_idx,
        &mut col_to_tables,
    )? {
        return Ok(from);
    }
    for join in &from.joins {
        if !push_table(
            &join.table,
            &mut tables,
            &mut row_counts,
            &mut qual_to_idx,
            &mut col_to_tables,
        )? {
            return Ok(from);
        }
    }

    // Lift the ON expressions out (one per INNER join). An INNER without an ON
    // shouldn't reach here, but if it did we'd punt rather than guess.
    let mut predicates: Vec<Expr> = Vec::with_capacity(from.joins.len());
    for join in &from.joins {
        match &join.on {
            Some(expr) => predicates.push(expr.clone()),
            None => return Ok(from),
        }
    }

    // Each predicate becomes a bitmask of the tables it references; bail on
    // an unresolvable reference rather than risk misplacing the predicate.
    let mut pred_refs: Vec<u32> = Vec::with_capacity(predicates.len());
    for predicate in &predicates {
        let mut refs: u32 = 0;
        let mut bail = false;
        collect_predicate_refs(
            predicate,
            &qual_to_idx,
            &col_to_tables,
            &mut refs,
            &mut bail,
        );
        if bail {
            return Ok(from);
        }
        pred_refs.push(refs);
    }

    // Enumerate orderings and pick the cheapest. The identity ordering is
    // enumerated first; ties prefer it because the comparison is strict.
    let mut indices: Vec<usize> = (0..n).collect();
    let mut best_order: Vec<usize> = indices.clone();
    let mut best_cost: u128 = u128::MAX;
    permute(&mut indices, 0, &mut |ord| {
        let cost = score_ordering(ord, &row_counts, &pred_refs);
        if cost < best_cost {
            best_cost = cost;
            best_order = ord.to_vec();
        }
    });

    if best_order == (0..n).collect::<Vec<_>>() {
        // The user's order was already (one of) the cheapest.
        return Ok(from);
    }

    let assignments = assign_predicates(&best_order, &pred_refs);
    let mut new_from = FromClause {
        table: tables[best_order[0]].clone(),
        joins: Vec::with_capacity(n - 1),
    };
    for step in 1..n {
        let here: Vec<Expr> = assignments[step - 1]
            .iter()
            .map(|&pi| predicates[pi].clone())
            .collect();
        let on = if here.is_empty() {
            Expr::Bool(true)
        } else {
            and_all(here)
        };
        new_from.joins.push(Join {
            kind: JoinKind::Inner,
            table: tables[best_order[step]].clone(),
            on: Some(on),
        });
    }
    Ok(new_from)
}

/// Sum of intermediate row counts over a left-deep build of `ord`. A step
/// whose new table has no predicate tying it to the rows already joined is
/// scored as a cross product (`prev * new`) — much worse than `max(prev, new)`
/// — so connected orderings naturally outscore disconnected ones.
fn score_ordering(ord: &[usize], sizes: &[u64], pred_refs: &[u32]) -> u128 {
    // `max(1, _)`: an empty table would otherwise zero out a product step and
    // wrongly crown a cross-product plan as free.
    let size_of = |i: usize| -> u128 { sizes[i].max(1) as u128 };
    let mut joined: u32 = 1u32 << ord[0];
    let mut intermediate: u128 = size_of(ord[0]);
    let mut cost: u128 = 0;
    for &new in ord.iter().skip(1) {
        let new_mask = 1u32 << new;
        let connected = pred_refs.iter().any(|&refs| {
            refs != 0
                && refs & joined != 0
                && refs & new_mask != 0
                && refs & !(joined | new_mask) == 0
        });
        intermediate = if connected {
            intermediate.max(size_of(new))
        } else {
            intermediate.saturating_mul(size_of(new))
        };
        cost = cost.saturating_add(intermediate);
        joined |= new_mask;
    }
    cost
}

/// For the chosen ordering, route each predicate to the earliest join step
/// where every table it references is in the joined set. Predicates with no
/// references (a constant `ON TRUE`-style expression) attach to step 1.
fn assign_predicates(ord: &[usize], pred_refs: &[u32]) -> Vec<Vec<usize>> {
    let mut assignments: Vec<Vec<usize>> = vec![Vec::new(); ord.len() - 1];
    let mut placed = vec![false; pred_refs.len()];
    let mut joined: u32 = 1u32 << ord[0];
    for step in 1..ord.len() {
        joined |= 1u32 << ord[step];
        for (i, &refs) in pred_refs.iter().enumerate() {
            if placed[i] {
                continue;
            }
            if refs & !joined == 0 {
                assignments[step - 1].push(i);
                placed[i] = true;
            }
        }
    }
    assignments
}

/// Visit every permutation of `items`, leaving `items[..k]` fixed. The first
/// permutation visited is `items` as given, so a strict best-cost compare
/// lets ties go to the input order.
fn permute<F: FnMut(&[usize])>(items: &mut [usize], k: usize, f: &mut F) {
    if k == items.len() {
        f(items);
        return;
    }
    for i in k..items.len() {
        items.swap(k, i);
        permute(items, k + 1, f);
        items.swap(k, i);
    }
}

/// Build the bitmask of chained tables a predicate touches. `bail` flips true
/// for an expression the analyzer cannot resolve cleanly — a column with an
/// unknown qualifier, an ambiguous bare reference, or an aggregate in an `ON`.
fn collect_predicate_refs(
    expr: &Expr,
    qual_to_idx: &HashMap<String, usize>,
    col_to_tables: &HashMap<String, Vec<usize>>,
    refs: &mut u32,
    bail: &mut bool,
) {
    if *bail {
        return;
    }
    match expr {
        Expr::Null | Expr::Integer(_) | Expr::Real(_) | Expr::Str(_) | Expr::Bool(_) => {}
        Expr::Column(colref) => match &colref.table {
            Some(qual) => match qual_to_idx.get(qual) {
                Some(&idx) => *refs |= 1 << idx,
                None => *bail = true,
            },
            None => match col_to_tables.get(&colref.name) {
                Some(owners) if owners.len() == 1 => *refs |= 1 << owners[0],
                _ => *bail = true,
            },
        },
        Expr::Aggregate(_) => *bail = true,
        Expr::Unary { expr, .. } => {
            collect_predicate_refs(expr, qual_to_idx, col_to_tables, refs, bail)
        }
        Expr::Binary { left, right, .. } => {
            collect_predicate_refs(left, qual_to_idx, col_to_tables, refs, bail);
            collect_predicate_refs(right, qual_to_idx, col_to_tables, refs, bail);
        }
        Expr::IsNull { expr, .. } => {
            collect_predicate_refs(expr, qual_to_idx, col_to_tables, refs, bail)
        }
        // A subquery is opaque to the reorder analyzer: the predicate refs
        // come from the *outer* expression only, and a subquery's inner refs
        // belong to its own scope. We do, however, recurse into the LHS so
        // an `outer.col IN (subquery)` still records `outer.col`.
        Expr::InSubquery { expr, .. }
        | Expr::CorrelatedInSubquery { expr, .. }
        | Expr::InList { expr, .. } => {
            collect_predicate_refs(expr, qual_to_idx, col_to_tables, refs, bail)
        }
        Expr::Exists(_)
        | Expr::ScalarSubquery(_)
        | Expr::CorrelatedExists(_)
        | Expr::CorrelatedScalarSubquery(_) => {
            // Bail out: a correlated subquery references outer columns we
            // can't analyse without re-implementing the substitution
            // here. The reorder is best-effort; falling back to the
            // user's order is always correct.
            *bail = true;
        }
    }
}

/// AND a non-empty list of predicates together, left-associatively.
fn and_all(mut preds: Vec<Expr>) -> Expr {
    let mut acc = preds.remove(0);
    for next in preds {
        acc = Expr::Binary {
            op: BinaryOp::And,
            left: Box::new(acc),
            right: Box::new(next),
        };
    }
    acc
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parse;
    use crate::storage::pager::wal_path;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempPath(PathBuf);

    impl Drop for TempPath {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
            let _ = std::fs::remove_file(wal_path(&self.0));
        }
    }

    /// A fresh pager + catalog. The tuple drops `catalog`, then `pager`
    /// (closing the file), then `TempPath` (deleting it) — in that order.
    fn fixture() -> (TempPath, Pager, Catalog) {
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        let path =
            std::env::temp_dir().join(format!("prehnite-planner-{}-{n}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(wal_path(&path));
        let mut pager = Pager::open(&path).unwrap();
        let catalog = Catalog::open(&mut pager).unwrap();
        (TempPath(path), pager, catalog)
    }

    fn plan_sql(pager: &mut Pager, catalog: &Catalog, sql: &str) -> Result<Plan> {
        plan(parse(sql).unwrap(), pager, catalog)
    }

    /// A `users(id INT, email TEXT)` table carrying the given indexes.
    fn users_schema(indexes: Vec<Index>) -> Schema {
        Schema {
            name: "users".into(),
            columns: vec![
                Column {
                    name: "id".into(),
                    ty: Type::Int,
                },
                Column {
                    name: "email".into(),
                    ty: Type::Text,
                },
            ],
            root: 5,
            next_rowid: 1,
            row_count: 0,
            indexes,
        }
    }

    fn access_of(plan: Plan) -> AccessPath {
        match plan {
            Plan::Select { access, .. } => access,
            other => panic!("expected a SELECT plan, got {other:?}"),
        }
    }

    #[test]
    fn rejects_duplicate_create_columns() {
        let (_tmp, mut pager, catalog) = fixture();
        assert!(plan_sql(&mut pager, &catalog, "CREATE TABLE t (a INT, a TEXT)").is_err());
    }

    #[test]
    fn rejects_mismatched_insert_arity() {
        let (_tmp, mut pager, catalog) = fixture();
        assert!(plan_sql(&mut pager, &catalog, "INSERT INTO t (a, b) VALUES (1)").is_err());
        assert!(plan_sql(
            &mut pager,
            &catalog,
            "INSERT INTO t (a, b) VALUES (1, 2), (3)"
        )
        .is_err());
    }

    #[test]
    fn full_scan_when_no_index_exists() {
        let (_tmp, mut pager, catalog) = fixture();
        catalog.put(&mut pager, &users_schema(vec![])).unwrap();
        let access = access_of(
            plan_sql(
                &mut pager,
                &catalog,
                "SELECT * FROM users WHERE email = 'a@b.com'",
            )
            .unwrap(),
        );
        assert_eq!(access, AccessPath::FullScan);
    }

    #[test]
    fn equality_predicate_drives_an_index_lookup() {
        let (_tmp, mut pager, catalog) = fixture();
        catalog
            .put(
                &mut pager,
                &users_schema(vec![Index {
                    name: "by_email".into(),
                    columns: vec![1],
                    root: 777,
                }]),
            )
            .unwrap();
        let access = access_of(
            plan_sql(
                &mut pager,
                &catalog,
                "SELECT id FROM users WHERE email = 'a@b.com' AND id > 0",
            )
            .unwrap(),
        );
        let prefix = codec::encode_index_value(&Value::Text("a@b.com".into()));
        assert_eq!(
            access,
            AccessPath::IndexScan {
                index_root: 777,
                lower: prefix.clone(),
                upper: codec::prefix_upper_bound(&prefix),
            }
        );
    }

    #[test]
    fn range_predicate_drives_an_index_scan() {
        let (_tmp, mut pager, catalog) = fixture();
        catalog
            .put(
                &mut pager,
                &users_schema(vec![Index {
                    name: "by_id".into(),
                    columns: vec![0],
                    root: 90,
                }]),
            )
            .unwrap();
        let access = access_of(
            plan_sql(&mut pager, &catalog, "SELECT * FROM users WHERE id >= 10").unwrap(),
        );
        assert_eq!(
            access,
            AccessPath::IndexScan {
                index_root: 90,
                lower: codec::encode_index_value(&Value::Int(10)),
                upper: None,
            }
        );
    }

    #[test]
    fn composite_index_uses_the_leftmost_prefix() {
        let (_tmp, mut pager, catalog) = fixture();
        catalog
            .put(
                &mut pager,
                &users_schema(vec![Index {
                    name: "by_id_email".into(),
                    columns: vec![0, 1],
                    root: 42,
                }]),
            )
            .unwrap();

        // Only the non-leading column: the index cannot be used.
        let trailing = access_of(
            plan_sql(
                &mut pager,
                &catalog,
                "SELECT * FROM users WHERE email = 'x'",
            )
            .unwrap(),
        );
        assert_eq!(trailing, AccessPath::FullScan);

        // The leading column alone: a prefix scan on id.
        let leading =
            access_of(plan_sql(&mut pager, &catalog, "SELECT * FROM users WHERE id = 5").unwrap());
        let id5 = codec::encode_index_value(&Value::Int(5));
        assert_eq!(
            leading,
            AccessPath::IndexScan {
                index_root: 42,
                lower: id5.clone(),
                upper: codec::prefix_upper_bound(&id5),
            }
        );
    }

    #[test]
    fn no_index_for_equality_against_null() {
        let (_tmp, mut pager, catalog) = fixture();
        catalog
            .put(
                &mut pager,
                &users_schema(vec![Index {
                    name: "by_email".into(),
                    columns: vec![1],
                    root: 777,
                }]),
            )
            .unwrap();
        let access = access_of(
            plan_sql(
                &mut pager,
                &catalog,
                "SELECT id FROM users WHERE email = NULL",
            )
            .unwrap(),
        );
        assert_eq!(access, AccessPath::FullScan);
    }

    #[test]
    fn index_scan_order_is_detected() {
        let (_tmp, mut pager, catalog) = fixture();
        catalog
            .put(
                &mut pager,
                &users_schema(vec![Index {
                    name: "by_id_email".into(),
                    columns: vec![0, 1],
                    root: 42,
                }]),
            )
            .unwrap();
        let presorted = |pager: &mut Pager, sql: &str| match plan_sql(pager, &catalog, sql).unwrap()
        {
            Plan::Select { presorted, .. } => presorted,
            other => panic!("expected a SELECT, got {other:?}"),
        };
        // `id` pinned, so the scan is ordered by `email` — ORDER BY email is free.
        assert!(presorted(
            &mut pager,
            "SELECT * FROM users WHERE id = 5 ORDER BY email"
        ));
        // A forward scan cannot satisfy a descending order.
        assert!(!presorted(
            &mut pager,
            "SELECT * FROM users WHERE id = 5 ORDER BY email DESC"
        ));
        // The scan is not ordered by `id` among the results.
        assert!(!presorted(
            &mut pager,
            "SELECT * FROM users WHERE id = 5 ORDER BY id"
        ));
    }

    // --- INNER-join reorder ----------------------------------------------

    /// `(name, columns, row_count)` short-form schema for join tests.
    fn put_table(
        pager: &mut Pager,
        catalog: &Catalog,
        name: &str,
        columns: Vec<(&str, Type)>,
        row_count: u64,
    ) {
        catalog
            .put(
                pager,
                &Schema {
                    name: name.into(),
                    columns: columns
                        .into_iter()
                        .map(|(n, t)| Column {
                            name: n.into(),
                            ty: t,
                        })
                        .collect(),
                    root: 1,
                    next_rowid: 1,
                    row_count,
                    indexes: vec![],
                },
            )
            .unwrap();
    }

    /// The base + each join's table, in plan order.
    fn join_order(plan: &Plan) -> Vec<String> {
        let Plan::Select { from, .. } = plan else {
            panic!("expected a SELECT plan, got {plan:?}");
        };
        std::iter::once(from.table.name.clone())
            .chain(from.joins.iter().map(|j| j.table.name.clone()))
            .collect()
    }

    #[test]
    fn two_table_inner_join_is_a_no_op() {
        // `max(left, right)` is commutative, so a two-table connected join
        // costs the same in either direction — the planner keeps the user's
        // order rather than reorder for no benefit. Reorder gains only show up
        // with three or more tables, where intermediates compound.
        let (_tmp, mut pager, catalog) = fixture();
        put_table(&mut pager, &catalog, "big", vec![("x", Type::Int)], 1000);
        put_table(&mut pager, &catalog, "small", vec![("y", Type::Int)], 10);
        let plan = plan_sql(
            &mut pager,
            &catalog,
            "SELECT * FROM big INNER JOIN small ON big.x = small.y",
        )
        .unwrap();
        assert_eq!(join_order(&plan), vec!["big", "small"]);
    }

    #[test]
    fn largest_table_is_pushed_to_the_end_of_a_chain() {
        let (_tmp, mut pager, catalog) = fixture();
        // A 3-table chain. Distinct column names so analysis is unambiguous.
        put_table(&mut pager, &catalog, "big", vec![("bid", Type::Int)], 1000);
        put_table(
            &mut pager,
            &catalog,
            "mid",
            vec![("mid_id", Type::Int), ("bid", Type::Int)],
            100,
        );
        put_table(
            &mut pager,
            &catalog,
            "tiny",
            vec![("tid", Type::Int), ("mid_id", Type::Int)],
            10,
        );
        let plan = plan_sql(
            &mut pager,
            &catalog,
            "SELECT * FROM big \
             INNER JOIN mid ON big.bid = mid.bid \
             INNER JOIN tiny ON mid.mid_id = tiny.mid_id",
        )
        .unwrap();
        let order = join_order(&plan);
        assert_eq!(
            order.last().unwrap(),
            "big",
            "biggest table should join last, got {order:?}"
        );
        assert_ne!(
            order[0], "big",
            "biggest table should not be the base, got {order:?}"
        );
    }

    #[test]
    fn left_join_keeps_user_order() {
        let (_tmp, mut pager, catalog) = fixture();
        put_table(&mut pager, &catalog, "big", vec![("x", Type::Int)], 1000);
        put_table(&mut pager, &catalog, "small", vec![("y", Type::Int)], 10);
        let plan = plan_sql(
            &mut pager,
            &catalog,
            "SELECT * FROM big LEFT JOIN small ON big.x = small.y",
        )
        .unwrap();
        // LEFT is not commutative — the planner must leave the order alone.
        assert_eq!(join_order(&plan), vec!["big", "small"]);
    }

    #[test]
    fn cross_join_keeps_user_order() {
        let (_tmp, mut pager, catalog) = fixture();
        put_table(&mut pager, &catalog, "big", vec![("x", Type::Int)], 1000);
        put_table(&mut pager, &catalog, "small", vec![("y", Type::Int)], 10);
        let plan = plan_sql(&mut pager, &catalog, "SELECT * FROM big CROSS JOIN small").unwrap();
        // Any non-INNER join in the chain freezes the layout.
        assert_eq!(join_order(&plan), vec!["big", "small"]);
    }

    #[test]
    fn ambiguous_bare_reference_punts_to_user_order() {
        let (_tmp, mut pager, catalog) = fixture();
        // Both tables have an `id` column, so a bare `id` on either side of the
        // ON cannot be attributed to one table — the planner declines to reorder.
        put_table(&mut pager, &catalog, "big", vec![("id", Type::Int)], 1000);
        put_table(&mut pager, &catalog, "small", vec![("id", Type::Int)], 10);
        let plan = plan_sql(
            &mut pager,
            &catalog,
            "SELECT * FROM big INNER JOIN small ON id = id",
        )
        .unwrap();
        assert_eq!(join_order(&plan), vec!["big", "small"]);
    }

    #[test]
    fn reorder_avoids_creating_a_cross_product() {
        let (_tmp, mut pager, catalog) = fixture();
        // `a` and `b` are tiny and only join through `hub`. A naive
        // "smallest first" picker would put a and b adjacent and produce a
        // cross product before reaching hub.
        put_table(&mut pager, &catalog, "a", vec![("hub_id", Type::Int)], 10);
        put_table(&mut pager, &catalog, "hub", vec![("id", Type::Int)], 100);
        put_table(&mut pager, &catalog, "b", vec![("hub_id", Type::Int)], 10);
        let plan = plan_sql(
            &mut pager,
            &catalog,
            "SELECT * FROM a \
             INNER JOIN hub ON a.hub_id = hub.id \
             INNER JOIN b ON b.hub_id = hub.id",
        )
        .unwrap();
        let order = join_order(&plan);
        // hub must reach the joined set no later than the first join — otherwise
        // step one joins a and b with no connecting predicate.
        assert!(
            order[0] == "hub" || order[1] == "hub",
            "hub must appear in the first two slots to avoid a cross product, got {order:?}"
        );
    }

    #[test]
    fn predicates_re_attach_to_the_step_with_all_refs() {
        let (_tmp, mut pager, catalog) = fixture();
        put_table(&mut pager, &catalog, "big", vec![("bid", Type::Int)], 1000);
        put_table(
            &mut pager,
            &catalog,
            "mid",
            vec![("mid_id", Type::Int), ("bid", Type::Int)],
            100,
        );
        put_table(
            &mut pager,
            &catalog,
            "tiny",
            vec![("tid", Type::Int), ("mid_id", Type::Int)],
            10,
        );
        let plan = plan_sql(
            &mut pager,
            &catalog,
            "SELECT * FROM big \
             INNER JOIN mid ON big.bid = mid.bid \
             INNER JOIN tiny ON mid.mid_id = tiny.mid_id",
        )
        .unwrap();
        let Plan::Select { from, .. } = plan else {
            panic!()
        };
        // Each join keeps an ON; none should be `ON TRUE` (an orphan). Each
        // predicate must mention the table joined at this step.
        for join in &from.joins {
            let on = join.on.as_ref().expect("INNER join keeps an ON predicate");
            assert!(
                !matches!(on, Expr::Bool(true)),
                "no step should be a bare ON TRUE: {join:?}"
            );
            // The new table's name must appear somewhere in the ON expression.
            let target = &join.table.name;
            assert!(
                contains_name(on, target),
                "join step adding `{target}` should reference it in its ON: {on:?}"
            );
        }
    }

    fn contains_name(expr: &Expr, name: &str) -> bool {
        match expr {
            Expr::Column(c) => c.table.as_deref() == Some(name),
            Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } => contains_name(expr, name),
            Expr::Binary { left, right, .. } => {
                contains_name(left, name) || contains_name(right, name)
            }
            Expr::InSubquery { expr, .. } | Expr::InList { expr, .. } => contains_name(expr, name),
            _ => false,
        }
    }
}
