//! The planner — the bind-and-plan pass between the parser and the executor.
//!
//! It has two jobs. First, **static validation**: catalog-free checks like
//! duplicate column names and mismatched `INSERT` arity, plus lowering parser
//! [`TypeName`](crate::sql::ast::TypeName)s into engine [`Type`]s. Second,
//! **access-path selection**: for a `SELECT`, `UPDATE`, or `DELETE` carrying a
//! `WHERE` clause, it consults the catalog and tries to turn the predicate into
//! a bounded scan over a secondary index.
//!
//! The access-path search classifies each top-level `AND` conjunct as an
//! equality or a range on a single column, then for each index walks its
//! columns left to right: equality predicates extend a pinned key prefix, and
//! the first non-equality column may contribute a single range bound — the
//! standard "leftmost prefix" rule. The planner also notices when the chosen
//! index scan already yields rows in `ORDER BY` order, letting the executor
//! skip the sort. Everything is best-effort: anything unclear falls back to
//! [`AccessPath::FullScan`], and the executor still has the final word.

use std::collections::HashSet;

use crate::engine::catalog::Catalog;
use crate::engine::codec;
use crate::engine::schema::{Column, Index, Schema};
use crate::engine::value::{coerce, Type, Value};
use crate::error::{Error, Result};
use crate::sql::ast::{BinaryOp, Expr, OrderKey, Projection, Statement};
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
        table: String,
        projection: Projection,
        filter: Option<Expr>,
        access: AccessPath,
        group_by: Vec<String>,
        order_by: Vec<OrderKey>,
        /// True when `access` already yields rows in `order_by` order, so the
        /// executor need not sort. Always false for a grouped query, whose
        /// output rows are groups rather than table rows.
        presorted: bool,
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
            table,
            projection,
            filter,
            group_by,
            order_by,
        } => {
            let (access, presorted) =
                choose_access(pager, catalog, &table, filter.as_ref(), &order_by)?;
            // A grouped query's rows are groups, not table rows, so an index's
            // row order cannot satisfy ORDER BY.
            let presorted = presorted && group_by.is_empty();
            Ok(Plan::Select {
                table,
                projection,
                filter,
                access,
                group_by,
                order_by,
                presorted,
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
                .is_some_and(|c| c.name == key.column)
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
    let (name, literal, op) = match (left.as_ref(), right.as_ref()) {
        (Expr::Column(name), other) => (name, literal_value(other)?, *op),
        (other, Expr::Column(name)) => (name, literal_value(other)?, flip_op(*op)),
        _ => return None,
    };
    let column = schema.column_index(name)?;
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
}
