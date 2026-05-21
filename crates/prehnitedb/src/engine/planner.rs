//! The planner — the bind-and-plan pass between the parser and the executor.
//!
//! It has two jobs. First, **static validation**: catalog-free checks like
//! duplicate column names and mismatched `INSERT` arity, plus lowering parser
//! [`TypeName`](crate::sql::ast::TypeName)s into engine [`Type`]s. Second,
//! **access-path selection**: for a `SELECT`, `UPDATE`, or `DELETE` carrying a
//! `WHERE` clause, it consults the catalog and — when an equality predicate
//! falls on an indexed column — plans an index lookup in place of a full scan.
//!
//! Access-path selection is best-effort. If anything is unclear (the table is
//! missing, no index fits, the predicate is not a simple equality) it falls
//! back to [`AccessPath::FullScan`] and lets the executor have the final,
//! authoritative word on validity.

use std::collections::HashSet;

use crate::engine::catalog::Catalog;
use crate::engine::schema::Column;
use crate::engine::value::{coerce, Type, Value};
use crate::error::{Error, Result};
use crate::sql::ast::{BinaryOp, Expr, Projection, Statement};
use crate::storage::Pager;

/// How the executor should find the rows a statement operates on.
#[derive(Debug, Clone, PartialEq)]
pub enum AccessPath {
    /// Walk every row of the table.
    FullScan,
    /// Look rows up through a secondary index, by an equality predicate.
    IndexEq { index_root: u32, value: Value },
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
        column: String,
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
            column,
        } => Ok(Plan::CreateIndex {
            name,
            table,
            column,
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
        } => {
            let access = choose_access(pager, catalog, &table, filter.as_ref())?;
            Ok(Plan::Select {
                table,
                projection,
                filter,
                access,
            })
        }

        Statement::Update {
            table,
            assignments,
            filter,
        } => {
            let access = choose_access(pager, catalog, &table, filter.as_ref())?;
            Ok(Plan::Update {
                table,
                assignments,
                filter,
                access,
            })
        }

        Statement::Delete { table, filter } => {
            let access = choose_access(pager, catalog, &table, filter.as_ref())?;
            Ok(Plan::Delete {
                table,
                filter,
                access,
            })
        }
    }
}

/// Pick an access path for a query: an index lookup when a `WHERE` conjunct is
/// an equality test on an indexed column, otherwise a full table scan.
fn choose_access(
    pager: &mut Pager,
    catalog: &Catalog,
    table: &str,
    filter: Option<&Expr>,
) -> Result<AccessPath> {
    let Some(filter) = filter else {
        return Ok(AccessPath::FullScan);
    };
    let Some(schema) = catalog.get(pager, table)? else {
        // Unknown table — leave the real error to the executor.
        return Ok(AccessPath::FullScan);
    };
    if schema.indexes.is_empty() {
        return Ok(AccessPath::FullScan);
    }

    let mut conjuncts = Vec::new();
    collect_conjuncts(filter, &mut conjuncts);
    for conjunct in conjuncts {
        let Some((column_name, literal)) = equality_terms(conjunct) else {
            continue;
        };
        let Some(column) = schema.column_index(column_name) else {
            continue;
        };
        let Some(index) = schema.index_on(column) else {
            continue;
        };
        // Index keys are built from values coerced to the column's type, so the
        // lookup value must coerce the same way; a NULL never matches via `=`.
        let Ok(value) = coerce(literal, schema.columns[column].ty) else {
            continue;
        };
        if value.is_null() {
            continue;
        }
        return Ok(AccessPath::IndexEq {
            index_root: index.root,
            value,
        });
    }
    Ok(AccessPath::FullScan)
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

/// If `expr` is `column = literal` (in either order), return the column name
/// and the literal as a [`Value`].
fn equality_terms(expr: &Expr) -> Option<(&str, Value)> {
    let Expr::Binary {
        op: BinaryOp::Eq,
        left,
        right,
    } = expr
    else {
        return None;
    };
    match (left.as_ref(), right.as_ref()) {
        (Expr::Column(name), other) => literal_value(other).map(|v| (name.as_str(), v)),
        (other, Expr::Column(name)) => literal_value(other).map(|v| (name.as_str(), v)),
        _ => None,
    }
}

/// A literal expression as a [`Value`]. Compound and `NULL` expressions return
/// `None`: neither can serve as an index lookup key.
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
    use crate::engine::schema::{Index, Schema};
    use crate::sql::parse;
    use crate::storage::pager::wal_path;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    /// A temp database file that deletes itself once dropped.
    struct TempPath(PathBuf);

    impl Drop for TempPath {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
            let _ = std::fs::remove_file(wal_path(&self.0));
        }
    }

    /// A fresh pager + catalog. The returned tuple drops `catalog`, then
    /// `pager` (closing the file), then `TempPath` (deleting it) — in order.
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
        let plan = plan_sql(
            &mut pager,
            &catalog,
            "SELECT * FROM users WHERE email = 'a@b.com'",
        )
        .unwrap();
        assert!(matches!(
            plan,
            Plan::Select {
                access: AccessPath::FullScan,
                ..
            }
        ));
    }

    #[test]
    fn picks_index_for_equality_predicate() {
        let (_tmp, mut pager, catalog) = fixture();
        let schema = users_schema(vec![Index {
            name: "by_email".into(),
            column: 1,
            root: 777,
        }]);
        catalog.put(&mut pager, &schema).unwrap();

        let plan = plan_sql(
            &mut pager,
            &catalog,
            "SELECT id FROM users WHERE email = 'a@b.com' AND id > 0",
        )
        .unwrap();
        match plan {
            Plan::Select {
                access: AccessPath::IndexEq { index_root, value },
                ..
            } => {
                assert_eq!(index_root, 777);
                assert_eq!(value, Value::Text("a@b.com".into()));
            }
            other => panic!("expected an index scan, got {other:?}"),
        }
    }

    #[test]
    fn no_index_for_equality_against_null() {
        let (_tmp, mut pager, catalog) = fixture();
        catalog
            .put(
                &mut pager,
                &users_schema(vec![Index {
                    name: "by_email".into(),
                    column: 1,
                    root: 777,
                }]),
            )
            .unwrap();
        // `email = NULL` is never TRUE, so it must not drive an index lookup.
        let plan = plan_sql(
            &mut pager,
            &catalog,
            "SELECT id FROM users WHERE email = NULL",
        )
        .unwrap();
        assert!(matches!(
            plan,
            Plan::Select {
                access: AccessPath::FullScan,
                ..
            }
        ));
    }
}
