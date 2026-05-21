//! The planner — a lowering and static-validation pass between the parser and
//! the executor.
//!
//! v0.1 has exactly one physical access path (a full table scan), so there is
//! no cost-based planning to do. What the planner *does* earn its place with is
//! the checks it can make without touching the catalog — duplicate column
//! names, mismatched `INSERT` arity — and lowering parser [`TypeName`]s into
//! engine [`Type`]s. It is also the natural home for a real query optimizer if
//! PrehniteDB ever grows one.

use std::collections::HashSet;

use crate::engine::schema::Column;
use crate::engine::value::Type;
use crate::error::{Error, Result};
use crate::sql::ast::{Expr, Projection, Statement};

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
    },
    Update {
        table: String,
        assignments: Vec<(String, Expr)>,
        filter: Option<Expr>,
    },
    Delete {
        table: String,
        filter: Option<Expr>,
    },
}

/// Lower and validate one statement.
pub fn plan(statement: Statement) -> Result<Plan> {
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
        } => Ok(Plan::Select {
            table,
            projection,
            filter,
        }),

        Statement::Update {
            table,
            assignments,
            filter,
        } => Ok(Plan::Update {
            table,
            assignments,
            filter,
        }),

        Statement::Delete { table, filter } => Ok(Plan::Delete { table, filter }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parse;

    fn plan_sql(sql: &str) -> Result<Plan> {
        plan(parse(sql).unwrap())
    }

    #[test]
    fn rejects_duplicate_create_columns() {
        assert!(plan_sql("CREATE TABLE t (a INT, a TEXT)").is_err());
    }

    #[test]
    fn rejects_mismatched_insert_arity() {
        assert!(plan_sql("INSERT INTO t (a, b) VALUES (1)").is_err());
        assert!(plan_sql("INSERT INTO t (a, b) VALUES (1, 2), (3)").is_err());
    }

    #[test]
    fn accepts_well_formed_statements() {
        assert!(plan_sql("CREATE TABLE t (a INT, b TEXT)").is_ok());
        assert!(plan_sql("INSERT INTO t (a, b) VALUES (1, 'x')").is_ok());
        assert!(plan_sql("SELECT * FROM t WHERE a > 0").is_ok());
    }
}
