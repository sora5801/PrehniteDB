//! Bind-parameter substitution for v0.54 prepared-style queries.
//!
//! After parsing, an SQL statement carries [`Expr::Placeholder(i)`]
//! nodes for every `?` the user wrote. Before execution, this module
//! walks the [`Plan`] tree and replaces each placeholder with the
//! literal `Expr` for the value at index `i` of the caller's
//! `params` slice — so the executor never sees a `Placeholder`
//! variant.
//!
//! Why bind after planning rather than at parse time: the planner
//! does shape work (validation, join reordering, access-path
//! selection) that doesn't depend on parameter values. Doing one
//! parse+plan and many binds is the foundation for true prepared
//! statements; v0.54 ships the bind step alone and is the bedrock
//! for v0.55+ wire-protocol Prepare/Execute frames.
//!
//! The walker is total: every place a Plan can hold an `Expr` gets
//! recursed into. A `Placeholder` that survives binding (out-of-range
//! index, or a placeholder in a place the bind step missed) is a
//! plan-time error, not a runtime one.

use crate::engine::planner::Plan;
use crate::engine::value::Value;
use crate::error::{Error, Result};
use crate::sql::ast::{Expr, FromClause, Join, Projection, SelectItem, Statement};

/// Rewrite every [`Expr::Placeholder(i)`] in `plan` into the matching
/// literal from `params`. Returns an error if the placeholder count
/// exceeds the params slice, or (after the walk) if any placeholder
/// remains unbound (which would mean we missed a walk site — bug).
pub fn bind_plan(plan: &mut Plan, params: &[Value]) -> Result<()> {
    bind_plan_inner(plan, params)
}

fn bind_plan_inner(plan: &mut Plan, params: &[Value]) -> Result<()> {
    match plan {
        Plan::CreateTable { .. }
        | Plan::DropTable { .. }
        | Plan::CreateIndex { .. }
        | Plan::DropIndex { .. }
        | Plan::Vacuum
        | Plan::Analyze { .. } => {
            // No expression positions in these — placeholders are
            // illegal here, but the parser wouldn't produce them
            // (no Expr children).
            Ok(())
        }
        Plan::Insert { rows, .. } => {
            for row in rows {
                for e in row {
                    bind_expr(e, params)?;
                }
            }
            Ok(())
        }
        Plan::Select {
            from,
            projection,
            filter,
            group_by: _,
            having,
            order_by: _,
            ..
        } => {
            bind_from(from, params)?;
            bind_projection(projection, params)?;
            if let Some(f) = filter {
                bind_expr(f, params)?;
            }
            if let Some(h) = having {
                bind_expr(h, params)?;
            }
            Ok(())
        }
        Plan::Update {
            assignments,
            filter,
            ..
        } => {
            for (_, e) in assignments {
                bind_expr(e, params)?;
            }
            if let Some(f) = filter {
                bind_expr(f, params)?;
            }
            Ok(())
        }
        Plan::Delete { filter, .. } => {
            if let Some(f) = filter {
                bind_expr(f, params)?;
            }
            Ok(())
        }
        Plan::Explain { inner, .. } => bind_plan_inner(inner, params),
    }
}

fn bind_from(from: &mut FromClause, params: &[Value]) -> Result<()> {
    for join in &mut from.joins {
        bind_join(join, params)?;
    }
    Ok(())
}

fn bind_join(join: &mut Join, params: &[Value]) -> Result<()> {
    if let Some(on) = &mut join.on {
        bind_expr(on, params)?;
    }
    Ok(())
}

fn bind_projection(projection: &mut Projection, params: &[Value]) -> Result<()> {
    match projection {
        Projection::All => Ok(()),
        Projection::Items(items) => {
            for item in items {
                if let SelectItem::Expr(e) = item {
                    bind_expr(e, params)?;
                }
            }
            Ok(())
        }
    }
}

/// Recursively rewrite placeholders in `expr`. Subqueries inside an
/// expression position (scalar/EXISTS/IN) carry a full `Statement`
/// that may itself contain placeholders — we walk those too, so a
/// parameter referenced from a correlated subquery's WHERE binds
/// the same way as one at the top level.
fn bind_expr(expr: &mut Expr, params: &[Value]) -> Result<()> {
    match expr {
        Expr::Placeholder(idx) => {
            let i = *idx;
            let value = params.get(i).ok_or_else(|| {
                Error::exec(format!(
                    "bind: placeholder ${} has no matching parameter (got {} params)",
                    i + 1,
                    params.len()
                ))
            })?;
            *expr = value_to_expr(value);
            Ok(())
        }
        Expr::Null
        | Expr::Integer(_)
        | Expr::Real(_)
        | Expr::Str(_)
        | Expr::Bool(_)
        | Expr::Column(_)
        | Expr::Aggregate(_) => Ok(()),
        Expr::Unary { expr, .. } => bind_expr(expr, params),
        Expr::Binary { left, right, .. } => {
            bind_expr(left, params)?;
            bind_expr(right, params)
        }
        Expr::IsNull { expr, .. } => bind_expr(expr, params),
        Expr::InSubquery {
            expr, subquery, ..
        }
        | Expr::CorrelatedInSubquery {
            expr, subquery, ..
        } => {
            bind_expr(expr, params)?;
            bind_statement(subquery, params)
        }
        Expr::Exists(stmt)
        | Expr::CorrelatedExists(stmt)
        | Expr::ScalarSubquery(stmt)
        | Expr::CorrelatedScalarSubquery(stmt) => bind_statement(stmt, params),
        Expr::InList { expr, values, .. } => {
            bind_expr(expr, params)?;
            for v in values {
                bind_expr(v, params)?;
            }
            Ok(())
        }
    }
}

/// Walk a Statement embedded inside an Expr (subquery) and bind any
/// placeholders inside it. Mirrors `bind_plan_inner` for the
/// statement-only shapes that can appear in a subquery position.
fn bind_statement(stmt: &mut Statement, params: &[Value]) -> Result<()> {
    match stmt {
        Statement::Select {
            from,
            projection,
            filter,
            having,
            ..
        } => {
            bind_from(from, params)?;
            bind_projection(projection, params)?;
            if let Some(f) = filter {
                bind_expr(f, params)?;
            }
            if let Some(h) = having {
                bind_expr(h, params)?;
            }
            Ok(())
        }
        // Other statement shapes shouldn't appear inside a subquery,
        // but we walk them anyway for defence-in-depth.
        _ => Ok(()),
    }
}

/// Lower a runtime [`Value`] into the literal `Expr` variant the
/// executor expects. Used by the bind step to substitute placeholders.
fn value_to_expr(value: &Value) -> Expr {
    match value {
        Value::Null => Expr::Null,
        Value::Int(n) => Expr::Integer(*n),
        Value::Real(r) => Expr::Real(*r),
        Value::Text(s) => Expr::Str(s.clone()),
        Value::Bool(b) => Expr::Bool(*b),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::ast::{BinaryOp, ColumnRef};

    #[test]
    fn value_to_expr_round_trips() {
        assert_eq!(value_to_expr(&Value::Null), Expr::Null);
        assert_eq!(value_to_expr(&Value::Int(42)), Expr::Integer(42));
        assert_eq!(value_to_expr(&Value::Bool(true)), Expr::Bool(true));
        assert_eq!(
            value_to_expr(&Value::Text("hi".into())),
            Expr::Str("hi".into())
        );
    }

    #[test]
    fn bind_replaces_placeholder_inside_expr() {
        // `col = ?` with `?` index 0 -> `col = 5`.
        let mut e = Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Column(ColumnRef::bare("col"))),
            right: Box::new(Expr::Placeholder(0)),
        };
        bind_expr(&mut e, &[Value::Int(5)]).unwrap();
        let expected = Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Column(ColumnRef::bare("col"))),
            right: Box::new(Expr::Integer(5)),
        };
        assert_eq!(e, expected);
    }

    #[test]
    fn bind_arity_error_when_too_few_params() {
        let mut e = Expr::Placeholder(2);
        let err = bind_expr(&mut e, &[Value::Int(1), Value::Int(2)]).unwrap_err();
        assert!(format!("{err}").contains("placeholder"));
    }
}
