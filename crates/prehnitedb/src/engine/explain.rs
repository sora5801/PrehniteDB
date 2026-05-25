//! `EXPLAIN <select>` — render a [`Plan`] as an indented operator tree
//! with per-node cardinality estimates.
//!
//! The output is one logical operator per line; children sit one indent
//! (two spaces) below their parent. Each line ends in a `(rows: N)`
//! estimate computed from the catalog's `Schema::row_count` and a
//! per-predicate selectivity model.
//!
//! The selectivity model is intentionally coarse — Postgres-style
//! defaults with no histograms, no MCV lists, no distinct-value counts:
//!
//! - `=`, `IS NULL` → 0.10
//! - `<>`           → 0.90
//! - `<`,`<=`,`>`,`>=` → 0.33
//! - `AND`          → product of conjuncts (independence assumption)
//! - `OR`           → `1 - (1-s1)*(1-s2)`
//! - `NOT`          → `1 - s`
//! - anything else  → 1.0 (treat as a pass-through)
//!
//! Good enough to spot an order-of-magnitude blunder (a 1M-row full scan
//! feeding a nested loop, say) without pretending to be a real cost model.
//! The reorder pass in `planner.rs` uses these same numbers via
//! `Schema::row_count` already; this is the user-facing readout.
//!
//! `EXPLAIN` is a read-only statement at the wire level — see
//! [`crate::write_scope`] — and never executes the inner statement, so a
//! `EXPLAIN INSERT INTO t VALUES (1)` does not write a row.

use std::fmt::Write;
use std::time::Duration;

use crate::engine::catalog::Catalog;
use crate::engine::planner::{AccessPath, Plan};
use crate::error::Result;
use crate::sql::ast::{
    Aggregate, AggregateArg, AggregateFunc, BinaryOp, ColumnRef, Expr, FromClause, JoinKind,
    OrderKey, Projection, SelectItem, UnaryOp,
};
use crate::storage::Pager;

/// Observed-run statistics from an `EXPLAIN ANALYZE`: the total number
/// of rows the inner statement actually yielded, and how long the run
/// took wall-clock. v0.40 surfaced these on the top operator line; v0.41
/// adds [`OperatorActuals`] for per-node detail. The total + elapsed
/// still drive the root annotation (a fallback when per-node data is
/// missing) and the `Execution time:` footer.
#[derive(Debug, Clone, Copy)]
pub struct AnalyzeStats {
    pub actual_rows: u64,
    pub elapsed: Duration,
}

/// Per-operator observed row counts, gathered by wrapping each
/// operator in a `Counting` adapter during ANALYZE execution. The
/// fields mirror the structure `format_plan_analyzed` walks: one
/// counter per operator that appears in the SELECT plan. `None` slots
/// describe operators that never appeared (no `WHERE` → no `filter`;
/// no `ORDER BY` → no `sort`; an `IndexNestedLoopJoin` has no
/// streaming right scan).
///
/// `join_outputs` and `join_right_scans` are indexed by the join's
/// position in `FromClause::joins` (build order, inner-to-outer
/// left-deep).
#[derive(Debug, Clone, Default)]
pub struct OperatorActuals {
    pub base_scan: Option<u64>,
    pub join_outputs: Vec<u64>,
    pub join_right_scans: Vec<Option<u64>>,
    pub filter: Option<u64>,
    pub sort: Option<u64>,
    pub project: Option<u64>,
    pub limit: Option<u64>,
    /// Materialised-grouped path: a single observation that covers
    /// HashAggregate / Having / Sort / Project / Limit collectively.
    pub grouped_output: Option<u64>,
}

/// Render `plan` as the multi-line text `EXPLAIN` returns. The caller
/// splits it into one row per line and wraps each in a `QUERY PLAN`
/// column.
pub fn format_plan(pager: &mut Pager, catalog: &Catalog, plan: &Plan) -> Result<String> {
    let mut out = String::new();
    fmt_plan(pager, catalog, plan, 0, &mut out)?;
    Ok(out)
}

/// `EXPLAIN ANALYZE` variant: same shape as [`format_plan`], but each
/// operator line picks up an `actual: N` annotation drawn from
/// `actuals`, and the output ends with an `Execution time: X.XXX ms`
/// footer. When `actuals` is `None` (the v0.40 fallback path) only the
/// root line is annotated, using `stats.actual_rows`.
pub fn format_plan_analyzed(
    pager: &mut Pager,
    catalog: &Catalog,
    plan: &Plan,
    stats: AnalyzeStats,
    actuals: Option<OperatorActuals>,
) -> Result<String> {
    let mut text = format_plan(pager, catalog, plan)?;
    text = match actuals {
        Some(a) => annotate_lines(&text, &a, stats.actual_rows),
        None => annotate_root_with_actual(&text, stats.actual_rows),
    };
    let ms = stats.elapsed.as_secs_f64() * 1000.0;
    let _ = write!(&mut text, "Execution time: {ms:.3} ms\n");
    Ok(text)
}

/// Walk `text` line by line, splicing `, actual: N` before each
/// operator line's closing `)`. The role of each line is detected from
/// its leading token (`Limit`, `Project`, `Filter`, `InnerJoin`,
/// `SeqScan`, ...) and matched to the matching counter slot in
/// [`OperatorActuals`].
///
/// Joins are emitted by `format_plan` in reverse build order
/// (outermost first), so the first join line takes
/// `join_outputs[N-1]`, the next `[N-2]`, and so on. Scans are
/// emitted base-first then per-join right scans in build order, so the
/// first scan line takes `base_scan` and the next takes
/// `join_right_scans[0]`, etc. Grouped queries hand every
/// post-aggregation operator (`Limit` / `Project` / `Sort` / `Having`
/// / `HashAggregate`) the same `grouped_output` count, because
/// `grouped_select` is a pipeline breaker that can't be wrapped per
/// operator.
///
/// The `root_total` is the v0.40-style observed grand total — used as
/// a fallback annotation on the top line when no per-line counter
/// resolves (e.g. an unknown operator added in a future session).
fn annotate_lines(text: &str, actuals: &OperatorActuals, root_total: u64) -> String {
    let total_joins = actuals.join_outputs.len();
    let mut joins_seen = 0usize;
    let mut scans_seen = 0usize;
    let is_grouped = actuals.grouped_output.is_some();
    let mut out = String::with_capacity(text.len() + 64);
    let mut annotated_any = false;

    for line in text.split_inclusive('\n') {
        let trimmed = line.trim_start();
        let actual: Option<u64> = if trimmed.starts_with("Limit") {
            if is_grouped {
                actuals.grouped_output
            } else {
                actuals.limit
            }
        } else if trimmed.starts_with("Project") {
            if is_grouped {
                actuals.grouped_output
            } else {
                actuals.project
            }
        } else if trimmed.starts_with("Sort") {
            if is_grouped {
                actuals.grouped_output
            } else {
                actuals.sort
            }
        } else if trimmed.starts_with("Having")
            || trimmed.starts_with("HashAggregate")
            || trimmed.starts_with("Aggregate")
        {
            actuals.grouped_output
        } else if trimmed.starts_with("Filter") {
            actuals.filter
        } else if trimmed.starts_with("InnerJoin")
            || trimmed.starts_with("LeftJoin")
            || trimmed.starts_with("CrossJoin")
            || trimmed.starts_with("SemiJoin")
            || trimmed.starts_with("AntiJoin")
        {
            let a = if joins_seen < total_joins {
                Some(actuals.join_outputs[total_joins - 1 - joins_seen])
            } else {
                None
            };
            joins_seen += 1;
            a
        } else if trimmed.starts_with("SeqScan") || trimmed.starts_with("IndexScan") {
            let a = if scans_seen == 0 {
                actuals.base_scan
            } else if scans_seen - 1 < actuals.join_right_scans.len() {
                actuals.join_right_scans[scans_seen - 1]
            } else {
                None
            };
            scans_seen += 1;
            a
        } else {
            None
        };

        if let Some(n) = actual {
            if let Some(stripped) = line.strip_suffix(")\n") {
                out.push_str(stripped);
                let _ = write!(&mut out, ", actual: {n})\n");
                annotated_any = true;
                continue;
            }
        }
        out.push_str(line);
    }

    // Fallback: if we annotated nothing (the plan had no operators we
    // recognise, or no per-line counter matched), use the root-total
    // path so the user still sees an observation.
    if !annotated_any {
        return annotate_root_with_actual(text, root_total);
    }
    out
}

/// Fallback annotator used when per-operator actuals aren't available
/// (the v0.40 path, or a plan shape `annotate_lines` doesn't match).
/// Splices `, actual: N` into the first `(rows: ...)`-shaped line.
fn annotate_root_with_actual(text: &str, actual: u64) -> String {
    let mut out = String::with_capacity(text.len() + 24);
    let mut annotated = false;
    for line in text.split_inclusive('\n') {
        if annotated {
            out.push_str(line);
            continue;
        }
        if let Some(stripped) = line.strip_suffix(")\n") {
            if stripped.rfind("(rows: ").is_some() {
                out.push_str(stripped);
                out.push_str(&format!(", actual: {actual})\n"));
                annotated = true;
                continue;
            }
        }
        out.push_str(line);
    }
    if !annotated {
        let _ = write!(&mut out, "(actual: {actual})\n");
    }
    out
}

fn fmt_plan(
    pager: &mut Pager,
    catalog: &Catalog,
    plan: &Plan,
    depth: usize,
    out: &mut String,
) -> Result<()> {
    match plan {
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
        } => fmt_select(
            pager,
            catalog,
            from,
            projection,
            filter.as_ref(),
            access,
            group_by,
            having.as_ref(),
            order_by,
            *presorted,
            *limit,
            *offset,
            depth,
            out,
        ),
        Plan::Insert { table, rows, .. } => {
            push(out, depth, &format!("Insert {table}  (rows: {})", rows.len()));
            Ok(())
        }
        Plan::Update {
            table,
            assignments,
            filter,
            access,
        } => {
            let cols = assignments
                .iter()
                .map(|(n, _)| n.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            push(out, depth, &format!("Update {table}  (set {cols})"));
            fmt_table_access(pager, catalog, table, access, filter.as_ref(), depth + 1, out)
        }
        Plan::Delete {
            table,
            filter,
            access,
        } => {
            push(out, depth, &format!("Delete from {table}"));
            fmt_table_access(pager, catalog, table, access, filter.as_ref(), depth + 1, out)
        }
        Plan::CreateTable {
            name,
            columns,
            primary_key_column,
            unique_columns,
        } => {
            let mut extras = String::new();
            if primary_key_column.is_some() {
                extras.push_str(", PK");
            }
            if !unique_columns.is_empty() {
                let _ = write!(&mut extras, ", {} UNIQUE", unique_columns.len());
            }
            push(
                out,
                depth,
                &format!(
                    "CreateTable {name}  ({} columns{extras})",
                    columns.len()
                ),
            );
            Ok(())
        }
        Plan::DropTable { name } => {
            push(out, depth, &format!("DropTable {name}"));
            Ok(())
        }
        Plan::CreateIndex {
            name,
            table,
            columns,
        } => {
            push(
                out,
                depth,
                &format!("CreateIndex {name} on {table}({})", columns.join(", ")),
            );
            Ok(())
        }
        Plan::DropIndex { name } => {
            push(out, depth, &format!("DropIndex {name}"));
            Ok(())
        }
        Plan::Vacuum => {
            push(out, depth, "Vacuum");
            Ok(())
        }
        Plan::Analyze { table } => {
            push(out, depth, &format!("Analyze {table}"));
            Ok(())
        }
        Plan::Explain { inner, analyze } => {
            // Rare but legal: EXPLAIN EXPLAIN <stmt>. Spell out the
            // wrapping and recurse, rather than looping. (The inner
            // here is the parsed plan; we just describe it.)
            let label = if *analyze { "Explain (ANALYZE)" } else { "Explain" };
            push(out, depth, label);
            fmt_plan(pager, catalog, inner, depth + 1, out)
        }
    }
}

/// Render a `SELECT` Plan top-down: Limit → Project → Sort → Aggregate →
/// Having → Filter → joins → base scan. Each layer's row estimate is
/// computed in advance from the body up, then emitted along the way.
#[allow(clippy::too_many_arguments)]
fn fmt_select(
    pager: &mut Pager,
    catalog: &Catalog,
    from: &FromClause,
    projection: &Projection,
    filter: Option<&Expr>,
    access: &AccessPath,
    group_by: &[ColumnRef],
    having: Option<&Expr>,
    order_by: &[OrderKey],
    presorted: bool,
    limit: Option<u64>,
    offset: Option<u64>,
    depth: usize,
    out: &mut String,
) -> Result<()> {
    // ----- bottom-up: estimate cardinalities ---------------------------------
    let base_rows = base_scan_rows(pager, catalog, &from.table.name, access)?;
    // v0.47: pass the base table's schema to selectivity() so per-column
    // stats inform the estimate — but only when the query is single-table.
    // Multi-table joins would need a richer scope concept (a future v0.48+).
    let base_schema = if from.joins.is_empty() {
        catalog.get(pager, &from.table.name)?
    } else {
        None
    };
    let stats_for_predicate: Option<&crate::engine::schema::Schema> = base_schema.as_ref();
    // Each join multiplies by max(1, other_rows) and applies the ON's
    // selectivity. Semi/Anti cap output at the left side.
    let mut joined = base_rows;
    for join in &from.joins {
        let inner = base_scan_rows(pager, catalog, &join.table.name, &AccessPath::FullScan)?;
        let on_sel = join.on.as_ref().map(|p| selectivity(p, None)).unwrap_or(1.0);
        joined = match join.kind {
            JoinKind::Inner | JoinKind::Left => {
                scale_rows(joined.saturating_mul(inner.max(1)), on_sel)
            }
            JoinKind::Cross => joined.saturating_mul(inner.max(1)),
            JoinKind::Semi | JoinKind::Anti => scale_rows(joined, on_sel),
        };
    }
    let after_where = match filter {
        Some(p) => scale_rows(joined, selectivity(p, stats_for_predicate)),
        None => joined,
    };
    let after_group = if !group_by.is_empty() {
        group_rows_estimate(after_where, group_by.len())
    } else if projection_has_aggregate(projection) {
        // Ungrouped aggregate collapses to one row.
        1
    } else {
        after_where
    };
    let after_having = match having {
        Some(p) => scale_rows(after_group, selectivity(p, stats_for_predicate)),
        None => after_group,
    };
    // Sort doesn't change row count. Limit clips after offset.
    let after_limit = {
        let off = offset.unwrap_or(0);
        let post_off = after_having.saturating_sub(off);
        match limit {
            Some(l) => post_off.min(l),
            None => post_off,
        }
    };

    // ----- top-down: render --------------------------------------------------
    let mut d = depth;

    if limit.is_some() || offset.is_some() {
        let mut detail = String::new();
        if let Some(l) = limit {
            let _ = write!(&mut detail, "limit={l}");
        }
        if let Some(o) = offset {
            if !detail.is_empty() {
                detail.push(' ');
            }
            let _ = write!(&mut detail, "offset={o}");
        }
        push(
            out,
            d,
            &format!("Limit  ({detail})  (rows: {after_limit})"),
        );
        d += 1;
    }

    let projection_label = projection_description(projection);
    push(
        out,
        d,
        &format!("Project  ({projection_label})  (rows: {after_having})"),
    );
    d += 1;

    if !order_by.is_empty() && !presorted {
        let keys = order_by
            .iter()
            .map(|k| {
                format!(
                    "{} {}",
                    k.column,
                    if k.descending { "DESC" } else { "ASC" }
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        push(
            out,
            d,
            &format!("Sort  ({keys})  (rows: {after_having})"),
        );
        d += 1;
    }

    if let Some(p) = having {
        push(
            out,
            d,
            &format!("Having  ({})  (rows: {after_having})", expr_str(p)),
        );
        d += 1;
    }

    if !group_by.is_empty() || projection_has_aggregate(projection) {
        let kind = if group_by.is_empty() {
            "Aggregate"
        } else {
            "HashAggregate"
        };
        let group_label = if group_by.is_empty() {
            String::new()
        } else {
            format!(
                "  group by {}",
                group_by
                    .iter()
                    .map(|c| c.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        push(
            out,
            d,
            &format!("{kind}{group_label}  (rows: {after_group})"),
        );
        d += 1;
    }

    if let Some(p) = filter {
        push(
            out,
            d,
            &format!("Filter  ({})  (rows: {after_where})", expr_str(p)),
        );
        d += 1;
    }

    fmt_from(pager, catalog, from, access, d, out)
}

/// Render the FROM subtree: a left-deep chain of joins, each with its
/// right side (a full-table scan) under it, with the base scan at the
/// deepest indent.
fn fmt_from(
    pager: &mut Pager,
    catalog: &Catalog,
    from: &FromClause,
    base_access: &AccessPath,
    depth: usize,
    out: &mut String,
) -> Result<()> {
    if from.joins.is_empty() {
        return fmt_scan(pager, catalog, &from.table.name, base_access, depth, out);
    }
    // Render outermost (last) join first: it's the root of the join tree.
    fmt_joins_recursive(pager, catalog, from, base_access, from.joins.len(), depth, out)
}

fn fmt_joins_recursive(
    pager: &mut Pager,
    catalog: &Catalog,
    from: &FromClause,
    base_access: &AccessPath,
    up_to: usize,
    depth: usize,
    out: &mut String,
) -> Result<()> {
    // The join we're about to render: from.joins[up_to - 1]. Its left
    // child is whatever's beneath it in the chain (either an earlier
    // join, or the base scan), and its right child is a scan of the
    // joined table.
    let join = &from.joins[up_to - 1];
    let left_rows = left_rows_at(pager, catalog, from, base_access, up_to - 1)?;
    let right_rows = base_scan_rows(pager, catalog, &join.table.name, &AccessPath::FullScan)?;
    let on_sel = join.on.as_ref().map(|p| selectivity(p, None)).unwrap_or(1.0);
    let out_rows = match join.kind {
        JoinKind::Inner | JoinKind::Left => {
            scale_rows(left_rows.saturating_mul(right_rows.max(1)), on_sel)
        }
        JoinKind::Cross => left_rows.saturating_mul(right_rows.max(1)),
        JoinKind::Semi | JoinKind::Anti => scale_rows(left_rows, on_sel),
    };
    let kind_str = match join.kind {
        JoinKind::Inner => "InnerJoin",
        JoinKind::Left => "LeftJoin",
        JoinKind::Cross => "CrossJoin",
        JoinKind::Semi => "SemiJoin",
        JoinKind::Anti => "AntiJoin",
    };
    let on_str = match &join.on {
        Some(e) => format!("  on {}", expr_str(e)),
        None => String::new(),
    };
    push(
        out,
        depth,
        &format!("{kind_str}{on_str}  (rows: {out_rows})"),
    );

    // Left child first, then the right scan.
    if up_to == 1 {
        fmt_scan(pager, catalog, &from.table.name, base_access, depth + 1, out)?;
    } else {
        fmt_joins_recursive(pager, catalog, from, base_access, up_to - 1, depth + 1, out)?;
    }
    fmt_scan(
        pager,
        catalog,
        &join.table.name,
        &AccessPath::FullScan,
        depth + 1,
        out,
    )
}

/// Row count visible at the boundary just above `from.joins[0..up_to]` —
/// the input the next join would see.
fn left_rows_at(
    pager: &mut Pager,
    catalog: &Catalog,
    from: &FromClause,
    base_access: &AccessPath,
    up_to: usize,
) -> Result<u64> {
    let mut rows = base_scan_rows(pager, catalog, &from.table.name, base_access)?;
    for join in &from.joins[..up_to] {
        let inner = base_scan_rows(pager, catalog, &join.table.name, &AccessPath::FullScan)?;
        let on_sel = join.on.as_ref().map(|p| selectivity(p, None)).unwrap_or(1.0);
        rows = match join.kind {
            JoinKind::Inner | JoinKind::Left => {
                scale_rows(rows.saturating_mul(inner.max(1)), on_sel)
            }
            JoinKind::Cross => rows.saturating_mul(inner.max(1)),
            JoinKind::Semi | JoinKind::Anti => scale_rows(rows, on_sel),
        };
    }
    Ok(rows)
}

/// Render an Update/Delete body: their access path + filter, identical
/// to a SELECT's bottom layers but no projection/sort.
fn fmt_table_access(
    pager: &mut Pager,
    catalog: &Catalog,
    table: &str,
    access: &AccessPath,
    filter: Option<&Expr>,
    depth: usize,
    out: &mut String,
) -> Result<()> {
    let base = base_scan_rows(pager, catalog, table, access)?;
    // v0.47: UPDATE/DELETE always target one table, so column stats
    // are always applicable.
    let schema = catalog.get(pager, table)?;
    let stats_for_predicate: Option<&crate::engine::schema::Schema> = schema.as_ref();
    let after_where = match filter {
        Some(p) => scale_rows(base, selectivity(p, stats_for_predicate)),
        None => base,
    };
    let mut d = depth;
    if let Some(p) = filter {
        push(
            out,
            d,
            &format!("Filter  ({})  (rows: {after_where})", expr_str(p)),
        );
        d += 1;
    }
    fmt_scan(pager, catalog, table, access, d, out)
}

/// Render the scan at the leaf of a FROM subtree.
fn fmt_scan(
    pager: &mut Pager,
    catalog: &Catalog,
    table: &str,
    access: &AccessPath,
    depth: usize,
    out: &mut String,
) -> Result<()> {
    let rows = base_scan_rows(pager, catalog, table, access)?;
    match access {
        AccessPath::FullScan => {
            push(out, depth, &format!("SeqScan {table}  (rows: {rows})"));
        }
        AccessPath::IndexScan {
            index_root,
            lower,
            upper,
        } => {
            let name = index_name(pager, catalog, table, *index_root)?
                .unwrap_or_else(|| format!("#root={index_root}"));
            let bounds = match (lower.is_empty(), upper) {
                (true, None) => "full".to_string(),
                (true, Some(_)) => format!("upper={} bytes", upper.as_ref().unwrap().len()),
                (false, None) => format!("lower={} bytes", lower.len()),
                (false, Some(u)) => format!("lower={} upper={} bytes", lower.len(), u.len()),
            };
            push(
                out,
                depth,
                &format!("IndexScan {table} using {name}  ({bounds})  (rows: {rows})"),
            );
        }
    }
    Ok(())
}

/// Estimated output cardinality of a base scan over `table`. A full
/// scan returns the whole table; an index scan returns the catalog's
/// row_count scaled by a rough range-size guess.
fn base_scan_rows(
    pager: &mut Pager,
    catalog: &Catalog,
    table: &str,
    access: &AccessPath,
) -> Result<u64> {
    let Some(schema) = catalog.get(pager, table)? else {
        return Ok(0);
    };
    match access {
        AccessPath::FullScan => Ok(schema.row_count),
        AccessPath::IndexScan { lower, upper, .. } => {
            // A bounded scan: roughly 10% if both bounds, 33% if one bound,
            // 100% if neither (a pinned-prefix scan with the prefix being the
            // whole index is rare but possible). Same baselines as `selectivity`.
            let frac = match (lower.is_empty(), upper.is_some()) {
                (false, true) => 0.10,
                (false, false) | (true, true) => 0.33,
                (true, false) => 1.0,
            };
            Ok((schema.row_count as f64 * frac).ceil() as u64)
        }
    }
}

/// Look up the named-form of an index from its root page — handy for
/// EXPLAIN since the planner only carries the root number through.
fn index_name(
    pager: &mut Pager,
    catalog: &Catalog,
    table: &str,
    root: u32,
) -> Result<Option<String>> {
    let Some(schema) = catalog.get(pager, table)? else {
        return Ok(None);
    };
    Ok(schema
        .indexes
        .iter()
        .find(|i| i.root == root)
        .map(|i| i.name.clone()))
}

/// Number of distinct groups expected from a `GROUP BY` over `key_columns`
/// keys. No NDV statistics yet, so use `sqrt(input)` capped by input — a
/// standard "we don't know" placeholder.
fn group_rows_estimate(input_rows: u64, _key_columns: usize) -> u64 {
    if input_rows == 0 {
        return 0;
    }
    let sq = (input_rows as f64).sqrt().ceil() as u64;
    sq.max(1).min(input_rows)
}

/// The fraction of rows a predicate is expected to keep.
///
/// When `stats` carries a single table's [`Schema`] (a single-table
/// SELECT), v0.47 consults each column's `ColumnStats` for sharper
/// estimates:
/// - `col = literal` → `1 / n_distinct` (else 0.10 default)
/// - `col > / >= / < / <= literal` → walk the equi-depth histogram,
///   sum fully-on-one-side buckets, linear-interpolate the straddling
///   one (else 1/3 default)
/// - `col IS NULL` → `null_count / total_rows` (else 0.10 default)
///
/// Joined queries pass `None` and fall back to the v0.39 defaults.
/// (A future version could thread per-table schemas through the
/// join scope.)
fn selectivity(expr: &Expr, stats: Option<&crate::engine::schema::Schema>) -> f64 {
    match expr {
        Expr::Binary { op, left, right } => match op {
            BinaryOp::And => selectivity(left, stats) * selectivity(right, stats),
            BinaryOp::Or => {
                let a = selectivity(left, stats);
                let b = selectivity(right, stats);
                1.0 - (1.0 - a) * (1.0 - b)
            }
            BinaryOp::Eq => sel_eq(left, right, stats).unwrap_or(0.10),
            BinaryOp::NotEq => 1.0 - sel_eq(left, right, stats).unwrap_or(0.10),
            BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq => {
                sel_range(*op, left, right, stats).unwrap_or(1.0 / 3.0)
            }
            // Arithmetic as a boolean predicate is unusual but legal —
            // SQL coerces to bool. Treat as a pass-through.
            _ => 1.0,
        },
        Expr::Unary {
            op: UnaryOp::Not,
            expr,
        } => (1.0 - selectivity(expr, stats)).max(0.0),
        Expr::IsNull { expr, negated } => {
            let frac = sel_is_null(expr, stats).unwrap_or(0.10);
            if *negated {
                1.0 - frac
            } else {
                frac
            }
        }
        Expr::InList { values, .. } => {
            // Each value is a hit, capped at 1.0.
            (values.len() as f64 * 0.10).min(1.0)
        }
        // Booleans: TRUE keeps every row, FALSE keeps none. Anything else
        // is opaque to the estimator — leave it at the default.
        Expr::Bool(true) => 1.0,
        Expr::Bool(false) => 0.0,
        // Subqueries: we don't pre-evaluate them here. Treat as opaque.
        _ => 1.0,
    }
}

/// Equality selectivity from `n_distinct`. Returns `None` if either
/// side isn't a (column, literal) pair, the schema doesn't know the
/// column, or the column has no stats yet.
fn sel_eq(
    left: &Expr,
    right: &Expr,
    stats: Option<&crate::engine::schema::Schema>,
) -> Option<f64> {
    let (col, _literal) = match orient_column_literal(left, right) {
        Some(pair) => pair,
        None => return None,
    };
    let schema = stats?;
    let idx = schema.column_index(&col.name)?;
    let s = schema.columns[idx].stats.as_ref()?;
    let non_null = s.total_rows.saturating_sub(s.null_count);
    if non_null == 0 || s.n_distinct == 0 {
        return None;
    }
    // `1 / n_distinct` is the equality selectivity over the non-NULL
    // rows; scale by the non-NULL fraction since NULL never satisfies
    // `=` (three-valued logic).
    let non_null_frac = non_null as f64 / s.total_rows as f64;
    Some((1.0 / s.n_distinct as f64) * non_null_frac)
}

/// Range selectivity by walking the equi-depth histogram. `None` for
/// unsupported shapes (no column, no stats, literal of wrong type, ...).
fn sel_range(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
    stats: Option<&crate::engine::schema::Schema>,
) -> Option<f64> {
    use crate::engine::value::Value;
    // Orient so the column is on the left; if the literal is on the
    // left, flip the operator too.
    let (col, literal, op) = match (left, right) {
        (Expr::Column(c), other) => {
            (c, literal_as_value(other)?, op)
        }
        (other, Expr::Column(c)) => {
            let flipped = match op {
                BinaryOp::Lt => BinaryOp::Gt,
                BinaryOp::LtEq => BinaryOp::GtEq,
                BinaryOp::Gt => BinaryOp::Lt,
                BinaryOp::GtEq => BinaryOp::LtEq,
                other => other,
            };
            (c, literal_as_value(other)?, flipped)
        }
        _ => return None,
    };
    let schema = stats?;
    let idx = schema.column_index(&col.name)?;
    let s = schema.columns[idx].stats.as_ref()?;
    if s.histogram.is_empty() || s.total_rows == 0 {
        return None;
    }
    // Order-preserving byte encoding gives total order for any one
    // type — same comparison the B+tree uses, same one `analyze_table`
    // sorted with.
    let lit_key = crate::engine::codec::encode_index_value(&literal);
    if matches!(literal, Value::Null) {
        return Some(0.0); // any comparison against NULL is NULL, never TRUE.
    }
    // Count matching rows by walking buckets.
    let non_null: u64 = s.histogram.iter().map(|b| b.count).sum();
    if non_null == 0 {
        return Some(0.0);
    }
    let mut matched: f64 = 0.0;
    for bucket in &s.histogram {
        let lo = crate::engine::codec::encode_index_value(&bucket.lower);
        let hi = crate::engine::codec::encode_index_value(&bucket.upper);
        let count = bucket.count as f64;
        // Each branch decides what fraction of `count` matches the
        // comparison. Equi-depth bucket assumption: values are
        // distributed uniformly across `[lo, hi]`, so a straddling
        // bucket contributes a linear fraction.
        match op {
            BinaryOp::Lt | BinaryOp::LtEq => {
                if lit_key < lo {
                    // Bucket entirely above the literal — no match.
                } else if (op == BinaryOp::LtEq && lit_key >= hi)
                    || (op == BinaryOp::Lt && lit_key > hi)
                {
                    // Bucket entirely below — all rows match.
                    matched += count;
                } else {
                    // Straddling — interpolate.
                    matched += count * interpolate(&lo, &hi, &lit_key);
                }
            }
            BinaryOp::Gt | BinaryOp::GtEq => {
                if lit_key > hi {
                    // Bucket entirely below the literal — no match.
                } else if (op == BinaryOp::GtEq && lit_key <= lo)
                    || (op == BinaryOp::Gt && lit_key < lo)
                {
                    // Bucket entirely above — all rows match.
                    matched += count;
                } else {
                    matched += count * (1.0 - interpolate(&lo, &hi, &lit_key));
                }
            }
            _ => return None,
        }
    }
    Some(matched / s.total_rows as f64)
}

/// IS NULL selectivity from `null_count / total_rows`. None if no
/// stats or no column.
fn sel_is_null(
    expr: &Expr,
    stats: Option<&crate::engine::schema::Schema>,
) -> Option<f64> {
    let Expr::Column(col) = expr else { return None };
    let schema = stats?;
    let idx = schema.column_index(&col.name)?;
    let s = schema.columns[idx].stats.as_ref()?;
    if s.total_rows == 0 {
        return None;
    }
    Some(s.null_count as f64 / s.total_rows as f64)
}

/// Orient a binary-comparison's operands into `(column, literal)`,
/// flipping if needed. Returns `None` if neither side is a column
/// or neither side is a literal.
fn orient_column_literal<'a>(
    left: &'a Expr,
    right: &'a Expr,
) -> Option<(&'a ColumnRef, crate::engine::value::Value)> {
    match (left, right) {
        (Expr::Column(c), other) => Some((c, literal_as_value(other)?)),
        (other, Expr::Column(c)) => Some((c, literal_as_value(other)?)),
        _ => None,
    }
}

/// Extract a constant `Value` from an Expr that's a literal.
fn literal_as_value(expr: &Expr) -> Option<crate::engine::value::Value> {
    use crate::engine::value::Value;
    Some(match expr {
        Expr::Null => Value::Null,
        Expr::Integer(n) => Value::Int(*n),
        Expr::Real(r) => Value::Real(*r),
        Expr::Str(s) => Value::Text(s.clone()),
        Expr::Bool(b) => Value::Bool(*b),
        _ => return None,
    })
}

/// Linear interpolation of where `lit` falls between `lo` and `hi`,
/// both expressed as order-preserving byte sequences. Returns a value
/// in `[0.0, 1.0]`. For string columns this is a coarse heuristic
/// (byte-distance ≠ semantic distance), but good enough for histogram
/// bucket-fraction estimates.
fn interpolate(lo: &[u8], hi: &[u8], lit: &[u8]) -> f64 {
    // Find the first byte position where any two of (lo, hi, lit)
    // differ. Before that position the prefix is identical and
    // contributes nothing to the position.
    let max_len = lo.len().max(hi.len()).max(lit.len());
    if max_len == 0 {
        return 0.0;
    }
    // Take the byte at each position (0 past the end) and treat the
    // suffixes as base-256 numbers. We only need the first few
    // differentiating bytes — once the byte at position k differs
    // among the three, the rest of the bytes barely shift the ratio.
    // Walking only the first 8 differing bytes keeps this O(1).
    let mut start = 0usize;
    while start < max_len {
        let l = lo.get(start).copied().unwrap_or(0);
        let h = hi.get(start).copied().unwrap_or(0);
        let m = lit.get(start).copied().unwrap_or(0);
        if l != h || l != m {
            break;
        }
        start += 1;
    }
    let take = (max_len - start).min(8);
    if take == 0 {
        return 0.0;
    }
    let bytes_to_u64 = |s: &[u8], off: usize, len: usize| -> u64 {
        let mut x: u64 = 0;
        for i in 0..len {
            x = (x << 8) | s.get(off + i).copied().unwrap_or(0) as u64;
        }
        x
    };
    let l = bytes_to_u64(lo, start, take);
    let h = bytes_to_u64(hi, start, take);
    let m = bytes_to_u64(lit, start, take);
    if h <= l {
        return 0.5; // degenerate bucket of one value
    }
    let frac = (m.saturating_sub(l)) as f64 / (h - l) as f64;
    frac.clamp(0.0, 1.0)
}

/// Multiply a row count by a `[0.0, 1.0]` selectivity. Rounds to the
/// nearest integer (which avoids f64-ulp noise from chained selectivity
/// products like `0.1 * 0.1` overshooting `0.01`), then clamps a
/// non-zero selectivity to at least one row — a `WHERE` clause the
/// estimator can't bound shouldn't read as "the planner guarantees zero
/// rows."
fn scale_rows(rows: u64, sel: f64) -> u64 {
    if rows == 0 {
        return 0;
    }
    let s = sel.clamp(0.0, 1.0);
    let scaled = (rows as f64 * s).round() as u64;
    if scaled == 0 && s > 0.0 {
        1
    } else {
        scaled
    }
}

/// Whether the projection contains any aggregate (`COUNT`/`SUM`/...).
fn projection_has_aggregate(projection: &Projection) -> bool {
    match projection {
        Projection::All => false,
        Projection::Items(items) => items.iter().any(|i| match i {
            SelectItem::Aggregate(_) => true,
            SelectItem::Expr(e) => expr_has_aggregate(e),
            SelectItem::Column(_) => false,
        }),
    }
}

fn expr_has_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Aggregate(_) => true,
        Expr::Unary { expr, .. } => expr_has_aggregate(expr),
        Expr::Binary { left, right, .. } => expr_has_aggregate(left) || expr_has_aggregate(right),
        Expr::IsNull { expr, .. } => expr_has_aggregate(expr),
        _ => false,
    }
}

/// A short human description of a projection: `*`, `a, b, c`, etc.
fn projection_description(projection: &Projection) -> String {
    match projection {
        Projection::All => "*".to_string(),
        Projection::Items(items) => items
            .iter()
            .map(|i| match i {
                SelectItem::Column(c) => c.to_string(),
                SelectItem::Aggregate(a) => aggregate_label(a),
                SelectItem::Expr(_) => "<expr>".to_string(),
            })
            .collect::<Vec<_>>()
            .join(", "),
    }
}

fn aggregate_label(agg: &Aggregate) -> String {
    let f = match agg.func {
        AggregateFunc::Count => "COUNT",
        AggregateFunc::Sum => "SUM",
        AggregateFunc::Avg => "AVG",
        AggregateFunc::Min => "MIN",
        AggregateFunc::Max => "MAX",
    };
    match &agg.arg {
        AggregateArg::Star => format!("{f}(*)"),
        AggregateArg::Column(c) => format!("{f}({c})"),
    }
}

/// A compact textual rendering of `expr` — close enough to the SQL it
/// came from to be readable in EXPLAIN, without trying to perfectly
/// re-parse to the original.
fn expr_str(expr: &Expr) -> String {
    match expr {
        Expr::Null => "NULL".to_string(),
        Expr::Integer(n) => n.to_string(),
        Expr::Real(r) => r.to_string(),
        Expr::Str(s) => format!("'{s}'"),
        Expr::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        // EXPLAIN of an unbound prepared statement: render with `?`.
        // After bind, Placeholders are gone and the literal value
        // renders via the variant above.
        Expr::Placeholder(idx) => format!("?{}", idx + 1),
        Expr::Column(c) => c.to_string(),
        Expr::Aggregate(a) => aggregate_label(a),
        Expr::Unary { op, expr } => {
            let opc = match op {
                UnaryOp::Neg => "-",
                UnaryOp::Not => "NOT ",
            };
            format!("{opc}{}", expr_str(expr))
        }
        Expr::Binary { op, left, right } => {
            let s = match op {
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
            };
            format!("({} {s} {})", expr_str(left), expr_str(right))
        }
        Expr::IsNull { expr, negated } => {
            format!(
                "{} IS {}NULL",
                expr_str(expr),
                if *negated { "NOT " } else { "" }
            )
        }
        Expr::InSubquery { expr, negated, .. } | Expr::CorrelatedInSubquery { expr, negated, .. } => {
            format!(
                "{} {}IN (subquery)",
                expr_str(expr),
                if *negated { "NOT " } else { "" }
            )
        }
        Expr::Exists(_) | Expr::CorrelatedExists(_) => "EXISTS (subquery)".to_string(),
        Expr::ScalarSubquery(_) | Expr::CorrelatedScalarSubquery(_) => "(subquery)".to_string(),
        Expr::InList { expr, values, negated, .. } => {
            let vs = values
                .iter()
                .map(expr_str)
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "{} {}IN ({vs})",
                expr_str(expr),
                if *negated { "NOT " } else { "" }
            )
        }
    }
}

/// Append one indented line to `out`.
fn push(out: &mut String, depth: usize, text: &str) {
    for _ in 0..depth {
        out.push_str("  ");
    }
    out.push_str(text);
    out.push('\n');
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selectivity_baselines() {
        let eq = Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Column(ColumnRef::bare("a"))),
            right: Box::new(Expr::Integer(1)),
        };
        assert!((selectivity(&eq, None) - 0.10).abs() < 1e-9);

        let gt = Expr::Binary {
            op: BinaryOp::Gt,
            left: Box::new(Expr::Column(ColumnRef::bare("a"))),
            right: Box::new(Expr::Integer(1)),
        };
        assert!((selectivity(&gt, None) - 1.0 / 3.0).abs() < 1e-9);

        let and = Expr::Binary {
            op: BinaryOp::And,
            left: Box::new(eq.clone()),
            right: Box::new(gt.clone()),
        };
        // 0.10 * 0.33... ≈ 0.0333
        let expected = 0.10 * (1.0 / 3.0);
        assert!((selectivity(&and, None) - expected).abs() < 1e-9);

        let or = Expr::Binary {
            op: BinaryOp::Or,
            left: Box::new(eq),
            right: Box::new(gt),
        };
        let expected = 1.0 - (1.0 - 0.10) * (1.0 - 1.0 / 3.0);
        assert!((selectivity(&or, None) - expected).abs() < 1e-9);
    }

    #[test]
    fn scale_rows_keeps_floor_of_one() {
        // Round-half-to-even would yield 0; non-zero selectivity floors to 1.
        assert_eq!(scale_rows(100, 0.001), 1);
        assert_eq!(scale_rows(0, 0.5), 0);
        assert_eq!(scale_rows(10, 1.0), 10);
        assert_eq!(scale_rows(10, 0.5), 5);
        // Chained 0.10 * 0.10 = 0.010000000000000002 in f64 — round handles
        // the noise where ceil would push to 2.
        assert_eq!(scale_rows(100, 0.10 * 0.10), 1);
    }

    #[test]
    fn group_rows_uses_sqrt() {
        assert_eq!(group_rows_estimate(0, 1), 0);
        assert_eq!(group_rows_estimate(1, 1), 1);
        assert_eq!(group_rows_estimate(100, 1), 10);
        assert_eq!(group_rows_estimate(10_000, 1), 100);
    }

    #[test]
    fn expr_str_renders_common_shapes() {
        let e = Expr::Binary {
            op: BinaryOp::Gt,
            left: Box::new(Expr::Column(ColumnRef::bare("a"))),
            right: Box::new(Expr::Integer(5)),
        };
        assert_eq!(expr_str(&e), "(a > 5)");

        let n = Expr::IsNull {
            expr: Box::new(Expr::Column(ColumnRef::bare("a"))),
            negated: true,
        };
        assert_eq!(expr_str(&n), "a IS NOT NULL");
    }
}
