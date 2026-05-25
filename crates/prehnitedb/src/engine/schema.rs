//! Table schemas — the description of a table that the catalog stores.

use crate::engine::value::{Type, Value};

/// One column of a table.
#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    pub name: String,
    pub ty: Type,
    /// `NOT NULL` constraint (v0.43). Set by either an explicit
    /// `NOT NULL` clause or the implicit one of `PRIMARY KEY`. INSERT
    /// and UPDATE reject a NULL value for this column.
    pub not_null: bool,
    /// `REFERENCES <table>(<column>)` — column-level foreign key
    /// (v0.45). Set at `CREATE TABLE`; consulted at INSERT/UPDATE on
    /// this row (the value must exist in the parent's column) and at
    /// DELETE/UPDATE of the parent row (RESTRICT — refuse if any
    /// child still references it). `None` means no FK on this column.
    pub foreign_key: Option<ForeignKeyTarget>,
    /// Column statistics gathered by `ANALYZE <table>` (v0.47).
    /// `None` until the first ANALYZE; otherwise the snapshot the
    /// planner's selectivity estimator consults for sharper
    /// `(rows: N)` cardinality predictions. Becomes stale when the
    /// table is mutated; a future auto-analyze could refresh.
    pub stats: Option<ColumnStats>,
}

/// The target a foreign-key column points at: a `(table, column)`
/// pair, both names stored exactly as written, plus an `on_delete`
/// referential action (v0.48). The parent column is always either
/// `PRIMARY KEY` or `UNIQUE` (the planner validates this at
/// `CREATE TABLE`), so a unique secondary index already exists for
/// the lookup the FK check needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKeyTarget {
    pub table: String,
    pub column: String,
    /// What happens to this child row when the referenced parent row
    /// is deleted (v0.48). Defaults to [`ForeignKeyAction::Restrict`]
    /// — the v0.45 behaviour — when the user writes no `ON DELETE`
    /// clause.
    pub on_delete: ForeignKeyAction,
}

/// The action to take on a child row when its parent is deleted
/// (v0.48). SQL standard names:
///
/// - **RESTRICT / NO ACTION**: refuse the parent delete if any
///   child still references it. v0.45's default and the only mode
///   v0.45 supported.
/// - **CASCADE**: delete the child row too. The cascade then
///   recurses through any FKs the child *itself* has — naturally,
///   because the child delete goes through the same engine path.
/// - **SET NULL**: leave the child row in place but set its FK
///   column to NULL. Runtime error if the child column is also
///   `NOT NULL` (a sane CREATE TABLE pattern would catch this at
///   declaration time, but v0.48 leaves the check to runtime so
///   the user can declare schemas in any order).
///
/// v0.48 implements ON DELETE only — `ON UPDATE` of a parent's
/// referenced column always RESTRICTs, regardless of this field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForeignKeyAction {
    Restrict,
    Cascade,
    SetNull,
}

/// Per-column statistics computed by `ANALYZE <table>` (v0.47) and
/// consulted by the planner's selectivity estimator.
///
/// Three pieces:
/// - `n_distinct` for equality estimates (`col = lit` → `1 / n_distinct`).
/// - `null_count` + `total_rows` for `IS NULL` estimates
///   (`null_frac = null_count / total_rows`).
/// - An equi-depth `histogram` for range estimates: each bucket
///   covers roughly `total / buckets` non-NULL rows, with bucket
///   widths varying so each holds the same row count. A `col > lit`
///   selectivity walks the buckets, summing those fully above the
///   literal and interpolating the bucket that straddles it.
///
/// The histogram is built over the **non-NULL** values. NULLs are
/// tracked separately via `null_count`, matching SQL's three-valued
/// `WHERE` (a comparison against `NULL` is `NULL`, never `TRUE`).
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnStats {
    pub n_distinct: u64,
    pub null_count: u64,
    pub total_rows: u64,
    pub histogram: Vec<HistogramBucket>,
}

/// One equi-depth bucket: every non-NULL value in `[lower, upper]`
/// (inclusive on both ends) belongs to this bucket; the bucket holds
/// `count` rows. `lower == upper` for a bucket of a single repeated
/// value. v0.47 builds buckets in column-type order using the same
/// PartialOrd `Value` does.
#[derive(Debug, Clone, PartialEq)]
pub struct HistogramBucket {
    pub lower: Value,
    pub upper: Value,
    pub count: u64,
}

/// A secondary index over one or more columns of a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Index {
    pub name: String,
    /// Positions of the indexed columns within the table's `columns`, in index
    /// order. The first is the *leading* column — a query must constrain it for
    /// the index to be usable.
    pub columns: Vec<usize>,
    /// Root page of the index's own B+tree.
    pub root: u32,
    /// `UNIQUE` flag (v0.43). When true, the index is auto-created for
    /// a `PRIMARY KEY` or `UNIQUE` column and the B+tree rejects
    /// duplicate keys (with NULL values exempt — SQL standard: multiple
    /// NULLs are allowed in a UNIQUE column). Non-unique indexes
    /// encode the rowid as part of the key so duplicates of the column
    /// value separate at the B+tree level; unique indexes encode the
    /// column value alone, so duplicates collide.
    pub unique: bool,
    /// v0.58: set while a `CREATE INDEX` is mid-build. The planner
    /// must not pick a building index as an access path (it's
    /// partially populated), but writers must still maintain it
    /// (INSERT/UPDATE/DELETE update entries) so the catch-up scan
    /// and concurrent writers converge to a complete index. After
    /// the build's final phase, this flips to `false` under a brief
    /// exclusive lock and the index becomes usable.
    ///
    /// On `Database::open`, any index still marked `is_building =
    /// true` is dropped — a previous run crashed mid-build, the
    /// build can't be resumed safely, so we discard the partial
    /// state and the user re-issues CREATE INDEX.
    pub is_building: bool,
}

/// Everything the engine needs to know about a table: its columns, its
/// secondary indexes, where its data lives, and how to number the next row.
#[derive(Debug, Clone, PartialEq)]
pub struct Schema {
    pub name: String,
    pub columns: Vec<Column>,
    /// Root page of this table's data B+tree.
    pub root: u32,
    /// The rowid (B+tree key) to assign to the next inserted row. Rowids are
    /// never reused, so this only ever climbs.
    pub next_rowid: u64,
    /// Number of rows currently in the table. Maintained by INSERT and DELETE
    /// and persisted in the catalog; the planner uses it to score join orders.
    pub row_count: u64,
    /// Secondary indexes defined on this table.
    pub indexes: Vec<Index>,
    /// Position of the `PRIMARY KEY` column within `columns`, if one is
    /// declared (v0.43). `None` for tables without a PK. At most one
    /// column per table may be PK; the PK column is automatically
    /// `not_null = true` and gets an auto-created unique index named
    /// `_pk_<table>`.
    pub primary_key_column: Option<usize>,
    /// Number of row-mutating statements (INSERT/UPDATE/DELETE)
    /// applied to this table since the last `ANALYZE` (v0.49). The
    /// background reclaimer thread (`prehnited`) consults this against
    /// a Postgres-style threshold `50 + 0.10 * prior_row_count` and
    /// triggers an auto-analyze when stats are too stale to drive
    /// good plans.
    pub mutations_since_analyze: u64,
}

impl Schema {
    /// Position of the column named `name`, if the table has one.
    /// Names are matched case-sensitively in v0.1.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    /// The column names, in declaration order.
    pub fn column_names(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.name.clone()).collect()
    }
}
