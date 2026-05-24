//! The abstract syntax tree produced by the parser and consumed by the engine.
//!
//! These types describe *shape*, not *meaning*: the parser guarantees a query
//! is well-formed, while the engine decides whether it is valid (does the table
//! exist? do the types line up?). Names here are stored exactly as written.

/// One parsed SQL statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    CreateTable {
        name: String,
        columns: Vec<ColumnDef>,
    },
    DropTable {
        name: String,
    },
    /// `CREATE INDEX name ON table (col1, col2, ...)`
    CreateIndex {
        name: String,
        table: String,
        columns: Vec<String>,
    },
    /// `DROP INDEX name`
    DropIndex {
        name: String,
    },
    Insert {
        table: String,
        /// Explicit column list, or `None` for "every column, in order".
        columns: Option<Vec<String>>,
        /// One `Vec<Expr>` per `(...)` tuple after `VALUES`.
        rows: Vec<Vec<Expr>>,
    },
    Select {
        from: FromClause,
        projection: Projection,
        filter: Option<Expr>,
        group_by: Vec<ColumnRef>,
        having: Option<Expr>,
        order_by: Vec<OrderKey>,
        /// `LIMIT` — the maximum number of rows to return, if given.
        limit: Option<u64>,
        /// `OFFSET` — rows to skip before the first returned, if given.
        offset: Option<u64>,
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
    /// `VACUUM` — rebuild the database file compactly.
    Vacuum,
    /// `EXPLAIN [ANALYZE] <select>` — describe the operator tree the
    /// planner + executor would build, with cardinality estimates. When
    /// `analyze` is false the inner statement is not executed; when
    /// true it *is* run, and each line of the output also carries the
    /// observed `actual` row count and a total execution-time footer.
    Explain {
        inner: Box<Statement>,
        analyze: bool,
    },
    /// `BEGIN` — open an explicit multi-statement transaction.
    Begin,
    /// `COMMIT` — durably commit the open transaction.
    Commit,
    /// `ROLLBACK` — discard the open transaction's changes.
    Rollback,
}

/// The `FROM` clause: a first table, then zero or more joins applied left to
/// right — `a JOIN b JOIN c` is `(a JOIN b) JOIN c`.
#[derive(Debug, Clone, PartialEq)]
pub struct FromClause {
    pub table: TableRef,
    pub joins: Vec<Join>,
}

/// A table named in a `FROM` clause, with an optional alias.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRef {
    pub name: String,
    /// An alias (`FROM users u`), or `None` to refer to the table by name.
    pub alias: Option<String>,
}

impl TableRef {
    /// The name a qualified column reference must use for this table — its
    /// alias if it has one, otherwise the table name itself.
    pub fn qualifier(&self) -> &str {
        self.alias.as_deref().unwrap_or(&self.name)
    }
}

/// One `JOIN` appended to a `FROM` clause.
#[derive(Debug, Clone, PartialEq)]
pub struct Join {
    pub kind: JoinKind,
    pub table: TableRef,
    /// The `ON` predicate; `None` only for `CROSS JOIN`.
    pub on: Option<Expr>,
}

/// The flavour of a join.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    /// `INNER JOIN` — only rows with a match on both sides.
    Inner,
    /// `LEFT JOIN` — every left row, with `NULL`s where the right has no match.
    Left,
    /// `CROSS JOIN` — every left row paired with every right row.
    Cross,
    /// **Semi-join** — each left row at most once, when *some* right row
    /// satisfies the `ON` predicate. Output is left columns only — no
    /// right columns, no `NULL`-padding. Executor-internal: the parser
    /// never emits this; the planner mints it when rewriting a
    /// correlated `EXISTS` subquery into a join.
    Semi,
    /// **Anti-join** — each left row once, when *no* right row satisfies
    /// the `ON` predicate. Output is left columns only. Planner-only,
    /// for `NOT EXISTS` rewrites.
    Anti,
}

/// A reference to a column, optionally qualified by a table name or alias:
/// `id`, or `users.id`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnRef {
    /// The table qualifier, if one was written (`users` in `users.id`).
    pub table: Option<String>,
    pub name: String,
}

impl ColumnRef {
    /// A bare, unqualified reference to `name`.
    pub fn bare(name: impl Into<String>) -> ColumnRef {
        ColumnRef {
            table: None,
            name: name.into(),
        }
    }
}

impl std::fmt::Display for ColumnRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.table {
            Some(table) => write!(f, "{table}.{}", self.name),
            None => f.write_str(&self.name),
        }
    }
}

/// A column declaration inside `CREATE TABLE`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: String,
    pub ty: TypeName,
}

/// A column type as written in SQL text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeName {
    Int,
    Text,
    Real,
    Bool,
}

/// What a `SELECT` returns: `*`, or a list of items each of which is a plain
/// column, an aggregate call, or any other expression.
#[derive(Debug, Clone, PartialEq)]
pub enum Projection {
    /// `SELECT *`
    All,
    /// `SELECT a, COUNT(*), ...`
    Items(Vec<SelectItem>),
}

/// One entry in a `SELECT` list.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectItem {
    Column(ColumnRef),
    Aggregate(Aggregate),
    /// Any other expression — arithmetic, a scalar subquery, a literal. The
    /// parser lowers a bare column or aggregate to one of the variants above;
    /// this catches the rest.
    Expr(Expr),
}

/// An aggregate call in a `SELECT` list.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Aggregate {
    pub func: AggregateFunc,
    pub arg: AggregateArg,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggregateFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

/// The argument of an aggregate: `*` or a single column.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AggregateArg {
    Star,
    Column(ColumnRef),
}

/// One `ORDER BY` key: a column and a direction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderKey {
    pub column: ColumnRef,
    pub descending: bool,
}

/// A scalar expression: a literal, a column reference, or an operation.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Null,
    Integer(i64),
    Real(f64),
    Str(String),
    Bool(bool),
    Column(ColumnRef),
    /// An aggregate call, e.g. `COUNT(*)` or `SUM(amount)`. Valid only in a
    /// `SELECT` list or a `HAVING` clause; the executor rejects it elsewhere.
    Aggregate(Aggregate),
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    Binary {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    /// `expr IS NULL` (or `IS NOT NULL` when `negated`).
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    /// `expr IN (subquery)` or `expr NOT IN (subquery)`. The subquery is
    /// uncorrelated: it executes once before the row loop, and the executor
    /// rewrites this node into [`Expr::InList`] with the materialised values.
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<Statement>,
        negated: bool,
    },
    /// `EXISTS (subquery)`. The parser writes this; the executor pre-evaluates
    /// it and rewrites the node to `Expr::Bool(any_rows)`.
    Exists(Box<Statement>),
    /// `(SELECT ...)` used as a value. The parser writes this; the executor
    /// pre-evaluates it and rewrites the node to a literal `Expr` variant.
    ScalarSubquery(Box<Statement>),
    /// Executor-internal: the resolved form of [`Expr::InSubquery`] once its
    /// subquery has run. `has_null` records whether any subquery row was
    /// `NULL`, so the standard three-valued logic applies. Never produced by
    /// the parser.
    InList {
        expr: Box<Expr>,
        values: Vec<Expr>,
        has_null: bool,
        negated: bool,
    },
    /// Executor-internal: a [`Expr::Exists`] whose subquery references an
    /// outer-query column. v0.31 evaluates these per outer row instead of
    /// pre-evaluating, by substituting the outer references with the
    /// current row's values and running the now-uncorrelated subquery.
    /// Never produced by the parser.
    CorrelatedExists(Box<Statement>),
    /// Executor-internal: a [`Expr::ScalarSubquery`] whose subquery
    /// references an outer-query column. Same per-row resolution.
    CorrelatedScalarSubquery(Box<Statement>),
    /// Executor-internal: an [`Expr::InSubquery`] whose subquery
    /// references an outer-query column. Same per-row resolution.
    CorrelatedInSubquery {
        expr: Box<Expr>,
        subquery: Box<Statement>,
        negated: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    /// Arithmetic negation, `-x`.
    Neg,
    /// Logical negation, `NOT x`.
    Not,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
}
