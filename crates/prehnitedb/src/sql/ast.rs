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
        table: String,
        projection: Projection,
        filter: Option<Expr>,
        order_by: Vec<OrderKey>,
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

/// What a `SELECT` returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Projection {
    /// `SELECT *`
    All,
    /// `SELECT a, b, c`
    Columns(Vec<String>),
    /// `SELECT COUNT(*), SUM(x)` — whole-table aggregates, one result row.
    Aggregates(Vec<Aggregate>),
}

/// An aggregate call in a `SELECT` list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Aggregate {
    pub func: AggregateFunc,
    pub arg: AggregateArg,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

/// The argument of an aggregate: `*` or a single column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregateArg {
    Star,
    Column(String),
}

/// One `ORDER BY` key: a column and a direction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderKey {
    pub column: String,
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
    Column(String),
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
