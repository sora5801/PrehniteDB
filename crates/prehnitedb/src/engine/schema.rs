//! Table schemas — the description of a table that the catalog stores.

use crate::engine::value::Type;

/// One column of a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub ty: Type,
    /// `NOT NULL` constraint (v0.43). Set by either an explicit
    /// `NOT NULL` clause or the implicit one of `PRIMARY KEY`. INSERT
    /// and UPDATE reject a NULL value for this column.
    pub not_null: bool,
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
