//! Table schemas — the description of a table that the catalog stores.

use crate::engine::value::Type;

/// One column of a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub ty: Type,
}

/// A secondary index over a single column of a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Index {
    pub name: String,
    /// Position of the indexed column within the table's `columns`.
    pub column: usize,
    /// Root page of the index's own B+tree.
    pub root: u32,
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
    /// Secondary indexes defined on this table.
    pub indexes: Vec<Index>,
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

    /// An index covering column `column`, if one exists. When several do, the
    /// first declared is returned.
    pub fn index_on(&self, column: usize) -> Option<&Index> {
        self.indexes.iter().find(|i| i.column == column)
    }
}
