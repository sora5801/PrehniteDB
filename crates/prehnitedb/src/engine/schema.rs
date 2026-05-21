//! Table schemas — the description of a table that the catalog stores.

use crate::engine::value::Type;

/// One column of a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub ty: Type,
}

/// Everything the engine needs to know about a table: its columns, where its
/// data lives, and how to number the next row.
#[derive(Debug, Clone, PartialEq)]
pub struct Schema {
    pub name: String,
    pub columns: Vec<Column>,
    /// Root page of this table's data B+tree.
    pub root: u32,
    /// The rowid (B+tree key) to assign to the next inserted row. Rowids are
    /// never reused, so this only ever climbs.
    pub next_rowid: u64,
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
