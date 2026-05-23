//! Columnar batches — the unit a vectorised operator passes around.
//!
//! A [`ColumnBatch`] is one chunk of up to [`BATCH_SIZE`] rows in
//! **struct-of-arrays** layout: each output column is its own typed value
//! array, paired with a packed null bitmap. The row-at-a-time `Operator`
//! still works for joins, sort, and grouping (which need full rows); the
//! batched path used by simple `SELECT`s — scan, filter, project, limit —
//! moves a whole batch through the tree at once, so dispatch overhead is
//! amortised, the per-column loops stay in tight cache-friendly inner
//! traversals, and a typed array is laid out the way a vector unit would
//! prefer it.
//!
//! NULL is tracked out-of-band: every column carries a [`NullMask`] of one
//! bit per row, `1` meaning the value at that position is valid and `0`
//! meaning it is `NULL`. The underlying typed slot at a null position holds
//! whatever the column's zero is — it is never read. This is the layout the
//! Apache Arrow family uses, and it lets a columnwise predicate scan a
//! contiguous slice of one type without branching on nullability.

use crate::engine::value::{Type, Value};
use crate::error::{Error, Result};

/// The size of one batch, in rows. 1024 is the value most vectorised engines
/// (DuckDB, Polars/Arrow with default settings) settle on: large enough that
/// per-call dispatch is dwarfed by the work, small enough that a batch fits
/// in L1 alongside a few intermediate columns.
pub const BATCH_SIZE: usize = 1024;

/// One contiguous chunk of rows, laid out column-by-column.
#[derive(Debug, Clone)]
pub struct ColumnBatch {
    /// One [`Column`] per output position.
    pub columns: Vec<Column>,
    /// The number of valid rows in this batch. Every column's `values` and
    /// `nulls` have at least this many entries; trailing slots are unused.
    pub n_rows: usize,
}

impl ColumnBatch {
    /// An empty batch typed for the given output columns. The columns are
    /// pre-sized for [`BATCH_SIZE`] rows so a builder loop avoids the early
    /// reallocations that an empty `Vec::push` would otherwise trigger.
    pub fn with_types(types: &[Type]) -> ColumnBatch {
        ColumnBatch {
            columns: types.iter().map(|&ty| Column::empty(ty)).collect(),
            n_rows: 0,
        }
    }

    /// Append one row of values, one per column. The types must match the
    /// batch's per-column type, or an error is returned. `NULL` matches any
    /// column type.
    pub fn push_row(&mut self, row: &[Value]) -> Result<()> {
        if row.len() != self.columns.len() {
            return Err(Error::corruption(format!(
                "batch row has {} value(s) but the batch has {} column(s)",
                row.len(),
                self.columns.len()
            )));
        }
        for (column, value) in self.columns.iter_mut().zip(row.iter()) {
            column.push_value(value)?;
        }
        self.n_rows += 1;
        Ok(())
    }

    /// Reconstruct one row of [`Value`]s at position `i` — used by the
    /// `BatchToRow` adapter to feed the row-at-a-time pipeline above the
    /// vectorised pipeline.
    pub fn row_at(&self, i: usize) -> Vec<Value> {
        self.columns.iter().map(|col| col.value_at(i)).collect()
    }

    /// True iff this batch has no rows.
    pub fn is_empty(&self) -> bool {
        self.n_rows == 0
    }
}

/// One typed column of a batch: a value array plus a null mask.
#[derive(Debug, Clone)]
pub enum Column {
    Int {
        values: Vec<i64>,
        nulls: NullMask,
    },
    Real {
        values: Vec<f64>,
        nulls: NullMask,
    },
    Text {
        values: Vec<String>,
        nulls: NullMask,
    },
    Bool {
        values: Vec<bool>,
        nulls: NullMask,
    },
}

impl Column {
    /// An empty column of `ty`, pre-allocated for [`BATCH_SIZE`] rows.
    pub fn empty(ty: Type) -> Column {
        match ty {
            Type::Int => Column::Int {
                values: Vec::with_capacity(BATCH_SIZE),
                nulls: NullMask::with_capacity(BATCH_SIZE),
            },
            Type::Real => Column::Real {
                values: Vec::with_capacity(BATCH_SIZE),
                nulls: NullMask::with_capacity(BATCH_SIZE),
            },
            Type::Text => Column::Text {
                values: Vec::with_capacity(BATCH_SIZE),
                nulls: NullMask::with_capacity(BATCH_SIZE),
            },
            Type::Bool => Column::Bool {
                values: Vec::with_capacity(BATCH_SIZE),
                nulls: NullMask::with_capacity(BATCH_SIZE),
            },
        }
    }

    /// The column's logical row count. All variants of one column agree.
    pub fn len(&self) -> usize {
        match self {
            Column::Int { nulls, .. }
            | Column::Real { nulls, .. }
            | Column::Text { nulls, .. }
            | Column::Bool { nulls, .. } => nulls.n_rows,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The column's data type.
    pub fn ty(&self) -> Type {
        match self {
            Column::Int { .. } => Type::Int,
            Column::Real { .. } => Type::Real,
            Column::Text { .. } => Type::Text,
            Column::Bool { .. } => Type::Bool,
        }
    }

    /// Append a single value to the column. `NULL` is accepted as a value of
    /// any type; otherwise the value's type must match the column's.
    pub fn push_value(&mut self, value: &Value) -> Result<()> {
        match (self, value) {
            (Column::Int { values, nulls }, Value::Int(n)) => {
                values.push(*n);
                nulls.push(true);
            }
            (Column::Int { values, nulls }, Value::Null) => {
                values.push(0);
                nulls.push(false);
            }
            (Column::Real { values, nulls }, Value::Real(r)) => {
                values.push(*r);
                nulls.push(true);
            }
            // Integer widens into REAL — same rule as scalar `coerce`.
            (Column::Real { values, nulls }, Value::Int(n)) => {
                values.push(*n as f64);
                nulls.push(true);
            }
            (Column::Real { values, nulls }, Value::Null) => {
                values.push(0.0);
                nulls.push(false);
            }
            (Column::Text { values, nulls }, Value::Text(s)) => {
                values.push(s.clone());
                nulls.push(true);
            }
            (Column::Text { values, nulls }, Value::Null) => {
                values.push(String::new());
                nulls.push(false);
            }
            (Column::Bool { values, nulls }, Value::Bool(b)) => {
                values.push(*b);
                nulls.push(true);
            }
            (Column::Bool { values, nulls }, Value::Null) => {
                values.push(false);
                nulls.push(false);
            }
            (column, other) => {
                return Err(Error::exec(format!(
                    "value of type {} does not fit a {} column",
                    other.type_name(),
                    column.ty()
                )));
            }
        }
        Ok(())
    }

    /// The [`Value`] at row `i`. `NULL` if the row's null bit is clear;
    /// otherwise the typed value at that index.
    pub fn value_at(&self, i: usize) -> Value {
        match self {
            Column::Int { values, nulls } => {
                if nulls.is_valid(i) {
                    Value::Int(values[i])
                } else {
                    Value::Null
                }
            }
            Column::Real { values, nulls } => {
                if nulls.is_valid(i) {
                    Value::Real(values[i])
                } else {
                    Value::Null
                }
            }
            Column::Text { values, nulls } => {
                if nulls.is_valid(i) {
                    Value::Text(values[i].clone())
                } else {
                    Value::Null
                }
            }
            Column::Bool { values, nulls } => {
                if nulls.is_valid(i) {
                    Value::Bool(values[i])
                } else {
                    Value::Null
                }
            }
        }
    }

    /// The column's null mask, by reference. Lets a caller read the per-row
    /// validity without matching on the column variant.
    pub fn nulls(&self) -> &NullMask {
        match self {
            Column::Int { nulls, .. }
            | Column::Real { nulls, .. }
            | Column::Text { nulls, .. }
            | Column::Bool { nulls, .. } => nulls,
        }
    }
}

/// One bit per row: `1` means valid (the typed slot holds a real value),
/// `0` means `NULL` (the typed slot is unused). Packed into u64 words so a
/// 1024-row mask is 128 bytes — well within L1.
#[derive(Debug, Clone)]
pub struct NullMask {
    bits: Vec<u64>,
    n_rows: usize,
}

impl NullMask {
    /// An empty mask sized to hold at least `cap` rows without reallocating
    /// its bit buffer.
    pub fn with_capacity(cap: usize) -> NullMask {
        let words = (cap + 63) / 64;
        NullMask {
            bits: Vec::with_capacity(words),
            n_rows: 0,
        }
    }

    /// A mask of `n_rows` bits, every one of them set (every row valid).
    pub fn all_valid(n_rows: usize) -> NullMask {
        let words = (n_rows + 63) / 64;
        let mut bits = vec![!0u64; words];
        let trailing = n_rows % 64;
        if trailing != 0 {
            // Clear the unused high bits of the last word so it reads as zero
            // for any row index past `n_rows`.
            let last = bits.len() - 1;
            bits[last] = (1u64 << trailing) - 1;
        }
        NullMask { bits, n_rows }
    }

    /// Append one bit to the end of the mask.
    pub fn push(&mut self, valid: bool) {
        let i = self.n_rows;
        let word = i / 64;
        if word >= self.bits.len() {
            self.bits.push(0);
        }
        if valid {
            self.bits[word] |= 1u64 << (i % 64);
        }
        self.n_rows += 1;
    }

    /// `true` if row `i` holds a real value; `false` if it is `NULL`.
    pub fn is_valid(&self, i: usize) -> bool {
        debug_assert!(i < self.n_rows);
        (self.bits[i / 64] >> (i % 64)) & 1 == 1
    }

    pub fn len(&self) -> usize {
        self.n_rows
    }

    pub fn is_empty(&self) -> bool {
        self.n_rows == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_batch_round_trip() {
        let batch = ColumnBatch::with_types(&[Type::Int, Type::Text]);
        assert_eq!(batch.n_rows, 0);
        assert_eq!(batch.columns.len(), 2);
        assert!(batch.is_empty());
    }

    #[test]
    fn batch_push_and_row_at_round_trips_values() {
        let mut batch = ColumnBatch::with_types(&[Type::Int, Type::Text, Type::Bool]);
        batch
            .push_row(&[Value::Int(1), Value::Text("a".into()), Value::Bool(true)])
            .unwrap();
        batch
            .push_row(&[Value::Null, Value::Text("b".into()), Value::Null])
            .unwrap();
        batch
            .push_row(&[Value::Int(3), Value::Null, Value::Bool(false)])
            .unwrap();
        assert_eq!(batch.n_rows, 3);
        assert_eq!(
            batch.row_at(0),
            vec![Value::Int(1), Value::Text("a".into()), Value::Bool(true)]
        );
        assert_eq!(
            batch.row_at(1),
            vec![Value::Null, Value::Text("b".into()), Value::Null]
        );
        assert_eq!(
            batch.row_at(2),
            vec![Value::Int(3), Value::Null, Value::Bool(false)]
        );
    }

    #[test]
    fn integer_widens_into_a_real_column() {
        let mut batch = ColumnBatch::with_types(&[Type::Real]);
        batch.push_row(&[Value::Int(7)]).unwrap();
        assert_eq!(batch.row_at(0), vec![Value::Real(7.0)]);
    }

    #[test]
    fn null_mask_round_trips_a_pattern_across_word_boundaries() {
        let mut mask = NullMask::with_capacity(128);
        // Alternate valid/null past one u64 boundary to catch off-by-one
        // indexing into the bit buffer.
        for i in 0..100 {
            mask.push(i % 2 == 0);
        }
        assert_eq!(mask.len(), 100);
        for i in 0..100 {
            assert_eq!(mask.is_valid(i), i % 2 == 0, "row {i}");
        }
    }

    #[test]
    fn all_valid_mask_marks_every_row_valid() {
        let mask = NullMask::all_valid(70);
        for i in 0..70 {
            assert!(mask.is_valid(i), "row {i}");
        }
    }

    #[test]
    fn pushing_wrong_type_into_a_column_errors() {
        let mut batch = ColumnBatch::with_types(&[Type::Int]);
        assert!(batch.push_row(&[Value::Text("nope".into())]).is_err());
    }
}
