//! Binary encoding of rows and schemas — the bridge between typed values and
//! the raw byte strings the B+tree stores.
//!
//! A **row** is a 16-byte MVCC header — `tx_min` (the TX that inserted the
//! row) followed by `tx_max` (the TX that deleted it, 0 = still live) — and
//! then a tag-prefixed sequence of values, one per column. A **schema** is
//! the value the catalog stores under a table's name. Everything is
//! little-endian and self-describing enough to be decoded back with only
//! the column count (for rows) or nothing at all (for schemas).

use crate::engine::schema::{Column, Index, Schema};
use crate::engine::value::{Type, Value};
use crate::error::{Error, Result};

const TAG_NULL: u8 = 0;
const TAG_INT: u8 = 1;
const TAG_REAL: u8 = 2;
const TAG_TEXT: u8 = 3;
const TAG_BOOL: u8 = 4;

/// The 8-byte big-endian B+tree key for a rowid. Big-endian so that the tree's
/// byte ordering matches numeric ordering.
pub fn rowid_key(id: u64) -> [u8; 8] {
    id.to_be_bytes()
}

/// One row as stored in a table B+tree: the MVCC visibility header plus the
/// decoded column values. `tx_min` is the transaction that created the row;
/// `tx_max == 0` means it has not been deleted, and any other value is the
/// transaction that marked it deleted. See [`crate::engine::database::Snapshot`]
/// for the visibility rules over these fields.
#[derive(Debug, Clone, PartialEq)]
pub struct RowRecord {
    pub tx_min: u64,
    pub tx_max: u64,
    pub values: Vec<Value>,
}

/// Encode one row — `(tx_min, tx_max, values)` — into the bytes stored as a
/// B+tree value. Writers stamp `tx_min = current_tx, tx_max = 0` on insert
/// and re-emit a deleted version with `tx_max = current_tx` on logical
/// delete.
pub fn encode_row(tx_min: u64, tx_max: u64, values: &[Value]) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + values.len() * 4);
    out.extend_from_slice(&tx_min.to_le_bytes());
    out.extend_from_slice(&tx_max.to_le_bytes());
    for value in values {
        match value {
            Value::Null => out.push(TAG_NULL),
            Value::Int(n) => {
                out.push(TAG_INT);
                out.extend_from_slice(&n.to_le_bytes());
            }
            Value::Real(r) => {
                out.push(TAG_REAL);
                out.extend_from_slice(&r.to_bits().to_le_bytes());
            }
            Value::Text(s) => {
                out.push(TAG_TEXT);
                out.extend_from_slice(&(s.len() as u32).to_le_bytes());
                out.extend_from_slice(s.as_bytes());
            }
            Value::Bool(b) => {
                out.push(TAG_BOOL);
                out.push(u8::from(*b));
            }
        }
    }
    out
}

/// Encode a sequence of [`Value`]s with the same tag-and-bytes format
/// as [`encode_row`] but *without* the MVCC header — used by v0.32's
/// external-sort spill files, which write rows to disk during a sort
/// and never need MVCC visibility metadata.
pub fn encode_values(values: &[Value]) -> Vec<u8> {
    let mut out = Vec::with_capacity(values.len() * 4);
    for value in values {
        match value {
            Value::Null => out.push(TAG_NULL),
            Value::Int(n) => {
                out.push(TAG_INT);
                out.extend_from_slice(&n.to_le_bytes());
            }
            Value::Real(r) => {
                out.push(TAG_REAL);
                out.extend_from_slice(&r.to_bits().to_le_bytes());
            }
            Value::Text(s) => {
                out.push(TAG_TEXT);
                out.extend_from_slice(&(s.len() as u32).to_le_bytes());
                out.extend_from_slice(s.as_bytes());
            }
            Value::Bool(b) => {
                out.push(TAG_BOOL);
                out.push(u8::from(*b));
            }
        }
    }
    out
}

/// Decode `column_count` values written by [`encode_values`].
pub fn decode_values(bytes: &[u8], column_count: usize) -> Result<Vec<Value>> {
    let mut reader = Reader::new(bytes);
    let mut values = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        let value = match reader.u8()? {
            TAG_NULL => Value::Null,
            TAG_INT => Value::Int(reader.i64()?),
            TAG_REAL => Value::Real(f64::from_bits(reader.u64()?)),
            TAG_TEXT => {
                let len = reader.u32()? as usize;
                let raw = reader.take(len)?;
                Value::Text(
                    String::from_utf8(raw.to_vec())
                        .map_err(|_| Error::corruption("spilled row holds non-UTF-8 text"))?,
                )
            }
            TAG_BOOL => Value::Bool(reader.u8()? != 0),
            other => return Err(Error::corruption(format!("unknown value tag {other}"))),
        };
        values.push(value);
    }
    if !reader.is_empty() {
        return Err(Error::corruption(
            "spilled row has trailing bytes after its columns",
        ));
    }
    Ok(values)
}

/// Decode the MVCC header and the `column_count` values that follow.
pub fn decode_row(bytes: &[u8], column_count: usize) -> Result<RowRecord> {
    let mut reader = Reader::new(bytes);
    let tx_min = reader.u64()?;
    let tx_max = reader.u64()?;
    let mut values = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        let value = match reader.u8()? {
            TAG_NULL => Value::Null,
            TAG_INT => Value::Int(reader.i64()?),
            TAG_REAL => Value::Real(f64::from_bits(reader.u64()?)),
            TAG_TEXT => {
                let len = reader.u32()? as usize;
                let raw = reader.take(len)?;
                Value::Text(
                    String::from_utf8(raw.to_vec())
                        .map_err(|_| Error::corruption("row holds non-UTF-8 text"))?,
                )
            }
            TAG_BOOL => Value::Bool(reader.u8()? != 0),
            other => return Err(Error::corruption(format!("unknown value tag {other}"))),
        };
        values.push(value);
    }
    if !reader.is_empty() {
        return Err(Error::corruption(
            "row has trailing bytes after its columns",
        ));
    }
    Ok(RowRecord {
        tx_min,
        tx_max,
        values,
    })
}

/// Encode a schema into the bytes stored under the table's name in the catalog.
pub fn encode_schema(schema: &Schema) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&schema.root.to_le_bytes());
    out.extend_from_slice(&schema.next_rowid.to_le_bytes());
    write_str(&mut out, &schema.name);
    out.extend_from_slice(&(schema.columns.len() as u16).to_le_bytes());
    for column in &schema.columns {
        out.push(type_tag(column.ty));
        write_str(&mut out, &column.name);
        // v0.43 (PREHNDB7): per-column NOT NULL flag.
        out.push(u8::from(column.not_null));
        // v0.45 (PREHNDB8): per-column FOREIGN KEY target — 0 byte
        // for "no FK"; 1 byte then two strings (parent table, parent
        // column) for "FK present". v0.48 (PREHNDB10) appends a
        // 1-byte ON DELETE action tag.
        match &column.foreign_key {
            None => out.push(0),
            Some(fk) => {
                out.push(1);
                write_str(&mut out, &fk.table);
                write_str(&mut out, &fk.column);
                out.push(fk_action_tag(fk.on_delete));
            }
        }
        // v0.47 (PREHNDB9): per-column statistics — 0 byte for
        // "never ANALYZEd"; 1 byte then the stats blob otherwise.
        // Each histogram bucket value is length-prefixed (u32 byte
        // length, then `encode_values` bytes) so the reader can pull
        // them out one at a time without knowing per-type sizes.
        match &column.stats {
            None => out.push(0),
            Some(stats) => {
                out.push(1);
                out.extend_from_slice(&stats.n_distinct.to_le_bytes());
                out.extend_from_slice(&stats.null_count.to_le_bytes());
                out.extend_from_slice(&stats.total_rows.to_le_bytes());
                out.extend_from_slice(&(stats.histogram.len() as u32).to_le_bytes());
                for bucket in &stats.histogram {
                    let lower_bytes = encode_values(std::slice::from_ref(&bucket.lower));
                    out.extend_from_slice(&(lower_bytes.len() as u32).to_le_bytes());
                    out.extend_from_slice(&lower_bytes);
                    let upper_bytes = encode_values(std::slice::from_ref(&bucket.upper));
                    out.extend_from_slice(&(upper_bytes.len() as u32).to_le_bytes());
                    out.extend_from_slice(&upper_bytes);
                    out.extend_from_slice(&bucket.count.to_le_bytes());
                }
            }
        }
    }
    out.extend_from_slice(&(schema.indexes.len() as u16).to_le_bytes());
    for index in &schema.indexes {
        out.extend_from_slice(&index.root.to_le_bytes());
        out.extend_from_slice(&(index.columns.len() as u16).to_le_bytes());
        for &column in &index.columns {
            out.extend_from_slice(&(column as u16).to_le_bytes());
        }
        write_str(&mut out, &index.name);
        // v0.43 (PREHNDB7): per-index UNIQUE flag.
        out.push(u8::from(index.unique));
    }
    out.extend_from_slice(&schema.row_count.to_le_bytes());
    // v0.43 (PREHNDB7): PRIMARY KEY column position. `u16::MAX` is
    // the sentinel for "no primary key" — well outside any realistic
    // column count.
    let pk_tag = schema.primary_key_column.map(|i| i as u16).unwrap_or(u16::MAX);
    out.extend_from_slice(&pk_tag.to_le_bytes());
    out
}

/// Decode a schema previously produced by [`encode_schema`].
pub fn decode_schema(bytes: &[u8]) -> Result<Schema> {
    let mut reader = Reader::new(bytes);
    let root = reader.u32()?;
    let next_rowid = reader.u64()?;
    let name = read_str(&mut reader)?;
    let column_count = reader.u16()? as usize;
    let mut columns = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        let ty = type_from_tag(reader.u8()?)?;
        let col_name = read_str(&mut reader)?;
        let not_null = reader.u8()? != 0;
        let foreign_key = match reader.u8()? {
            0 => None,
            1 => {
                let table = read_str(&mut reader)?;
                let column = read_str(&mut reader)?;
                let on_delete = fk_action_from_tag(reader.u8()?)?;
                Some(crate::engine::schema::ForeignKeyTarget {
                    table,
                    column,
                    on_delete,
                })
            }
            other => {
                return Err(Error::corruption(format!(
                    "unknown column foreign-key tag {other}"
                )));
            }
        };
        let stats = match reader.u8()? {
            0 => None,
            1 => {
                let n_distinct = reader.u64()?;
                let null_count = reader.u64()?;
                let total_rows = reader.u64()?;
                let bucket_count = reader.u32()? as usize;
                let mut histogram = Vec::with_capacity(bucket_count);
                for _ in 0..bucket_count {
                    let lower_len = reader.u32()? as usize;
                    let lower_bytes = reader.take(lower_len)?;
                    let lower = decode_values(lower_bytes, 1)?.into_iter().next().unwrap();
                    let upper_len = reader.u32()? as usize;
                    let upper_bytes = reader.take(upper_len)?;
                    let upper = decode_values(upper_bytes, 1)?.into_iter().next().unwrap();
                    let count = reader.u64()?;
                    histogram.push(crate::engine::schema::HistogramBucket {
                        lower,
                        upper,
                        count,
                    });
                }
                Some(crate::engine::schema::ColumnStats {
                    n_distinct,
                    null_count,
                    total_rows,
                    histogram,
                })
            }
            other => {
                return Err(Error::corruption(format!(
                    "unknown column stats tag {other}"
                )));
            }
        };
        columns.push(Column {
            name: col_name,
            ty,
            not_null,
            foreign_key,
            stats,
        });
    }
    let index_count = reader.u16()? as usize;
    let mut indexes = Vec::with_capacity(index_count);
    for _ in 0..index_count {
        let root = reader.u32()?;
        let column_count = reader.u16()? as usize;
        let mut columns = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            columns.push(reader.u16()? as usize);
        }
        let name = read_str(&mut reader)?;
        let unique = reader.u8()? != 0;
        indexes.push(Index {
            name,
            columns,
            root,
            unique,
        });
    }
    let row_count = reader.u64()?;
    let pk_tag = reader.u16()?;
    let primary_key_column = if pk_tag == u16::MAX {
        None
    } else {
        Some(pk_tag as usize)
    };
    Ok(Schema {
        name,
        columns,
        root,
        next_rowid,
        row_count,
        indexes,
        primary_key_column,
    })
}

fn type_tag(ty: Type) -> u8 {
    match ty {
        Type::Int => 1,
        Type::Real => 2,
        Type::Text => 3,
        Type::Bool => 4,
    }
}

fn type_from_tag(tag: u8) -> Result<Type> {
    Ok(match tag {
        1 => Type::Int,
        2 => Type::Real,
        3 => Type::Text,
        4 => Type::Bool,
        other => return Err(Error::corruption(format!("unknown type tag {other}"))),
    })
}

fn fk_action_tag(action: crate::engine::schema::ForeignKeyAction) -> u8 {
    use crate::engine::schema::ForeignKeyAction;
    match action {
        ForeignKeyAction::Restrict => 1,
        ForeignKeyAction::Cascade => 2,
        ForeignKeyAction::SetNull => 3,
    }
}

fn fk_action_from_tag(tag: u8) -> Result<crate::engine::schema::ForeignKeyAction> {
    use crate::engine::schema::ForeignKeyAction;
    Ok(match tag {
        1 => ForeignKeyAction::Restrict,
        2 => ForeignKeyAction::Cascade,
        3 => ForeignKeyAction::SetNull,
        other => return Err(Error::corruption(format!("unknown FK action tag {other}"))),
    })
}

fn write_str(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as u16).to_le_bytes());
    out.extend_from_slice(s.as_bytes());
}

fn read_str(reader: &mut Reader) -> Result<String> {
    let len = reader.u16()? as usize;
    let raw = reader.take(len)?;
    String::from_utf8(raw.to_vec()).map_err(|_| Error::corruption("schema holds non-UTF-8 text"))
}

/// A bounds-checked cursor over a byte slice.
struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(data: &'a [u8]) -> Reader<'a> {
        Reader { data, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|&e| e <= self.data.len())
            .ok_or_else(|| Error::corruption("unexpected end of encoded data"))?;
        let slice = &self.data[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn is_empty(&self) -> bool {
        self.pos >= self.data.len()
    }
}

// --- secondary-index key encoding -----------------------------------------
//
// An index B+tree key is an order-preserving encoding of the indexed value
// followed by the 8-byte rowid of the row it points at. Order preservation
// means the tree's byte ordering matches SQL value ordering, so an equality
// lookup is a plain key-range scan. The rowid suffix keeps keys distinct when
// many rows share one column value.

const IDX_NULL: u8 = 0x00;
const IDX_BOOL: u8 = 0x01;
const IDX_INT: u8 = 0x02;
const IDX_REAL: u8 = 0x03;
const IDX_TEXT: u8 = 0x04;

/// Order-preserving encoding of one value. Concatenating these for an index's
/// columns yields a key whose byte order matches tuple order; each encoding is
/// self-delimiting, so the column boundaries stay unambiguous.
pub fn encode_index_value(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    match value {
        Value::Null => out.push(IDX_NULL),
        Value::Bool(b) => {
            out.push(IDX_BOOL);
            out.push(u8::from(*b));
        }
        Value::Int(n) => {
            out.push(IDX_INT);
            // Flipping the sign bit turns two's-complement order into the
            // unsigned big-endian byte order the B+tree compares with.
            out.extend_from_slice(&((*n as u64) ^ (1 << 63)).to_be_bytes());
        }
        Value::Real(r) => {
            out.push(IDX_REAL);
            // Remap IEEE-754 bits so byte order matches numeric order.
            let bits = r.to_bits();
            let ordered = if bits & (1 << 63) != 0 {
                !bits
            } else {
                bits | (1 << 63)
            };
            out.extend_from_slice(&ordered.to_be_bytes());
        }
        Value::Text(s) => {
            out.push(IDX_TEXT);
            // Escape interior NULs (0x00 -> 0x00 0x01) and append a 0x00 0x00
            // terminator, so the value is self-delimiting before the rowid.
            for &byte in s.as_bytes() {
                out.push(byte);
                if byte == 0x00 {
                    out.push(0x01);
                }
            }
            out.push(0x00);
            out.push(0x00);
        }
    }
    out
}

/// A full index key for a row: the order-preserving encodings of the values at
/// `columns`, concatenated in index order, followed by the 8-byte rowid key.
pub fn encode_index_key(values: &[Value], columns: &[usize], rowid_key: &[u8]) -> Vec<u8> {
    let mut key = Vec::new();
    for &column in columns {
        key.extend_from_slice(&encode_index_value(&values[column]));
    }
    key.extend_from_slice(rowid_key);
    key
}

/// The smallest byte string strictly greater than every string having `prefix`
/// as a prefix — the exclusive upper bound of an equality range scan. `None`
/// only when `prefix` is empty or entirely `0xFF`.
pub fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut bound = prefix.to_vec();
    while let Some(last) = bound.pop() {
        if last != 0xFF {
            bound.push(last + 1);
            return Some(bound);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_round_trip() {
        let values = vec![
            Value::Int(-7),
            Value::Text("hello".into()),
            Value::Null,
            Value::Bool(true),
            Value::Real(2.5),
        ];
        let encoded = encode_row(42, 0, &values);
        let record = decode_row(&encoded, 5).unwrap();
        assert_eq!(record.tx_min, 42);
        assert_eq!(record.tx_max, 0);
        assert_eq!(record.values, values);
    }

    #[test]
    fn mvcc_header_round_trips_deleted_rows() {
        // A logically-deleted row carries a non-zero tx_max — the TX that
        // tombstoned it. Visibility checks against a snapshot use it.
        let values = vec![Value::Int(1)];
        let encoded = encode_row(7, 12, &values);
        let record = decode_row(&encoded, 1).unwrap();
        assert_eq!((record.tx_min, record.tx_max), (7, 12));
        assert_eq!(record.values, values);
    }

    #[test]
    fn decode_rejects_wrong_column_count() {
        let encoded = encode_row(1, 0, &[Value::Int(1), Value::Int(2)]);
        assert!(decode_row(&encoded, 1).is_err()); // trailing bytes
        assert!(decode_row(&encoded, 3).is_err()); // truncated
    }

    fn sample_schema() -> Schema {
        Schema {
            name: "widgets".into(),
            columns: vec![
                Column {
                    name: "id".into(),
                    ty: Type::Int,
                    not_null: false,
                    foreign_key: None,
                    stats: None,
                },
                Column {
                    name: "label".into(),
                    ty: Type::Text,
                    not_null: false,
                    foreign_key: None,
                    stats: None,
                },
            ],
            root: 12,
            next_rowid: 99,
            row_count: 7,
            primary_key_column: None,
            indexes: vec![Index {
                name: "by_label".into(),
                columns: vec![1],
                root: 30,
                unique: false,
            }],
        }
    }

    #[test]
    fn schema_round_trip() {
        let schema = sample_schema();
        let encoded = encode_schema(&schema);
        assert_eq!(decode_schema(&encoded).unwrap(), schema);
    }

    #[test]
    fn index_prefixes_sort_like_their_values() {
        let ints = [
            Value::Int(i64::MIN),
            Value::Int(-1000),
            Value::Int(-1),
            Value::Int(0),
            Value::Int(1),
            Value::Int(1000),
            Value::Int(i64::MAX),
        ];
        let keys: Vec<Vec<u8>> = ints.iter().map(encode_index_value).collect();
        assert!(keys.windows(2).all(|w| w[0] < w[1]), "INT keys must ascend");

        let reals = [
            Value::Real(-2.5),
            Value::Real(-1.0),
            Value::Real(0.0),
            Value::Real(1.0),
            Value::Real(2.5),
        ];
        let keys: Vec<Vec<u8>> = reals.iter().map(encode_index_value).collect();
        assert!(
            keys.windows(2).all(|w| w[0] < w[1]),
            "REAL keys must ascend"
        );

        let texts = [
            Value::Text(String::new()),
            Value::Text("a".into()),
            Value::Text("ab".into()),
            Value::Text("b".into()),
        ];
        let keys: Vec<Vec<u8>> = texts.iter().map(encode_index_value).collect();
        assert!(
            keys.windows(2).all(|w| w[0] < w[1]),
            "TEXT keys must ascend"
        );
    }

    #[test]
    fn index_key_ends_with_the_rowid() {
        let row = [Value::Int(1), Value::Text("hello".into())];
        let key = encode_index_key(&row, &[1], &rowid_key(7));
        assert_eq!(&key[key.len() - 8..], rowid_key(7).as_slice());
    }

    #[test]
    fn multi_column_index_keys_order_by_tuple() {
        let key = |a: i64, b: &str| {
            encode_index_key(
                &[Value::Int(a), Value::Text(b.into())],
                &[0, 1],
                &rowid_key(0),
            )
        };
        // Tuple order: (1,"a") < (1,"b") < (2,"a") < (2,"aa").
        assert!(key(1, "a") < key(1, "b"));
        assert!(key(1, "b") < key(2, "a"));
        assert!(key(2, "a") < key(2, "aa"));
    }

    #[test]
    fn prefix_upper_bound_brackets_a_prefix() {
        let prefix = encode_index_value(&Value::Int(42));
        let upper = prefix_upper_bound(&prefix).unwrap();
        let low = encode_index_key(&[Value::Int(42)], &[0], &rowid_key(0));
        let high = encode_index_key(&[Value::Int(42)], &[0], &rowid_key(u64::MAX));
        let other = encode_index_key(&[Value::Int(43)], &[0], &rowid_key(0));
        assert!(prefix <= low && low < upper);
        assert!(high < upper);
        assert!(other >= upper);
    }
}
