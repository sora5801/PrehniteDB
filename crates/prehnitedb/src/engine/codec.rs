//! Binary encoding of rows and schemas — the bridge between typed values and
//! the raw byte strings the B+tree stores.
//!
//! A **row** is a tag-prefixed sequence of values, one per column. A **schema**
//! is the value the catalog stores under a table's name. Everything is
//! little-endian and self-describing enough to be decoded back with only the
//! column count (for rows) or nothing at all (for schemas).

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

/// Encode one row's values into the bytes stored as a B+tree value.
pub fn encode_row(values: &[Value]) -> Vec<u8> {
    let mut out = Vec::new();
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

/// Decode a row of exactly `column_count` values.
pub fn decode_row(bytes: &[u8], column_count: usize) -> Result<Vec<Value>> {
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
    Ok(values)
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
    }
    out.extend_from_slice(&(schema.indexes.len() as u16).to_le_bytes());
    for index in &schema.indexes {
        out.extend_from_slice(&index.root.to_le_bytes());
        out.extend_from_slice(&(index.columns.len() as u16).to_le_bytes());
        for &column in &index.columns {
            out.extend_from_slice(&(column as u16).to_le_bytes());
        }
        write_str(&mut out, &index.name);
    }
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
        columns.push(Column {
            name: read_str(&mut reader)?,
            ty,
        });
    }
    // The index section is absent in schemas written before v0.2; an already
    // exhausted reader simply yields a table with no indexes.
    let mut indexes = Vec::new();
    if !reader.is_empty() {
        let index_count = reader.u16()? as usize;
        for _ in 0..index_count {
            let root = reader.u32()?;
            let column_count = reader.u16()? as usize;
            let mut columns = Vec::with_capacity(column_count);
            for _ in 0..column_count {
                columns.push(reader.u16()? as usize);
            }
            let name = read_str(&mut reader)?;
            indexes.push(Index {
                name,
                columns,
                root,
            });
        }
    }
    Ok(Schema {
        name,
        columns,
        root,
        next_rowid,
        indexes,
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
        let row = vec![
            Value::Int(-7),
            Value::Text("hello".into()),
            Value::Null,
            Value::Bool(true),
            Value::Real(2.5),
        ];
        let encoded = encode_row(&row);
        assert_eq!(decode_row(&encoded, 5).unwrap(), row);
    }

    #[test]
    fn decode_rejects_wrong_column_count() {
        let encoded = encode_row(&[Value::Int(1), Value::Int(2)]);
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
                },
                Column {
                    name: "label".into(),
                    ty: Type::Text,
                },
            ],
            root: 12,
            next_rowid: 99,
            indexes: vec![Index {
                name: "by_label".into(),
                columns: vec![1],
                root: 30,
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
    fn schema_without_index_section_decodes() {
        // A schema written before v0.2 ends right after its columns. Encode an
        // index-free schema and drop its trailing u16 index count to mimic one.
        let mut schema = sample_schema();
        schema.indexes.clear();
        let mut encoded = encode_schema(&schema);
        encoded.truncate(encoded.len() - 2);
        assert!(decode_schema(&encoded).unwrap().indexes.is_empty());
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
