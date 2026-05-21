//! Binary encoding of rows and schemas — the bridge between typed values and
//! the raw byte strings the B+tree stores.
//!
//! A **row** is a tag-prefixed sequence of values, one per column. A **schema**
//! is the value the catalog stores under a table's name. Everything is
//! little-endian and self-describing enough to be decoded back with only the
//! column count (for rows) or nothing at all (for schemas).

use crate::engine::schema::{Column, Schema};
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
    Ok(Schema {
        name,
        columns,
        root,
        next_rowid,
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

    #[test]
    fn schema_round_trip() {
        let schema = Schema {
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
        };
        let encoded = encode_schema(&schema);
        assert_eq!(decode_schema(&encoded).unwrap(), schema);
    }
}
