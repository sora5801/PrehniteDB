//! The wire protocol spoken between the `prehnite` client and the `prehnited`
//! server.
//!
//! Every message is a single length-prefixed frame:
//!
//! ```text
//!   [ tag: u8 ] [ length: u32 big-endian ] [ payload: `length` bytes ]
//! ```
//!
//! The client sends a [`Request`]; the server answers with exactly one
//! [`Response`]. All multi-byte integers on the wire are big-endian.

use std::io::{ErrorKind, Read, Write};

use crate::engine::value::Value;
use crate::error::{Error, Result};

const TAG_QUERY: u8 = 0x01;
const TAG_ACK: u8 = 0x10;
const TAG_ROWS_BEGIN: u8 = 0x11;
const TAG_ERROR: u8 = 0x12;
const TAG_ROW: u8 = 0x13;
const TAG_ROWS_END: u8 = 0x14;

const VAL_NULL: u8 = 0;
const VAL_INT: u8 = 1;
const VAL_REAL: u8 = 2;
const VAL_TEXT: u8 = 3;
const VAL_BOOL: u8 = 4;

/// Upper bound on a single frame's payload (64 MiB) — generous for v0.1, but
/// small enough that a hostile or confused peer cannot trigger a huge alloc.
const MAX_FRAME: usize = 64 * 1024 * 1024;

/// A message from client to server.
#[derive(Debug, Clone, PartialEq)]
pub enum Request {
    /// Execute this SQL text.
    Query(String),
}

/// A message from server to client.
///
/// A result set is *streamed*, not sent whole: a [`Response::RowsBegin`], then
/// one [`Response::Row`] per row, then a [`Response::RowsEnd`] — or, if the
/// query faults partway through, a [`Response::Error`] in place of the end.
#[derive(Debug, Clone, PartialEq)]
pub enum Response {
    /// A statement succeeded; carries a human-readable summary.
    Ack(String),
    /// The statement failed; carries the error message. Stands alone, or ends
    /// a half-sent result set when a query faults mid-stream.
    Error(String),
    /// The header of a result set: its column names. The rows follow.
    RowsBegin { columns: Vec<String> },
    /// One row of a result set, streamed after `RowsBegin`.
    Row { values: Vec<Value> },
    /// The end of a result set — every row has been sent.
    RowsEnd,
}

/// Frame and send a request.
pub fn write_request(stream: &mut impl Write, request: &Request) -> Result<()> {
    let Request::Query(sql) = request;
    write_frame(stream, TAG_QUERY, sql.as_bytes())
}

/// Read one request. `Ok(None)` means the peer closed the connection cleanly
/// between messages.
pub fn read_request(stream: &mut impl Read) -> Result<Option<Request>> {
    let Some((tag, payload)) = read_frame(stream)? else {
        return Ok(None);
    };
    match tag {
        TAG_QUERY => Ok(Some(Request::Query(utf8(payload)?))),
        other => Err(Error::protocol(format!("unknown request tag {other:#04x}"))),
    }
}

/// Frame and send one response message.
pub fn write_response(stream: &mut impl Write, response: &Response) -> Result<()> {
    match response {
        Response::Ack(message) => write_frame(stream, TAG_ACK, message.as_bytes()),
        Response::Error(message) => write_frame(stream, TAG_ERROR, message.as_bytes()),
        Response::RowsBegin { columns } => {
            write_frame(stream, TAG_ROWS_BEGIN, &encode_columns(columns))
        }
        Response::Row { values } => write_frame(stream, TAG_ROW, &encode_row(values)),
        Response::RowsEnd => write_frame(stream, TAG_ROWS_END, &[]),
    }
}

/// Read one response message. EOF here is an error: after a request the client
/// always expects at least one frame back.
pub fn read_response(stream: &mut impl Read) -> Result<Response> {
    let (tag, payload) = read_frame(stream)?
        .ok_or_else(|| Error::protocol("server closed the connection without replying"))?;
    match tag {
        TAG_ACK => Ok(Response::Ack(utf8(payload)?)),
        TAG_ERROR => Ok(Response::Error(utf8(payload)?)),
        TAG_ROWS_BEGIN => decode_rows_begin(&payload),
        TAG_ROW => decode_row(&payload),
        TAG_ROWS_END => Ok(Response::RowsEnd),
        other => Err(Error::protocol(format!(
            "unknown response tag {other:#04x}"
        ))),
    }
}

fn encode_columns(columns: &[String]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(columns.len() as u16).to_be_bytes());
    for name in columns {
        out.extend_from_slice(&(name.len() as u16).to_be_bytes());
        out.extend_from_slice(name.as_bytes());
    }
    out
}

fn decode_rows_begin(payload: &[u8]) -> Result<Response> {
    let mut reader = FrameReader::new(payload);
    let count = reader.u16()? as usize;
    let mut columns = Vec::with_capacity(count);
    for _ in 0..count {
        let len = reader.u16()? as usize;
        columns.push(utf8(reader.take(len)?.to_vec())?);
    }
    Ok(Response::RowsBegin { columns })
}

fn encode_row(values: &[Value]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(values.len() as u16).to_be_bytes());
    for value in values {
        encode_value(&mut out, value);
    }
    out
}

fn decode_row(payload: &[u8]) -> Result<Response> {
    let mut reader = FrameReader::new(payload);
    let count = reader.u16()? as usize;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(decode_value(&mut reader)?);
    }
    Ok(Response::Row { values })
}

fn encode_value(out: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Null => out.push(VAL_NULL),
        Value::Int(n) => {
            out.push(VAL_INT);
            out.extend_from_slice(&n.to_be_bytes());
        }
        Value::Real(r) => {
            out.push(VAL_REAL);
            out.extend_from_slice(&r.to_bits().to_be_bytes());
        }
        Value::Text(s) => {
            out.push(VAL_TEXT);
            out.extend_from_slice(&(s.len() as u32).to_be_bytes());
            out.extend_from_slice(s.as_bytes());
        }
        Value::Bool(b) => {
            out.push(VAL_BOOL);
            out.push(u8::from(*b));
        }
    }
}

fn decode_value(reader: &mut FrameReader) -> Result<Value> {
    Ok(match reader.u8()? {
        VAL_NULL => Value::Null,
        VAL_INT => Value::Int(reader.i64()?),
        VAL_REAL => Value::Real(f64::from_bits(reader.u64()?)),
        VAL_TEXT => {
            let len = reader.u32()? as usize;
            Value::Text(utf8(reader.take(len)?.to_vec())?)
        }
        VAL_BOOL => Value::Bool(reader.u8()? != 0),
        other => return Err(Error::protocol(format!("unknown value tag {other}"))),
    })
}

fn write_frame(stream: &mut impl Write, tag: u8, payload: &[u8]) -> Result<()> {
    if payload.len() > MAX_FRAME {
        return Err(Error::protocol(format!(
            "message of {} bytes exceeds the {MAX_FRAME}-byte frame limit",
            payload.len()
        )));
    }
    let mut header = [0u8; 5];
    header[0] = tag;
    header[1..5].copy_from_slice(&(payload.len() as u32).to_be_bytes());
    stream.write_all(&header)?;
    stream.write_all(payload)?;
    stream.flush()?;
    Ok(())
}

fn read_frame(stream: &mut impl Read) -> Result<Option<(u8, Vec<u8>)>> {
    let mut header = [0u8; 5];
    if !fill(stream, &mut header)? {
        return Ok(None); // clean EOF between frames
    }
    let length = u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
    if length > MAX_FRAME {
        return Err(Error::protocol(format!(
            "frame of {length} bytes exceeds the {MAX_FRAME}-byte limit"
        )));
    }
    let mut payload = vec![0u8; length];
    if length > 0 && !fill(stream, &mut payload)? {
        return Err(Error::protocol(
            "connection closed in the middle of a frame",
        ));
    }
    Ok(Some((header[0], payload)))
}

/// Read exactly `buf.len()` bytes. `Ok(true)` if filled, `Ok(false)` if EOF
/// arrived before *any* byte, `Err` if EOF arrived partway through.
fn fill(stream: &mut impl Read, buf: &mut [u8]) -> Result<bool> {
    let mut filled = 0;
    while filled < buf.len() {
        match stream.read(&mut buf[filled..]) {
            Ok(0) => {
                return if filled == 0 {
                    Ok(false)
                } else {
                    Err(Error::protocol("connection closed mid-frame"))
                };
            }
            Ok(n) => filled += n,
            Err(e) if e.kind() == ErrorKind::Interrupted => continue,
            Err(e) => return Err(Error::Io(e)),
        }
    }
    Ok(true)
}

fn utf8(bytes: Vec<u8>) -> Result<String> {
    String::from_utf8(bytes).map_err(|_| Error::protocol("message was not valid UTF-8"))
}

/// A bounds-checked, big-endian cursor over a frame payload.
struct FrameReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> FrameReader<'a> {
    fn new(data: &'a [u8]) -> FrameReader<'a> {
        FrameReader { data, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|&e| e <= self.data.len())
            .ok_or_else(|| Error::protocol("frame payload ended unexpectedly"))?;
        let slice = &self.data[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn round_trip_response(response: Response) -> Response {
        let mut buffer = Vec::new();
        write_response(&mut buffer, &response).unwrap();
        read_response(&mut Cursor::new(buffer)).unwrap()
    }

    #[test]
    fn request_round_trip() {
        let mut buffer = Vec::new();
        write_request(&mut buffer, &Request::Query("SELECT 1".into())).unwrap();
        let mut cursor = Cursor::new(buffer);
        assert_eq!(
            read_request(&mut cursor).unwrap(),
            Some(Request::Query("SELECT 1".into()))
        );
        // A second read on the drained stream sees a clean end-of-stream.
        assert_eq!(read_request(&mut cursor).unwrap(), None);
    }

    #[test]
    fn ack_and_error_round_trip() {
        assert_eq!(
            round_trip_response(Response::Ack("1 row inserted".into())),
            Response::Ack("1 row inserted".into())
        );
        assert_eq!(
            round_trip_response(Response::Error("no such table".into())),
            Response::Error("no such table".into())
        );
    }

    #[test]
    fn streamed_result_set_round_trips_every_value_kind() {
        let begin = Response::RowsBegin {
            columns: vec!["i".into(), "r".into(), "t".into(), "b".into(), "n".into()],
        };
        assert_eq!(round_trip_response(begin.clone()), begin);

        let row = Response::Row {
            values: vec![
                Value::Int(-9),
                Value::Real(3.5),
                Value::Text("hello".into()),
                Value::Bool(true),
                Value::Null,
            ],
        };
        assert_eq!(round_trip_response(row.clone()), row);

        // A zero-column row and the terminator both round-trip.
        let empty = Response::Row { values: Vec::new() };
        assert_eq!(round_trip_response(empty.clone()), empty);
        assert_eq!(round_trip_response(Response::RowsEnd), Response::RowsEnd);
    }

    #[test]
    fn empty_stream_is_clean_eof_for_request() {
        assert_eq!(read_request(&mut Cursor::new(Vec::new())).unwrap(), None);
    }

    #[test]
    fn truncated_frame_is_an_error() {
        // A header promising 100 payload bytes, with none supplied.
        let mut bytes = vec![TAG_QUERY];
        bytes.extend_from_slice(&100u32.to_be_bytes());
        assert!(read_request(&mut Cursor::new(bytes)).is_err());
    }
}
