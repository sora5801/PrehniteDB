//! Write-ahead log — the engine's durability and crash-atomicity mechanism.
//!
//! A transaction is accumulated in the log incrementally: every page the
//! transaction writes is appended here as a full, CRC-checked image. Most
//! arrive at commit time, but some arrive earlier — when the buffer pool
//! evicts a dirty page to reclaim memory, that page is spilled here.
//! [`Wal::seal`] then appends a commit marker and fsyncs. Only once the marker
//! is durable are the page images copied into the database file.
//!
//! If the process dies before the marker is written, the log has no valid
//! marker and is discarded on the next open, leaving the database untouched;
//! if it dies after, [`Wal::recover`] replays it. Recovery streams the log one
//! record at a time, so replaying a transaction — even one far larger than
//! memory — costs only a single page buffer of RAM. The pager resets the log
//! after every successful commit.

use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::error::{Error, Result};
use crate::storage::page::PAGE_SIZE;

const REC_PAGE: u8 = 1;
const REC_COMMIT: u8 = 2;

/// Bytes of a page record: tag + page number + image + CRC.
const PAGE_RECORD_LEN: usize = 1 + 4 + PAGE_SIZE + 4;
/// Bytes of the commit record: tag + page count + CRC.
const COMMIT_RECORD_LEN: usize = 1 + 4 + 4;

/// A handle to the write-ahead log file.
pub struct Wal {
    file: File,
    /// Offset at which the next record will be appended.
    cursor: u64,
    /// Page records appended since the log was last reset — the count the
    /// commit marker carries and recovery checks.
    records: u32,
}

impl Wal {
    /// Open (creating if necessary) the log at `path`.
    pub fn open(path: &Path) -> Result<Wal> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        Ok(Wal {
            file,
            cursor: 0,
            records: 0,
        })
    }

    /// Append one page image and return the file offset of the image bytes,
    /// so an evicted page can be read back later with [`read_page_at`](Self::read_page_at).
    /// The append is *not* fsync'd — durability is established once, by
    /// [`seal`](Self::seal).
    pub fn append_page(&mut self, no: u32, image: &[u8; PAGE_SIZE]) -> Result<u64> {
        self.file.seek(SeekFrom::Start(self.cursor))?;
        let no_bytes = no.to_le_bytes();
        let mut record = Vec::with_capacity(PAGE_RECORD_LEN);
        record.push(REC_PAGE);
        record.extend_from_slice(&no_bytes);
        record.extend_from_slice(&image[..]);
        record.extend_from_slice(&crc32(&[&no_bytes, &image[..]]).to_le_bytes());
        self.file.write_all(&record)?;

        let image_offset = self.cursor + 1 + 4;
        self.cursor += PAGE_RECORD_LEN as u64;
        self.records += 1;
        Ok(image_offset)
    }

    /// Append the commit marker and fsync. Once this returns the transaction
    /// is durable: a crash from here on is repaired by replaying the log.
    pub fn seal(&mut self) -> Result<()> {
        self.file.seek(SeekFrom::Start(self.cursor))?;
        let count = self.records.to_le_bytes();
        let mut record = Vec::with_capacity(COMMIT_RECORD_LEN);
        record.push(REC_COMMIT);
        record.extend_from_slice(&count);
        record.extend_from_slice(&crc32(&[&count]).to_le_bytes());
        self.file.write_all(&record)?;
        self.file.sync_all()?;
        self.cursor += COMMIT_RECORD_LEN as u64;
        Ok(())
    }

    /// Read back a page image given the offset a prior [`append_page`](Self::append_page)
    /// returned. Used to fetch an evicted dirty page that has been asked for
    /// again before the transaction commits.
    pub fn read_page_at(&mut self, image_offset: u64) -> Result<Box<[u8; PAGE_SIZE]>> {
        let mut buf = Box::new([0u8; PAGE_SIZE]);
        self.file.seek(SeekFrom::Start(image_offset))?;
        self.file.read_exact(&mut buf[..])?;
        Ok(buf)
    }

    /// Inspect the log at open time. If it holds a complete committed
    /// transaction, replay every page image into `db`; otherwise discard it.
    /// Either way the log is empty when this returns. The bool reports whether
    /// a transaction was replayed.
    pub fn recover(&mut self, db: &mut File) -> Result<bool> {
        self.file.seek(SeekFrom::Start(0))?;
        let committed = {
            let mut reader = BufReader::new(&mut self.file);
            scan(&mut reader)?
        };
        if committed {
            self.apply(db)?;
        }
        self.reset()?;
        Ok(committed)
    }

    /// Copy every page image in the log into `db` and fsync it. The caller
    /// must already know the log holds a complete committed transaction —
    /// because [`scan`] confirmed it, or because it was just [`seal`](Self::seal)ed.
    /// Replaying is idempotent, so a crash mid-apply is repaired by another.
    pub fn apply(&mut self, db: &mut File) -> Result<()> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut reader = BufReader::new(&mut self.file);
        let mut tag = [0u8; 1];
        let mut rest = vec![0u8; PAGE_RECORD_LEN - 1];
        while read_filled(&mut reader, &mut tag)? {
            if tag[0] != REC_PAGE {
                break; // the commit marker — every page image is behind us
            }
            if !read_filled(&mut reader, &mut rest)? {
                break;
            }
            let no = u32::from_le_bytes(rest[0..4].try_into().unwrap());
            db.seek(SeekFrom::Start(no as u64 * PAGE_SIZE as u64))?;
            db.write_all(&rest[4..4 + PAGE_SIZE])?;
        }
        db.sync_all()?;
        Ok(())
    }

    /// Truncate the log back to empty and fsync the truncation.
    pub fn reset(&mut self) -> Result<()> {
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        self.file.sync_all()?;
        self.cursor = 0;
        self.records = 0;
        Ok(())
    }

    /// Abandon an in-progress (unsealed) transaction. The file is left as is;
    /// the next [`append_page`](Self::append_page) simply overwrites it from
    /// the start. This needs no I/O, and is safe precisely because an unsealed
    /// log carries no commit marker — so its stale bytes can never be mistaken
    /// for a committed transaction — and page records are fixed-size, so a
    /// later transaction's records stay aligned over the old ones.
    pub fn discard(&mut self) {
        self.cursor = 0;
        self.records = 0;
    }
}

/// Stream the log confirming it ends in a complete, CRC-valid commit marker.
/// Returns whether a committed transaction is present; a truncated, corrupt,
/// or unsealed log yields `false` rather than an error. Holds only one record
/// in memory at a time, so the log may be arbitrarily large.
fn scan(reader: &mut impl Read) -> Result<bool> {
    let mut records: u32 = 0;
    let mut tag = [0u8; 1];
    let mut page_rest = vec![0u8; PAGE_RECORD_LEN - 1];
    let mut commit_rest = [0u8; COMMIT_RECORD_LEN - 1];
    loop {
        if !read_filled(reader, &mut tag)? {
            return Ok(false); // ran out before any commit marker
        }
        match tag[0] {
            REC_PAGE => {
                if !read_filled(reader, &mut page_rest)? {
                    return Ok(false);
                }
                let stored = u32::from_le_bytes(page_rest[4 + PAGE_SIZE..].try_into().unwrap());
                if crc32(&[&page_rest[0..4], &page_rest[4..4 + PAGE_SIZE]]) != stored {
                    return Ok(false);
                }
                records += 1;
            }
            REC_COMMIT => {
                if !read_filled(reader, &mut commit_rest)? {
                    return Ok(false);
                }
                let count = u32::from_le_bytes(commit_rest[0..4].try_into().unwrap());
                let stored = u32::from_le_bytes(commit_rest[4..8].try_into().unwrap());
                return Ok(crc32(&[&commit_rest[0..4]]) == stored && count == records);
            }
            _ => return Ok(false),
        }
    }
}

/// Fill `buf` completely; `Ok(false)` if the reader reaches end-of-file first
/// (an expected outcome for a truncated or unsealed log).
fn read_filled(reader: &mut impl Read, buf: &mut [u8]) -> Result<bool> {
    match reader.read_exact(buf) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
        Err(e) => Err(Error::from(e)),
    }
}

/// Standard CRC-32 (IEEE 802.3, polynomial `0xEDB88320`) over a sequence of
/// byte slices. Bit-at-a-time: not the fastest, but the log is far from a hot
/// path and a table would only add noise.
fn crc32(parts: &[&[u8]]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for part in parts {
        for &byte in *part {
            crc ^= byte as u32;
            for _ in 0..8 {
                let mask = (crc & 1).wrapping_neg();
                crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
            }
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn crc32_check_value() {
        // The canonical CRC-32 check string hashes to 0xCBF43926.
        assert_eq!(crc32(&[b"123456789"]), 0xCBF4_3926);
    }

    #[test]
    fn crc32_is_split_agnostic() {
        assert_eq!(crc32(&[b"hello world"]), crc32(&[b"hello", b" ", b"world"]));
    }

    /// A valid page record for page `no`, its image filled with `byte`.
    fn page_record(no: u32, byte: u8) -> Vec<u8> {
        let image = [byte; PAGE_SIZE];
        let no_bytes = no.to_le_bytes();
        let mut rec = vec![REC_PAGE];
        rec.extend_from_slice(&no_bytes);
        rec.extend_from_slice(&image);
        rec.extend_from_slice(&crc32(&[&no_bytes, &image]).to_le_bytes());
        rec
    }

    /// A commit marker claiming `count` preceding page records.
    fn commit_marker(count: u32) -> Vec<u8> {
        let count_bytes = count.to_le_bytes();
        let mut rec = vec![REC_COMMIT];
        rec.extend_from_slice(&count_bytes);
        rec.extend_from_slice(&crc32(&[&count_bytes]).to_le_bytes());
        rec
    }

    #[test]
    fn empty_log_is_not_committed() {
        assert!(!scan(&mut Cursor::new(Vec::new())).unwrap());
    }

    #[test]
    fn missing_commit_marker_is_discarded() {
        // A lone page record with no marker must not count as committed.
        assert!(!scan(&mut Cursor::new(page_record(3, 7))).unwrap());
    }

    #[test]
    fn a_sealed_log_is_committed() {
        let mut log = page_record(3, 7);
        log.extend(page_record(9, 1));
        log.extend(commit_marker(2));
        assert!(scan(&mut Cursor::new(log)).unwrap());
    }

    #[test]
    fn bit_flip_fails_crc() {
        let mut log = page_record(3, 7);
        log.extend(commit_marker(1));
        assert!(scan(&mut Cursor::new(log.clone())).unwrap());

        log[10] ^= 0xFF; // corrupt a byte inside the page image
        assert!(!scan(&mut Cursor::new(log)).unwrap());
    }

    #[test]
    fn wrong_page_count_is_rejected() {
        // Two page records but a marker claiming one: a truncated tail.
        let mut log = page_record(1, 1);
        log.extend(page_record(2, 2));
        log.extend(commit_marker(1));
        assert!(!scan(&mut Cursor::new(log)).unwrap());
    }
}
