//! Write-ahead log — the engine's durability and crash-atomicity mechanism.
//!
//! Before any committed page reaches the database file, a full image of every
//! page in the transaction is appended here and fsync'd, followed by a commit
//! marker. Only then are the pages copied into the database file. If the
//! process dies anywhere in between, [`Wal::recover`] sorts it out on the next
//! open: a transaction whose CRC-checked commit marker is present and complete
//! is replayed; anything else is discarded, leaving the database untouched.
//!
//! The pager resets the log after every successful commit, so the file holds
//! at most one transaction at a time.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::error::Result;
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
        Ok(Wal { file })
    }

    /// Inspect the log at open time. If it holds a complete committed
    /// transaction, replay every page image into `db`; otherwise discard it.
    /// Either way the log is empty when this returns. The bool reports whether
    /// a transaction was replayed.
    pub fn recover(&mut self, db: &mut File) -> Result<bool> {
        let mut bytes = Vec::new();
        self.file.seek(SeekFrom::Start(0))?;
        self.file.read_to_end(&mut bytes)?;

        let replayed = match parse_committed(&bytes) {
            Some(pages) if !pages.is_empty() => {
                for (no, image) in &pages {
                    db.seek(SeekFrom::Start(*no as u64 * PAGE_SIZE as u64))?;
                    db.write_all(image)?;
                }
                db.sync_all()?;
                true
            }
            _ => false,
        };
        self.reset()?;
        Ok(replayed)
    }

    /// Append every page of a transaction, then a commit marker, then fsync.
    /// Returns only once the transaction is durably recorded.
    pub fn write_transaction(&mut self, pages: &[(u32, &[u8; PAGE_SIZE])]) -> Result<()> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut buf = Vec::with_capacity(pages.len() * PAGE_RECORD_LEN + COMMIT_RECORD_LEN);

        for (no, image) in pages {
            let no_bytes = no.to_le_bytes();
            buf.push(REC_PAGE);
            buf.extend_from_slice(&no_bytes);
            buf.extend_from_slice(&image[..]);
            buf.extend_from_slice(&crc32(&[&no_bytes, &image[..]]).to_le_bytes());
        }

        let count = pages.len() as u32;
        buf.push(REC_COMMIT);
        buf.extend_from_slice(&count.to_le_bytes());
        buf.extend_from_slice(&crc32(&[&count.to_le_bytes()]).to_le_bytes());

        self.file.write_all(&buf)?;
        self.file.sync_all()?;
        Ok(())
    }

    /// Truncate the log back to empty and fsync the truncation.
    pub fn reset(&mut self) -> Result<()> {
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        self.file.sync_all()?;
        Ok(())
    }
}

/// Parse a fully committed transaction out of raw log bytes. Returns `None` if
/// the log is empty, truncated, corrupt, or never reached a commit marker — in
/// every such case the transaction must be treated as never having happened.
fn parse_committed(bytes: &[u8]) -> Option<Vec<(u32, Vec<u8>)>> {
    let mut pages: Vec<(u32, Vec<u8>)> = Vec::new();
    let mut pos = 0usize;
    loop {
        let tag = *bytes.get(pos)?;
        pos += 1;
        match tag {
            REC_PAGE => {
                if pos + 4 + PAGE_SIZE + 4 > bytes.len() {
                    return None;
                }
                let no = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap());
                let image = &bytes[pos + 4..pos + 4 + PAGE_SIZE];
                let stored = u32::from_le_bytes(
                    bytes[pos + 4 + PAGE_SIZE..pos + 8 + PAGE_SIZE]
                        .try_into()
                        .unwrap(),
                );
                if crc32(&[&no.to_le_bytes(), image]) != stored {
                    return None;
                }
                pages.push((no, image.to_vec()));
                pos += 4 + PAGE_SIZE + 4;
            }
            REC_COMMIT => {
                if pos + 8 > bytes.len() {
                    return None;
                }
                let count = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap());
                let stored = u32::from_le_bytes(bytes[pos + 4..pos + 8].try_into().unwrap());
                if crc32(&[&count.to_le_bytes()]) != stored {
                    return None;
                }
                if count as usize != pages.len() {
                    return None;
                }
                return Some(pages);
            }
            _ => return None,
        }
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

    #[test]
    fn crc32_check_value() {
        // The canonical CRC-32 check string hashes to 0xCBF43926.
        assert_eq!(crc32(&[b"123456789"]), 0xCBF4_3926);
    }

    #[test]
    fn crc32_is_split_agnostic() {
        assert_eq!(crc32(&[b"hello world"]), crc32(&[b"hello", b" ", b"world"]));
    }

    #[test]
    fn empty_log_yields_nothing() {
        assert!(parse_committed(&[]).is_none());
    }

    #[test]
    fn missing_commit_marker_is_discarded() {
        // A lone page record with no commit marker must not be replayed.
        let image = [7u8; PAGE_SIZE];
        let no = 3u32.to_le_bytes();
        let mut log = vec![REC_PAGE];
        log.extend_from_slice(&no);
        log.extend_from_slice(&image);
        log.extend_from_slice(&crc32(&[&no, &image]).to_le_bytes());
        assert!(parse_committed(&log).is_none());
    }

    #[test]
    fn bit_flip_fails_crc() {
        let image = [7u8; PAGE_SIZE];
        let no = 3u32.to_le_bytes();
        let mut log = vec![REC_PAGE];
        log.extend_from_slice(&no);
        log.extend_from_slice(&image);
        log.extend_from_slice(&crc32(&[&no, &image]).to_le_bytes());
        log.push(REC_COMMIT);
        log.extend_from_slice(&1u32.to_le_bytes());
        log.extend_from_slice(&crc32(&[&1u32.to_le_bytes()]).to_le_bytes());
        assert!(parse_committed(&log).is_some());

        log[10] ^= 0xFF; // corrupt a byte inside the page image
        assert!(parse_committed(&log).is_none());
    }
}
