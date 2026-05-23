//! The commit log — a per-database, append-only file of transaction outcomes.
//!
//! Every row in the storage layer carries an MVCC `(tx_min, tx_max)`. The
//! visibility check has to answer two questions about each:
//!
//! 1. Is this TX committed? (Or rolled back? Or still in flight?)
//! 2. If committed, did it commit *before* the reader's snapshot?
//!
//! Question 2 is `tx_id < snapshot.next_tx`. Question 1 is what the clog
//! exists to answer: a single source of truth for what every TX ID resolved
//! to, persistent across crashes.
//!
//! The clog is a tiny file (`<db>-clog`) of fixed-size 9-byte records: an
//! 8-byte little-endian TX ID and a 1-byte status (`1` = committed, `2` =
//! rolled back). It is append-only and `fsync`ed on every write — the
//! durability story is the same as the WAL's, just on its own file.
//!
//! On open, the whole clog is scanned into an in-memory `HashMap<u64, Status>`
//! so lookups are O(1). A TX ID *not* in the map is either (a) still in
//! flight (its writer holds it in [`crate::engine::transaction::TxState`]'s
//! `in_flight` set), or (b) never assigned. Either way, it is not visible
//! to any snapshot.
//!
//! Crash recovery: on open, every TX ID `<= next_tx_id` (from the database
//! header) that does *not* appear in the clog is treated as **rolled back**.
//! A writer that crashed mid-flight will have stamped rows with its TX ID
//! but never written its commit/rollback record, so those rows become
//! invisible to every future snapshot.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::error::{Error, Result};

/// The on-disk size of one clog record: 8 bytes for the TX ID, 1 for status.
const RECORD_SIZE: usize = 9;

const STATUS_COMMITTED: u8 = 1;
const STATUS_ROLLED_BACK: u8 = 2;

/// The outcome a TX resolved to. A TX still in flight has no clog entry yet;
/// see [`Clog::status_or_rolled_back`] for the "treat absent as rolled back"
/// helper crash recovery uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Committed,
    RolledBack,
}

/// The path of the clog file beside the database file.
pub fn clog_path(db: &Path) -> PathBuf {
    let mut name = db.as_os_str().to_os_string();
    name.push("-clog");
    PathBuf::from(name)
}

/// The clog handle, shared via [`Arc`] so every `Database` open on one file
/// uses the same instance. Internally `Arc<Mutex<...>>` — appends serialise
/// briefly under the mutex.
#[derive(Clone)]
pub struct Clog {
    inner: Arc<Mutex<ClogInner>>,
}

impl std::fmt::Debug for Clog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let len = self.inner.lock().map(|i| i.map.len()).unwrap_or(0);
        write!(f, "Clog({len} records)")
    }
}

struct ClogInner {
    file: File,
    map: HashMap<u64, Status>,
}

impl Clog {
    /// Open or create the clog file at `<path>-clog`, reading every existing
    /// record into the in-memory map. The file's records are append-only;
    /// the in-memory map mirrors them.
    pub fn open(db_path: &Path) -> Result<Clog> {
        let path = clog_path(db_path);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        let mut map = HashMap::new();
        let mut buf = [0u8; RECORD_SIZE];
        file.seek(SeekFrom::Start(0))?;
        loop {
            match file.read_exact(&mut buf) {
                Ok(()) => {
                    let tx_id = u64::from_le_bytes(buf[0..8].try_into().unwrap());
                    let status = match buf[8] {
                        STATUS_COMMITTED => Status::Committed,
                        STATUS_ROLLED_BACK => Status::RolledBack,
                        other => {
                            return Err(Error::corruption(format!(
                                "unknown clog status tag {other}"
                            )))
                        }
                    };
                    map.insert(tx_id, status);
                }
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
        }
        // Position at the end so future appends go in the right place.
        file.seek(SeekFrom::End(0))?;
        Ok(Clog {
            inner: Arc::new(Mutex::new(ClogInner { file, map })),
        })
    }

    /// The status of `tx_id`. `None` means "not in the clog" — either still
    /// in flight (held by `TxState`) or never assigned.
    pub fn status(&self, tx_id: u64) -> Option<Status> {
        let inner = self.inner.lock().expect("poisoned clog");
        inner.map.get(&tx_id).copied()
    }

    /// Bulk lookup: status of `tx_id` for snapshot visibility. A TX ID below
    /// `oldest_active` (the watermark of "everything has resolved") that has
    /// no clog entry is treated as **rolled back** — this is the crash
    /// recovery rule. Above the watermark, "not in clog" means "still in
    /// flight" and the caller (the snapshot) is expected to know which.
    pub fn status_or_rolled_back(&self, tx_id: u64, oldest_active: u64) -> Option<Status> {
        let inner = self.inner.lock().expect("poisoned clog");
        match inner.map.get(&tx_id) {
            Some(&status) => Some(status),
            None if tx_id < oldest_active => Some(Status::RolledBack),
            None => None,
        }
    }

    /// Append a record marking `tx_id` as committed. fsynced before return.
    pub fn record_commit(&self, tx_id: u64) -> Result<()> {
        self.append(tx_id, Status::Committed)
    }

    /// Append a record marking `tx_id` as rolled back. fsynced before return.
    pub fn record_rollback(&self, tx_id: u64) -> Result<()> {
        self.append(tx_id, Status::RolledBack)
    }

    fn append(&self, tx_id: u64, status: Status) -> Result<()> {
        let tag = match status {
            Status::Committed => STATUS_COMMITTED,
            Status::RolledBack => STATUS_ROLLED_BACK,
        };
        let mut buf = [0u8; RECORD_SIZE];
        buf[0..8].copy_from_slice(&tx_id.to_le_bytes());
        buf[8] = tag;
        let mut inner = self.inner.lock().expect("poisoned clog");
        inner.file.write_all(&buf)?;
        inner.file.sync_all()?;
        inner.map.insert(tx_id, status);
        Ok(())
    }

    /// Whether `tx_id` is recorded as committed.
    pub fn is_committed(&self, tx_id: u64) -> bool {
        self.status(tx_id) == Some(Status::Committed)
    }

    /// Whether `tx_id` is recorded as rolled back.
    pub fn is_rolled_back(&self, tx_id: u64) -> bool {
        self.status(tx_id) == Some(Status::RolledBack)
    }

    /// How many records the in-memory map holds. Diagnostic.
    pub fn len(&self) -> usize {
        self.inner.lock().expect("poisoned clog").map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn tmp_db() -> PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir().join(format!("prehnite-clog-{}-{n}.db", std::process::id()))
    }

    fn cleanup(path: &Path) {
        let _ = std::fs::remove_file(clog_path(path));
    }

    #[test]
    fn records_round_trip_across_reopen() {
        let db = tmp_db();
        cleanup(&db);
        {
            let clog = Clog::open(&db).unwrap();
            clog.record_commit(1).unwrap();
            clog.record_rollback(2).unwrap();
            clog.record_commit(3).unwrap();
        }
        // Reopen reads every record back.
        let clog = Clog::open(&db).unwrap();
        assert_eq!(clog.status(1), Some(Status::Committed));
        assert_eq!(clog.status(2), Some(Status::RolledBack));
        assert_eq!(clog.status(3), Some(Status::Committed));
        assert_eq!(clog.status(4), None);
        cleanup(&db);
    }

    #[test]
    fn status_or_rolled_back_handles_crash_recovery() {
        // A TX below the watermark with no clog entry is treated as
        // rolled back — the crash-recovery rule.
        let db = tmp_db();
        cleanup(&db);
        let clog = Clog::open(&db).unwrap();
        clog.record_commit(5).unwrap();
        // TX 3 was in flight at "crash"; watermark is 6.
        assert_eq!(clog.status_or_rolled_back(3, 6), Some(Status::RolledBack));
        assert_eq!(clog.status_or_rolled_back(5, 6), Some(Status::Committed));
        // TX 7 hasn't been assigned; above the watermark.
        assert_eq!(clog.status_or_rolled_back(7, 6), None);
        cleanup(&db);
    }

    #[test]
    fn append_is_durable() {
        // Each append fsyncs; the next open must see every record even
        // without a clean close.
        let db = tmp_db();
        cleanup(&db);
        let clog = Clog::open(&db).unwrap();
        for i in 1..=100u64 {
            clog.record_commit(i).unwrap();
        }
        assert_eq!(clog.len(), 100);
        // Mid-flight reopen sees all 100 records.
        drop(clog);
        let clog = Clog::open(&db).unwrap();
        assert_eq!(clog.len(), 100);
        for i in 1..=100u64 {
            assert!(clog.is_committed(i));
        }
        cleanup(&db);
    }
}
