//! The pager — owner of the database file and gatekeeper of all page access.
//!
//! Reads come straight from the file (or from not-yet-committed writes); writes
//! are buffered in memory until [`Pager::commit`], which routes them through
//! the WAL before touching the database file. A statement that fails partway
//! calls [`Pager::rollback`] to drop the buffer. PrehniteDB's unit of atomicity
//! is therefore exactly one SQL statement: it lands whole or not at all.
//!
//! Page 0 is the database header; the pager owns it and never exposes it as a
//! tree page. Every other page is handed out by number to the B+tree layer.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::storage::page::PAGE_SIZE;
use crate::storage::wal::Wal;

/// Identifies the file format; bumped if the on-disk layout ever changes.
const MAGIC: &[u8; 8] = b"PREHNDB3";

const HDR_MAGIC: usize = 0;
const HDR_PAGE_SIZE: usize = 8;
const HDR_PAGE_COUNT: usize = 12;
const HDR_FREELIST: usize = 16;
const HDR_CATALOG: usize = 20;

/// Database-wide metadata, mirrored in page 0.
#[derive(Clone, Copy)]
struct Meta {
    /// Total pages in the file, including the header page.
    page_count: u32,
    /// Head of the free-page list, or 0 if there are no free pages.
    freelist_head: u32,
    /// Root page of the catalog B+tree, or 0 before it is created.
    catalog_root: u32,
}

/// Mediates all access to one database file.
pub struct Pager {
    file: File,
    wal: Wal,
    /// Working metadata, reflecting allocations made in the current statement.
    meta: Meta,
    /// Last durably committed metadata; restored verbatim on rollback.
    committed: Meta,
    /// Pages modified since the last commit, keyed by page number.
    dirty: HashMap<u32, Box<[u8; PAGE_SIZE]>>,
}

impl Pager {
    /// Open the database at `path`, creating it if absent. Any leftover WAL is
    /// recovered first, so the file is always consistent before use.
    pub fn open(path: impl AsRef<Path>) -> Result<Pager> {
        let path = path.as_ref();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        let mut wal = Wal::open(&wal_path(path))?;
        wal.recover(&mut file)?;

        let len = file.metadata()?.len();
        let mut pager = Pager {
            file,
            wal,
            meta: Meta {
                page_count: 1,
                freelist_head: 0,
                catalog_root: 0,
            },
            committed: Meta {
                page_count: 1,
                freelist_head: 0,
                catalog_root: 0,
            },
            dirty: HashMap::new(),
        };

        if len == 0 {
            // A brand-new file: lay down the header page and flush it.
            pager.write_page(0, encode_header(pager.meta));
            pager.commit()?;
        } else {
            let mut hdr = [0u8; PAGE_SIZE];
            pager.file.seek(SeekFrom::Start(0))?;
            pager.file.read_exact(&mut hdr)?;
            let meta = decode_header(&hdr)?;
            pager.meta = meta;
            pager.committed = meta;
        }
        Ok(pager)
    }

    /// Total pages in the database, including the header page.
    pub fn page_count(&self) -> u32 {
        self.meta.page_count
    }

    /// Root page of the catalog B+tree (0 means "not created yet").
    pub fn catalog_root(&self) -> u32 {
        self.meta.catalog_root
    }

    /// Record the catalog root. Persisted on the next [`commit`](Self::commit).
    pub fn set_catalog_root(&mut self, root: u32) {
        self.meta.catalog_root = root;
    }

    /// Fetch a page by number, returning an owned copy the caller may mutate.
    pub fn read_page(&mut self, no: u32) -> Result<Box<[u8; PAGE_SIZE]>> {
        if let Some(buf) = self.dirty.get(&no) {
            return Ok(buf.clone());
        }
        if no >= self.meta.page_count {
            return Err(Error::corruption(format!(
                "read of page {no}, past the end of a {}-page database",
                self.meta.page_count
            )));
        }
        let mut buf = Box::new([0u8; PAGE_SIZE]);
        self.file
            .seek(SeekFrom::Start(no as u64 * PAGE_SIZE as u64))?;
        self.file.read_exact(&mut buf[..])?;
        Ok(buf)
    }

    /// Stage a page to be written on the next commit.
    pub fn write_page(&mut self, no: u32, buf: Box<[u8; PAGE_SIZE]>) {
        self.dirty.insert(no, buf);
    }

    /// Allocate a fresh page, reusing the free list when possible. The returned
    /// page is staged as zero-filled; the caller is expected to write it.
    pub fn alloc_page(&mut self) -> Result<u32> {
        let no = if self.meta.freelist_head != 0 {
            let head = self.meta.freelist_head;
            let page = self.read_page(head)?;
            self.meta.freelist_head = u32::from_le_bytes(page[0..4].try_into().unwrap());
            head
        } else {
            let n = self.meta.page_count;
            self.meta.page_count += 1;
            n
        };
        self.write_page(no, Box::new([0u8; PAGE_SIZE]));
        Ok(no)
    }

    /// Return a page to the free list. Takes effect on the next commit.
    pub fn free_page(&mut self, no: u32) {
        let mut buf = Box::new([0u8; PAGE_SIZE]);
        buf[0..4].copy_from_slice(&self.meta.freelist_head.to_le_bytes());
        self.write_page(no, buf);
        self.meta.freelist_head = no;
    }

    /// Durably commit every staged page as one atomic transaction.
    pub fn commit(&mut self) -> Result<()> {
        if self.dirty.is_empty() {
            return Ok(());
        }
        // Refresh page 0 so metadata lands atomically with the data it describes.
        self.dirty.insert(0, encode_header(self.meta));

        // 1. Record the whole transaction in the WAL and fsync it.
        let pages: Vec<(u32, &[u8; PAGE_SIZE])> =
            self.dirty.iter().map(|(no, buf)| (*no, &**buf)).collect();
        self.wal.write_transaction(&pages)?;
        drop(pages);

        // 2. The WAL is durable; now it is safe to update the database file.
        for (no, buf) in &self.dirty {
            self.file
                .seek(SeekFrom::Start(*no as u64 * PAGE_SIZE as u64))?;
            self.file.write_all(&buf[..])?;
        }
        self.file.sync_all()?;

        // 3. The database file is durable; the WAL is no longer needed.
        self.wal.reset()?;
        self.dirty.clear();
        self.committed = self.meta;
        Ok(())
    }

    /// Discard every staged page and restore metadata to the last commit.
    pub fn rollback(&mut self) {
        self.dirty.clear();
        self.meta = self.committed;
    }

    /// Replace the whole database with the contents of the file at `source` —
    /// a compact copy built by `VACUUM`. The swap is one WAL-protected commit,
    /// so a crash leaves either the old database intact or the new one.
    pub fn replace_with(&mut self, source: &Path) -> Result<()> {
        let mut source_file = File::open(source)?;
        let page_count = (source_file.metadata()?.len() / PAGE_SIZE as u64) as u32;
        self.dirty.clear();
        for no in 0..page_count {
            let mut buf = Box::new([0u8; PAGE_SIZE]);
            source_file.read_exact(&mut buf[..])?;
            self.dirty.insert(no, buf);
        }
        let header = self
            .dirty
            .get(&0)
            .ok_or_else(|| Error::corruption("vacuum image is missing its header page"))?;
        self.meta = decode_header(header)?;
        self.commit()?;
        // The pre-vacuum file may be longer than the compact image; drop the
        // tail. A crash before this point is harmless — the header wins.
        self.file
            .set_len(self.meta.page_count as u64 * PAGE_SIZE as u64)?;
        self.file.sync_all()?;
        Ok(())
    }
}

/// The WAL lives beside the database file with a `-wal` suffix.
pub(crate) fn wal_path(db: &Path) -> PathBuf {
    let mut name = db.as_os_str().to_os_string();
    name.push("-wal");
    PathBuf::from(name)
}

fn encode_header(meta: Meta) -> Box<[u8; PAGE_SIZE]> {
    let mut buf = Box::new([0u8; PAGE_SIZE]);
    buf[HDR_MAGIC..HDR_MAGIC + 8].copy_from_slice(MAGIC);
    buf[HDR_PAGE_SIZE..HDR_PAGE_SIZE + 4].copy_from_slice(&(PAGE_SIZE as u32).to_le_bytes());
    buf[HDR_PAGE_COUNT..HDR_PAGE_COUNT + 4].copy_from_slice(&meta.page_count.to_le_bytes());
    buf[HDR_FREELIST..HDR_FREELIST + 4].copy_from_slice(&meta.freelist_head.to_le_bytes());
    buf[HDR_CATALOG..HDR_CATALOG + 4].copy_from_slice(&meta.catalog_root.to_le_bytes());
    buf
}

fn decode_header(buf: &[u8; PAGE_SIZE]) -> Result<Meta> {
    if &buf[HDR_MAGIC..HDR_MAGIC + 8] != MAGIC {
        return Err(Error::corruption(
            "not a PrehniteDB v0.5 file (bad or outdated magic number)",
        ));
    }
    let page_size = u32::from_le_bytes(buf[HDR_PAGE_SIZE..HDR_PAGE_SIZE + 4].try_into().unwrap());
    if page_size as usize != PAGE_SIZE {
        return Err(Error::corruption(format!(
            "database uses {page_size}-byte pages, but this build expects {PAGE_SIZE}"
        )));
    }
    Ok(Meta {
        page_count: u32::from_le_bytes(buf[HDR_PAGE_COUNT..HDR_PAGE_COUNT + 4].try_into().unwrap()),
        freelist_head: u32::from_le_bytes(buf[HDR_FREELIST..HDR_FREELIST + 4].try_into().unwrap()),
        catalog_root: u32::from_le_bytes(buf[HDR_CATALOG..HDR_CATALOG + 4].try_into().unwrap()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDb {
        path: PathBuf,
    }

    impl TempDb {
        fn new() -> TempDb {
            let n = COUNTER.fetch_add(1, Ordering::SeqCst);
            let path =
                std::env::temp_dir().join(format!("prehnite-pager-{}-{n}.db", std::process::id()));
            let _ = std::fs::remove_file(&path);
            let _ = std::fs::remove_file(wal_path(&path));
            TempDb { path }
        }
    }

    impl Drop for TempDb {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
            let _ = std::fs::remove_file(wal_path(&self.path));
        }
    }

    fn filled(byte: u8) -> Box<[u8; PAGE_SIZE]> {
        Box::new([byte; PAGE_SIZE])
    }

    #[test]
    fn alloc_write_commit_reopen() {
        let db = TempDb::new();
        let mut pager = Pager::open(&db.path).unwrap();
        let a = pager.alloc_page().unwrap();
        let b = pager.alloc_page().unwrap();
        pager.write_page(a, filled(0xAA));
        pager.write_page(b, filled(0xBB));
        pager.commit().unwrap();
        drop(pager);

        let mut pager = Pager::open(&db.path).unwrap();
        assert_eq!(&pager.read_page(a).unwrap()[..], &[0xAA; PAGE_SIZE][..]);
        assert_eq!(&pager.read_page(b).unwrap()[..], &[0xBB; PAGE_SIZE][..]);
    }

    #[test]
    fn rollback_discards_writes_and_allocations() {
        let db = TempDb::new();
        let mut pager = Pager::open(&db.path).unwrap();
        let before = pager.page_count();
        let p = pager.alloc_page().unwrap();
        pager.write_page(p, filled(0x99));
        pager.rollback();
        // The allocation is undone: the page count is back where it started.
        assert_eq!(pager.page_count(), before);
    }

    #[test]
    fn freed_pages_are_recycled() {
        let db = TempDb::new();
        let mut pager = Pager::open(&db.path).unwrap();
        let p = pager.alloc_page().unwrap();
        pager.commit().unwrap();
        pager.free_page(p);
        pager.commit().unwrap();
        // The next allocation should hand back the very page we freed.
        assert_eq!(pager.alloc_page().unwrap(), p);
    }

    #[test]
    fn catalog_root_persists() {
        let db = TempDb::new();
        {
            let mut pager = Pager::open(&db.path).unwrap();
            pager.set_catalog_root(7);
            let p = pager.alloc_page().unwrap();
            pager.write_page(p, filled(1));
            pager.commit().unwrap();
        }
        let pager = Pager::open(&db.path).unwrap();
        assert_eq!(pager.catalog_root(), 7);
    }
}
