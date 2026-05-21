//! The pager — owner of the database file and gatekeeper of all page access.
//!
//! Pages pass through a fixed-size **buffer pool**: a bounded cache that holds
//! recently used pages and bounds the pager's memory no matter how large a
//! statement grows. When the pool is full it evicts under a CLOCK policy — a
//! clean victim is simply dropped (the database file still has it), a dirty
//! victim is *spilled* to the WAL. This is the classic "steal" discipline: a
//! statement's uncommitted writes need not all fit in memory at once.
//!
//! [`Pager::commit`] flushes whatever dirty pages are still resident to the
//! WAL, seals it with a marker, and only then copies the transaction into the
//! database file — so a statement still lands whole or not at all.
//! [`Pager::rollback`] drops the dirty pages and discards the WAL.
//!
//! Page 0 is the database header; the pager owns it and never exposes it as a
//! tree page. Every other page is handed out by number to the B+tree layer.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
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

/// How many pages the buffer pool holds before it must evict. At 4 KiB a page,
/// 1024 frames caps the pager's page cache at 4 MiB; a larger working set is
/// served by spilling to the WAL rather than by growing memory.
const POOL_CAPACITY: usize = 1024;

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

/// One resident page in the buffer pool.
struct Frame {
    no: u32,
    page: Box<[u8; PAGE_SIZE]>,
    /// Written since the last commit — must be spilled, not dropped, if evicted.
    dirty: bool,
    /// CLOCK's "second chance" bit: set on use, cleared by the sweeping hand.
    referenced: bool,
}

/// A bounded cache of pages with CLOCK eviction.
struct BufferPool {
    capacity: usize,
    frames: Vec<Frame>,
    /// Page number to its slot in `frames`.
    index: HashMap<u32, usize>,
    /// The CLOCK hand: the slot the next eviction sweep resumes from.
    hand: usize,
}

impl BufferPool {
    fn new(capacity: usize) -> BufferPool {
        BufferPool {
            capacity,
            frames: Vec::new(),
            index: HashMap::new(),
            hand: 0,
        }
    }

    /// The cached image of page `no`, if resident, marked as recently used.
    /// `read_page` copies the bytes straight out, so a caller never holds a
    /// reference into the pool — which is what makes eviction always safe and
    /// pin counts unnecessary.
    fn lookup(&mut self, no: u32) -> Option<&[u8; PAGE_SIZE]> {
        let &slot = self.index.get(&no)?;
        self.frames[slot].referenced = true;
        Some(&self.frames[slot].page)
    }

    /// Make page `no` resident. If a *dirty* page has to be evicted to make
    /// room, it is returned for the caller to spill to the WAL; a clean victim
    /// is just dropped, since the database file still holds it.
    fn put(
        &mut self,
        no: u32,
        page: Box<[u8; PAGE_SIZE]>,
        dirty: bool,
    ) -> Option<(u32, Box<[u8; PAGE_SIZE]>)> {
        if let Some(&slot) = self.index.get(&no) {
            let frame = &mut self.frames[slot];
            frame.page = page;
            frame.dirty |= dirty;
            frame.referenced = true;
            return None;
        }
        let frame = Frame {
            no,
            page,
            dirty,
            referenced: true,
        };
        if self.frames.len() < self.capacity {
            self.index.insert(no, self.frames.len());
            self.frames.push(frame);
            return None;
        }
        let slot = self.victim_slot();
        let evicted = std::mem::replace(&mut self.frames[slot], frame);
        self.index.remove(&evicted.no);
        self.index.insert(no, slot);
        evicted.dirty.then_some((evicted.no, evicted.page))
    }

    /// CLOCK: advance the hand, clearing reference bits as it passes, until it
    /// lands on an unreferenced frame. It terminates within two sweeps — the
    /// first clears every bit, so the second is guaranteed a hit.
    fn victim_slot(&mut self) -> usize {
        loop {
            self.hand = (self.hand + 1) % self.frames.len();
            if self.frames[self.hand].referenced {
                self.frames[self.hand].referenced = false;
            } else {
                return self.hand;
            }
        }
    }

    fn has_dirty(&self) -> bool {
        self.frames.iter().any(|frame| frame.dirty)
    }

    /// Drop every dirty frame (used by rollback). Clean frames are kept — they
    /// still match the database file, so they remain a valid warm cache.
    fn drop_dirty(&mut self) {
        self.frames.retain(|frame| !frame.dirty);
        self.reindex();
    }

    /// Mark every resident page clean — they match the database file once a
    /// commit has applied them.
    fn mark_all_clean(&mut self) {
        for frame in &mut self.frames {
            frame.dirty = false;
        }
    }

    /// Forget every page (used when VACUUM replaces the whole file).
    fn clear(&mut self) {
        self.frames.clear();
        self.index.clear();
        self.hand = 0;
    }

    /// Rebuild `index` after `frames` has been compacted by `retain`.
    fn reindex(&mut self) {
        self.index.clear();
        for (slot, frame) in self.frames.iter().enumerate() {
            self.index.insert(frame.no, slot);
        }
        self.hand = 0;
    }
}

/// Mediates all access to one database file.
pub struct Pager {
    file: File,
    wal: Wal,
    /// Working metadata, reflecting allocations made in the current statement.
    meta: Meta,
    /// Last durably committed metadata; restored verbatim on rollback.
    committed: Meta,
    /// The bounded page cache.
    pool: BufferPool,
    /// For each dirty page evicted to the WAL, the offset of its latest image
    /// there — so `read_page` can fetch it back. Emptied at commit/rollback.
    wal_index: HashMap<u32, u64>,
}

impl Pager {
    /// Open the database at `path`, creating it if absent. Any leftover WAL is
    /// recovered first, so the file is always consistent before use.
    pub fn open(path: impl AsRef<Path>) -> Result<Pager> {
        Pager::open_with_capacity(path, POOL_CAPACITY)
    }

    /// `open`, but with an explicit pool size — used by tests to force heavy
    /// eviction with a tiny pool.
    fn open_with_capacity(path: impl AsRef<Path>, capacity: usize) -> Result<Pager> {
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
            pool: BufferPool::new(capacity),
            wal_index: HashMap::new(),
        };

        if len == 0 {
            // A brand-new file: lay down the header page and flush it.
            pager.write_page(0, encode_header(pager.meta))?;
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
        // 1. Resident in the pool — the fast path, no I/O.
        if let Some(page) = self.pool.lookup(no) {
            return Ok(Box::new(*page));
        }
        // 2. Dirty, but evicted to the WAL to reclaim memory: read it back.
        if let Some(&offset) = self.wal_index.get(&no) {
            let page = self.wal.read_page_at(offset)?;
            self.admit(no, page.clone(), true)?;
            return Ok(page);
        }
        // 3. Clean on disk.
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
        self.admit(no, buf.clone(), false)?;
        Ok(buf)
    }

    /// Stage a page to be written on the next commit. May trigger a spill: if
    /// the pool is full, admitting this page evicts another, and a dirty
    /// evictee is written out to the WAL.
    pub fn write_page(&mut self, no: u32, buf: Box<[u8; PAGE_SIZE]>) -> Result<()> {
        self.admit(no, buf, true)
    }

    /// Bring `page` into the pool. If admitting it evicts a dirty page, spill
    /// that page to the WAL and remember where, so a later `read_page` of it
    /// can fetch it back.
    fn admit(&mut self, no: u32, page: Box<[u8; PAGE_SIZE]>, dirty: bool) -> Result<()> {
        if let Some((evicted_no, evicted_page)) = self.pool.put(no, page, dirty) {
            let offset = self.wal.append_page(evicted_no, &evicted_page)?;
            self.wal_index.insert(evicted_no, offset);
        }
        Ok(())
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
        self.write_page(no, Box::new([0u8; PAGE_SIZE]))?;
        Ok(no)
    }

    /// Return a page to the free list. Takes effect on the next commit.
    pub fn free_page(&mut self, no: u32) -> Result<()> {
        let mut buf = Box::new([0u8; PAGE_SIZE]);
        buf[0..4].copy_from_slice(&self.meta.freelist_head.to_le_bytes());
        self.write_page(no, buf)?;
        self.meta.freelist_head = no;
        Ok(())
    }

    /// Durably commit every staged page as one atomic transaction.
    pub fn commit(&mut self) -> Result<()> {
        // Nothing written since the last commit? Then there is nothing to do.
        if !self.pool.has_dirty() && self.wal_index.is_empty() {
            return Ok(());
        }
        // Refresh page 0 so metadata lands atomically with the data it describes.
        self.write_page(0, encode_header(self.meta))?;

        // 1. Append every still-resident dirty page to the WAL. Pages already
        //    spilled by eviction are already there; the marker then makes the
        //    whole transaction durable.
        self.flush_dirty()?;
        self.wal.seal()?;

        // 2. The transaction is durable in the WAL; copy it into the database
        //    file. `apply` is the very same routine crash recovery runs.
        self.wal.apply(&mut self.file)?;

        // 3. The database file is durable; the WAL is no longer needed.
        self.wal.reset()?;
        self.pool.mark_all_clean();
        self.wal_index.clear();
        self.committed = self.meta;
        Ok(())
    }

    /// Append every resident dirty page to the WAL.
    fn flush_dirty(&mut self) -> Result<()> {
        for slot in 0..self.pool.frames.len() {
            let frame = &self.pool.frames[slot];
            if frame.dirty {
                self.wal.append_page(frame.no, &frame.page)?;
            }
        }
        Ok(())
    }

    /// Discard every staged page and restore metadata to the last commit.
    pub fn rollback(&mut self) {
        self.pool.drop_dirty();
        self.wal_index.clear();
        self.wal.discard();
        self.meta = self.committed;
    }

    /// Replace the whole database with the contents of the file at `source` —
    /// a compact copy built by `VACUUM`. The image is streamed through the WAL
    /// one page at a time, so the swap is crash-safe and needs no more memory
    /// than a single page: a crash leaves either the old database or the new.
    pub fn replace_with(&mut self, source: &Path) -> Result<()> {
        // Abandon any cached or pending state; the whole file is changing.
        self.pool.clear();
        self.wal_index.clear();
        self.wal.discard();

        let mut source_file = File::open(source)?;
        let page_count = (source_file.metadata()?.len() / PAGE_SIZE as u64) as u32;

        let mut buf = Box::new([0u8; PAGE_SIZE]);
        let mut new_meta = self.meta;
        for no in 0..page_count {
            source_file.read_exact(&mut buf[..])?;
            if no == 0 {
                new_meta = decode_header(&buf)?;
            }
            self.wal.append_page(no, &buf)?;
        }
        self.wal.seal()?;
        self.wal.apply(&mut self.file)?;
        self.wal.reset()?;

        self.meta = new_meta;
        self.committed = new_meta;
        // The pre-swap file may be longer than the compact image; drop the
        // tail. A crash before this point is harmless — the header wins.
        self.file.set_len(page_count as u64 * PAGE_SIZE as u64)?;
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
            "not a PrehniteDB database file (bad or outdated magic number)",
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
        pager.write_page(a, filled(0xAA)).unwrap();
        pager.write_page(b, filled(0xBB)).unwrap();
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
        pager.write_page(p, filled(0x99)).unwrap();
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
        pager.free_page(p).unwrap();
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
            pager.write_page(p, filled(1)).unwrap();
            pager.commit().unwrap();
        }
        let pager = Pager::open(&db.path).unwrap();
        assert_eq!(pager.catalog_root(), 7);
    }

    #[test]
    fn commit_gathers_spilled_pages() {
        let db = TempDb::new();
        let mut allocated = Vec::new();
        {
            // A pool far smaller than the working set forces constant
            // eviction, so most of these pages reach the WAL by spilling.
            let mut pager = Pager::open_with_capacity(&db.path, 4).unwrap();
            for i in 0..40u32 {
                let p = pager.alloc_page().unwrap();
                pager.write_page(p, filled(i as u8)).unwrap();
                allocated.push(p);
            }
            pager.commit().unwrap();
        }
        // Reopen with a normal pool; every page must have survived eviction,
        // the WAL, and the commit's apply intact.
        let mut pager = Pager::open(&db.path).unwrap();
        for (i, &p) in allocated.iter().enumerate() {
            assert_eq!(&pager.read_page(p).unwrap()[..], &[i as u8; PAGE_SIZE][..]);
        }
    }

    #[test]
    fn evicted_dirty_page_reads_back() {
        let db = TempDb::new();
        let mut pager = Pager::open_with_capacity(&db.path, 4).unwrap();
        let target = pager.alloc_page().unwrap();
        pager.write_page(target, filled(0x5A)).unwrap();
        // Touch many other pages so `target` is certainly evicted.
        for _ in 0..20 {
            let p = pager.alloc_page().unwrap();
            pager.write_page(p, filled(0x11)).unwrap();
        }
        // `target` is no longer resident, but its dirty image lives in the
        // WAL; reading it back must return what we wrote, not stale zeros.
        assert_eq!(
            &pager.read_page(target).unwrap()[..],
            &[0x5A; PAGE_SIZE][..]
        );
    }

    #[test]
    fn rollback_discards_spilled_pages() {
        let db = TempDb::new();
        let mut pager = Pager::open_with_capacity(&db.path, 4).unwrap();
        let before = pager.page_count();
        for _ in 0..30 {
            let p = pager.alloc_page().unwrap();
            pager.write_page(p, filled(0xEE)).unwrap();
        }
        pager.rollback();
        // Every allocation and every spill is undone.
        assert_eq!(pager.page_count(), before);
        // The pager — and the reused WAL — are still sound: a fresh
        // transaction commits cleanly over the abandoned one.
        let p = pager.alloc_page().unwrap();
        pager.write_page(p, filled(0x01)).unwrap();
        pager.commit().unwrap();
        assert_eq!(&pager.read_page(p).unwrap()[..], &[0x01; PAGE_SIZE][..]);
    }
}
