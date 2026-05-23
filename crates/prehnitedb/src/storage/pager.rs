//! The pager — owner of the database file and gatekeeper of all page access.
//!
//! Pages pass through a fixed-size **buffer pool**: a bounded cache that holds
//! recently used pages and bounds the pager's memory no matter how large a
//! statement grows. When the pool is full it evicts under a CLOCK policy — a
//! clean victim is simply dropped (the database file still has it), a dirty
//! victim is *spilled* to the WAL. This is the classic "steal" discipline: a
//! statement's uncommitted writes need not all fit in memory at once.
//!
//! The pool is a [`SharedPool`]: every pager open on one database file — the
//! server's writer and each concurrent reader alike — points at the *same*
//! cache. v0.20 splits that cache into [`POOL_SHARDS`] independent shards,
//! each with its own mutex and its own CLOCK hand: a page is routed to a
//! shard by `page_no % shard_count`, so two readers touching pages in
//! different shards never serialise. `read_page` hands back a [`PageRef`] — a
//! pinned, reference-counted handle onto a frame, copied from nothing — and
//! while that handle lives the pool will not evict the frame it names; the
//! CLOCK sweep simply steps over a pinned frame.
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
use std::sync::{Arc, Mutex, MutexGuard};

use crate::error::{Error, Result};
use crate::storage::page::PAGE_SIZE;
use crate::storage::wal::Wal;

/// Identifies the file format; bumped if the on-disk layout ever changes.
const MAGIC: &[u8; 8] = b"PREHNDB4";

const HDR_MAGIC: usize = 0;
const HDR_PAGE_SIZE: usize = 8;
const HDR_PAGE_COUNT: usize = 12;
const HDR_FREELIST: usize = 16;
const HDR_CATALOG: usize = 20;

/// How many pages the buffer pool holds before it must evict. At 4 KiB a page,
/// 1024 frames caps the pager's page cache at 4 MiB; a larger working set is
/// served by spilling to the WAL rather than by growing memory.
const POOL_CAPACITY: usize = 1024;

/// The default number of shards a [`SharedPool`] splits into. Each shard owns
/// its own mutex, CLOCK state, and `POOL_CAPACITY / POOL_SHARDS` frames. With
/// 16 shards a uniformly distributed read workload contends on each shard's
/// mutex one sixteenth as often as a one-mutex pool. A small total capacity
/// (the tests use a few-frame pool to force eviction) clamps the shard count
/// down to capacity, so each shard always owns at least one frame.
pub const POOL_SHARDS: usize = 16;

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

/// One cached page: a page number and its bytes, immutable once admitted.
/// Shared by [`Arc`], so a [`PageRef`] can lend the bytes out copy-free while
/// the pool keeps its own reference for the cache.
struct Frame {
    no: u32,
    page: Box<[u8; PAGE_SIZE]>,
}

/// A pool slot: a frame plus the bookkeeping the pool mutates under its lock.
/// The dirty and CLOCK bits live here, off the shared [`Frame`], so the frame
/// itself stays immutable.
struct Slot {
    frame: Arc<Frame>,
    /// Written since the last commit — must be spilled, not dropped, if evicted.
    dirty: bool,
    /// CLOCK's "second chance" bit: set on use, cleared by the sweeping hand.
    referenced: bool,
}

/// A bounded cache of pages with CLOCK eviction.
struct BufferPool {
    capacity: usize,
    slots: Vec<Slot>,
    /// Page number to its index in `slots`.
    index: HashMap<u32, usize>,
    /// The CLOCK hand: the slot the next eviction sweep resumes from.
    hand: usize,
}

impl BufferPool {
    fn new(capacity: usize) -> BufferPool {
        BufferPool {
            capacity,
            slots: Vec::new(),
            index: HashMap::new(),
            hand: 0,
        }
    }

    /// The frame holding page `no`, if resident, marked as recently used. The
    /// returned `Arc` is a fresh handle — that clone is what pins the frame.
    fn lookup(&mut self, no: u32) -> Option<Arc<Frame>> {
        let idx = *self.index.get(&no)?;
        self.slots[idx].referenced = true;
        Some(Arc::clone(&self.slots[idx].frame))
    }

    /// Make `frame` resident under page number `frame.no`. If a *dirty* page is
    /// evicted to make room it is returned, for the caller to spill to the WAL;
    /// a clean victim is just dropped. Fails only if every frame is pinned.
    fn put(&mut self, frame: Arc<Frame>, dirty: bool) -> Result<Option<Arc<Frame>>> {
        let no = frame.no;
        if let Some(&idx) = self.index.get(&no) {
            // Already resident: install the new image in place.
            let slot = &mut self.slots[idx];
            slot.frame = frame;
            slot.dirty |= dirty;
            slot.referenced = true;
            return Ok(None);
        }
        let new = Slot {
            frame,
            dirty,
            referenced: true,
        };
        if self.slots.len() < self.capacity {
            self.index.insert(no, self.slots.len());
            self.slots.push(new);
            return Ok(None);
        }
        let victim = self.victim_slot().ok_or_else(|| {
            Error::exhausted(format!(
                "buffer pool is full and all {} frames are pinned",
                self.capacity
            ))
        })?;
        let evicted = std::mem::replace(&mut self.slots[victim], new);
        self.index.remove(&evicted.frame.no);
        self.index.insert(no, victim);
        Ok(evicted.dirty.then_some(evicted.frame))
    }

    /// CLOCK over the *unpinned* slots: advance the hand, clearing reference
    /// bits as it passes, until it reaches an unpinned, unreferenced slot. A
    /// pinned slot — one a live [`PageRef`] still holds — is never a victim. At
    /// most two sweeps, so it always ends; `None` if every slot is pinned.
    fn victim_slot(&mut self) -> Option<usize> {
        let n = self.slots.len();
        for _ in 0..2 * n {
            self.hand = (self.hand + 1) % n;
            if Arc::strong_count(&self.slots[self.hand].frame) > 1 {
                continue; // pinned: a PageRef is out — never evict it
            }
            if self.slots[self.hand].referenced {
                self.slots[self.hand].referenced = false;
            } else {
                return Some(self.hand);
            }
        }
        None
    }

    fn has_dirty(&self) -> bool {
        self.slots.iter().any(|slot| slot.dirty)
    }

    /// Drop every dirty frame (used by rollback). Clean frames are kept — they
    /// still match the database file, so they remain a valid warm cache.
    fn drop_dirty(&mut self) {
        self.slots.retain(|slot| !slot.dirty);
        self.reindex();
    }

    /// Mark every resident page clean — they match the database file once a
    /// commit has applied them.
    fn mark_all_clean(&mut self) {
        for slot in &mut self.slots {
            slot.dirty = false;
        }
    }

    /// Forget every page (used when VACUUM replaces the whole file).
    fn clear(&mut self) {
        self.slots.clear();
        self.index.clear();
        self.hand = 0;
    }

    /// Rebuild `index` after `slots` has been compacted by `retain`.
    fn reindex(&mut self) {
        self.index.clear();
        for (i, slot) in self.slots.iter().enumerate() {
            self.index.insert(slot.frame.no, i);
        }
        self.hand = 0;
    }
}

/// The buffer pool, shared by every [`Pager`] open on one database file.
///
/// v0.12 gave each concurrent reader its own pager *and its own pool*. v0.13
/// hands every pager — the writer's and the readers' — one `SharedPool`, so
/// they split a single warm cache. v0.14 stopped copying: `read_page` returns
/// a [`PageRef`] borrowed straight from a frame, so the lock guards only the
/// pool's bookkeeping — the frame's bytes live in an [`Arc`] of their own,
/// reachable without the lock once handed out. v0.20 splits the pool into
/// [`POOL_SHARDS`] independent shards: each shard is a CLOCK cache with its
/// own mutex, and a page is routed by `page_no % shard_count`. Two readers
/// touching pages in different shards no longer contend on one mutex.
///
/// Cloning a `SharedPool` is an [`Arc`] bump: every clone is the same pool.
#[derive(Clone)]
pub struct SharedPool {
    /// One bounded CLOCK cache per shard. Stored behind an [`Arc<[T]>`] so
    /// every clone of the pool shares the same shards.
    shards: Arc<[Mutex<BufferPool>]>,
}

impl SharedPool {
    /// A new, empty shared pool of the default capacity.
    pub fn new() -> SharedPool {
        SharedPool::with_capacity(POOL_CAPACITY)
    }

    /// A new, empty shared pool whose shards together hold `capacity` frames.
    /// The capacity is divided as evenly as possible across [`POOL_SHARDS`]
    /// shards; if `capacity` is smaller than `POOL_SHARDS` the shard count is
    /// clamped to `capacity`, so every shard always owns at least one frame.
    fn with_capacity(capacity: usize) -> SharedPool {
        let capacity = capacity.max(1);
        let shard_count = capacity.min(POOL_SHARDS);
        let per_shard = capacity / shard_count;
        let remainder = capacity % shard_count;
        let mut shards = Vec::with_capacity(shard_count);
        for i in 0..shard_count {
            // Spread the remainder one frame at a time over the leading
            // shards so the totals add up exactly to `capacity`.
            let cap = per_shard + usize::from(i < remainder);
            shards.push(Mutex::new(BufferPool::new(cap)));
        }
        SharedPool {
            shards: Arc::from(shards.into_boxed_slice()),
        }
    }

    /// Lock the shard that owns page `no`. Cheap when the shard is
    /// uncontended; with [`POOL_SHARDS`] shards the contention rate is
    /// `1 / POOL_SHARDS` of a single-mutex pool's, for a uniformly
    /// distributed workload.
    fn shard(&self, no: u32) -> MutexGuard<'_, BufferPool> {
        // The compiler turns `% N` into a bitmask when `N` is a power of two,
        // which `POOL_SHARDS` always is.
        let idx = (no as usize) % self.shards.len();
        self.shards[idx]
            .lock()
            .expect("a thread panicked while holding a buffer-pool shard")
    }

    /// Lock shard `idx`. Used by the iteration helpers that walk every shard
    /// (commit, rollback, clear), which do not have a page number to route by.
    fn shard_at(&self, idx: usize) -> MutexGuard<'_, BufferPool> {
        self.shards[idx]
            .lock()
            .expect("a thread panicked while holding a buffer-pool shard")
    }

    /// The frame for page `no` if it is resident, marked recently used. The
    /// returned `Arc` is a fresh handle; holding it pins the frame.
    fn get(&self, no: u32) -> Option<Arc<Frame>> {
        self.shard(no).lookup(no)
    }

    /// Admit `frame`, returning a dirty evictee for the caller to spill. Fails
    /// only when the *destination shard* is full and every frame in it is
    /// pinned — other shards are unaffected.
    fn put(&self, frame: Arc<Frame>, dirty: bool) -> Result<Option<Arc<Frame>>> {
        let no = frame.no;
        self.shard(no).put(frame, dirty)
    }

    /// Whether any resident page in any shard has been written since the last
    /// commit. Stops as soon as a dirty page is found.
    fn has_dirty(&self) -> bool {
        (0..self.shards.len()).any(|i| self.shard_at(i).has_dirty())
    }

    /// Call `f` on every resident dirty page, walking the shards in order and
    /// holding each shard's lock only while its slots are visited. A commit
    /// runs with no reader active, so this contends with no one; the per-shard
    /// locking is mainly tidy bookkeeping.
    fn for_each_dirty(&self, mut f: impl FnMut(u32, &[u8; PAGE_SIZE]) -> Result<()>) -> Result<()> {
        for i in 0..self.shards.len() {
            let pool = self.shard_at(i);
            for slot in &pool.slots {
                if slot.dirty {
                    f(slot.frame.no, &slot.frame.page)?;
                }
            }
        }
        Ok(())
    }

    /// Mark every resident page in every shard clean — a commit has applied
    /// them all.
    fn mark_all_clean(&self) {
        for i in 0..self.shards.len() {
            self.shard_at(i).mark_all_clean();
        }
    }

    /// Drop every dirty frame in every shard, keeping the clean ones as a
    /// warm cache.
    fn drop_dirty(&self) {
        for i in 0..self.shards.len() {
            self.shard_at(i).drop_dirty();
        }
    }

    /// Forget every page in every shard.
    fn clear(&self) {
        for i in 0..self.shards.len() {
            self.shard_at(i).clear();
        }
    }
}

impl Default for SharedPool {
    fn default() -> SharedPool {
        SharedPool::new()
    }
}

/// A pinned, read-only handle to a cached page.
///
/// [`Pager::read_page`] returns one of these rather than copying the page out:
/// it is an [`Arc`] reference to the frame, and that reference *is* the pin —
/// while a `PageRef` lives, the pool will not evict the frame it names.
/// Dropping it releases the pin. Producing one costs a single atomic increment.
pub struct PageRef {
    frame: Arc<Frame>,
}

impl PageRef {
    /// The page's bytes, borrowed straight from the cached frame.
    pub fn bytes(&self) -> &[u8; PAGE_SIZE] {
        &self.frame.page
    }
}

impl std::ops::Deref for PageRef {
    type Target = [u8; PAGE_SIZE];

    fn deref(&self) -> &[u8; PAGE_SIZE] {
        &self.frame.page
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
    /// The page cache — shared with every other pager open on this file.
    pool: SharedPool,
    /// For each dirty page evicted to the WAL, the offset of its latest image
    /// there — so `read_page` can fetch it back. Emptied at commit/rollback.
    wal_index: HashMap<u32, u64>,
}

impl Pager {
    /// Open the database at `path`, creating it if absent, with a private page
    /// cache. Any leftover WAL is recovered first, so the file is consistent
    /// before use.
    pub fn open(path: impl AsRef<Path>) -> Result<Pager> {
        Pager::open_with_pool(path, SharedPool::new())
    }

    /// Open the database at `path`, taking `pool` as the page cache. The server
    /// hands one pool to the writer and to every reader, so a reader opens
    /// against a warm cache rather than filling a private one.
    pub fn open_with_pool(path: impl AsRef<Path>, pool: SharedPool) -> Result<Pager> {
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
            pool,
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

    /// `open`, with an explicit pool capacity — tests use a tiny pool to force
    /// heavy eviction.
    #[cfg(test)]
    fn open_with_capacity(path: impl AsRef<Path>, capacity: usize) -> Result<Pager> {
        Pager::open_with_pool(path, SharedPool::with_capacity(capacity))
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

    /// Fetch page `no` as a [`PageRef`] — a pinned, copy-free handle onto the
    /// cached frame. The frame is not evicted while the `PageRef` lives.
    pub fn read_page(&mut self, no: u32) -> Result<PageRef> {
        // 1. Resident in the pool — the fast path, no I/O.
        if let Some(frame) = self.pool.get(no) {
            return Ok(PageRef { frame });
        }
        // 2. Dirty, but evicted to the WAL to reclaim memory: read it back.
        if let Some(&offset) = self.wal_index.get(&no) {
            let page = self.wal.read_page_at(offset)?;
            return Ok(PageRef {
                frame: self.admit(no, page, true)?,
            });
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
        Ok(PageRef {
            frame: self.admit(no, buf, false)?,
        })
    }

    /// Stage a page to be written on the next commit. May trigger a spill: if
    /// the pool is full, admitting this page evicts another, and a dirty
    /// evictee is written out to the WAL.
    pub fn write_page(&mut self, no: u32, buf: Box<[u8; PAGE_SIZE]>) -> Result<()> {
        self.admit(no, buf, true)?;
        Ok(())
    }

    /// Bring `page` into the pool as page `no`, returning its frame. If
    /// admitting it evicts a dirty page, spill that page to the WAL and
    /// remember where, so a later `read_page` of it can fetch it back.
    fn admit(&mut self, no: u32, page: Box<[u8; PAGE_SIZE]>, dirty: bool) -> Result<Arc<Frame>> {
        let frame = Arc::new(Frame { no, page });
        if let Some(evicted) = self.pool.put(Arc::clone(&frame), dirty)? {
            let offset = self.wal.append_page(evicted.no, &evicted.page)?;
            self.wal_index.insert(evicted.no, offset);
        }
        Ok(frame)
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
        let wal = &mut self.wal;
        self.pool
            .for_each_dirty(|no, page| wal.append_page(no, page).map(drop))
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

    #[test]
    fn a_shared_pool_shadows_the_disk() {
        use std::io::Write;

        // A page committed through one pager stays resident in the shared
        // pool. A second pager on that pool serves the page from memory, not
        // the file — so corrupting the file behind its back changes nothing
        // the second pager can see.
        let db = TempDb::new();
        let pool = SharedPool::new();
        let target = {
            let mut pager = Pager::open_with_pool(&db.path, pool.clone()).unwrap();
            let p = pager.alloc_page().unwrap();
            pager.write_page(p, filled(0x7C)).unwrap();
            pager.commit().unwrap();
            p
        };

        // Overwrite the page's on-disk image with garbage.
        let mut raw = OpenOptions::new().write(true).open(&db.path).unwrap();
        raw.seek(SeekFrom::Start(target as u64 * PAGE_SIZE as u64))
            .unwrap();
        raw.write_all(&[0xFF; PAGE_SIZE]).unwrap();
        raw.sync_all().unwrap();
        drop(raw);

        // The second pager shares the warm pool, so it reads the pristine
        // bytes the first pager left there — not the garbage now on disk.
        let mut pager = Pager::open_with_pool(&db.path, pool).unwrap();
        assert_eq!(
            &pager.read_page(target).unwrap()[..],
            &[0x7C; PAGE_SIZE][..]
        );
    }

    #[test]
    fn concurrent_pagers_share_one_pool() {
        // Fill a database, then read it back from eight threads at once, each
        // through its own pager over the one shared pool. A small pool forces
        // the threads to evict and re-admit under contention; a data race or a
        // deadlock would surface here. Capacity 64 keeps each of v0.20's 16
        // shards holding 4 frames — fewer and the test races on a one-frame
        // shard where two threads' pins fight for the same slot.
        let db = TempDb::new();
        let pool = SharedPool::with_capacity(64);
        let pages: Vec<u32> = {
            let mut pager = Pager::open_with_pool(&db.path, pool.clone()).unwrap();
            let mut pages = Vec::new();
            for i in 0..60u32 {
                let p = pager.alloc_page().unwrap();
                pager.write_page(p, filled(i as u8)).unwrap();
                pages.push(p);
            }
            pager.commit().unwrap();
            pages
        };

        let mut handles = Vec::new();
        for _ in 0..8 {
            let pool = pool.clone();
            let path = db.path.clone();
            let pages = pages.clone();
            handles.push(std::thread::spawn(move || {
                let mut pager = Pager::open_with_pool(&path, pool).unwrap();
                for (i, &p) in pages.iter().enumerate() {
                    assert_eq!(&pager.read_page(p).unwrap()[..], &[i as u8; PAGE_SIZE][..]);
                }
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn one_shard_pinned_does_not_block_other_shards() {
        // With 16 shards × 1 frame, page numbers fall into shards by
        // `no % 16`. Pinning page 16 saturates shard 0 — a second admission
        // to that shard fails — but admissions to other shards keep working.
        let db = TempDb::new();
        let mut pager = Pager::open_with_capacity(&db.path, 16).unwrap();
        for _ in 0..48u32 {
            let p = pager.alloc_page().unwrap();
            pager.write_page(p, filled(0xAA)).unwrap();
        }
        pager.commit().unwrap();

        // Page 16 -> shard 0; page 32 -> shard 0; page 17 -> shard 1.
        let pin = pager.read_page(16).unwrap();

        // Shard 0's only frame is pinned, so another shard-0 page cannot be
        // admitted — the per-shard `put` returns Exhausted.
        assert!(pager.read_page(32).is_err());
        // Shard 1 is untouched by shard 0's pin, so an admission there
        // succeeds normally. This is the value of sharding: contention stays
        // local to one shard.
        assert!(pager.read_page(17).is_ok());

        drop(pin);
        // With the pin gone, shard 0 evicts page 16 and admits page 32.
        assert!(pager.read_page(32).is_ok());
    }

    #[test]
    fn shard_count_is_clamped_to_capacity() {
        // A pool too small for the default shard count uses fewer shards so
        // every shard owns at least one frame. The total capacity is exactly
        // what the caller asked for — none of the budget is lost.
        let pool = SharedPool::with_capacity(4);
        assert_eq!(pool.shards.len(), 4);
        let total: usize = (0..pool.shards.len())
            .map(|i| pool.shard_at(i).capacity)
            .sum();
        assert_eq!(total, 4);

        // The default capacity hits the full shard count.
        let pool = SharedPool::with_capacity(POOL_CAPACITY);
        assert_eq!(pool.shards.len(), POOL_SHARDS);
        let total: usize = (0..pool.shards.len())
            .map(|i| pool.shard_at(i).capacity)
            .sum();
        assert_eq!(total, POOL_CAPACITY);
    }

    #[test]
    fn pinned_pages_block_eviction() {
        // A PageRef pins its frame: while it lives the pool may not evict that
        // frame. Fill a tiny pool with pinned pages and a further admission has
        // nowhere to land — until a pin is dropped.
        let db = TempDb::new();
        let mut pager = Pager::open_with_capacity(&db.path, 4).unwrap();
        let mut pages = Vec::new();
        for i in 0..5u32 {
            let p = pager.alloc_page().unwrap();
            pager.write_page(p, filled(i as u8)).unwrap();
            pages.push(p);
        }
        pager.commit().unwrap();

        // Pin four distinct pages — the whole four-frame pool.
        let pin0 = pager.read_page(pages[0]).unwrap();
        let _pin1 = pager.read_page(pages[1]).unwrap();
        let _pin2 = pager.read_page(pages[2]).unwrap();
        let _pin3 = pager.read_page(pages[3]).unwrap();

        // A fifth page cannot be admitted: every frame is pinned.
        assert!(pager.read_page(pages[4]).is_err());

        // Drop one pin and the fifth page has somewhere to go.
        drop(pin0);
        assert_eq!(
            &pager.read_page(pages[4]).unwrap()[..],
            &[4u8; PAGE_SIZE][..]
        );
    }
}
