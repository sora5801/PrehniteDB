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

use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::error::{Error, Result};
use crate::storage::page::PAGE_SIZE;
use crate::storage::wal::Wal;

/// Identifies the file format; bumped if the on-disk layout ever changes.
/// v0.49 bumped this to PREHNDB11 — Schema now carries
/// `mutations_since_analyze` for the v0.49 auto-analyze loop in
/// `prehnited`'s reclaimer thread. v0.48 databases and earlier hit
/// a clear "incompatible file format" error on open.
///
/// Versions 1–9 used `b"PREHNDB<ascii-digit>"`; v0.10+ overflows one
/// ASCII digit, so v0.48 switched to `b"PREHNDB" + <raw-u8>` — same
/// first seven bytes (still recognisable as PREHNDB), last byte is
/// the version as a raw integer. Room for 256 schema versions total
/// before we'd need a longer magic.
const MAGIC: &[u8; 8] = b"PREHNDB\x0b"; // 0x0b = 11 = v0.49 format

const HDR_MAGIC: usize = 0;
const HDR_PAGE_SIZE: usize = 8;
const HDR_PAGE_COUNT: usize = 12;
const HDR_FREELIST: usize = 16;
const HDR_CATALOG: usize = 20;
const HDR_NEXT_TX: usize = 24;

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
pub(crate) struct Meta {
    /// Total pages in the file, including the header page.
    page_count: u32,
    /// Head of the free-page list, or 0 if there are no free pages.
    freelist_head: u32,
    /// Root page of the catalog B+tree, or 0 before it is created.
    catalog_root: u32,
    /// Next unused MVCC transaction ID. Each writer takes the current value
    /// at BEGIN, stamps it into the rows it touches, and durably increments
    /// on COMMIT. A rollback leaves the in-memory value advanced — the
    /// reserved ID becomes a gap, since no row in the file carries it.
    next_tx_id: u64,
}

/// The database header (`Meta`) shared across every `Pager` open on one
/// file. Every allocation, freelist update, and catalog-root change goes
/// through this mutex, so two concurrent writers cannot hand out the same
/// page number or trample each other's freelist updates.
///
/// Cloning a `SharedMeta` is an [`Arc`] bump; every clone names the same
/// underlying state.
///
/// Unlike v0.27's per-pager `Meta`, the shared meta is **not reverted on
/// rollback**: an allocator that aborts leaves its bumps in place rather
/// than risk stomping on a peer writer's later allocation. The pages it
/// allocated are caught by the pager's `pending_freelist` for reuse, and
/// any that remain truly orphaned are reclaimed by `VACUUM`.
///
/// Also tracks a monotonic counter used to mint unique WAL file names
/// — each `Pager` opens its own log file at `<db>-wal-<id>` so two
/// concurrent writers' cursors do not collide on one shared file.
#[derive(Clone)]
pub struct SharedMeta {
    inner: Arc<Mutex<SharedMetaInner>>,
    /// v0.53: a separate lock held only during a commit's
    /// flush + WAL apply + header-write phase, NOT during routine
    /// allocations. Splitting this from the meta `inner` mutex
    /// means a peer pager can bump `page_count` (under `inner`)
    /// while another pager is mid-commit (under `apply_lock`),
    /// instead of waiting for the commit to finish. v0.52's
    /// single-mutex fix correctly serialised commits but at the
    /// cost of also serialising allocations.
    ///
    /// The split is safe because the commit's "snapshot the latest
    /// meta and write it to page 0" step happens inside a brief
    /// `inner` lock acquisition *within* the apply_lock-held
    /// section. A peer allocator that bumps meta during commit
    /// either finishes before the commit's snapshot (its
    /// allocation is captured) or finishes after (the next commit
    /// captures it). Either way the on-disk header eventually
    /// reflects the allocation.
    apply_lock: Arc<Mutex<()>>,
}

struct SharedMetaInner {
    meta: Meta,
    /// Monotonic counter for per-pager WAL file naming.
    next_wal_id: u32,
}

impl SharedMeta {
    fn new(meta: Meta) -> SharedMeta {
        SharedMeta {
            inner: Arc::new(Mutex::new(SharedMetaInner {
                meta,
                next_wal_id: 0,
            })),
            apply_lock: Arc::new(Mutex::new(())),
        }
    }

    fn lock(&self) -> MutexGuard<'_, SharedMetaInner> {
        self.inner.lock().expect("poisoned shared meta")
    }

    /// A by-value copy of the current meta — used at commit time to
    /// encode the header page and at recovery to seed the in-memory state.
    fn snapshot(&self) -> Meta {
        self.lock().meta
    }

    /// v0.53: run a commit's flush+apply closure under the dedicated
    /// `apply_lock`, then snapshot meta and write the header — the
    /// snapshot+write is done inside a brief `inner` (meta) lock
    /// acquisition so allocators racing against the commit are
    /// serialised with the header write itself rather than the full
    /// apply.
    ///
    /// **What this fixes vs v0.52.** v0.52 held the single
    /// `shared_meta.inner` mutex across the entire apply phase,
    /// which correctly serialised commits but also blocked
    /// allocations (which also take that mutex) for the apply
    /// duration. v0.53 splits the lock:
    ///
    /// - `apply_lock` (this method): held for the full
    ///   flush + WAL apply + header write — serialises peer
    ///   commits, prevents the v0.52 lost-write race.
    /// - `inner` (meta) lock: still taken by allocators, plus
    ///   briefly here to snapshot meta + write header. Allocators
    ///   no longer wait for the slow apply phase.
    ///
    /// The race-correctness argument: any peer allocator that
    /// bumps meta during our apply either (a) finishes before
    /// our snapshot, in which case our header captures it, or
    /// (b) finishes after our snapshot, in which case the peer's
    /// own next commit captures it via the same path. Either
    /// way the on-disk header eventually reflects every commit.
    fn commit_apply(
        &self,
        file: &mut File,
        apply: impl FnOnce(&mut File) -> Result<()>,
    ) -> Result<()> {
        use std::io::Write;
        // Hold apply_lock for the duration. Peer commits queue up
        // here; peer allocators do NOT.
        let _apply_guard = self
            .apply_lock
            .lock()
            .expect("poisoned apply lock");
        // 1. Apply the WAL to the data file. Peer commits wait;
        //    peer allocators continue (they take `inner`, not
        //    `apply_lock`).
        apply(file)?;
        // 2. Snapshot meta + write header. The `inner` lock is
        //    held only here — long enough to encode the bytes and
        //    keep no allocator in flight while we serialise the
        //    header to disk.
        let snapshot = self.lock().meta;
        let bytes = encode_header(snapshot);
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&bytes[..])?;
        file.sync_all()?;
        Ok(())
    }

    fn page_count(&self) -> u32 {
        self.lock().meta.page_count
    }

    fn catalog_root(&self) -> u32 {
        self.lock().meta.catalog_root
    }

    fn set_catalog_root(&self, root: u32) {
        self.lock().meta.catalog_root = root;
    }

    fn next_tx_id(&self) -> u64 {
        self.lock().meta.next_tx_id
    }

    /// Bump the next-TX counter to at least `id`. A no-op if the counter
    /// is already past it — necessary because every writer races with
    /// every other writer to update this.
    fn bump_next_tx_id_to(&self, id: u64) {
        let mut guard = self.lock();
        if id > guard.meta.next_tx_id {
            guard.meta.next_tx_id = id;
        }
    }

    /// Replace the entire meta — used by `Pager::replace_with` (VACUUM)
    /// when it adopts a freshly built compact image.
    fn install(&self, meta: Meta) {
        self.lock().meta = meta;
    }

    /// Allocate a fresh WAL ID for a new `Pager`. Each pager writes to a
    /// distinct `<db>-wal-<id>` file so their append cursors never
    /// collide on one shared file.
    fn mint_wal_id(&self) -> u32 {
        let mut guard = self.lock();
        let id = guard.next_wal_id;
        guard.next_wal_id += 1;
        id
    }
}

/// A read latch on one B+tree page, owned without any borrowed lifetime
/// in the type. v0.30's optimistic descent walks down a tree releasing
/// the parent latch only after taking the child's, so guards have to
/// outlive the call frame they were acquired in — which `std::sync`'s
/// borrowed guards make awkward. This wrapper bundles the `Arc<RwLock>`
/// with its guard; field-drop order ensures the guard releases the lock
/// before the Arc decrements.
///
/// The `'static` transmute is sound because the `Arc<RwLock>` is moved
/// into the struct, kept alive for the guard's entire lifetime, and
/// dropped *after* the guard (Rust drops struct fields in declaration
/// order).
pub struct OwnedReadLatch {
    // Declaration order matters: `guard` drops before `_lock`. Both
    // fields exist only to live for the lifetime of the struct, so
    // their value is never read after construction.
    #[allow(dead_code)]
    guard: RwLockReadGuard<'static, ()>,
    #[allow(dead_code)]
    _lock: Arc<RwLock<()>>,
}

impl OwnedReadLatch {
    /// Acquire the read lock and return an owned guard.
    pub fn acquire(lock: Arc<RwLock<()>>) -> OwnedReadLatch {
        let guard = lock.read().expect("poisoned page latch");
        // SAFETY: the Arc `lock` is moved into the struct alongside the
        // guard, keeping the underlying `RwLock` alive for the guard's
        // entire lifetime. Field-drop order (declaration order) drops
        // `guard` first, releasing the read lock, before `_lock` drops
        // and decrements the Arc — so the borrow remains valid for
        // every observable use of `self`.
        let guard = unsafe {
            std::mem::transmute::<RwLockReadGuard<'_, ()>, RwLockReadGuard<'static, ()>>(guard)
        };
        OwnedReadLatch { guard, _lock: lock }
    }
}

/// Write latch counterpart of [`OwnedReadLatch`].
pub struct OwnedWriteLatch {
    #[allow(dead_code)]
    guard: RwLockWriteGuard<'static, ()>,
    #[allow(dead_code)]
    _lock: Arc<RwLock<()>>,
}

impl OwnedWriteLatch {
    pub fn acquire(lock: Arc<RwLock<()>>) -> OwnedWriteLatch {
        let guard = lock.write().expect("poisoned page latch");
        // SAFETY: same argument as `OwnedReadLatch::acquire`.
        let guard = unsafe {
            std::mem::transmute::<RwLockWriteGuard<'_, ()>, RwLockWriteGuard<'static, ()>>(guard)
        };
        OwnedWriteLatch { guard, _lock: lock }
    }
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

    /// Forget every page (used when VACUUM replaces the whole file).
    fn clear(&mut self) {
        self.slots.clear();
        self.index.clear();
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
    /// Per-page latches — v0.30's B+tree latch crabbing acquires these
    /// to serialise concurrent writers at the page level instead of the
    /// table level. Created lazily on first request, never shrunk
    /// (freed pages keep their latch entry; the entry costs ~80 bytes
    /// and the map is bounded by the file's page count).
    latches: Arc<Mutex<HashMap<u32, Arc<RwLock<()>>>>>,
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
            latches: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// The latch protecting page `no`. Lazily created on first use.
    /// Callers wrap the returned `Arc<RwLock<()>>` in
    /// [`OwnedReadLatch`] or [`OwnedWriteLatch`] to hold across
    /// recursive descent without lifetime gymnastics.
    pub fn latch(&self, no: u32) -> Arc<RwLock<()>> {
        let mut latches = self
            .latches
            .lock()
            .expect("a thread panicked while holding the latch table");
        Arc::clone(
            latches
                .entry(no)
                .or_insert_with(|| Arc::new(RwLock::new(()))),
        )
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

    /// Mark these specific pages clean. Used by a `Pager` after its own
    /// commit has applied its writes — only its pages should lose the
    /// dirty bit, not pages a concurrent peer writer is still staging.
    fn mark_clean(&self, pages: &HashSet<u32>) {
        for &no in pages {
            let mut shard = self.shard(no);
            if let Some(&idx) = shard.index.get(&no) {
                shard.slots[idx].dirty = false;
            }
        }
    }

    /// Drop these specific pages from the pool. Used by a `Pager`'s
    /// rollback to evict its own in-flight writes (the pool still holds
    /// the bytes a peer might be writing through the same frame). A page
    /// not resident in the pool is silently skipped.
    fn drop_pages(&self, pages: &HashSet<u32>) {
        for &no in pages {
            let mut shard = self.shard(no);
            if let Some(&idx) = shard.index.get(&no) {
                let last = shard.slots.len() - 1;
                shard.slots.swap_remove(idx);
                shard.index.remove(&no);
                if idx != last {
                    // The swapped-in slot now lives at `idx`; reindex it.
                    let moved = shard.slots[idx].frame.no;
                    shard.index.insert(moved, idx);
                }
                if shard.hand >= shard.slots.len() && !shard.slots.is_empty() {
                    shard.hand = 0;
                }
            }
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
    /// Path the file was opened from. Stored (v0.50) so a parallel
    /// scan's coordinator thread can `open_shared_with_meta` a peer
    /// Pager onto the same file, sharing this Pager's pool + meta.
    path: PathBuf,
    wal: Wal,
    /// Database header — shared with every other pager open on this file
    /// so concurrent writers cannot hand out the same page number or
    /// trample each other's freelist updates.
    shared_meta: SharedMeta,
    /// The page cache — shared with every other pager open on this file.
    pool: SharedPool,
    /// For each dirty page evicted to the WAL, the offset of its latest image
    /// there — so `read_page` can fetch it back. Emptied at commit/rollback.
    wal_index: HashMap<u32, u64>,
    /// Pages *this pager* has written since the last commit/rollback. The
    /// shared pool's per-frame dirty bit alone can't disambiguate
    /// concurrent writers — a peer pager may have dirtied other frames.
    /// Commit flushes only these pages; rollback drops only these from
    /// the pool. The set is empty between transactions.
    dirty_pages: HashSet<u32>,
    /// Pages this pager allocated since the last commit. On rollback they
    /// go to `pending_freelist` (a per-pager freelist of pages that were
    /// bumped from `shared_meta` but never reached disk), where the next
    /// allocation reuses them. On commit they become part of the file.
    allocated_pages: HashSet<u32>,
    /// Per-pager rebound freelist: pages we allocated then rolled back.
    /// We can't add them back to the shared freelist on rollback (that
    /// would require writing the page's "next" pointer, which rollback
    /// has no way to do), so we keep them in memory and reuse them
    /// before going back to the shared meta. If the connection drops,
    /// these pages truly leak until VACUUM reclaims them.
    pending_freelist: Vec<u32>,
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

        // Crash recovery: replay any leftover per-pager WAL files in
        // sequence, then delete them. Each WAL holds at most one
        // committed transaction, so the order they replay in doesn't
        // matter for correctness — the result is the same as if the
        // crash had happened just before the next clean shutdown.
        // We also replay the legacy single-WAL path (`<db>-wal`) for
        // backwards compatibility with v0.27.
        let legacy = wal_path(path);
        if legacy.exists() {
            let mut legacy_wal = Wal::open(&legacy)?;
            legacy_wal.recover(&mut file)?;
            drop(legacy_wal);
            let _ = std::fs::remove_file(&legacy);
        }
        for orphan in list_orphan_wals(path)? {
            let mut orphan_wal = Wal::open(&orphan)?;
            orphan_wal.recover(&mut file)?;
            drop(orphan_wal);
            let _ = std::fs::remove_file(&orphan);
        }

        // Read the durable meta from disk (or seed a fresh one for an
        // empty file). The very first pager open on a file creates the
        // SharedMeta; peer pagers later receive a clone via
        // `open_shared_with_meta`.
        let len = file.metadata()?.len();
        let initial_meta = if len == 0 {
            Meta {
                page_count: 1,
                freelist_head: 0,
                catalog_root: 0,
                next_tx_id: 1,
            }
        } else {
            let mut hdr = [0u8; PAGE_SIZE];
            file.seek(SeekFrom::Start(0))?;
            file.read_exact(&mut hdr)?;
            decode_header(&hdr)?
        };
        let shared_meta = SharedMeta::new(initial_meta);
        // Mint our own WAL id; future peer pagers get distinct ids from
        // the same shared counter.
        let wal_id = shared_meta.mint_wal_id();
        let wal = Wal::open(&pager_wal_path(path, wal_id))?;
        let mut pager = Pager {
            file,
            path: path.to_path_buf(),
            wal,
            shared_meta,
            pool,
            wal_index: HashMap::new(),
            dirty_pages: HashSet::new(),
            allocated_pages: HashSet::new(),
            pending_freelist: Vec::new(),
        };

        if len == 0 {
            // A brand-new file: lay down the header page and flush it.
            pager.write_page(0, encode_header(initial_meta))?;
            pager.commit()?;
        }
        Ok(pager)
    }

    /// Open the database at `path` against an existing `SharedMeta` —
    /// used by `Database::open_shared` so peer pagers on the same file
    /// coordinate allocations through the same header lock.
    pub fn open_shared_with_meta(
        path: impl AsRef<Path>,
        pool: SharedPool,
        shared_meta: SharedMeta,
    ) -> Result<Pager> {
        let path = path.as_ref();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        // Peer pagers get a fresh, distinct WAL file. The first pager on
        // this file has already recovered any orphaned logs; we don't
        // need to scan again.
        let wal_id = shared_meta.mint_wal_id();
        let wal = Wal::open(&pager_wal_path(path, wal_id))?;
        Ok(Pager {
            file,
            path: path.to_path_buf(),
            wal,
            shared_meta,
            pool,
            wal_index: HashMap::new(),
            dirty_pages: HashSet::new(),
            allocated_pages: HashSet::new(),
            pending_freelist: Vec::new(),
        })
    }

    /// The shared header. Cloned by `Database` so peer pagers can be
    /// opened against the same coordinator.
    pub fn shared_meta(&self) -> SharedMeta {
        self.shared_meta.clone()
    }

    /// The shared buffer pool — cloneable (Arc-based). v0.50's
    /// parallel scan hands this to a coordinator thread that opens
    /// a peer Pager so its scan I/O runs in parallel with the
    /// foreground query path.
    pub fn pool(&self) -> SharedPool {
        self.pool.clone()
    }

    /// Path the file was opened from (v0.50). Used by parallel-scan
    /// coordinator threads to `open_shared_with_meta` a peer Pager.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The latch protecting page `no`. v0.30's B+tree wraps these in
    /// owned guards to crab from root to leaf during descent.
    pub fn latch(&self, no: u32) -> Arc<RwLock<()>> {
        self.pool.latch(no)
    }

    /// `open`, with an explicit pool capacity — tests use a tiny pool to force
    /// heavy eviction.
    #[cfg(test)]
    fn open_with_capacity(path: impl AsRef<Path>, capacity: usize) -> Result<Pager> {
        Pager::open_with_pool(path, SharedPool::with_capacity(capacity))
    }

    /// Total pages in the database, including the header page.
    pub fn page_count(&self) -> u32 {
        self.shared_meta.page_count()
    }

    /// Root page of the catalog B+tree (0 means "not created yet").
    pub fn catalog_root(&self) -> u32 {
        self.shared_meta.catalog_root()
    }

    /// Record the catalog root. Persisted on the next [`commit`](Self::commit).
    pub fn set_catalog_root(&mut self, root: u32) {
        self.shared_meta.set_catalog_root(root);
    }

    /// The next-unused MVCC transaction ID, as last persisted. A reader uses
    /// this to bound its snapshot; a writer takes it as its own TX ID at
    /// BEGIN and advances the in-memory counter.
    pub fn next_tx_id(&self) -> u64 {
        self.shared_meta.next_tx_id()
    }

    /// Record a new value for the next-TX counter, to be persisted on the
    /// next commit. Used by the writer at BEGIN, after it has taken the
    /// current value as its own TX ID. Only bumps up — a concurrent peer
    /// writer may already be past `id`.
    pub fn set_next_tx_id(&mut self, id: u64) {
        self.shared_meta.bump_next_tx_id_to(id);
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
        // 3. Validate against the shared meta — a read past the allocator's
        //    high-water mark is a real bug.
        let count = self.shared_meta.page_count();
        if no >= count {
            return Err(Error::corruption(format!(
                "read of page {no}, past the end of a {count}-page database",
            )));
        }
        // 4. Clean on disk. A peer writer may have bumped `page_count`
        //    without yet committing — the file is shorter than the meta
        //    advertises. Tolerate the resulting short read by returning a
        //    zero-filled page: nothing committed references such a page
        //    until the peer's WAL apply extends the file.
        let mut buf = Box::new([0u8; PAGE_SIZE]);
        self.file
            .seek(SeekFrom::Start(no as u64 * PAGE_SIZE as u64))?;
        read_full_or_zero(&mut self.file, &mut buf[..])?;
        Ok(PageRef {
            frame: self.admit(no, buf, false)?,
        })
    }

    /// Stage a page to be written on the next commit. May trigger a spill: if
    /// the pool is full, admitting this page evicts another, and a dirty
    /// evictee is written out to the WAL.
    pub fn write_page(&mut self, no: u32, buf: Box<[u8; PAGE_SIZE]>) -> Result<()> {
        self.admit(no, buf, true)?;
        self.dirty_pages.insert(no);
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

    /// Allocate a fresh page, reusing this pager's own rolled-back
    /// allocations first, then the shared freelist, then bumping the
    /// shared `page_count`. The returned page is staged as zero-filled;
    /// the caller is expected to write it.
    pub fn alloc_page(&mut self) -> Result<u32> {
        // 1. Our own rolled-back allocations — no shared-meta touch.
        if let Some(no) = self.pending_freelist.pop() {
            self.write_page(no, Box::new([0u8; PAGE_SIZE]))?;
            self.allocated_pages.insert(no);
            return Ok(no);
        }
        // 2. Coordinated allocation through the shared header. Hold the
        //    lock through both the read-back of the freelist head and
        //    the bump so a peer writer's concurrent allocation cannot
        //    hand out the same page number.
        let meta = self.shared_meta.clone();
        let no = {
            let mut guard = meta.lock();
            if guard.meta.freelist_head != 0 {
                let head = guard.meta.freelist_head;
                // Reading the head page may go to disk but does not
                // touch the meta lock (different mutex).
                let next = {
                    let page = self.read_page(head)?;
                    u32::from_le_bytes(page[0..4].try_into().unwrap())
                };
                guard.meta.freelist_head = next;
                head
            } else {
                let n = guard.meta.page_count;
                guard.meta.page_count += 1;
                n
            }
        };
        self.write_page(no, Box::new([0u8; PAGE_SIZE]))?;
        self.allocated_pages.insert(no);
        Ok(no)
    }

    /// Return a page to the shared free list. Takes effect on the next
    /// commit — the freed page's "next" pointer is written before the
    /// shared `freelist_head` is updated, so a peer reading the freelist
    /// always sees a coherent chain.
    pub fn free_page(&mut self, no: u32) -> Result<()> {
        let meta = self.shared_meta.clone();
        let mut guard = meta.lock();
        let head = guard.meta.freelist_head;
        let mut buf = Box::new([0u8; PAGE_SIZE]);
        buf[0..4].copy_from_slice(&head.to_le_bytes());
        self.write_page(no, buf)?;
        guard.meta.freelist_head = no;
        Ok(())
    }

    /// Durably commit every staged page as one atomic transaction.
    pub fn commit(&mut self) -> Result<()> {
        // Nothing written since the last commit? Then there is nothing to do.
        if self.dirty_pages.is_empty() && self.wal_index.is_empty() {
            return Ok(());
        }

        // v0.52: page 0 is NOT flushed through the per-pager WAL.
        // It's written directly to the file post-apply by
        // `commit_apply`, under the meta mutex, so the latest
        // committed shared_meta always wins on disk regardless of
        // which order peer pagers' applies complete in.
        self.dirty_pages.remove(&0);

        // v0.52: the WHOLE commit — flush this pager's dirty pages
        // to the WAL, seal, apply to the file, write the header —
        // happens under the shared-meta mutex. This serialises peer
        // commits and prevents the multi-stage lost-write race:
        //
        // 1. Without serialisation, peer A's `flush_own_dirty` could
        //    read the pool's leaf L *before* peer B's write_page(L)
        //    lands, and peer A's WAL would carry the pre-B L. If A's
        //    apply then ran AFTER B's apply, the file's L would
        //    revert to A's pre-B version — losing B's row.
        // 2. Page 0 staleness on its own (the obvious form of the
        //    bug) is also covered by the post-apply header write.
        //
        // The trade-off is brief allocator stalls and serialised
        // commit applies; a future version could replace the
        // meta-mutex serialisation with finer-grained locking once
        // we have a Postgres-style write-ahead log structure that
        // doesn't need per-pager applies.
        let pool = &self.pool;
        let dirty = &self.dirty_pages;
        let wal_index = &self.wal_index;
        let wal = &mut self.wal;
        self.shared_meta.commit_apply(&mut self.file, |file| {
            // Flush data pages into our WAL under the lock — pool
            // reads happen with no peer writer able to commit
            // mid-flush, so the WAL contains a consistent point-in-
            // time view of every leaf we touched.
            for &no in dirty {
                if wal_index.contains_key(&no) {
                    continue; // already spilled
                }
                let frame = pool.get(no).ok_or_else(|| {
                    Error::corruption(format!(
                        "dirty page {no} vanished from pool without WAL spill",
                    ))
                })?;
                wal.append_page(no, &frame.page)?;
            }
            wal.seal()?;
            wal.apply(file)?;
            Ok(())
        })?;

        // The database file is durable; the WAL is no longer needed.
        self.wal.reset()?;
        // Mark only this pager's pages clean — a peer pager may have other
        // frames dirty for its own in-flight transaction.
        self.pool.mark_clean(&self.dirty_pages);
        self.dirty_pages.clear();
        self.allocated_pages.clear();
        self.wal_index.clear();
        Ok(())
    }

    /// Append this pager's own dirty pages — those still resident, not
    /// already spilled — to the WAL.
    ///
    /// **Page 0 (the header) is special** (v0.52 fix): instead of
    /// reading it from the pool, we re-snapshot `shared_meta` here at
    /// append time and encode the header on the fly. The pool's
    /// page 0 may be stale — a peer pager's allocations bump
    /// `shared_meta.page_count`/`freelist_head` immediately, but those
    /// bumps don't propagate to the pool's page 0 until somebody
    /// calls `write_page(0, ...)`. If we appended the pool's page 0
    /// blindly, our commit's WAL would carry a header with a
    /// `page_count` from before the peer's allocations; the apply
    /// would overwrite the file's header with our stale view,
    /// orphaning every page the peer just allocated.
    ///
    /// Reading `shared_meta.snapshot()` here always gives the
    /// freshest committed shared state, so concurrent commits race
    /// on the header but every commit writes the *latest* version —
    /// order-independent.
    fn flush_own_dirty(&mut self) -> Result<()> {
        // Walk our own dirty set instead of the pool's global one. A
        // concurrent peer's dirty pages don't belong in our WAL.
        for &no in &self.dirty_pages {
            if self.wal_index.contains_key(&no) {
                // Already spilled — its image is in the WAL ahead of where
                // the seal will land.
                continue;
            }
            if no == 0 {
                // v0.52: re-snapshot shared_meta at WAL-append time so
                // concurrent peer allocations are captured in our
                // header write.
                let header = encode_header(self.shared_meta.snapshot());
                self.wal.append_page(0, &header)?;
                continue;
            }
            // The page must be resident: we wrote it via `write_page`,
            // which `admit`s into the pool. If it was evicted, the
            // evictee was spilled and recorded in `wal_index`, which we
            // checked above.
            let frame = self
                .pool
                .get(no)
                .ok_or_else(|| Error::corruption(format!(
                    "dirty page {no} vanished from pool without WAL spill",
                )))?;
            self.wal.append_page(no, &frame.page)?;
        }
        Ok(())
    }

    /// Discard every staged page. Unlike v0.27, the shared meta is not
    /// reverted — a peer writer may have allocated past our bumps, so
    /// rolling them back would risk handing our (still-bumped) page
    /// numbers to a peer that already allocated theirs. Our allocated
    /// pages go to `pending_freelist` instead, where this pager will
    /// reuse them on its next allocation. Pages that escape that reuse
    /// (the connection drops) are reclaimed by `VACUUM`.
    pub fn rollback(&mut self) {
        self.pool.drop_pages(&self.dirty_pages);
        for no in self.allocated_pages.drain() {
            self.pending_freelist.push(no);
        }
        self.dirty_pages.clear();
        self.wal_index.clear();
        self.wal.discard();
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
        let mut new_meta = self.shared_meta.snapshot();
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

        // Install the compact image's meta into the shared header so
        // every peer pager sees the swap.
        self.shared_meta.install(new_meta);
        self.allocated_pages.clear();
        self.dirty_pages.clear();
        self.pending_freelist.clear();
        // The pre-swap file may be longer than the compact image; drop the
        // tail. A crash before this point is harmless — the header wins.
        self.file.set_len(page_count as u64 * PAGE_SIZE as u64)?;
        self.file.sync_all()?;
        Ok(())
    }
}

impl Drop for Pager {
    /// Best-effort cleanup of this pager's WAL file. After a successful
    /// `commit` (or with no work staged) the WAL is empty, so this just
    /// removes an unused file. A crashed process leaves the file behind;
    /// the next `open_with_pool` finds it via `list_orphan_wals` and
    /// recovers it. Failure to delete is silent — recovery handles it.
    fn drop(&mut self) {
        // Drop any uncommitted work so the WAL file is empty before
        // we delete it. (Committed work has already been applied to
        // the database file.)
        self.wal.discard();
        if let Err(e) = self.wal.reset() {
            eprintln!(
                "prehnitedb: failed to truncate WAL at {:?}: {e}",
                self.wal.path()
            );
            return;
        }
        let _ = std::fs::remove_file(self.wal.path());
    }
}

/// Read into `buf` until full or EOF. The buffer is zero-filled before
/// the read, so a short read leaves trailing bytes as zeros — used to
/// service reads for pages a peer writer has allocated but not yet
/// committed (the meta advertises them but the file is still short).
fn read_full_or_zero(file: &mut File, buf: &mut [u8]) -> std::io::Result<()> {
    for byte in buf.iter_mut() {
        *byte = 0;
    }
    let mut got = 0;
    while got < buf.len() {
        match file.read(&mut buf[got..]) {
            Ok(0) => break,
            Ok(n) => got += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// The legacy single WAL path beside the database file (`-wal` suffix).
/// Used only for v0.27-and-earlier compatibility — v0.28 mints one WAL
/// per pager via [`pager_wal_path`].
pub(crate) fn wal_path(db: &Path) -> PathBuf {
    let mut name = db.as_os_str().to_os_string();
    name.push("-wal");
    PathBuf::from(name)
}

/// The WAL path for one pager: `<db>-wal-<id>`. Each `Pager` opens its
/// own log so concurrent writers' append cursors never collide on one
/// shared file.
fn pager_wal_path(db: &Path, id: u32) -> PathBuf {
    let mut name = db.as_os_str().to_os_string();
    name.push(format!("-wal-{id}"));
    PathBuf::from(name)
}

/// Find every leftover per-pager WAL file beside `db` — for crash
/// recovery on the very first open after a process death. Each one
/// matches `<db-stem>-wal-<digits>`.
fn list_orphan_wals(db: &Path) -> std::io::Result<Vec<PathBuf>> {
    let parent = match db.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => PathBuf::from("."),
    };
    let stem = match db.file_name() {
        Some(s) => s.to_string_lossy().into_owned(),
        None => return Ok(Vec::new()),
    };
    let prefix = format!("{stem}-wal-");
    let mut out = Vec::new();
    let entries = match std::fs::read_dir(&parent) {
        Ok(it) => it,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if let Some(rest) = name.strip_prefix(&prefix) {
            if rest.chars().all(|c| c.is_ascii_digit()) {
                out.push(entry.path());
            }
        }
    }
    Ok(out)
}

fn encode_header(meta: Meta) -> Box<[u8; PAGE_SIZE]> {
    let mut buf = Box::new([0u8; PAGE_SIZE]);
    buf[HDR_MAGIC..HDR_MAGIC + 8].copy_from_slice(MAGIC);
    buf[HDR_PAGE_SIZE..HDR_PAGE_SIZE + 4].copy_from_slice(&(PAGE_SIZE as u32).to_le_bytes());
    buf[HDR_PAGE_COUNT..HDR_PAGE_COUNT + 4].copy_from_slice(&meta.page_count.to_le_bytes());
    buf[HDR_FREELIST..HDR_FREELIST + 4].copy_from_slice(&meta.freelist_head.to_le_bytes());
    buf[HDR_CATALOG..HDR_CATALOG + 4].copy_from_slice(&meta.catalog_root.to_le_bytes());
    buf[HDR_NEXT_TX..HDR_NEXT_TX + 8].copy_from_slice(&meta.next_tx_id.to_le_bytes());
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
        next_tx_id: u64::from_le_bytes(buf[HDR_NEXT_TX..HDR_NEXT_TX + 8].try_into().unwrap()),
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
    fn rollback_recycles_allocated_pages_for_reuse() {
        // v0.28: shared meta means rollback does *not* revert
        // `page_count` — a peer writer may have allocated past us in the
        // meantime, and rewinding would risk handing them our (still
        // bumped) numbers. Allocated-then-rolled-back pages go to a
        // per-pager `pending_freelist` so this pager reuses them on its
        // next allocation. The shared `page_count` stays bumped.
        let db = TempDb::new();
        let mut pager = Pager::open(&db.path).unwrap();
        let before = pager.page_count();
        let p = pager.alloc_page().unwrap();
        pager.write_page(p, filled(0x99)).unwrap();
        pager.rollback();
        // page_count stays bumped — the page is in our pending_freelist.
        assert!(pager.page_count() > before);
        // The next allocation reuses the rolled-back page.
        assert_eq!(pager.alloc_page().unwrap(), p);
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
        // v0.28: shared meta isn't reverted by rollback. The page_count
        // stays bumped; the 30 allocated pages are in pending_freelist,
        // ready for reuse by this pager.
        assert!(pager.page_count() > before);
        // The pager — and the reused WAL — are still sound: a fresh
        // transaction commits cleanly over the abandoned one, reusing
        // the rolled-back pages.
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
