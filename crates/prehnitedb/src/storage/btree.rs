//! A B+tree mapping byte-string keys to byte-string values, layered on the
//! pager. Both table data and the catalog are stored this way.
//!
//! Interior nodes only route; every key/value pair lives in a leaf, and leaves
//! are chained left-to-right (via [`Page::right_link`]) so an ordered scan is a
//! single walk. The root keeps a *fixed page number* for its entire life: a
//! root split copies the old root's contents aside and rebuilds the root page
//! in place. That lets the catalog refer to a table by a number that never
//! moves.
//!
//! A value too large to sit inline is spilled into a chain of *overflow
//! pages*; the leaf cell then holds only a one-byte tag and a 4-byte pointer to
//! the chain. Keys are never spilled, so a key must still fit [`MAX_CELL`].
//!
//! Delete rebalances: after a key is removed, an underfull node is merged with
//! a sibling whenever their combined entries fit one page, the merge cascading
//! up; a root left with a single child is collapsed into it. So deletes
//! reclaim pages, not just whole-tree drops. (The merge criterion is purely
//! "do they fit together" — there is no separate redistribution step.)

use crate::error::{Error, Result};
use crate::storage::page::{self, Page, MAX_CELL, USABLE};
use crate::storage::pager::Pager;

/// Tag prefixed to a stored value: the bytes after it are the value itself.
const TAG_INLINE: u8 = 0;
/// Tag prefixed to a stored value: the 4 bytes after it are the first page of
/// an overflow chain holding the value.
const TAG_OVERFLOW: u8 = 1;
/// Bytes an overflow page spends on its header: next pointer + chunk length.
const OVERFLOW_HEADER: usize = 8;

/// A B+tree identified by its (immortal) root page number.
pub struct BTree {
    root: u32,
}

impl BTree {
    /// Create an empty tree: allocate a root page and initialize it as a leaf.
    pub fn create(pager: &mut Pager) -> Result<BTree> {
        let root = pager.alloc_page()?;
        pager.write_page(root, Page::new_leaf().into_buf())?;
        Ok(BTree { root })
    }

    /// Reopen an existing tree rooted at `root`.
    pub fn open(root: u32) -> BTree {
        BTree { root }
    }

    /// The tree's root page number — stable for the life of the tree.
    pub fn root(&self) -> u32 {
        self.root
    }

    /// Look up `key`, returning its value if present.
    pub fn search(&self, pager: &mut Pager, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut no = self.root;
        loop {
            let page = Page::from_ref(pager.read_page(no)?);
            if page.is_leaf() {
                return match page.find_leaf_slot(key) {
                    Ok(slot) => {
                        let stored = page.leaf_value(slot).to_vec();
                        Ok(Some(unwrap_value(pager, &stored)?))
                    }
                    Err(_) => Ok(None),
                };
            }
            no = page.internal_child(page.find_child(key));
        }
    }

    /// Insert `key`/`value`, replacing any existing value for `key`.
    pub fn insert(&self, pager: &mut Pager, key: &[u8], value: &[u8]) -> Result<()> {
        // The key, plus the smallest possible stored value (a tag byte and a
        // 4-byte overflow pointer), must fit a cell — keys are never spilled.
        if page::LEAF_CELL_OVERHEAD + key.len() + 1 + 4 > MAX_CELL {
            return Err(Error::TooLarge(format!(
                "key of {} bytes is too large (limit is {} bytes)",
                key.len(),
                MAX_CELL - page::LEAF_CELL_OVERHEAD - 5
            )));
        }
        // A value that fits is stored inline (tag 0); a larger one spills into
        // an overflow chain, leaving just a tag and the chain's first page.
        let stored = if page::LEAF_CELL_OVERHEAD + key.len() + 1 + value.len() <= MAX_CELL {
            let mut bytes = Vec::with_capacity(1 + value.len());
            bytes.push(TAG_INLINE);
            bytes.extend_from_slice(value);
            bytes
        } else {
            let first = write_overflow(pager, value)?;
            let mut bytes = Vec::with_capacity(5);
            bytes.push(TAG_OVERFLOW);
            bytes.extend_from_slice(&first.to_le_bytes());
            bytes
        };
        if let Some((sep, right_no)) = self.insert_into(pager, self.root, key, &stored)? {
            // The root overflowed. `self.root` now holds only the left half;
            // move that aside and rebuild the root as a two-child interior
            // node so the root page number stays put.
            let left_no = pager.alloc_page()?;
            let left_buf = Box::new(*pager.read_page(self.root)?);
            pager.write_page(left_no, left_buf)?;

            let old_root = Page::from_ref(pager.read_page(self.root)?);
            let low = first_key(&old_root);
            let new_root = page::build_internal(&[(low, left_no), (sep, right_no)])?;
            pager.write_page(self.root, new_root.into_buf())?;
        }
        Ok(())
    }

    /// Recursive insert. Returns `Some((separator, new_page))` when `no` split,
    /// where `no` keeps the left half and `new_page` is the new right sibling.
    fn insert_into(
        &self,
        pager: &mut Pager,
        no: u32,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<(Vec<u8>, u32)>> {
        let page = Page::from_ref(pager.read_page(no)?);

        if page.is_leaf() {
            let mut entries = page.leaf_entries();
            match entries.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
                Ok(slot) => {
                    // Replacing a key: reclaim the old value's overflow chain.
                    free_if_overflow(pager, &entries[slot].1)?;
                    entries[slot].1 = value.to_vec();
                }
                Err(slot) => entries.insert(slot, (key.to_vec(), value.to_vec())),
            }
            let right_link = page.right_link();
            let footprints: Vec<usize> = entries
                .iter()
                .map(|(k, v)| page::leaf_footprint(k, v))
                .collect();

            if footprints.iter().sum::<usize>() <= USABLE {
                pager.write_page(no, page::build_leaf(&entries, right_link)?.into_buf())?;
                return Ok(None);
            }
            // Overflow: split the leaf and relink the chain.
            let s = split_index(&footprints);
            let new_no = pager.alloc_page()?;
            let separator = entries[s].0.clone();
            let left = page::build_leaf(&entries[..s], new_no)?;
            let right = page::build_leaf(&entries[s..], right_link)?;
            pager.write_page(no, left.into_buf())?;
            pager.write_page(new_no, right.into_buf())?;
            Ok(Some((separator, new_no)))
        } else {
            let mut entries = page.internal_entries();
            let idx = page.find_child(key);
            let child = entries[idx].1;

            let Some((sep, new_child)) = self.insert_into(pager, child, key, value)? else {
                return Ok(None);
            };
            // The child split; record its new right sibling beside it.
            entries.insert(idx + 1, (sep, new_child));
            let footprints: Vec<usize> = entries
                .iter()
                .map(|(k, _)| page::internal_footprint(k))
                .collect();

            if footprints.iter().sum::<usize>() <= USABLE {
                pager.write_page(no, page::build_internal(&entries)?.into_buf())?;
                return Ok(None);
            }
            // This interior node overflowed too; split it and propagate.
            let s = split_index(&footprints);
            let new_no = pager.alloc_page()?;
            let separator = entries[s].0.clone();
            let left = page::build_internal(&entries[..s])?;
            let right = page::build_internal(&entries[s..])?;
            pager.write_page(no, left.into_buf())?;
            pager.write_page(new_no, right.into_buf())?;
            Ok(Some((separator, new_no)))
        }
    }

    /// Delete `key`, returning whether it was present. Underfull nodes are
    /// merged with a sibling on the way back up, and a root reduced to a single
    /// child is collapsed, so deletes reclaim pages.
    pub fn delete(&self, pager: &mut Pager, key: &[u8]) -> Result<bool> {
        let found = self.delete_from(pager, self.root, key)?;
        // Collapse a root that merging has reduced to a single child, copying
        // the child up so the root keeps its page number.
        loop {
            let root = Page::from_ref(pager.read_page(self.root)?);
            if !root.is_internal() || root.cell_count() != 1 {
                break;
            }
            let only_child = root.internal_child(0);
            let child = Box::new(*pager.read_page(only_child)?);
            pager.write_page(self.root, child)?;
            pager.free_page(only_child)?;
        }
        Ok(found)
    }

    /// Recursive delete. After removing the key from a leaf, each interior
    /// level on the way back up tries to merge the just-visited child with a
    /// neighbour.
    fn delete_from(&self, pager: &mut Pager, no: u32, key: &[u8]) -> Result<bool> {
        let page = Page::from_ref(pager.read_page(no)?);
        if page.is_leaf() {
            let mut entries = page.leaf_entries();
            return match entries.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
                Ok(slot) => {
                    free_if_overflow(pager, &entries[slot].1)?;
                    entries.remove(slot);
                    pager.write_page(
                        no,
                        page::build_leaf(&entries, page.right_link())?.into_buf(),
                    )?;
                    Ok(true)
                }
                Err(_) => Ok(false),
            };
        }
        let child_idx = page.find_child(key);
        let child = page.internal_child(child_idx);
        let found = self.delete_from(pager, child, key)?;
        if found {
            merge_child(pager, no, child_idx)?;
        }
        Ok(found)
    }

    /// Every key/value pair in the tree, in ascending key order.
    pub fn scan(&self, pager: &mut Pager) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut cursor = self.cursor(pager, None, None)?;
        let mut out = Vec::new();
        while let Some(entry) = cursor.next(pager)? {
            out.push(entry);
        }
        Ok(out)
    }

    /// Every key/value pair with `start <= key`, and `key < end` when `end` is
    /// `Some`, in ascending key order. The primitive that index lookups (point
    /// and range) are built on.
    pub fn scan_range(
        &self,
        pager: &mut Pager,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut cursor = self.cursor(pager, Some(start), end.map(|e| e.to_vec()))?;
        let mut out = Vec::new();
        while let Some(entry) = cursor.next(pager)? {
            out.push(entry);
        }
        Ok(out)
    }

    /// Open a streaming cursor over `[start, end)` in ascending key order:
    /// `start` of `None` begins at the first key, `end` of `None` runs to the
    /// last. The cursor buffers only the current leaf, so walking a huge tree
    /// costs one page of memory rather than the whole tree.
    pub fn cursor(
        &self,
        pager: &mut Pager,
        start: Option<&[u8]>,
        end: Option<Vec<u8>>,
    ) -> Result<Cursor> {
        // Descend to the leaf that would hold `start` (or the leftmost leaf).
        let mut no = self.root;
        loop {
            let page = Page::from_ref(pager.read_page(no)?);
            if page.is_leaf() {
                break;
            }
            no = match start {
                Some(key) => page.internal_child(page.find_child(key)),
                None => page.internal_child(0),
            };
        }
        let leaf = Page::from_ref(pager.read_page(no)?);
        // The first leaf may begin partway in; later leaves are taken whole.
        let slot = match start {
            Some(key) => match leaf.find_leaf_slot(key) {
                Ok(i) | Err(i) => i,
            },
            None => 0,
        };
        Ok(Cursor {
            cells: leaf.leaf_entries(),
            slot,
            next_leaf: leaf.right_link(),
            upper: end,
        })
    }

    /// Return every page of the tree to the pager's free list.
    pub fn free_all(&self, pager: &mut Pager) -> Result<()> {
        free_subtree(pager, self.root)
    }
}

/// A streaming forward iterator over a B+tree's key/value pairs. It buffers a
/// single leaf at a time and follows the leaf chain, so a scan's memory stays
/// constant however large the tree. Created by [`BTree::cursor`].
pub struct Cursor {
    /// Raw `(key, stored value)` cells of the leaf currently buffered.
    cells: Vec<(Vec<u8>, Vec<u8>)>,
    /// Index of the next cell in `cells` to yield.
    slot: usize,
    /// Leaf page to load once `cells` is drained; 0 once the chain ends.
    next_leaf: u32,
    /// Exclusive upper bound: once a key reaches it, the cursor is done.
    upper: Option<Vec<u8>>,
}

impl Cursor {
    /// The next `(key, value)` pair in ascending key order, or `None` at the
    /// end. A spilled value is reassembled here — one row at a time — so the
    /// cursor never holds more than the current leaf plus one decoded value.
    pub fn next(&mut self, pager: &mut Pager) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        loop {
            if self.slot < self.cells.len() {
                let (key, stored) = &self.cells[self.slot];
                if let Some(upper) = &self.upper {
                    if key.as_slice() >= upper.as_slice() {
                        self.next_leaf = 0;
                        self.cells.clear();
                        return Ok(None);
                    }
                }
                let key = key.clone();
                let stored = stored.clone();
                self.slot += 1;
                return Ok(Some((key, unwrap_value(pager, &stored)?)));
            }
            if self.next_leaf == 0 {
                return Ok(None);
            }
            let leaf = Page::from_ref(pager.read_page(self.next_leaf)?);
            if !leaf.is_leaf() {
                return Err(Error::corruption("leaf chain reached a non-leaf page"));
            }
            self.cells = leaf.leaf_entries();
            self.slot = 0;
            self.next_leaf = leaf.right_link();
        }
    }
}

fn free_subtree(pager: &mut Pager, no: u32) -> Result<()> {
    let page = Page::from_ref(pager.read_page(no)?);
    if page.is_internal() {
        let children: Vec<u32> = (0..page.cell_count())
            .map(|i| page.internal_child(i))
            .collect();
        for child in children {
            free_subtree(pager, child)?;
        }
    } else {
        // A leaf: reclaim the overflow chain behind every spilled value.
        for i in 0..page.cell_count() {
            free_if_overflow(pager, page.leaf_value(i))?;
        }
    }
    pager.free_page(no)?;
    Ok(())
}

/// After a delete beneath child `child_idx` of interior node `parent_no`, merge
/// that child with a neighbouring sibling when their combined entries fit a
/// single page. The merge frees the right page of the pair and drops one cell
/// from the parent; if nothing fits, the tree is left as is.
fn merge_child(pager: &mut Pager, parent_no: u32, child_idx: usize) -> Result<()> {
    let mut parent_entries = Page::from_ref(pager.read_page(parent_no)?).internal_entries();
    // Pick a pair to merge: the child and its right sibling, or its left
    // sibling and the child.
    let (left_idx, right_idx) = if child_idx + 1 < parent_entries.len() {
        (child_idx, child_idx + 1)
    } else if child_idx > 0 {
        (child_idx - 1, child_idx)
    } else {
        return Ok(()); // an only child has no sibling to merge with
    };
    let left_no = parent_entries[left_idx].1;
    let right_no = parent_entries[right_idx].1;
    let left = Page::from_ref(pager.read_page(left_no)?);
    let right = Page::from_ref(pager.read_page(right_no)?);

    if left.is_leaf() {
        let mut merged = left.leaf_entries();
        merged.extend(right.leaf_entries());
        let used: usize = merged.iter().map(|(k, v)| page::leaf_footprint(k, v)).sum();
        if used > USABLE {
            return Ok(()); // the two would not fit one page
        }
        // The merged leaf inherits the right leaf's forward chain link.
        pager.write_page(
            left_no,
            page::build_leaf(&merged, right.right_link())?.into_buf(),
        )?;
    } else {
        let mut merged = left.internal_entries();
        merged.extend(right.internal_entries());
        let used: usize = merged
            .iter()
            .map(|(k, _)| page::internal_footprint(k))
            .sum();
        if used > USABLE {
            return Ok(());
        }
        pager.write_page(left_no, page::build_internal(&merged)?.into_buf())?;
    }
    // The merged node lives in `left_no`; `right_no` is freed and the parent
    // loses the cell that pointed at it.
    pager.free_page(right_no)?;
    parent_entries.remove(right_idx);
    pager.write_page(parent_no, page::build_internal(&parent_entries)?.into_buf())?;
    Ok(())
}

/// Spill a value across a freshly allocated chain of overflow pages, returning
/// the chain's first page. Each page is `[next u32][chunk_len u32][chunk]`.
fn write_overflow(pager: &mut Pager, value: &[u8]) -> Result<u32> {
    let capacity = page::PAGE_SIZE - OVERFLOW_HEADER;
    // Write chunks back to front so each page can name the one after it.
    let mut next = 0u32;
    for chunk in value.chunks(capacity).rev() {
        let no = pager.alloc_page()?;
        let mut buf = Box::new([0u8; page::PAGE_SIZE]);
        buf[0..4].copy_from_slice(&next.to_le_bytes());
        buf[4..8].copy_from_slice(&(chunk.len() as u32).to_le_bytes());
        buf[OVERFLOW_HEADER..OVERFLOW_HEADER + chunk.len()].copy_from_slice(chunk);
        pager.write_page(no, buf)?;
        next = no;
    }
    Ok(next)
}

/// Reassemble a value spilled across the overflow chain starting at `first`.
fn read_overflow(pager: &mut Pager, first: u32) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    let mut no = first;
    while no != 0 {
        let buf = pager.read_page(no)?;
        let next = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let chunk_len = u32::from_le_bytes(buf[4..8].try_into().unwrap()) as usize;
        if OVERFLOW_HEADER + chunk_len > page::PAGE_SIZE {
            return Err(Error::corruption(
                "overflow page has an invalid chunk length",
            ));
        }
        out.extend_from_slice(&buf[OVERFLOW_HEADER..OVERFLOW_HEADER + chunk_len]);
        no = next;
    }
    Ok(out)
}

/// Return every page of the overflow chain starting at `first` to the pager.
fn free_overflow(pager: &mut Pager, first: u32) -> Result<()> {
    let mut no = first;
    while no != 0 {
        let buf = pager.read_page(no)?;
        let next = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        pager.free_page(no)?;
        no = next;
    }
    Ok(())
}

/// Decode a stored value: an inline value, or one reassembled from a chain.
fn unwrap_value(pager: &mut Pager, stored: &[u8]) -> Result<Vec<u8>> {
    match stored.split_first() {
        Some((&TAG_INLINE, value)) => Ok(value.to_vec()),
        Some((&TAG_OVERFLOW, stub)) => {
            let first = u32::from_le_bytes(
                stub.try_into()
                    .map_err(|_| Error::corruption("overflow value stub is malformed"))?,
            );
            read_overflow(pager, first)
        }
        _ => Err(Error::corruption("stored value is missing its tag")),
    }
}

/// Free the overflow chain a stored value points at, if it is a spilled value.
fn free_if_overflow(pager: &mut Pager, stored: &[u8]) -> Result<()> {
    if let Some((&TAG_OVERFLOW, stub)) = stored.split_first() {
        let first = u32::from_le_bytes(
            stub.try_into()
                .map_err(|_| Error::corruption("overflow value stub is malformed"))?,
        );
        free_overflow(pager, first)?;
    }
    Ok(())
}

/// The smallest key reachable through `page` (its first cell's key).
fn first_key(page: &Page) -> Vec<u8> {
    if page.is_leaf() {
        page.leaf_key(0).to_vec()
    } else {
        page.internal_key(0).to_vec()
    }
}

/// Choose where to split an overflowing node, given each entry's footprint.
///
/// The preferred cut is the balanced one — the smallest prefix holding at least
/// half the bytes. When skewed entry sizes make that cut overflow a side, it
/// falls back to the largest prefix that fits; the [`MAX_CELL`] cap guarantees
/// that fallback always yields two pages that each fit, with a non-empty side
/// on either end.
fn split_index(footprints: &[usize]) -> usize {
    let total: usize = footprints.iter().sum();
    let n = footprints.len();
    debug_assert!(
        n >= 2,
        "a node only splits once it holds at least two cells"
    );

    // Preferred: the balanced cut.
    let mut acc = 0;
    for (i, &fp) in footprints.iter().enumerate() {
        acc += fp;
        if acc * 2 >= total {
            let balanced = i + 1;
            if balanced < n && acc <= USABLE && total - acc <= USABLE {
                return balanced;
            }
            break;
        }
    }

    // Fallback: the largest prefix that fits.
    let mut acc = 0;
    let mut s = 0;
    for (i, &fp) in footprints.iter().enumerate() {
        if acc + fp > USABLE {
            break;
        }
        acc += fp;
        s = i + 1;
    }
    s.clamp(1, n - 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::pager::wal_path;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDb {
        path: PathBuf,
    }

    impl TempDb {
        fn new() -> TempDb {
            let n = COUNTER.fetch_add(1, Ordering::SeqCst);
            let path =
                std::env::temp_dir().join(format!("prehnite-btree-{}-{n}.db", std::process::id()));
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

    fn key(i: u64) -> Vec<u8> {
        i.to_be_bytes().to_vec()
    }

    fn value(i: u64) -> Vec<u8> {
        let mut v = format!("value-for-row-{i}").into_bytes();
        v.resize(150, b'.');
        v
    }

    #[test]
    fn empty_tree_finds_nothing() {
        let db = TempDb::new();
        let mut pager = Pager::open(&db.path).unwrap();
        let tree = BTree::create(&mut pager).unwrap();
        assert_eq!(tree.search(&mut pager, &key(1)).unwrap(), None);
        assert!(tree.scan(&mut pager).unwrap().is_empty());
    }

    #[test]
    fn insert_search_upsert() {
        let db = TempDb::new();
        let mut pager = Pager::open(&db.path).unwrap();
        let tree = BTree::create(&mut pager).unwrap();

        tree.insert(&mut pager, b"k", b"first").unwrap();
        assert_eq!(
            tree.search(&mut pager, b"k").unwrap().as_deref(),
            Some(&b"first"[..])
        );

        tree.insert(&mut pager, b"k", b"second").unwrap();
        assert_eq!(
            tree.search(&mut pager, b"k").unwrap().as_deref(),
            Some(&b"second"[..])
        );
        assert_eq!(tree.scan(&mut pager).unwrap().len(), 1);
    }

    #[test]
    fn many_inserts_stay_ordered_across_splits() {
        let db = TempDb::new();
        let mut pager = Pager::open(&db.path).unwrap();
        let tree = BTree::create(&mut pager).unwrap();

        const N: u64 = 2000;
        // Insert in a permuted order so splits happen all over the tree.
        for step in 0..N {
            let i = (step * 7919) % N;
            tree.insert(&mut pager, &key(i), &value(i)).unwrap();
        }

        // The tree must have grown past a single leaf.
        let root = Page::from_ref(pager.read_page(tree.root()).unwrap());
        assert!(root.is_internal(), "2000 rows should force interior nodes");

        let scanned = tree.scan(&mut pager).unwrap();
        assert_eq!(scanned.len() as u64, N);
        for (i, (k, v)) in scanned.iter().enumerate() {
            assert_eq!(k, &key(i as u64));
            assert_eq!(v, &value(i as u64));
        }
        for i in 0..N {
            assert_eq!(tree.search(&mut pager, &key(i)).unwrap(), Some(value(i)));
        }
        assert_eq!(tree.search(&mut pager, &key(N + 1)).unwrap(), None);
    }

    #[test]
    fn delete_removes_keys() {
        let db = TempDb::new();
        let mut pager = Pager::open(&db.path).unwrap();
        let tree = BTree::create(&mut pager).unwrap();

        for i in 0..200 {
            tree.insert(&mut pager, &key(i), &value(i)).unwrap();
        }
        for i in (0..200).step_by(2) {
            assert!(tree.delete(&mut pager, &key(i)).unwrap());
        }
        assert!(!tree.delete(&mut pager, &key(0)).unwrap()); // already gone

        let remaining = tree.scan(&mut pager).unwrap();
        assert_eq!(remaining.len(), 100);
        assert!(remaining
            .iter()
            .all(|(k, _)| { u64::from_be_bytes(k.as_slice().try_into().unwrap()) % 2 == 1 }));
    }

    #[test]
    fn data_survives_reopen() {
        let db = TempDb::new();
        let root;
        {
            let mut pager = Pager::open(&db.path).unwrap();
            let tree = BTree::create(&mut pager).unwrap();
            for i in 0..500 {
                tree.insert(&mut pager, &key(i), &value(i)).unwrap();
            }
            root = tree.root();
            pager.commit().unwrap();
        }
        let mut pager = Pager::open(&db.path).unwrap();
        let tree = BTree::open(root);
        assert_eq!(tree.scan(&mut pager).unwrap().len(), 500);
        assert_eq!(
            tree.search(&mut pager, &key(321)).unwrap(),
            Some(value(321))
        );
    }

    #[test]
    fn range_scan_returns_bounded_slices() {
        let db = TempDb::new();
        let mut pager = Pager::open(&db.path).unwrap();
        let tree = BTree::create(&mut pager).unwrap();
        for i in 0..300u64 {
            tree.insert(&mut pager, &key(i), &value(i)).unwrap();
        }
        // [key(100), key(200)) is exactly 100 entries.
        let mid = tree
            .scan_range(&mut pager, &key(100), Some(key(200).as_slice()))
            .unwrap();
        assert_eq!(mid.len(), 100);
        assert_eq!(mid[0].0, key(100));
        assert_eq!(mid[99].0, key(199));
        // An open-ended scan runs to the last key.
        let tail = tree.scan_range(&mut pager, &key(250), None).unwrap();
        assert_eq!(tail.len(), 50);
        // A start past every key yields nothing.
        let empty = tree.scan_range(&mut pager, &key(999), None).unwrap();
        assert!(empty.is_empty());
    }

    #[test]
    fn large_values_spill_to_overflow_pages() {
        let db = TempDb::new();
        let big = |seed: u8| -> Vec<u8> { vec![seed; 20_000] };
        let root;
        {
            let mut pager = Pager::open(&db.path).unwrap();
            let tree = BTree::create(&mut pager).unwrap();
            // Values far larger than a page spill into overflow chains.
            tree.insert(&mut pager, &key(1), &big(0xAA)).unwrap();
            tree.insert(&mut pager, &key(2), &big(0xBB)).unwrap();
            tree.insert(&mut pager, &key(3), b"still small").unwrap();
            assert_eq!(tree.search(&mut pager, &key(1)).unwrap(), Some(big(0xAA)));
            assert_eq!(tree.search(&mut pager, &key(2)).unwrap(), Some(big(0xBB)));

            // Overwriting a spilled value reclaims the old chain.
            tree.insert(&mut pager, &key(1), &big(0xCC)).unwrap();
            assert_eq!(tree.search(&mut pager, &key(1)).unwrap(), Some(big(0xCC)));

            // Deleting a spilled value reclaims its chain.
            assert!(tree.delete(&mut pager, &key(2)).unwrap());
            assert_eq!(tree.search(&mut pager, &key(2)).unwrap(), None);

            // A scan reassembles spilled values.
            assert_eq!(tree.scan(&mut pager).unwrap().len(), 2);
            root = tree.root();
            pager.commit().unwrap();
        }
        // Spilled values survive a close and reopen.
        let mut pager = Pager::open(&db.path).unwrap();
        let tree = BTree::open(root);
        assert_eq!(tree.search(&mut pager, &key(1)).unwrap(), Some(big(0xCC)));
        assert_eq!(
            tree.search(&mut pager, &key(3)).unwrap(),
            Some(b"still small".to_vec())
        );
    }

    #[test]
    fn deletes_merge_nodes_and_collapse_the_root() {
        let db = TempDb::new();
        let mut pager = Pager::open(&db.path).unwrap();
        let tree = BTree::create(&mut pager).unwrap();

        const N: u64 = 1500;
        for i in 0..N {
            tree.insert(&mut pager, &key(i), &value(i)).unwrap();
        }
        assert!(
            Page::from_ref(pager.read_page(tree.root()).unwrap()).is_internal(),
            "1500 rows should build a multi-level tree"
        );

        // Delete every key — merging cascades and the root collapses back to a
        // single empty leaf.
        for i in 0..N {
            assert!(tree.delete(&mut pager, &key(i)).unwrap());
        }
        let root = Page::from_ref(pager.read_page(tree.root()).unwrap());
        assert!(
            root.is_leaf(),
            "an emptied tree should collapse to one leaf"
        );
        assert_eq!(root.cell_count(), 0);
        assert!(tree.scan(&mut pager).unwrap().is_empty());

        // The tree is still usable afterward.
        tree.insert(&mut pager, &key(42), &value(42)).unwrap();
        assert_eq!(tree.search(&mut pager, &key(42)).unwrap(), Some(value(42)));
    }

    #[test]
    fn delete_keeps_surviving_keys_intact() {
        let db = TempDb::new();
        let mut pager = Pager::open(&db.path).unwrap();
        let tree = BTree::create(&mut pager).unwrap();
        for i in 0..800u64 {
            tree.insert(&mut pager, &key(i), &value(i)).unwrap();
        }
        // Delete a large, scattered subset, triggering merges all over.
        for i in 0..800u64 {
            if i % 4 != 0 {
                assert!(tree.delete(&mut pager, &key(i)).unwrap());
            }
        }
        let survivors = tree.scan(&mut pager).unwrap();
        assert_eq!(survivors.len(), 200);
        for (k, v) in &survivors {
            let i = u64::from_be_bytes(k.as_slice().try_into().unwrap());
            assert_eq!(i % 4, 0);
            assert_eq!(v, &value(i));
        }
        assert_eq!(
            tree.search(&mut pager, &key(400)).unwrap(),
            Some(value(400))
        );
        assert_eq!(tree.search(&mut pager, &key(401)).unwrap(), None);
    }

    #[test]
    fn split_index_balances_uniform_entries() {
        let footprints = vec![100usize; 10];
        assert_eq!(split_index(&footprints), 5);
    }

    #[test]
    fn split_index_handles_one_huge_entry() {
        // A near-max entry wedged among small ones must still split legally.
        let footprints = vec![20, MAX_CELL, 20, 20];
        let s = split_index(&footprints);
        let left: usize = footprints[..s].iter().sum();
        let right: usize = footprints[s..].iter().sum();
        assert!(s >= 1 && s < footprints.len());
        assert!(left <= USABLE && right <= USABLE);
    }

    #[test]
    fn cursor_streams_and_stops_early() {
        let db = TempDb::new();
        let mut pager = Pager::open(&db.path).unwrap();
        let tree = BTree::create(&mut pager).unwrap();
        for i in 0..1000 {
            tree.insert(&mut pager, &key(i), &value(i)).unwrap();
        }

        // A cursor need not be drained: pull a handful and drop it. Only the
        // current leaf is ever buffered, never the whole tree.
        let mut cursor = tree.cursor(&mut pager, None, None).unwrap();
        for i in 0..5u64 {
            let (k, v) = cursor.next(&mut pager).unwrap().unwrap();
            assert_eq!(k, key(i));
            assert_eq!(v, value(i));
        }
        drop(cursor);

        // A bounded cursor yields `[start, end)` and stops there.
        let mut ranged = tree
            .cursor(&mut pager, Some(key(500).as_slice()), Some(key(503)))
            .unwrap();
        let mut seen = Vec::new();
        while let Some((k, _)) = ranged.next(&mut pager).unwrap() {
            seen.push(k);
        }
        assert_eq!(seen, vec![key(500), key(501), key(502)]);
    }
}
