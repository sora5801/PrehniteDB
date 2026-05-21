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
//! v0.1 deliberately keeps two things simple:
//!
//! * **Delete does not rebalance.** A removed key is dropped from its leaf; an
//!   emptied leaf stays in the chain. Space is reclaimed only when a whole tree
//!   is dropped. Merging underfull nodes is left for a later version.
//! * **No overflow pages.** A key plus value must fit within [`MAX_CELL`];
//!   larger payloads are rejected rather than spilled across pages.

use crate::error::{Error, Result};
use crate::storage::page::{self, Page, MAX_CELL, USABLE};
use crate::storage::pager::Pager;

/// A B+tree identified by its (immortal) root page number.
pub struct BTree {
    root: u32,
}

impl BTree {
    /// Create an empty tree: allocate a root page and initialize it as a leaf.
    pub fn create(pager: &mut Pager) -> Result<BTree> {
        let root = pager.alloc_page()?;
        pager.write_page(root, Page::new_leaf().into_buf());
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
            let page = Page::from_buf(pager.read_page(no)?);
            if page.is_leaf() {
                return Ok(match page.find_leaf_slot(key) {
                    Ok(slot) => Some(page.leaf_value(slot).to_vec()),
                    Err(_) => None,
                });
            }
            no = page.internal_child(page.find_child(key));
        }
    }

    /// Insert `key`/`value`, replacing any existing value for `key`.
    pub fn insert(&self, pager: &mut Pager, key: &[u8], value: &[u8]) -> Result<()> {
        if page::LEAF_CELL_OVERHEAD + key.len() + value.len() > MAX_CELL {
            return Err(Error::TooLarge(format!(
                "key+value is {} bytes; the per-row limit is {} bytes",
                key.len() + value.len(),
                MAX_CELL - page::LEAF_CELL_OVERHEAD
            )));
        }
        if let Some((sep, right_no)) = self.insert_into(pager, self.root, key, value)? {
            // The root overflowed. `self.root` now holds only the left half;
            // move that aside and rebuild the root as a two-child interior
            // node so the root page number stays put.
            let left_no = pager.alloc_page()?;
            let left_buf = pager.read_page(self.root)?;
            pager.write_page(left_no, left_buf);

            let old_root = Page::from_buf(pager.read_page(self.root)?);
            let low = first_key(&old_root);
            let new_root = page::build_internal(&[(low, left_no), (sep, right_no)])?;
            pager.write_page(self.root, new_root.into_buf());
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
        let page = Page::from_buf(pager.read_page(no)?);

        if page.is_leaf() {
            let mut entries = page.leaf_entries();
            match entries.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
                Ok(slot) => entries[slot].1 = value.to_vec(),
                Err(slot) => entries.insert(slot, (key.to_vec(), value.to_vec())),
            }
            let right_link = page.right_link();
            let footprints: Vec<usize> = entries
                .iter()
                .map(|(k, v)| page::leaf_footprint(k, v))
                .collect();

            if footprints.iter().sum::<usize>() <= USABLE {
                pager.write_page(no, page::build_leaf(&entries, right_link)?.into_buf());
                return Ok(None);
            }
            // Overflow: split the leaf and relink the chain.
            let s = split_index(&footprints);
            let new_no = pager.alloc_page()?;
            let separator = entries[s].0.clone();
            let left = page::build_leaf(&entries[..s], new_no)?;
            let right = page::build_leaf(&entries[s..], right_link)?;
            pager.write_page(no, left.into_buf());
            pager.write_page(new_no, right.into_buf());
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
                pager.write_page(no, page::build_internal(&entries)?.into_buf());
                return Ok(None);
            }
            // This interior node overflowed too; split it and propagate.
            let s = split_index(&footprints);
            let new_no = pager.alloc_page()?;
            let separator = entries[s].0.clone();
            let left = page::build_internal(&entries[..s])?;
            let right = page::build_internal(&entries[s..])?;
            pager.write_page(no, left.into_buf());
            pager.write_page(new_no, right.into_buf());
            Ok(Some((separator, new_no)))
        }
    }

    /// Delete `key`, returning whether it was present. No rebalancing (v0.1).
    pub fn delete(&self, pager: &mut Pager, key: &[u8]) -> Result<bool> {
        let mut no = self.root;
        loop {
            let page = Page::from_buf(pager.read_page(no)?);
            if page.is_leaf() {
                let mut entries = page.leaf_entries();
                return match entries.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
                    Ok(slot) => {
                        entries.remove(slot);
                        let rebuilt = page::build_leaf(&entries, page.right_link())?;
                        pager.write_page(no, rebuilt.into_buf());
                        Ok(true)
                    }
                    Err(_) => Ok(false),
                };
            }
            no = page.internal_child(page.find_child(key));
        }
    }

    /// Every key/value pair in the tree, in ascending key order.
    pub fn scan(&self, pager: &mut Pager) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // Descend to the leftmost leaf, then follow the leaf chain.
        let mut no = self.root;
        loop {
            let page = Page::from_buf(pager.read_page(no)?);
            if page.is_leaf() {
                break;
            }
            no = page.internal_child(0);
        }
        let mut out = Vec::new();
        while no != 0 {
            let page = Page::from_buf(pager.read_page(no)?);
            if !page.is_leaf() {
                return Err(Error::corruption("leaf chain reached a non-leaf page"));
            }
            for i in 0..page.cell_count() {
                out.push((page.leaf_key(i).to_vec(), page.leaf_value(i).to_vec()));
            }
            no = page.right_link();
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
        // Descend to the leaf that would hold `start`.
        let mut no = self.root;
        loop {
            let page = Page::from_buf(pager.read_page(no)?);
            if page.is_leaf() {
                break;
            }
            no = page.internal_child(page.find_child(start));
        }
        let mut out = Vec::new();
        let mut first = true;
        while no != 0 {
            let page = Page::from_buf(pager.read_page(no)?);
            if !page.is_leaf() {
                return Err(Error::corruption("leaf chain reached a non-leaf page"));
            }
            // Only the first leaf may start partway in; later leaves lie wholly
            // above `start`.
            let begin = if first {
                match page.find_leaf_slot(start) {
                    Ok(i) | Err(i) => i,
                }
            } else {
                0
            };
            first = false;
            for i in begin..page.cell_count() {
                let key = page.leaf_key(i);
                if let Some(end) = end {
                    if key >= end {
                        return Ok(out);
                    }
                }
                out.push((key.to_vec(), page.leaf_value(i).to_vec()));
            }
            no = page.right_link();
        }
        Ok(out)
    }

    /// Return every page of the tree to the pager's free list.
    pub fn free_all(&self, pager: &mut Pager) -> Result<()> {
        free_subtree(pager, self.root)
    }
}

fn free_subtree(pager: &mut Pager, no: u32) -> Result<()> {
    let page = Page::from_buf(pager.read_page(no)?);
    if page.is_internal() {
        let children: Vec<u32> = (0..page.cell_count())
            .map(|i| page.internal_child(i))
            .collect();
        for child in children {
            free_subtree(pager, child)?;
        }
    }
    pager.free_page(no);
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
        let root = Page::from_buf(pager.read_page(tree.root()).unwrap());
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
}
