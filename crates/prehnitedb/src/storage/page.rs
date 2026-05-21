//! Fixed-size slotted pages — the on-disk unit of the storage engine.
//!
//! Every page is [`PAGE_SIZE`] bytes and opens with a 16-byte header. The body
//! holds a *slot array* growing up from the header and *cells* growing down
//! from the end of the page; the gap between them is free space.
//!
//! ```text
//!   0      16                            free_end           PAGE_SIZE
//!   +------+--------+-----------+ ....... +--------+----------+
//!   | hdr  | slot 0 | slot 1 .. |  free   | cell 1 |  cell 0  |
//!   +------+--------+-----------+ ....... +--------+----------+
//! ```
//!
//! Slot `i` is a little-endian `u16` offset to the `i`-th cell *in key order*.
//! Two cell shapes exist:
//!
//! * **leaf cell** — `[key_len u16][val_len u32][key bytes][val bytes]`
//! * **internal cell** — `[child u32][key_len u16][key bytes]`
//!
//! Mutation never edits a page in place: a caller reads every cell out with
//! [`Page::leaf_entries`] / [`Page::internal_entries`], edits the `Vec`, and
//! rebuilds the page with [`build_leaf`] / [`build_internal`]. That keeps pages
//! permanently free of fragmentation, at the cost of rewriting a whole page per
//! change — a fine trade for v0.1 and an obvious thing to optimize later.

use std::cmp::Ordering;

use crate::error::{Error, Result};

/// Size of every page, in bytes.
pub const PAGE_SIZE: usize = 4096;
/// Size of the per-page header, in bytes.
pub const HEADER_SIZE: usize = 16;
/// Bytes in a page available for the slot array plus cell content.
pub const USABLE: usize = PAGE_SIZE - HEADER_SIZE;
/// Largest a single cell may be. Capping cells just under half the usable area
/// guarantees an overflowing node always splits into two pages that each fit
/// (see `btree::split_index`). Larger values would need overflow pages, which
/// v0.1 does not implement.
pub const MAX_CELL: usize = USABLE / 2 - 2;

const OFF_TYPE: usize = 0;
const OFF_CELL_COUNT: usize = 2;
const OFF_FREE_END: usize = 4;
const OFF_RIGHT_LINK: usize = 8;

/// Bytes of fixed header in a leaf cell: `key_len(2) + val_len(4)`.
pub const LEAF_CELL_OVERHEAD: usize = 6;
// Bytes of fixed header in an internal cell: child(4) + key_len(2).
const INTERNAL_CELL_HEADER: usize = 6;
const SLOT_SIZE: usize = 2;

/// An interior B+tree node: routes a search toward a child.
pub const PAGE_INTERNAL: u8 = 1;
/// A B+tree leaf: holds the actual key/value pairs.
pub const PAGE_LEAF: u8 = 2;

/// An owned, typed view over a single page buffer.
pub struct Page {
    buf: Box<[u8; PAGE_SIZE]>,
}

impl Page {
    /// Wrap an existing raw page buffer.
    pub fn from_buf(buf: Box<[u8; PAGE_SIZE]>) -> Page {
        Page { buf }
    }

    /// Consume the page, yielding the raw buffer to hand back to the pager.
    pub fn into_buf(self) -> Box<[u8; PAGE_SIZE]> {
        self.buf
    }

    /// A fresh, empty leaf page.
    pub fn new_leaf() -> Page {
        let mut buf = Box::new([0u8; PAGE_SIZE]);
        buf[OFF_TYPE] = PAGE_LEAF;
        put_u16(&mut buf[..], OFF_FREE_END, PAGE_SIZE as u16);
        Page { buf }
    }

    /// A fresh, empty interior page.
    pub fn new_internal() -> Page {
        let mut buf = Box::new([0u8; PAGE_SIZE]);
        buf[OFF_TYPE] = PAGE_INTERNAL;
        put_u16(&mut buf[..], OFF_FREE_END, PAGE_SIZE as u16);
        Page { buf }
    }

    pub fn page_type(&self) -> u8 {
        self.buf[OFF_TYPE]
    }

    pub fn is_leaf(&self) -> bool {
        self.page_type() == PAGE_LEAF
    }

    pub fn is_internal(&self) -> bool {
        self.page_type() == PAGE_INTERNAL
    }

    /// Number of cells (key/value or key/child pairs) on the page.
    pub fn cell_count(&self) -> usize {
        get_u16(&self.buf[..], OFF_CELL_COUNT) as usize
    }

    /// Leaf only: the next leaf in left-to-right scan order (0 = none).
    pub fn right_link(&self) -> u32 {
        get_u32(&self.buf[..], OFF_RIGHT_LINK)
    }

    pub fn set_right_link(&mut self, page: u32) {
        put_u32(&mut self.buf[..], OFF_RIGHT_LINK, page);
    }

    fn slot(&self, i: usize) -> usize {
        get_u16(&self.buf[..], HEADER_SIZE + i * SLOT_SIZE) as usize
    }

    /// Key of leaf cell `i`.
    pub fn leaf_key(&self, i: usize) -> &[u8] {
        let o = self.slot(i);
        let klen = get_u16(&self.buf[..], o) as usize;
        &self.buf[o + LEAF_CELL_OVERHEAD..o + LEAF_CELL_OVERHEAD + klen]
    }

    /// Value of leaf cell `i`.
    pub fn leaf_value(&self, i: usize) -> &[u8] {
        let o = self.slot(i);
        let klen = get_u16(&self.buf[..], o) as usize;
        let vlen = get_u32(&self.buf[..], o + 2) as usize;
        let start = o + LEAF_CELL_OVERHEAD + klen;
        &self.buf[start..start + vlen]
    }

    /// Separator key of interior cell `i`.
    pub fn internal_key(&self, i: usize) -> &[u8] {
        let o = self.slot(i);
        let klen = get_u16(&self.buf[..], o + 4) as usize;
        &self.buf[o + INTERNAL_CELL_HEADER..o + INTERNAL_CELL_HEADER + klen]
    }

    /// Child page number of interior cell `i`.
    pub fn internal_child(&self, i: usize) -> u32 {
        get_u32(&self.buf[..], self.slot(i))
    }

    /// Binary-search a leaf for `key`: `Ok(slot)` if present, else `Err(slot)`
    /// giving the slot at which it would be inserted.
    pub fn find_leaf_slot(&self, key: &[u8]) -> std::result::Result<usize, usize> {
        let (mut lo, mut hi) = (0usize, self.cell_count());
        while lo < hi {
            let mid = (lo + hi) / 2;
            match self.leaf_key(mid).cmp(key) {
                Ordering::Less => lo = mid + 1,
                Ordering::Greater => hi = mid,
                Ordering::Equal => return Ok(mid),
            }
        }
        Err(lo)
    }

    /// Pick the interior child to follow for `key`: the last cell whose
    /// separator is `<= key`, or cell 0 when `key` precedes every separator.
    pub fn find_child(&self, key: &[u8]) -> usize {
        let (mut lo, mut hi) = (0usize, self.cell_count());
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.internal_key(mid) <= key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo.saturating_sub(1)
    }

    /// Every (key, value) pair on a leaf, in key order.
    pub fn leaf_entries(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        (0..self.cell_count())
            .map(|i| (self.leaf_key(i).to_vec(), self.leaf_value(i).to_vec()))
            .collect()
    }

    /// Every (separator, child) pair on an interior node, in key order.
    pub fn internal_entries(&self) -> Vec<(Vec<u8>, u32)> {
        (0..self.cell_count())
            .map(|i| (self.internal_key(i).to_vec(), self.internal_child(i)))
            .collect()
    }
}

/// Build a leaf page from ordered entries and a right-link.
pub fn build_leaf(entries: &[(Vec<u8>, Vec<u8>)], right_link: u32) -> Result<Page> {
    let body: usize = entries
        .iter()
        .map(|(k, v)| SLOT_SIZE + LEAF_CELL_OVERHEAD + k.len() + v.len())
        .sum();
    if body > USABLE {
        return Err(Error::corruption(
            "build_leaf: entries exceed page capacity",
        ));
    }
    let mut page = Page::new_leaf();
    page.set_right_link(right_link);
    let mut cursor = PAGE_SIZE;
    for (i, (k, v)) in entries.iter().enumerate() {
        let cell_len = LEAF_CELL_OVERHEAD + k.len() + v.len();
        cursor -= cell_len;
        put_u16(&mut page.buf[..], cursor, k.len() as u16);
        put_u32(&mut page.buf[..], cursor + 2, v.len() as u32);
        let kstart = cursor + LEAF_CELL_OVERHEAD;
        page.buf[kstart..kstart + k.len()].copy_from_slice(k);
        let vstart = kstart + k.len();
        page.buf[vstart..vstart + v.len()].copy_from_slice(v);
        put_u16(
            &mut page.buf[..],
            HEADER_SIZE + i * SLOT_SIZE,
            cursor as u16,
        );
    }
    put_u16(&mut page.buf[..], OFF_CELL_COUNT, entries.len() as u16);
    put_u16(&mut page.buf[..], OFF_FREE_END, cursor as u16);
    Ok(page)
}

/// Build an interior page from ordered (separator, child) entries.
pub fn build_internal(entries: &[(Vec<u8>, u32)]) -> Result<Page> {
    let body: usize = entries
        .iter()
        .map(|(k, _)| SLOT_SIZE + INTERNAL_CELL_HEADER + k.len())
        .sum();
    if body > USABLE {
        return Err(Error::corruption(
            "build_internal: entries exceed page capacity",
        ));
    }
    let mut page = Page::new_internal();
    let mut cursor = PAGE_SIZE;
    for (i, (k, child)) in entries.iter().enumerate() {
        let cell_len = INTERNAL_CELL_HEADER + k.len();
        cursor -= cell_len;
        put_u32(&mut page.buf[..], cursor, *child);
        put_u16(&mut page.buf[..], cursor + 4, k.len() as u16);
        let kstart = cursor + INTERNAL_CELL_HEADER;
        page.buf[kstart..kstart + k.len()].copy_from_slice(k);
        put_u16(
            &mut page.buf[..],
            HEADER_SIZE + i * SLOT_SIZE,
            cursor as u16,
        );
    }
    put_u16(&mut page.buf[..], OFF_CELL_COUNT, entries.len() as u16);
    put_u16(&mut page.buf[..], OFF_FREE_END, cursor as u16);
    Ok(page)
}

/// Footprint (cell bytes plus slot) of one leaf entry.
pub fn leaf_footprint(key: &[u8], value: &[u8]) -> usize {
    SLOT_SIZE + LEAF_CELL_OVERHEAD + key.len() + value.len()
}

/// Footprint (cell bytes plus slot) of one interior entry.
pub fn internal_footprint(key: &[u8]) -> usize {
    SLOT_SIZE + INTERNAL_CELL_HEADER + key.len()
}

fn get_u16(b: &[u8], o: usize) -> u16 {
    u16::from_le_bytes([b[o], b[o + 1]])
}

fn get_u32(b: &[u8], o: usize) -> u32 {
    u32::from_le_bytes([b[o], b[o + 1], b[o + 2], b[o + 3]])
}

fn put_u16(b: &mut [u8], o: usize, v: u16) {
    b[o..o + 2].copy_from_slice(&v.to_le_bytes());
}

fn put_u32(b: &mut [u8], o: usize, v: u32) {
    b[o..o + 4].copy_from_slice(&v.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leaf_round_trip() {
        let entries = vec![
            (b"alpha".to_vec(), b"1".to_vec()),
            (b"bravo".to_vec(), b"two".to_vec()),
            (b"charlie".to_vec(), Vec::new()),
        ];
        let page = build_leaf(&entries, 42).unwrap();
        assert!(page.is_leaf());
        assert_eq!(page.cell_count(), 3);
        assert_eq!(page.right_link(), 42);
        assert_eq!(page.leaf_entries(), entries);
        assert_eq!(page.leaf_value(2), b"");
    }

    #[test]
    fn internal_round_trip() {
        let entries = vec![
            (b"".to_vec(), 1u32),
            (b"m".to_vec(), 7u32),
            (b"x".to_vec(), 9u32),
        ];
        let page = build_internal(&entries).unwrap();
        assert!(page.is_internal());
        assert_eq!(page.internal_entries(), entries);
    }

    #[test]
    fn leaf_search() {
        let entries = vec![
            (vec![1], b"a".to_vec()),
            (vec![3], b"c".to_vec()),
            (vec![5], b"e".to_vec()),
        ];
        let page = build_leaf(&entries, 0).unwrap();
        assert_eq!(page.find_leaf_slot(&[3]), Ok(1));
        assert_eq!(page.find_leaf_slot(&[0]), Err(0));
        assert_eq!(page.find_leaf_slot(&[4]), Err(2));
        assert_eq!(page.find_leaf_slot(&[9]), Err(3));
    }

    #[test]
    fn internal_routing() {
        // separators 10, 20, 30 -> children 100, 200, 300, 400
        let entries = vec![(vec![10], 100u32), (vec![20], 200u32), (vec![30], 300u32)];
        let page = build_internal(&entries).unwrap();
        assert_eq!(page.find_child(&[5]), 0); // precedes everything
        assert_eq!(page.find_child(&[10]), 0);
        assert_eq!(page.find_child(&[15]), 0);
        assert_eq!(page.find_child(&[20]), 1);
        assert_eq!(page.find_child(&[25]), 1);
        assert_eq!(page.find_child(&[99]), 2);
    }

    #[test]
    fn buffer_round_trips_through_raw_bytes() {
        let entries = vec![(b"k".to_vec(), b"v".to_vec())];
        let page = build_leaf(&entries, 5).unwrap();
        let raw = page.into_buf();
        let page = Page::from_buf(raw);
        assert_eq!(page.leaf_entries(), entries);
        assert_eq!(page.right_link(), 5);
    }
}
