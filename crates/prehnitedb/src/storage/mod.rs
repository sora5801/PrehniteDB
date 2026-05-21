//! The storage engine: fixed-size slotted pages, the pager that owns the
//! database file, the write-ahead log, and the B+tree built on top of them.
//!
//! Nothing in this module knows what a "row" or a "table" is — it deals purely
//! in pages and byte-string keys and values. The [`engine`](crate::engine)
//! layer gives those bytes meaning.

pub mod btree;
pub mod page;
pub mod pager;
pub mod wal;

pub use btree::{BTree, Cursor};
pub use page::PAGE_SIZE;
pub use pager::Pager;
