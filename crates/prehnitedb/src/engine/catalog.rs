//! The catalog — the table of tables.
//!
//! The catalog is itself a B+tree, keyed by table name with an encoded
//! [`Schema`] as the value. Its root page number lives in the database header,
//! so the engine can always find it. When a database is brand new the catalog
//! does not exist yet; [`Catalog::open`] creates it on first use.
//!
//! v0.28 makes catalog mutations safe under concurrent writers. Every
//! `INSERT` / `UPDATE` / `DELETE` updates the target schema's
//! `next_rowid` and `row_count` via [`Catalog::put`], which is a
//! read-modify-write on a catalog leaf page. Two writers on
//! *different* tables (each holding its own per-table mutex) still
//! share that leaf page when both schemas live on it, so a brief
//! `write_lock` here serialises only the catalog-page write — the
//! rest of each INSERT runs concurrently.

use std::sync::{Arc, Mutex};

use crate::engine::codec;
use crate::engine::schema::Schema;
use crate::error::{Error, Result};
use crate::storage::{BTree, Pager};

/// Handle to the catalog B+tree.
pub struct Catalog {
    tree: BTree,
    /// Serialises catalog-page writes across every `Catalog` open on
    /// this file. Cloned from `TxState::catalog_lock`; a no-op `Arc` is
    /// used for stand-alone `Database::open` calls and for VACUUM's
    /// scratch destination pager.
    write_lock: Arc<Mutex<()>>,
}

impl Catalog {
    /// Open the catalog with a fresh, unshared `write_lock` — single-
    /// writer use. The server uses [`Catalog::open_with_lock`] instead
    /// so every connection's catalog mutations serialise on one lock.
    ///
    /// A freshly created catalog stages a write to the database header; the
    /// caller must commit the pager for it to persist.
    pub fn open(pager: &mut Pager) -> Result<Catalog> {
        Catalog::open_with_lock(pager, Arc::new(Mutex::new(())))
    }

    /// Open the catalog with an externally-supplied `write_lock`. The
    /// lock is held only inside [`put`](Self::put) and
    /// [`remove`](Self::remove) — readers (`get`, `scan`, `table_names`)
    /// do not take it and run free.
    pub fn open_with_lock(pager: &mut Pager, write_lock: Arc<Mutex<()>>) -> Result<Catalog> {
        let tree = match pager.catalog_root() {
            0 => {
                let tree = BTree::create(pager)?;
                pager.set_catalog_root(tree.root());
                tree
            }
            root => BTree::open(root),
        };
        Ok(Catalog { tree, write_lock })
    }

    /// Fetch the schema for `table`, or `None` if no such table exists.
    pub fn get(&self, pager: &mut Pager, table: &str) -> Result<Option<Schema>> {
        match self.tree.search(pager, table.as_bytes())? {
            Some(bytes) => Ok(Some(codec::decode_schema(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Insert or replace the schema for a table. Serialised by the
    /// catalog `write_lock` so concurrent writers' read-modify-write
    /// cycles on a shared catalog leaf can't lose each other's
    /// updates.
    pub fn put(&self, pager: &mut Pager, schema: &Schema) -> Result<()> {
        let _guard = self.write_lock.lock().expect("poisoned catalog write lock");
        self.tree
            .insert(pager, schema.name.as_bytes(), &codec::encode_schema(schema))
    }

    /// Remove a table's schema. Returns whether it was present.
    /// Serialised by the catalog `write_lock`.
    pub fn remove(&self, pager: &mut Pager, table: &str) -> Result<bool> {
        let _guard = self.write_lock.lock().expect("poisoned catalog write lock");
        self.tree.delete(pager, table.as_bytes())
    }

    /// Every table name, in sorted order.
    pub fn table_names(&self, pager: &mut Pager) -> Result<Vec<String>> {
        let mut names = Vec::new();
        for (key, _) in self.tree.scan(pager)? {
            names.push(
                String::from_utf8(key)
                    .map_err(|_| Error::corruption("catalog holds a non-UTF-8 table name"))?,
            );
        }
        Ok(names)
    }

    /// Find the table whose schema owns an index named `index_name`, returning
    /// that schema together with the index's position in `schema.indexes`.
    /// Index names are unique database-wide.
    pub fn table_with_index(
        &self,
        pager: &mut Pager,
        index_name: &str,
    ) -> Result<Option<(Schema, usize)>> {
        for (_table, encoded) in self.tree.scan(pager)? {
            let schema = codec::decode_schema(&encoded)?;
            if let Some(pos) = schema.indexes.iter().position(|i| i.name == index_name) {
                return Ok(Some((schema, pos)));
            }
        }
        Ok(None)
    }
}
