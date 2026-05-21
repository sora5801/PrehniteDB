//! The catalog — the table of tables.
//!
//! The catalog is itself a B+tree, keyed by table name with an encoded
//! [`Schema`] as the value. Its root page number lives in the database header,
//! so the engine can always find it. When a database is brand new the catalog
//! does not exist yet; [`Catalog::open`] creates it on first use.

use crate::engine::codec;
use crate::engine::schema::Schema;
use crate::error::{Error, Result};
use crate::storage::{BTree, Pager};

/// Handle to the catalog B+tree.
pub struct Catalog {
    tree: BTree,
}

impl Catalog {
    /// Open the catalog, creating it if this database has none yet.
    ///
    /// A freshly created catalog stages a write to the database header; the
    /// caller must commit the pager for it to persist.
    pub fn open(pager: &mut Pager) -> Result<Catalog> {
        let tree = match pager.catalog_root() {
            0 => {
                let tree = BTree::create(pager)?;
                pager.set_catalog_root(tree.root());
                tree
            }
            root => BTree::open(root),
        };
        Ok(Catalog { tree })
    }

    /// Fetch the schema for `table`, or `None` if no such table exists.
    pub fn get(&self, pager: &mut Pager, table: &str) -> Result<Option<Schema>> {
        match self.tree.search(pager, table.as_bytes())? {
            Some(bytes) => Ok(Some(codec::decode_schema(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Insert or replace the schema for a table.
    pub fn put(&self, pager: &mut Pager, schema: &Schema) -> Result<()> {
        self.tree
            .insert(pager, schema.name.as_bytes(), &codec::encode_schema(schema))
    }

    /// Remove a table's schema. Returns whether it was present.
    pub fn remove(&self, pager: &mut Pager, table: &str) -> Result<bool> {
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
}
