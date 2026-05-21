//! [`Database`] — the public face of PrehniteDB.
//!
//! A `Database` owns a [`Pager`] and a [`Catalog`] and turns SQL text into
//! results. Each call to [`Database::execute`] is its own transaction: the
//! statement runs against the pager's staged-write buffer and is either
//! committed whole on success or rolled back entirely on failure.

use std::path::{Path, PathBuf};

use crate::engine::catalog::Catalog;
use crate::engine::executor::{self, QueryResult};
use crate::engine::planner::{self, Plan};
use crate::engine::schema::{Index, Schema};
use crate::error::{Error, Result};
use crate::storage::pager::wal_path;
use crate::storage::{BTree, Pager};

/// An open PrehniteDB database.
pub struct Database {
    pager: Pager,
    catalog: Catalog,
    path: PathBuf,
}

impl Database {
    /// Open the database at `path`, creating it if it does not exist.
    pub fn open(path: impl AsRef<Path>) -> Result<Database> {
        let path = path.as_ref().to_path_buf();
        let mut pager = Pager::open(&path)?;
        let catalog = Catalog::open(&mut pager)?;
        // Persist the catalog if `Catalog::open` just created it. When the
        // catalog already existed nothing is staged and this is a no-op.
        pager.commit()?;
        Ok(Database {
            pager,
            catalog,
            path,
        })
    }

    /// Parse, plan, and run one SQL statement.
    ///
    /// On success the statement's writes are committed durably before the
    /// result is returned. On failure every staged change is discarded, so a
    /// rejected statement never leaves a partial effect.
    pub fn execute(&mut self, sql: &str) -> Result<QueryResult> {
        let statement = crate::sql::parse(sql)?;
        let plan = planner::plan(statement, &mut self.pager, &self.catalog)?;
        // VACUUM rewrites the pager's whole file, which the executor cannot do.
        if matches!(plan, Plan::Vacuum) {
            return self.vacuum();
        }
        match executor::execute(&mut self.pager, &self.catalog, plan) {
            Ok(result) => {
                self.pager.commit()?;
                Ok(result)
            }
            Err(err) => {
                self.pager.rollback();
                Err(err)
            }
        }
    }

    /// The names of all tables, in sorted order.
    pub fn table_names(&mut self) -> Result<Vec<String>> {
        self.catalog.table_names(&mut self.pager)
    }

    /// Rebuild the database compactly: every table and index is re-created in
    /// a fresh temp file with no free space, then that file's contents replace
    /// the live database in a single WAL-protected commit.
    fn vacuum(&mut self) -> Result<QueryResult> {
        let temp = vacuum_temp_path(&self.path);
        let _ = std::fs::remove_file(&temp);
        let _ = std::fs::remove_file(wal_path(&temp));

        // Build the compact copy in `temp`; its pager closes at scope end.
        {
            let mut dest = Pager::open(&temp)?;
            let dest_catalog = Catalog::open(&mut dest)?;
            for name in self.catalog.table_names(&mut self.pager)? {
                let schema = self
                    .catalog
                    .get(&mut self.pager, &name)?
                    .ok_or_else(|| Error::corruption("catalog lists a table it cannot return"))?;

                // Copy the rows into a fresh, densely packed B+tree.
                let table = BTree::create(&mut dest)?;
                for (key, value) in BTree::open(schema.root).scan(&mut self.pager)? {
                    table.insert(&mut dest, &key, &value)?;
                }

                // Rebuild each index the same way.
                let mut indexes = Vec::with_capacity(schema.indexes.len());
                for index in &schema.indexes {
                    let rebuilt = BTree::create(&mut dest)?;
                    for (key, _) in BTree::open(index.root).scan(&mut self.pager)? {
                        rebuilt.insert(&mut dest, &key, &[])?;
                    }
                    indexes.push(Index {
                        name: index.name.clone(),
                        columns: index.columns.clone(),
                        root: rebuilt.root(),
                    });
                }

                dest_catalog.put(
                    &mut dest,
                    &Schema {
                        name: schema.name,
                        columns: schema.columns,
                        root: table.root(),
                        next_rowid: schema.next_rowid,
                        indexes,
                    },
                )?;
            }
            dest.commit()?;
        }

        // Adopt the compact image as our own contents, then discard the temp.
        self.pager.replace_with(&temp)?;
        let _ = std::fs::remove_file(&temp);
        let _ = std::fs::remove_file(wal_path(&temp));
        // The catalog's root page has moved; reopen it.
        self.catalog = Catalog::open(&mut self.pager)?;
        Ok(QueryResult::Ack("database compacted".to_string()))
    }
}

/// The scratch file VACUUM builds its compact copy in, beside the database.
fn vacuum_temp_path(db: &Path) -> PathBuf {
    let mut name = db.as_os_str().to_os_string();
    name.push(".vacuum");
    PathBuf::from(name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::value::Value;
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
                std::env::temp_dir().join(format!("prehnite-db-{}-{n}.db", std::process::id()));
            let _ = std::fs::remove_file(&path);
            let _ = std::fs::remove_file(wal_path(&path));
            TempDb { path }
        }

        fn open(&self) -> Database {
            Database::open(&self.path).unwrap()
        }
    }

    impl Drop for TempDb {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
            let _ = std::fs::remove_file(wal_path(&self.path));
        }
    }

    fn rows(result: QueryResult) -> Vec<Vec<Value>> {
        match result {
            QueryResult::Rows { rows, .. } => rows,
            other => panic!("expected a result set, got {other:?}"),
        }
    }

    #[test]
    fn full_lifecycle() {
        let tmp = TempDb::new();
        let mut db = tmp.open();
        db.execute("CREATE TABLE users (id INT, name TEXT, active BOOL)")
            .unwrap();
        db.execute("INSERT INTO users VALUES (1, 'ada', true), (2, 'grace', false)")
            .unwrap();
        db.execute("INSERT INTO users (id, name) VALUES (3, 'edsger')")
            .unwrap();

        let all = rows(db.execute("SELECT * FROM users").unwrap());
        assert_eq!(all.len(), 3);
        assert_eq!(all[2][2], Value::Null); // unspecified column defaulted to NULL

        let active = rows(
            db.execute("SELECT name FROM users WHERE active = true")
                .unwrap(),
        );
        assert_eq!(active, vec![vec![Value::Text("ada".into())]]);

        db.execute("UPDATE users SET active = true WHERE id = 3")
            .unwrap();
        let active = rows(
            db.execute("SELECT id FROM users WHERE active = true")
                .unwrap(),
        );
        assert_eq!(active.len(), 2);

        db.execute("DELETE FROM users WHERE id = 2").unwrap();
        assert_eq!(rows(db.execute("SELECT * FROM users").unwrap()).len(), 2);

        db.execute("DROP TABLE users").unwrap();
        assert!(db.execute("SELECT * FROM users").is_err());
    }

    #[test]
    fn data_persists_across_reopen() {
        let tmp = TempDb::new();
        {
            let mut db = tmp.open();
            db.execute("CREATE TABLE t (n INT)").unwrap();
            for i in 0..50 {
                db.execute(&format!("INSERT INTO t VALUES ({i})")).unwrap();
            }
        }
        let mut db = tmp.open();
        let all = rows(db.execute("SELECT n FROM t").unwrap());
        assert_eq!(all.len(), 50);
        assert_eq!(all[0][0], Value::Int(0));
        assert_eq!(all[49][0], Value::Int(49));
    }

    #[test]
    fn failed_statement_rolls_back() {
        let tmp = TempDb::new();
        let mut db = tmp.open();
        db.execute("CREATE TABLE t (n INT)").unwrap();
        db.execute("INSERT INTO t VALUES (1)").unwrap();

        // The second row has the wrong type; the whole INSERT must fail
        // atomically, leaving only the one good row already present.
        assert!(db
            .execute("INSERT INTO t VALUES (2), ('not an int')")
            .is_err());
        let all = rows(db.execute("SELECT n FROM t").unwrap());
        assert_eq!(all, vec![vec![Value::Int(1)]]);

        // The database is still fully usable after the failure.
        db.execute("INSERT INTO t VALUES (3)").unwrap();
        assert_eq!(rows(db.execute("SELECT n FROM t").unwrap()).len(), 2);
    }

    #[test]
    fn rejects_semantic_errors() {
        let tmp = TempDb::new();
        let mut db = tmp.open();
        db.execute("CREATE TABLE t (n INT)").unwrap();
        assert!(db.execute("CREATE TABLE t (n INT)").is_err()); // table already exists
        assert!(db.execute("SELECT missing FROM t").is_err()); // unknown column
        assert!(db.execute("INSERT INTO t VALUES (1, 2)").is_err()); // wrong arity
        assert!(db.execute("SELECT * FROM ghost").is_err()); // unknown table
    }

    #[test]
    fn arithmetic_and_filters_in_queries() {
        let tmp = TempDb::new();
        let mut db = tmp.open();
        db.execute("CREATE TABLE nums (x INT)").unwrap();
        db.execute("INSERT INTO nums VALUES (1), (2), (3), (4), (5)")
            .unwrap();
        let big = rows(db.execute("SELECT x FROM nums WHERE x * 2 >= 6").unwrap());
        assert_eq!(big.len(), 3); // 3, 4, 5
        assert_eq!(db.table_names().unwrap(), vec!["nums".to_string()]);
    }
}
