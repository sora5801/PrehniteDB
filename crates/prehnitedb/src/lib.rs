//! # PrehniteDB
//!
//! A relational database built from scratch in Rust, with **no external
//! dependencies** — only the standard library.
//!
//! The crate is organized as a stack of layers, each of which only knows about
//! the one below it:
//!
//! ```text
//!   protocol   wire framing for the network server / client
//!   engine     catalog, planner, executor, SQL value model
//!   sql        lexer, parser, abstract syntax tree
//!   storage    pager, write-ahead log, B+tree
//! ```
//!
//! The public entry point is [`Database`]: open a file, hand it SQL text, get
//! back a [`QueryResult`].
//!
//! ```no_run
//! use prehnitedb::Database;
//!
//! let mut db = Database::open("example.db").unwrap();
//! db.execute("CREATE TABLE users (id INT, name TEXT)").unwrap();
//! db.execute("INSERT INTO users VALUES (1, 'ada')").unwrap();
//! let result = db.execute("SELECT name FROM users WHERE id = 1").unwrap();
//! println!("{result}");
//! ```

pub mod engine;
pub mod error;
pub mod protocol;
pub mod sql;
pub mod storage;

pub use crate::engine::database::Database;
pub use crate::engine::executor::{Execution, QueryResult, RowStream};
pub use crate::engine::transaction::TxState;
pub use crate::engine::value::{Type, Value};
pub use crate::error::{Error, Result};
pub use crate::storage::SharedPool;

/// Whether `sql` is a read-only statement — one a concurrent reader may run
/// without excluding writers. Only a `SELECT` qualifies; every other
/// statement, and any input that fails to parse, counts as a write.
pub fn is_read_only(sql: &str) -> bool {
    matches!(
        crate::sql::parse(sql),
        Ok(crate::sql::ast::Statement::Select { .. })
            | Ok(crate::sql::ast::Statement::Explain { .. })
    )
}

/// How a write statement intends to use the target table's lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableAccess {
    /// Multiple writers can hold this concurrently — they synchronise
    /// at the B+tree's per-page latch level inside the engine.
    /// `INSERT` / `UPDATE` / `DELETE` use this.
    Shared,
    /// Exclusive access to the whole table — no other writers or
    /// readers may touch it. `CREATE INDEX` rebuilds the index from a
    /// full scan and must see a consistent table.
    Exclusive,
}

/// What runtime locks a write statement needs the server to take. Reads
/// (every `SELECT`) take no locks at all and run lock-free. The other
/// shapes:
///
/// - [`WriteScope::Table`]: a per-table RwLock — `INSERT`, `UPDATE`,
///   `DELETE` take it shared (parallel-safe via per-page latches);
///   `CREATE INDEX` takes it exclusive (rebuilds the whole index).
/// - [`WriteScope::Catalog`]: the catalog mutex — `CREATE TABLE`,
///   `DROP TABLE`, `VACUUM`, `DROP INDEX`. Schema changes serialise
///   against each other but not against per-table data writes
///   (`VACUUM` is the exception: its `replace_with` is exclusive, so
///   it conflicts with anything in flight — for v0.28+ the engine
///   doesn't let `VACUUM` run inside a transaction, and the caller is
///   expected to coordinate VACUUM with peer writers externally).
/// - [`WriteScope::None`]: BEGIN/COMMIT/ROLLBACK — purely transactional
///   bookkeeping at the engine layer.
/// - [`WriteScope::Unknown`]: the SQL failed to parse. The caller is
///   expected to take the catalog mutex as a conservative fallback;
///   the actual execute will return a parse error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteScope {
    /// Names the single table this statement writes and how it uses
    /// the table lock.
    Table(String, TableAccess),
    /// A catalog-level change: CREATE/DROP TABLE, VACUUM, DROP INDEX.
    Catalog,
    /// No runtime lock — BEGIN, COMMIT, ROLLBACK.
    None,
    /// Couldn't parse — fall back to the catalog mutex.
    Unknown,
}

/// Classify a write statement for the server's per-statement locking.
/// Reads should use [`is_read_only`] first; this function classifies the
/// shape of the *write* path.
pub fn write_scope(sql: &str) -> WriteScope {
    use crate::sql::ast::Statement;
    match crate::sql::parse(sql) {
        Ok(Statement::Insert { table, .. })
        | Ok(Statement::Update { table, .. })
        | Ok(Statement::Delete { table, .. }) => {
            WriteScope::Table(table, TableAccess::Shared)
        }
        Ok(Statement::CreateIndex { table, .. }) => {
            WriteScope::Table(table, TableAccess::Exclusive)
        }
        Ok(Statement::DropIndex { .. }) => {
            // We don't know the target table from the statement alone (the
            // index name is the only handle). The catalog mutex is correct
            // — DROP INDEX is rare and conflicts only with other catalog
            // ops, which is the conservative choice.
            WriteScope::Catalog
        }
        Ok(Statement::CreateTable { .. })
        | Ok(Statement::DropTable { .. })
        | Ok(Statement::Vacuum) => WriteScope::Catalog,
        Ok(Statement::Begin) | Ok(Statement::Commit) | Ok(Statement::Rollback) => {
            WriteScope::None
        }
        Ok(Statement::Select { .. }) | Ok(Statement::Explain { .. }) => {
            // SELECT and EXPLAIN are read-only at the wire level —
            // is_read_only catches SELECT directly; EXPLAIN never
            // writes, even with ANALYZE (the parser restricts the
            // inner to SELECT, and ANALYZE is just observation).
            WriteScope::None
        }
        Err(_) => WriteScope::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::is_read_only;

    #[test]
    fn classifies_statements() {
        assert!(is_read_only("SELECT * FROM t"));
        assert!(is_read_only("  select n from t where n > 1  "));
        assert!(!is_read_only("INSERT INTO t VALUES (1)"));
        assert!(!is_read_only("UPDATE t SET n = 1"));
        assert!(!is_read_only("DELETE FROM t"));
        assert!(!is_read_only("CREATE TABLE t (n INT)"));
        assert!(!is_read_only("DROP TABLE t"));
        assert!(!is_read_only("VACUUM"));
        assert!(!is_read_only("BEGIN"));
        assert!(!is_read_only("COMMIT"));
        // Malformed input is not a SELECT, so it is treated as a write.
        assert!(!is_read_only("not valid sql"));
        assert!(!is_read_only(""));
    }
}
