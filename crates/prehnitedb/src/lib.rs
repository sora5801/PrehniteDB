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
    )
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
