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
pub use crate::engine::executor::QueryResult;
pub use crate::engine::value::{Type, Value};
pub use crate::error::{Error, Result};
