//! The SQL frontend: the lexer, the abstract syntax tree, and the
//! recursive-descent parser that connects them.
//!
//! This layer is purely syntactic. It turns text into a [`Statement`] and
//! rejects malformed input, but it knows nothing of tables, types, or storage
//! — that judgement belongs to the [`engine`](crate::engine).

pub mod ast;
pub mod lexer;
pub mod parser;
pub mod token;

pub use ast::Statement;
pub use parser::parse;
