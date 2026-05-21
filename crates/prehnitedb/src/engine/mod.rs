//! The execution engine — it gives the storage layer's raw bytes meaning as
//! tables, rows, and typed values.
//!
//! The flow of a statement: parsed [`Statement`](crate::sql::Statement) ->
//! [`planner`] lowers and validates it into a [`Plan`](planner::Plan) ->
//! [`executor`] runs it against the [`catalog`] and the pager. [`Database`]
//! ties it together and wraps each statement in a transaction.

pub mod catalog;
pub mod codec;
pub mod database;
pub mod executor;
pub mod planner;
pub mod schema;
pub mod value;

pub use database::Database;
pub use executor::QueryResult;
pub use schema::{Column, Schema};
pub use value::{Type, Value};
