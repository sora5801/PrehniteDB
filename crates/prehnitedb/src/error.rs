//! The single error type shared across every layer of PrehniteDB.
//!
//! Each variant marks the layer that detected the fault, which makes failures
//! easy to attribute when they surface at the SQL or protocol boundary.

use std::fmt;

/// Convenience alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Every failure PrehniteDB can produce.
#[derive(Debug)]
pub enum Error {
    /// An underlying filesystem or socket operation failed.
    Io(std::io::Error),
    /// On-disk structures are internally inconsistent or damaged. This is
    /// never the caller's fault and always indicates a bug or a corrupt file.
    Corruption(String),
    /// SQL text could not be tokenized or parsed.
    Parse(String),
    /// A statement parsed cleanly but is semantically invalid: an unknown
    /// table, a type mismatch, a duplicate column, and so on.
    Exec(String),
    /// A key or value exceeded a hard structural limit of the storage engine.
    TooLarge(String),
    /// A peer violated the wire protocol.
    Protocol(String),
}

impl Error {
    /// Build a [`Error::Corruption`] from anything string-like.
    pub fn corruption(msg: impl Into<String>) -> Self {
        Error::Corruption(msg.into())
    }

    /// Build a [`Error::Parse`] from anything string-like.
    pub fn parse(msg: impl Into<String>) -> Self {
        Error::Parse(msg.into())
    }

    /// Build a [`Error::Exec`] from anything string-like.
    pub fn exec(msg: impl Into<String>) -> Self {
        Error::Exec(msg.into())
    }

    /// Build a [`Error::Protocol`] from anything string-like.
    pub fn protocol(msg: impl Into<String>) -> Self {
        Error::Protocol(msg.into())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "i/o error: {e}"),
            Error::Corruption(m) => write!(f, "corruption: {m}"),
            Error::Parse(m) => write!(f, "parse error: {m}"),
            Error::Exec(m) => write!(f, "{m}"),
            Error::TooLarge(m) => write!(f, "limit exceeded: {m}"),
            Error::Protocol(m) => write!(f, "protocol error: {m}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}
