//! The SQL value model — column types and the runtime values that inhabit
//! them.

use std::fmt;

use crate::error::{Error, Result};
use crate::sql::ast::TypeName;

/// The type of a column. PrehniteDB has four scalar types and no `NULL` type —
/// nullability is a property of a *value*, not a column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Type {
    Int,
    Real,
    Text,
    Bool,
}

impl Type {
    /// The SQL spelling of the type, used in messages.
    pub fn name(self) -> &'static str {
        match self {
            Type::Int => "INT",
            Type::Real => "REAL",
            Type::Text => "TEXT",
            Type::Bool => "BOOL",
        }
    }
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl From<TypeName> for Type {
    fn from(name: TypeName) -> Type {
        match name {
            TypeName::Int => Type::Int,
            TypeName::Text => Type::Text,
            TypeName::Real => Type::Real,
            TypeName::Bool => Type::Bool,
        }
    }
}

/// A single runtime value. `Null` is a member of every type.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Int(i64),
    Real(f64),
    Text(String),
    Bool(bool),
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// A short type name for the value, used in error messages.
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => "NULL",
            Value::Int(_) => "INT",
            Value::Real(_) => "REAL",
            Value::Text(_) => "TEXT",
            Value::Bool(_) => "BOOL",
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => f.write_str("NULL"),
            Value::Int(n) => write!(f, "{n}"),
            Value::Real(r) => write!(f, "{r}"),
            Value::Text(s) => f.write_str(s),
            Value::Bool(b) => write!(f, "{b}"),
        }
    }
}

/// Adapt a value to the type of the column it is about to be stored in or
/// compared against. `NULL` fits anywhere, and an integer widens into a `REAL`
/// column; nothing else is converted implicitly.
pub(crate) fn coerce(value: Value, target: Type) -> Result<Value> {
    match (value, target) {
        (Value::Null, _) => Ok(Value::Null),
        (Value::Int(n), Type::Int) => Ok(Value::Int(n)),
        (Value::Int(n), Type::Real) => Ok(Value::Real(n as f64)),
        (Value::Real(r), Type::Real) => Ok(Value::Real(r)),
        (Value::Text(s), Type::Text) => Ok(Value::Text(s)),
        (Value::Bool(b), Type::Bool) => Ok(Value::Bool(b)),
        (other, target) => Err(Error::exec(format!(
            "cannot store {} in a {} column",
            other.type_name(),
            target
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coercion_rules() {
        assert_eq!(coerce(Value::Int(4), Type::Int).unwrap(), Value::Int(4));
        assert_eq!(coerce(Value::Int(4), Type::Real).unwrap(), Value::Real(4.0));
        assert_eq!(coerce(Value::Null, Type::Bool).unwrap(), Value::Null);
        assert!(coerce(Value::Real(4.0), Type::Int).is_err());
        assert!(coerce(Value::Text("x".into()), Type::Int).is_err());
    }
}
