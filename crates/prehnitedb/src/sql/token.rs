//! Lexical tokens for the SQL frontend.

/// A reserved word. The lexer recognizes these case-insensitively; any other
/// run of identifier characters becomes a [`Token::Ident`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Keyword {
    Select,
    From,
    Where,
    Insert,
    Into,
    Values,
    Create,
    Table,
    Drop,
    Update,
    Set,
    Delete,
    Index,
    On,
    Join,
    Inner,
    Left,
    Cross,
    As,
    Order,
    By,
    Asc,
    Desc,
    Group,
    Having,
    Vacuum,
    Begin,
    Commit,
    Rollback,
    Limit,
    Offset,
    And,
    Or,
    Not,
    Is,
    In,
    Exists,
    Null,
    True,
    False,
    Int,
    Integer,
    Text,
    Real,
    Float,
    Bool,
    Boolean,
}

impl Keyword {
    /// Map a word to its keyword, case-insensitively; `None` if it is not one.
    pub fn from_word(word: &str) -> Option<Keyword> {
        use Keyword::*;
        Some(match word.to_ascii_uppercase().as_str() {
            "SELECT" => Select,
            "FROM" => From,
            "WHERE" => Where,
            "INSERT" => Insert,
            "INTO" => Into,
            "VALUES" => Values,
            "CREATE" => Create,
            "TABLE" => Table,
            "DROP" => Drop,
            "UPDATE" => Update,
            "SET" => Set,
            "DELETE" => Delete,
            "INDEX" => Index,
            "ON" => On,
            "JOIN" => Join,
            "INNER" => Inner,
            "LEFT" => Left,
            "CROSS" => Cross,
            "AS" => As,
            "ORDER" => Order,
            "BY" => By,
            "ASC" => Asc,
            "DESC" => Desc,
            "GROUP" => Group,
            "HAVING" => Having,
            "VACUUM" => Vacuum,
            "BEGIN" => Begin,
            "COMMIT" => Commit,
            "ROLLBACK" => Rollback,
            "LIMIT" => Limit,
            "OFFSET" => Offset,
            "AND" => And,
            "OR" => Or,
            "NOT" => Not,
            "IS" => Is,
            "IN" => In,
            "EXISTS" => Exists,
            "NULL" => Null,
            "TRUE" => True,
            "FALSE" => False,
            "INT" => Int,
            "INTEGER" => Integer,
            "TEXT" => Text,
            "REAL" => Real,
            "FLOAT" => Float,
            "BOOL" => Bool,
            "BOOLEAN" => Boolean,
            _ => return None,
        })
    }
}

/// A single lexical token produced by the lexer.
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    /// An integer literal, e.g. `42`.
    Integer(i64),
    /// A floating-point literal, e.g. `3.14`.
    Real(f64),
    /// A single-quoted string literal, with `''` already unescaped to `'`.
    Str(String),
    /// An identifier — a table or column name.
    Ident(String),
    /// A reserved word.
    Keyword(Keyword),
    Comma,
    /// `.` — qualifies a column reference, as in `table.column`.
    Dot,
    Semicolon,
    LParen,
    RParen,
    /// `*` — either "all columns" or the multiply operator, per context.
    Star,
    Plus,
    Minus,
    Slash,
    Eq,
    /// `!=` or `<>`.
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}
