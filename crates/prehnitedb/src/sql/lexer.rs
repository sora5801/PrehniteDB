//! The SQL lexer: turns source text into a flat token stream.
//!
//! It works over a `Vec<char>` so multi-byte characters inside string literals
//! behave, while every token of interest is plain ASCII. `--` begins a comment
//! that runs to end of line.

use crate::error::{Error, Result};
use crate::sql::token::{Keyword, Token};

/// Tokenize one chunk of SQL text.
pub fn tokenize(input: &str) -> Result<Vec<Token>> {
    let chars: Vec<char> = input.chars().collect();
    let mut tokens = Vec::new();
    let mut i = 0;

    while i < chars.len() {
        let c = chars[i];
        match c {
            c if c.is_whitespace() => i += 1,

            // `--` line comment.
            '-' if i + 1 < chars.len() && chars[i + 1] == '-' => {
                while i < chars.len() && chars[i] != '\n' {
                    i += 1;
                }
            }

            ',' => push(&mut tokens, Token::Comma, &mut i),
            '.' => push(&mut tokens, Token::Dot, &mut i),
            ';' => push(&mut tokens, Token::Semicolon, &mut i),
            '(' => push(&mut tokens, Token::LParen, &mut i),
            ')' => push(&mut tokens, Token::RParen, &mut i),
            '*' => push(&mut tokens, Token::Star, &mut i),
            '+' => push(&mut tokens, Token::Plus, &mut i),
            '-' => push(&mut tokens, Token::Minus, &mut i),
            '/' => push(&mut tokens, Token::Slash, &mut i),
            '=' => push(&mut tokens, Token::Eq, &mut i),
            '?' => push(&mut tokens, Token::Question, &mut i),

            '<' => {
                if peek_eq(&chars, i + 1, '=') {
                    tokens.push(Token::LtEq);
                    i += 2;
                } else if peek_eq(&chars, i + 1, '>') {
                    tokens.push(Token::NotEq);
                    i += 2;
                } else {
                    tokens.push(Token::Lt);
                    i += 1;
                }
            }
            '>' => {
                if peek_eq(&chars, i + 1, '=') {
                    tokens.push(Token::GtEq);
                    i += 2;
                } else {
                    tokens.push(Token::Gt);
                    i += 1;
                }
            }
            '!' => {
                if peek_eq(&chars, i + 1, '=') {
                    tokens.push(Token::NotEq);
                    i += 2;
                } else {
                    return Err(Error::parse("'!' must be part of '!=' "));
                }
            }

            '\'' => {
                let (tok, next) = lex_string(&chars, i)?;
                tokens.push(tok);
                i = next;
            }

            c if c.is_ascii_digit() => {
                let (tok, next) = lex_number(&chars, i)?;
                tokens.push(tok);
                i = next;
            }

            c if c.is_alphabetic() || c == '_' => {
                let start = i;
                while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
                let word: String = chars[start..i].iter().collect();
                match Keyword::from_word(&word) {
                    Some(kw) => tokens.push(Token::Keyword(kw)),
                    None => tokens.push(Token::Ident(word)),
                }
            }

            other => {
                return Err(Error::parse(format!("unexpected character {other:?}")));
            }
        }
    }

    Ok(tokens)
}

fn push(tokens: &mut Vec<Token>, tok: Token, i: &mut usize) {
    tokens.push(tok);
    *i += 1;
}

fn peek_eq(chars: &[char], idx: usize, want: char) -> bool {
    chars.get(idx) == Some(&want)
}

/// Lex a `'...'` string literal starting at the opening quote. A doubled `''`
/// is an escaped single quote.
fn lex_string(chars: &[char], start: usize) -> Result<(Token, usize)> {
    let mut i = start + 1; // skip opening quote
    let mut s = String::new();
    loop {
        match chars.get(i) {
            None => return Err(Error::parse("unterminated string literal")),
            Some('\'') => {
                if chars.get(i + 1) == Some(&'\'') {
                    s.push('\'');
                    i += 2;
                } else {
                    return Ok((Token::Str(s), i + 1));
                }
            }
            Some(&ch) => {
                s.push(ch);
                i += 1;
            }
        }
    }
}

/// Lex an integer or real literal starting at a digit.
fn lex_number(chars: &[char], start: usize) -> Result<(Token, usize)> {
    let mut i = start;
    while i < chars.len() && chars[i].is_ascii_digit() {
        i += 1;
    }
    // A real number needs a `.` followed by at least one more digit.
    let is_real =
        chars.get(i) == Some(&'.') && chars.get(i + 1).is_some_and(|d| d.is_ascii_digit());
    if is_real {
        i += 1;
        while i < chars.len() && chars[i].is_ascii_digit() {
            i += 1;
        }
        let text: String = chars[start..i].iter().collect();
        let value: f64 = text
            .parse()
            .map_err(|_| Error::parse(format!("invalid number literal {text:?}")))?;
        Ok((Token::Real(value), i))
    } else {
        let text: String = chars[start..i].iter().collect();
        let value: i64 = text
            .parse()
            .map_err(|_| Error::parse(format!("integer literal {text:?} is out of range")))?;
        Ok((Token::Integer(value), i))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keywords_and_identifiers() {
        let toks = tokenize("SELECT id FROM Users").unwrap();
        assert_eq!(
            toks,
            vec![
                Token::Keyword(Keyword::Select),
                Token::Ident("id".into()),
                Token::Keyword(Keyword::From),
                Token::Ident("Users".into()),
            ]
        );
    }

    #[test]
    fn keywords_are_case_insensitive() {
        assert_eq!(
            tokenize("sElEcT").unwrap(),
            vec![Token::Keyword(Keyword::Select)]
        );
    }

    #[test]
    fn operators() {
        let toks = tokenize("<= >= <> != < > = + - * /").unwrap();
        assert_eq!(
            toks,
            vec![
                Token::LtEq,
                Token::GtEq,
                Token::NotEq,
                Token::NotEq,
                Token::Lt,
                Token::Gt,
                Token::Eq,
                Token::Plus,
                Token::Minus,
                Token::Star,
                Token::Slash,
            ]
        );
    }

    #[test]
    fn numbers() {
        assert_eq!(
            tokenize("0 42 3.5").unwrap(),
            vec![Token::Integer(0), Token::Integer(42), Token::Real(3.5)]
        );
    }

    #[test]
    fn dotted_identifier() {
        assert_eq!(
            tokenize("users.id").unwrap(),
            vec![
                Token::Ident("users".into()),
                Token::Dot,
                Token::Ident("id".into()),
            ]
        );
        // A real literal is still one token, not Integer Dot Integer.
        assert_eq!(tokenize("3.5").unwrap(), vec![Token::Real(3.5)]);
    }

    #[test]
    fn strings_with_escaped_quote() {
        assert_eq!(
            tokenize("'it''s fine'").unwrap(),
            vec![Token::Str("it's fine".into())]
        );
    }

    #[test]
    fn line_comments_are_skipped() {
        let toks = tokenize("a -- this is ignored\n b").unwrap();
        assert_eq!(
            toks,
            vec![Token::Ident("a".into()), Token::Ident("b".into())]
        );
    }

    #[test]
    fn rejects_unterminated_string() {
        assert!(tokenize("'oops").is_err());
    }

    #[test]
    fn rejects_stray_character() {
        assert!(tokenize("a @ b").is_err());
    }
}
