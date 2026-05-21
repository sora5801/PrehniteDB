//! A hand-written recursive-descent parser: token stream into a [`Statement`].
//!
//! Expression precedence, loosest to tightest:
//!
//! ```text
//!   OR  <  AND  <  NOT  <  comparisons  <  + -  <  * /  <  unary -  <  primary
//! ```
//!
//! `IS [NOT] NULL` binds as a postfix on a primary.

use crate::error::{Error, Result};
use crate::sql::ast::{
    Aggregate, AggregateArg, AggregateFunc, BinaryOp, ColumnDef, Expr, OrderKey, Projection,
    SelectItem, Statement, TypeName, UnaryOp,
};
use crate::sql::lexer::tokenize;
use crate::sql::token::{Keyword, Token};

/// Parse exactly one SQL statement. A single trailing `;` is tolerated.
pub fn parse(input: &str) -> Result<Statement> {
    let tokens = tokenize(input)?;
    let mut parser = Parser { tokens, pos: 0 };
    let statement = parser.statement()?;
    if parser.peek() == Some(&Token::Semicolon) {
        parser.pos += 1;
    }
    if let Some(extra) = parser.peek() {
        return Err(Error::parse(format!(
            "unexpected input after statement: {extra:?}"
        )));
    }
    Ok(statement)
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn advance(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.pos).cloned();
        if token.is_some() {
            self.pos += 1;
        }
        token
    }

    fn at_keyword(&self, kw: Keyword) -> bool {
        matches!(self.peek(), Some(Token::Keyword(k)) if *k == kw)
    }

    /// Consume `want` or fail.
    fn expect(&mut self, want: &Token) -> Result<()> {
        match self.peek() {
            Some(found) if found == want => {
                self.pos += 1;
                Ok(())
            }
            found => Err(Error::parse(format!("expected {want:?}, found {found:?}"))),
        }
    }

    /// Consume keyword `kw` or fail.
    fn expect_keyword(&mut self, kw: Keyword) -> Result<()> {
        match self.peek() {
            Some(Token::Keyword(k)) if *k == kw => {
                self.pos += 1;
                Ok(())
            }
            found => Err(Error::parse(format!(
                "expected keyword {kw:?}, found {found:?}"
            ))),
        }
    }

    /// Consume an identifier (a table or column name) or fail.
    fn expect_name(&mut self) -> Result<String> {
        match self.advance() {
            Some(Token::Ident(name)) => Ok(name),
            found => Err(Error::parse(format!("expected a name, found {found:?}"))),
        }
    }

    fn statement(&mut self) -> Result<Statement> {
        match self.peek() {
            Some(Token::Keyword(Keyword::Select)) => self.select(),
            Some(Token::Keyword(Keyword::Insert)) => self.insert(),
            Some(Token::Keyword(Keyword::Create)) => self.create(),
            Some(Token::Keyword(Keyword::Drop)) => self.drop_statement(),
            Some(Token::Keyword(Keyword::Update)) => self.update(),
            Some(Token::Keyword(Keyword::Delete)) => self.delete(),
            Some(Token::Keyword(Keyword::Vacuum)) => {
                self.pos += 1;
                Ok(Statement::Vacuum)
            }
            Some(found) => Err(Error::parse(format!(
                "expected the start of a statement, found {found:?}"
            ))),
            None => Err(Error::parse("empty statement")),
        }
    }

    /// `CREATE` introduces either a table or an index.
    fn create(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Create)?;
        if self.at_keyword(Keyword::Table) {
            self.create_table()
        } else if self.at_keyword(Keyword::Index) {
            self.create_index()
        } else {
            Err(Error::parse(format!(
                "expected TABLE or INDEX after CREATE, found {:?}",
                self.peek()
            )))
        }
    }

    fn create_table(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Table)?;
        let name = self.expect_name()?;
        self.expect(&Token::LParen)?;
        let mut columns = Vec::new();
        loop {
            let col = self.expect_name()?;
            let ty = self.type_name()?;
            columns.push(ColumnDef { name: col, ty });
            match self.advance() {
                Some(Token::Comma) => continue,
                Some(Token::RParen) => break,
                found => {
                    return Err(Error::parse(format!(
                        "expected ',' or ')' in column list, found {found:?}"
                    )))
                }
            }
        }
        Ok(Statement::CreateTable { name, columns })
    }

    fn create_index(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Index)?;
        let name = self.expect_name()?;
        self.expect_keyword(Keyword::On)?;
        let table = self.expect_name()?;
        self.expect(&Token::LParen)?;
        let mut columns = Vec::new();
        loop {
            columns.push(self.expect_name()?);
            match self.advance() {
                Some(Token::Comma) => continue,
                Some(Token::RParen) => break,
                found => {
                    return Err(Error::parse(format!(
                        "expected ',' or ')' in column list, found {found:?}"
                    )))
                }
            }
        }
        Ok(Statement::CreateIndex {
            name,
            table,
            columns,
        })
    }

    fn type_name(&mut self) -> Result<TypeName> {
        match self.advance() {
            Some(Token::Keyword(Keyword::Int | Keyword::Integer)) => Ok(TypeName::Int),
            Some(Token::Keyword(Keyword::Text)) => Ok(TypeName::Text),
            Some(Token::Keyword(Keyword::Real | Keyword::Float)) => Ok(TypeName::Real),
            Some(Token::Keyword(Keyword::Bool | Keyword::Boolean)) => Ok(TypeName::Bool),
            found => Err(Error::parse(format!(
                "expected a column type (INT, TEXT, REAL, BOOL), found {found:?}"
            ))),
        }
    }

    /// `DROP` removes either a table or an index.
    fn drop_statement(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Drop)?;
        if self.at_keyword(Keyword::Table) {
            self.expect_keyword(Keyword::Table)?;
            Ok(Statement::DropTable {
                name: self.expect_name()?,
            })
        } else if self.at_keyword(Keyword::Index) {
            self.expect_keyword(Keyword::Index)?;
            Ok(Statement::DropIndex {
                name: self.expect_name()?,
            })
        } else {
            Err(Error::parse(format!(
                "expected TABLE or INDEX after DROP, found {:?}",
                self.peek()
            )))
        }
    }

    fn insert(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Insert)?;
        self.expect_keyword(Keyword::Into)?;
        let table = self.expect_name()?;

        let columns = if self.peek() == Some(&Token::LParen) {
            self.pos += 1;
            let mut names = Vec::new();
            loop {
                names.push(self.expect_name()?);
                match self.advance() {
                    Some(Token::Comma) => continue,
                    Some(Token::RParen) => break,
                    found => {
                        return Err(Error::parse(format!(
                            "expected ',' or ')' in column list, found {found:?}"
                        )))
                    }
                }
            }
            Some(names)
        } else {
            None
        };

        self.expect_keyword(Keyword::Values)?;
        let mut rows = Vec::new();
        loop {
            self.expect(&Token::LParen)?;
            let mut row = Vec::new();
            loop {
                row.push(self.expr()?);
                match self.advance() {
                    Some(Token::Comma) => continue,
                    Some(Token::RParen) => break,
                    found => {
                        return Err(Error::parse(format!(
                            "expected ',' or ')' in value list, found {found:?}"
                        )))
                    }
                }
            }
            rows.push(row);
            if self.peek() == Some(&Token::Comma) {
                self.pos += 1;
            } else {
                break;
            }
        }
        Ok(Statement::Insert {
            table,
            columns,
            rows,
        })
    }

    fn select(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Select)?;
        let projection = self.projection()?;
        self.expect_keyword(Keyword::From)?;
        let table = self.expect_name()?;
        let filter = self.optional_where()?;
        let group_by = self.optional_group_by()?;
        let having = self.optional_having()?;
        let order_by = self.optional_order_by()?;
        Ok(Statement::Select {
            table,
            projection,
            filter,
            group_by,
            having,
            order_by,
        })
    }

    /// A `SELECT` projection: `*`, or a list of items each of which is a plain
    /// column or an aggregate call. Whether a mix is meaningful (it needs
    /// `GROUP BY`) is the executor's call, not the parser's.
    fn projection(&mut self) -> Result<Projection> {
        if self.peek() == Some(&Token::Star) {
            self.pos += 1;
            return Ok(Projection::All);
        }
        let mut items = Vec::new();
        loop {
            let name = self.expect_name()?;
            // A name followed by `(` is an aggregate call; otherwise a column.
            if self.peek() == Some(&Token::LParen) {
                items.push(SelectItem::Aggregate(self.parse_aggregate_call(&name)?));
            } else {
                items.push(SelectItem::Column(name));
            }
            if self.peek() == Some(&Token::Comma) {
                self.pos += 1;
            } else {
                break;
            }
        }
        Ok(Projection::Items(items))
    }

    /// Parse the `(arg)` of an aggregate call whose name has just been read.
    fn parse_aggregate_call(&mut self, name: &str) -> Result<Aggregate> {
        let func = aggregate_func(name)?;
        self.expect(&Token::LParen)?;
        let arg = if self.peek() == Some(&Token::Star) {
            self.pos += 1;
            AggregateArg::Star
        } else {
            AggregateArg::Column(self.expect_name()?)
        };
        self.expect(&Token::RParen)?;
        Ok(Aggregate { func, arg })
    }

    /// An optional `GROUP BY col, ...` clause.
    fn optional_group_by(&mut self) -> Result<Vec<String>> {
        if !self.at_keyword(Keyword::Group) {
            return Ok(Vec::new());
        }
        self.pos += 1;
        self.expect_keyword(Keyword::By)?;
        let mut columns = Vec::new();
        loop {
            columns.push(self.expect_name()?);
            if self.peek() == Some(&Token::Comma) {
                self.pos += 1;
            } else {
                break;
            }
        }
        Ok(columns)
    }

    /// An optional `HAVING <expr>` clause — a predicate over each group.
    fn optional_having(&mut self) -> Result<Option<Expr>> {
        if self.at_keyword(Keyword::Having) {
            self.pos += 1;
            Ok(Some(self.expr()?))
        } else {
            Ok(None)
        }
    }

    /// An optional `ORDER BY col [ASC|DESC], ...` clause.
    fn optional_order_by(&mut self) -> Result<Vec<OrderKey>> {
        if !self.at_keyword(Keyword::Order) {
            return Ok(Vec::new());
        }
        self.pos += 1;
        self.expect_keyword(Keyword::By)?;
        let mut keys = Vec::new();
        loop {
            let column = self.expect_name()?;
            let mut descending = false;
            if self.at_keyword(Keyword::Desc) {
                self.pos += 1;
                descending = true;
            } else if self.at_keyword(Keyword::Asc) {
                self.pos += 1;
            }
            keys.push(OrderKey { column, descending });
            if self.peek() == Some(&Token::Comma) {
                self.pos += 1;
            } else {
                break;
            }
        }
        Ok(keys)
    }

    fn update(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Update)?;
        let table = self.expect_name()?;
        self.expect_keyword(Keyword::Set)?;
        let mut assignments = Vec::new();
        loop {
            let column = self.expect_name()?;
            self.expect(&Token::Eq)?;
            let value = self.expr()?;
            assignments.push((column, value));
            if self.peek() == Some(&Token::Comma) {
                self.pos += 1;
            } else {
                break;
            }
        }
        let filter = self.optional_where()?;
        Ok(Statement::Update {
            table,
            assignments,
            filter,
        })
    }

    fn delete(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Delete)?;
        self.expect_keyword(Keyword::From)?;
        let table = self.expect_name()?;
        let filter = self.optional_where()?;
        Ok(Statement::Delete { table, filter })
    }

    fn optional_where(&mut self) -> Result<Option<Expr>> {
        if self.at_keyword(Keyword::Where) {
            self.pos += 1;
            Ok(Some(self.expr()?))
        } else {
            Ok(None)
        }
    }

    // --- expression grammar, loosest binding first ----------------------

    fn expr(&mut self) -> Result<Expr> {
        self.or_expr()
    }

    fn or_expr(&mut self) -> Result<Expr> {
        let mut left = self.and_expr()?;
        while self.at_keyword(Keyword::Or) {
            self.pos += 1;
            let right = self.and_expr()?;
            left = binary(BinaryOp::Or, left, right);
        }
        Ok(left)
    }

    fn and_expr(&mut self) -> Result<Expr> {
        let mut left = self.not_expr()?;
        while self.at_keyword(Keyword::And) {
            self.pos += 1;
            let right = self.not_expr()?;
            left = binary(BinaryOp::And, left, right);
        }
        Ok(left)
    }

    fn not_expr(&mut self) -> Result<Expr> {
        if self.at_keyword(Keyword::Not) {
            self.pos += 1;
            Ok(Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(self.not_expr()?),
            })
        } else {
            self.comparison()
        }
    }

    fn comparison(&mut self) -> Result<Expr> {
        let mut left = self.additive()?;
        while let Some(op) = self.peek().and_then(comparison_op) {
            self.pos += 1;
            let right = self.additive()?;
            left = binary(op, left, right);
        }
        Ok(left)
    }

    fn additive(&mut self) -> Result<Expr> {
        let mut left = self.multiplicative()?;
        loop {
            let op = match self.peek() {
                Some(Token::Plus) => BinaryOp::Add,
                Some(Token::Minus) => BinaryOp::Sub,
                _ => break,
            };
            self.pos += 1;
            let right = self.multiplicative()?;
            left = binary(op, left, right);
        }
        Ok(left)
    }

    fn multiplicative(&mut self) -> Result<Expr> {
        let mut left = self.unary()?;
        loop {
            let op = match self.peek() {
                Some(Token::Star) => BinaryOp::Mul,
                Some(Token::Slash) => BinaryOp::Div,
                _ => break,
            };
            self.pos += 1;
            let right = self.unary()?;
            left = binary(op, left, right);
        }
        Ok(left)
    }

    fn unary(&mut self) -> Result<Expr> {
        if self.peek() == Some(&Token::Minus) {
            self.pos += 1;
            Ok(Expr::Unary {
                op: UnaryOp::Neg,
                expr: Box::new(self.unary()?),
            })
        } else {
            self.postfix()
        }
    }

    fn postfix(&mut self) -> Result<Expr> {
        let inner = self.primary()?;
        if self.at_keyword(Keyword::Is) {
            self.pos += 1;
            let negated = if self.at_keyword(Keyword::Not) {
                self.pos += 1;
                true
            } else {
                false
            };
            self.expect_keyword(Keyword::Null)?;
            Ok(Expr::IsNull {
                expr: Box::new(inner),
                negated,
            })
        } else {
            Ok(inner)
        }
    }

    fn primary(&mut self) -> Result<Expr> {
        match self.advance() {
            Some(Token::Integer(n)) => Ok(Expr::Integer(n)),
            Some(Token::Real(r)) => Ok(Expr::Real(r)),
            Some(Token::Str(s)) => Ok(Expr::Str(s)),
            Some(Token::Keyword(Keyword::True)) => Ok(Expr::Bool(true)),
            Some(Token::Keyword(Keyword::False)) => Ok(Expr::Bool(false)),
            Some(Token::Keyword(Keyword::Null)) => Ok(Expr::Null),
            Some(Token::Ident(name)) => {
                // A name followed by `(` is an aggregate call (valid in HAVING).
                if self.peek() == Some(&Token::LParen) {
                    Ok(Expr::Aggregate(self.parse_aggregate_call(&name)?))
                } else {
                    Ok(Expr::Column(name))
                }
            }
            Some(Token::LParen) => {
                let inner = self.expr()?;
                self.expect(&Token::RParen)?;
                Ok(inner)
            }
            found => Err(Error::parse(format!(
                "expected an expression, found {found:?}"
            ))),
        }
    }
}

fn binary(op: BinaryOp, left: Expr, right: Expr) -> Expr {
    Expr::Binary {
        op,
        left: Box::new(left),
        right: Box::new(right),
    }
}

fn comparison_op(token: &Token) -> Option<BinaryOp> {
    Some(match token {
        Token::Eq => BinaryOp::Eq,
        Token::NotEq => BinaryOp::NotEq,
        Token::Lt => BinaryOp::Lt,
        Token::LtEq => BinaryOp::LtEq,
        Token::Gt => BinaryOp::Gt,
        Token::GtEq => BinaryOp::GtEq,
        _ => return None,
    })
}

/// Resolve an aggregate function name (case-insensitively).
fn aggregate_func(name: &str) -> Result<AggregateFunc> {
    Ok(match name.to_ascii_uppercase().as_str() {
        "COUNT" => AggregateFunc::Count,
        "SUM" => AggregateFunc::Sum,
        "AVG" => AggregateFunc::Avg,
        "MIN" => AggregateFunc::Min,
        "MAX" => AggregateFunc::Max,
        _ => return Err(Error::parse(format!("unknown function '{name}'"))),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_star() {
        assert_eq!(
            parse("SELECT * FROM users").unwrap(),
            Statement::Select {
                table: "users".into(),
                projection: Projection::All,
                filter: None,
                group_by: vec![],
                having: None,
                order_by: vec![],
            }
        );
    }

    #[test]
    fn select_columns_with_filter() {
        let stmt = parse("SELECT a, b FROM t WHERE a >= 1 AND b <> 2;").unwrap();
        match stmt {
            Statement::Select {
                table,
                projection,
                filter,
                ..
            } => {
                assert_eq!(table, "t");
                assert_eq!(
                    projection,
                    Projection::Items(vec![
                        SelectItem::Column("a".into()),
                        SelectItem::Column("b".into()),
                    ])
                );
                assert!(matches!(
                    filter,
                    Some(Expr::Binary {
                        op: BinaryOp::And,
                        ..
                    })
                ));
            }
            other => panic!("expected SELECT, got {other:?}"),
        }
    }

    #[test]
    fn order_by_clause() {
        let Statement::Select { order_by, .. } =
            parse("SELECT a FROM t ORDER BY b DESC, c").unwrap()
        else {
            panic!("expected a SELECT");
        };
        assert_eq!(
            order_by,
            vec![
                OrderKey {
                    column: "b".into(),
                    descending: true,
                },
                OrderKey {
                    column: "c".into(),
                    descending: false,
                },
            ]
        );
    }

    #[test]
    fn select_items_and_group_by() {
        let Statement::Select {
            projection,
            group_by,
            ..
        } = parse("SELECT region, COUNT(*) FROM t GROUP BY region").unwrap()
        else {
            panic!("expected a SELECT");
        };
        assert_eq!(
            projection,
            Projection::Items(vec![
                SelectItem::Column("region".into()),
                SelectItem::Aggregate(Aggregate {
                    func: AggregateFunc::Count,
                    arg: AggregateArg::Star,
                }),
            ])
        );
        assert_eq!(group_by, vec!["region".to_string()]);
        // Mixing columns and aggregates now parses (GROUP BY makes it
        // meaningful); the executor enforces the semantic rule.
        assert!(parse("SELECT a, COUNT(*) FROM t").is_ok());
        // An unknown function is still rejected.
        assert!(parse("SELECT frob(x) FROM t").is_err());
    }

    #[test]
    fn create_table_all_types() {
        let stmt = parse("CREATE TABLE t (id INT, name TEXT, score REAL, ok BOOL)").unwrap();
        match stmt {
            Statement::CreateTable { name, columns } => {
                assert_eq!(name, "t");
                assert_eq!(columns.len(), 4);
                assert_eq!(columns[0].ty, TypeName::Int);
                assert_eq!(columns[2].ty, TypeName::Real);
            }
            other => panic!("expected CREATE TABLE, got {other:?}"),
        }
    }

    #[test]
    fn insert_multiple_rows() {
        let stmt = parse("INSERT INTO t (a, b) VALUES (1, 'x'), (2, 'y')").unwrap();
        match stmt {
            Statement::Insert {
                table,
                columns,
                rows,
            } => {
                assert_eq!(table, "t");
                assert_eq!(columns, Some(vec!["a".into(), "b".into()]));
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[1][0], Expr::Integer(2));
            }
            other => panic!("expected INSERT, got {other:?}"),
        }
    }

    #[test]
    fn update_and_delete() {
        assert!(matches!(
            parse("UPDATE t SET x = 1, y = x + 1 WHERE id = 5").unwrap(),
            Statement::Update { .. }
        ));
        assert!(matches!(
            parse("DELETE FROM t WHERE id = 1").unwrap(),
            Statement::Delete { .. }
        ));
        assert!(matches!(
            parse("DROP TABLE t").unwrap(),
            Statement::DropTable { .. }
        ));
    }

    #[test]
    fn index_statements() {
        assert_eq!(
            parse("CREATE INDEX idx_email ON users (email)").unwrap(),
            Statement::CreateIndex {
                name: "idx_email".into(),
                table: "users".into(),
                columns: vec!["email".into()],
            }
        );
        assert_eq!(
            parse("CREATE INDEX combo ON t (a, b, c)").unwrap(),
            Statement::CreateIndex {
                name: "combo".into(),
                table: "t".into(),
                columns: vec!["a".into(), "b".into(), "c".into()],
            }
        );
        assert_eq!(
            parse("DROP INDEX idx_email").unwrap(),
            Statement::DropIndex {
                name: "idx_email".into(),
            }
        );
        assert!(parse("CREATE FROB x").is_err());
    }

    #[test]
    fn arithmetic_precedence() {
        // 1 + 2 * 3  must parse as  1 + (2 * 3)
        let stmt = parse("SELECT * FROM t WHERE x = 1 + 2 * 3").unwrap();
        let Statement::Select {
            filter: Some(Expr::Binary { right, .. }),
            ..
        } = stmt
        else {
            panic!("expected a filtered SELECT");
        };
        assert_eq!(
            *right,
            binary(
                BinaryOp::Add,
                Expr::Integer(1),
                binary(BinaryOp::Mul, Expr::Integer(2), Expr::Integer(3)),
            )
        );
    }

    #[test]
    fn is_null_postfix() {
        let stmt = parse("SELECT * FROM t WHERE name IS NOT NULL").unwrap();
        let Statement::Select {
            filter: Some(f), ..
        } = stmt
        else {
            panic!("expected a filtered SELECT");
        };
        assert_eq!(
            f,
            Expr::IsNull {
                expr: Box::new(Expr::Column("name".into())),
                negated: true,
            }
        );
    }

    #[test]
    fn rejects_garbage_and_truncation() {
        assert!(parse("SELECT").is_err());
        assert!(parse("SELECT * FROM").is_err());
        assert!(parse("wat").is_err());
        assert!(parse("SELECT * FROM t extra").is_err());
    }
}
