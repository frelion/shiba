//! Safe compiler for the row-local predicate subset.

use pgrx::datum::JsonB;
use pgrx::prelude::*;
use serde_json::json;
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Ident(String),
    Literal(String),
    And,
    Or,
    Not,
    Is,
    Null,
    LParen,
    RParen,
    Compare(String),
}

struct Parser {
    tokens: Vec<Token>,
    position: usize,
    aliases: BTreeSet<String>,
}

impl Parser {
    fn parse(mut self) -> Result<(String, Vec<String>), String> {
        let sql = self.parse_or()?;
        if self.position != self.tokens.len() {
            return Err("unexpected token after filter expression".into());
        }
        Ok((sql, self.aliases.into_iter().collect()))
    }

    fn parse_or(&mut self) -> Result<String, String> {
        let mut sql = self.parse_and()?;
        while self.consume(&Token::Or) {
            sql = format!("({sql} OR {})", self.parse_and()?);
        }
        Ok(sql)
    }

    fn parse_and(&mut self) -> Result<String, String> {
        let mut sql = self.parse_not()?;
        while self.consume(&Token::And) {
            sql = format!("({sql} AND {})", self.parse_not()?);
        }
        Ok(sql)
    }

    fn parse_not(&mut self) -> Result<String, String> {
        if self.consume(&Token::Not) {
            return Ok(format!("(NOT {})", self.parse_not()?));
        }
        if self.consume(&Token::LParen) {
            let sql = self.parse_or()?;
            if !self.consume(&Token::RParen) {
                return Err("missing ')' in filter expression".into());
            }
            return Ok(format!("({sql})"));
        }
        self.parse_comparison()
    }

    fn parse_comparison(&mut self) -> Result<String, String> {
        let first = self.take_ident()?;
        let (alias, column) = if self.peek_ident_after_dot() {
            self.position += 1; // the lexer represents '.' as Compare(".")
            (Some(first), self.take_ident()?)
        } else {
            (None, first)
        };
        if let Some(alias) = alias {
            self.aliases.insert(alias);
        }
        let column = quote_identifier(&column);
        if self.consume(&Token::Is) {
            let negated = self.consume(&Token::Not);
            if !self.consume(&Token::Null) {
                return Err("IS only supports NULL in a Shiba filter".into());
            }
            return Ok(format!(
                "(input.row).{column} IS {}NULL",
                if negated { "NOT " } else { "" }
            ));
        }
        let operator = match self.tokens.get(self.position) {
            Some(Token::Compare(operator)) if operator != "." => operator.clone(),
            _ => return Err("expected a comparison operator".into()),
        };
        self.position += 1;
        let literal = match self.tokens.get(self.position) {
            Some(Token::Literal(literal)) => literal.clone(),
            Some(Token::Null) => {
                return Err("use IS NULL or IS NOT NULL instead of comparing with NULL".into())
            }
            _ => return Err("expected a numeric, boolean, or quoted string literal".into()),
        };
        self.position += 1;
        Ok(format!("(input.row).{column} {operator} {literal}"))
    }

    fn peek_ident_after_dot(&self) -> bool {
        matches!(self.tokens.get(self.position), Some(Token::Compare(dot)) if dot == ".")
            && matches!(self.tokens.get(self.position + 1), Some(Token::Ident(_)))
    }

    fn take_ident(&mut self) -> Result<String, String> {
        match self.tokens.get(self.position) {
            Some(Token::Ident(identifier)) => {
                self.position += 1;
                Ok(identifier.clone())
            }
            _ => Err("expected a column reference".into()),
        }
    }

    fn consume(&mut self, expected: &Token) -> bool {
        if self.tokens.get(self.position) == Some(expected) {
            self.position += 1;
            true
        } else {
            false
        }
    }
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn tokenize(input: &str) -> Result<Vec<Token>, String> {
    let chars: Vec<char> = input.chars().collect();
    let mut tokens = Vec::new();
    let mut index = 0;
    while index < chars.len() {
        if chars[index].is_whitespace() {
            index += 1;
            continue;
        }
        match chars[index] {
            '(' => {
                tokens.push(Token::LParen);
                index += 1;
            }
            ')' => {
                tokens.push(Token::RParen);
                index += 1;
            }
            '.' => {
                tokens.push(Token::Compare(".".into()));
                index += 1;
            }
            '=' | '<' | '>' => {
                let start = index;
                index += 1;
                if index < chars.len()
                    && matches!(
                        (chars[start], chars[index]),
                        ('<', '=') | ('>', '=') | ('<', '>')
                    )
                {
                    index += 1;
                }
                tokens.push(Token::Compare(chars[start..index].iter().collect()));
            }
            '\'' => {
                let mut value = String::from("'");
                index += 1;
                loop {
                    if index >= chars.len() {
                        return Err("unterminated quoted string in filter".into());
                    }
                    value.push(chars[index]);
                    if chars[index] == '\'' {
                        if index + 1 < chars.len() && chars[index + 1] == '\'' {
                            value.push('\'');
                            index += 2;
                            continue;
                        }
                        index += 1;
                        break;
                    }
                    index += 1;
                }
                tokens.push(Token::Literal(value));
            }
            '+' | '-' | '0'..='9' => {
                let start = index;
                if matches!(chars[index], '+' | '-') {
                    index += 1;
                }
                let mut digits = 0;
                while index < chars.len() && chars[index].is_ascii_digit() {
                    digits += 1;
                    index += 1;
                }
                if index < chars.len() && chars[index] == '.' {
                    index += 1;
                    while index < chars.len() && chars[index].is_ascii_digit() {
                        digits += 1;
                        index += 1;
                    }
                }
                if digits == 0 {
                    return Err("invalid numeric literal in filter".into());
                }
                tokens.push(Token::Literal(chars[start..index].iter().collect()));
            }
            character if character.is_ascii_alphabetic() || character == '_' => {
                let start = index;
                index += 1;
                while index < chars.len()
                    && (chars[index].is_ascii_alphanumeric() || chars[index] == '_')
                {
                    index += 1;
                }
                let word: String = chars[start..index].iter().collect();
                tokens.push(match word.to_ascii_lowercase().as_str() {
                    "and" => Token::And,
                    "or" => Token::Or,
                    "not" => Token::Not,
                    "is" => Token::Is,
                    "null" => Token::Null,
                    "true" | "false" => Token::Literal(word.to_ascii_lowercase()),
                    _ => Token::Ident(word),
                });
            }
            _ => {
                return Err(format!(
                    "unsupported character '{}' in filter",
                    chars[index]
                ))
            }
        }
    }
    Ok(tokens)
}

fn compile(expression: &str) -> Result<(String, Vec<String>), String> {
    Parser {
        tokens: tokenize(expression)?,
        position: 0,
        aliases: BTreeSet::new(),
    }
    .parse()
}

#[pg_extern]
pub fn compile_filter_expression(expression: &str) -> JsonB {
    match compile(expression) {
        Ok((sql, aliases)) => JsonB(json!({ "sql": sql, "aliases": aliases })),
        Err(message) => error!("invalid Shiba filter: {message}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compiles_boolean_expression() {
        let (sql, aliases) =
            compile("s.amount >= 20 AND (s.item_id = 1 OR NOT s.amount < 100)").unwrap();
        assert!(sql.contains("AND"));
        assert!(sql.contains("OR"));
        assert_eq!(aliases, vec!["s"]);
    }

    #[test]
    fn rejects_function_calls() {
        assert!(compile("pg_sleep(1) = 1").is_err());
    }

    #[test]
    fn compiles_null_test() {
        let (sql, _) = compile("amount IS NOT NULL").unwrap();
        assert!(sql.ends_with("IS NOT NULL"));
    }
}
