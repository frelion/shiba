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
            // PostgreSQL folds unquoted identifiers to lowercase. Treat aliases
            // with different source casing as the same input identity.
            self.aliases.insert(alias.to_ascii_lowercase());
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

    fn compiled(expression: &str) -> (String, Vec<String>) {
        compile(expression)
            .unwrap_or_else(|message| panic!("expected {expression:?} to compile, got {message:?}"))
    }

    fn rejected(expression: &str, expected_message: &str) {
        let message =
            compile(expression).expect_err(&format!("expected {expression:?} to be rejected"));
        assert_eq!(message, expected_message, "expression: {expression:?}");
    }

    #[test]
    fn tokenizer_recognizes_keywords_case_insensitively() {
        assert_eq!(
            tokenize("AnD or NOT Is NuLl TRUE false identifier"),
            Ok(vec![
                Token::And,
                Token::Or,
                Token::Not,
                Token::Is,
                Token::Null,
                Token::Literal("true".into()),
                Token::Literal("false".into()),
                Token::Ident("identifier".into()),
            ])
        );
    }

    #[test]
    fn tokenizer_recognizes_punctuation_and_all_supported_operators() {
        assert_eq!(
            tokenize("(a.b = 1) <> 2 <= 3 >= 4 < 5 > 6"),
            Ok(vec![
                Token::LParen,
                Token::Ident("a".into()),
                Token::Compare(".".into()),
                Token::Ident("b".into()),
                Token::Compare("=".into()),
                Token::Literal("1".into()),
                Token::RParen,
                Token::Compare("<>".into()),
                Token::Literal("2".into()),
                Token::Compare("<=".into()),
                Token::Literal("3".into()),
                Token::Compare(">=".into()),
                Token::Literal("4".into()),
                Token::Compare("<".into()),
                Token::Literal("5".into()),
                Token::Compare(">".into()),
                Token::Literal("6".into()),
            ])
        );
    }

    #[test]
    fn tokenizer_preserves_quoted_strings_and_escaped_quotes() {
        assert_eq!(
            tokenize("'plain' 'O''Reilly' '''' ''"),
            Ok(vec![
                Token::Literal("'plain'".into()),
                Token::Literal("'O''Reilly'".into()),
                Token::Literal("''''".into()),
                Token::Literal("''".into()),
            ])
        );
    }

    #[test]
    fn tokenizer_accepts_numeric_boundaries() {
        assert_eq!(
            tokenize("0 +1 -1 1.25 +.5 -.5 1."),
            Ok(vec![
                Token::Literal("0".into()),
                Token::Literal("+1".into()),
                Token::Literal("-1".into()),
                Token::Literal("1.25".into()),
                Token::Literal("+.5".into()),
                Token::Literal("-.5".into()),
                Token::Literal("1.".into()),
            ])
        );
    }

    #[test]
    fn tokenizer_ignores_all_whitespace_between_tokens() {
        assert_eq!(
            tokenize("\t\n amount \r\n = \u{2003} 1 "),
            Ok(vec![
                Token::Ident("amount".into()),
                Token::Compare("=".into()),
                Token::Literal("1".into()),
            ])
        );
    }

    #[test]
    fn tokenizer_rejects_unterminated_strings_and_invalid_numbers() {
        assert_eq!(
            tokenize("'unterminated"),
            Err("unterminated quoted string in filter".into())
        );
        for expression in ["+", "-", "+.", "-."] {
            assert_eq!(
                tokenize(expression),
                Err("invalid numeric literal in filter".into()),
                "expression: {expression:?}"
            );
        }
    }

    #[test]
    fn tokenizer_rejects_every_sql_punctuation_not_in_the_grammar() {
        for character in [
            ';', ',', '!', '"', '`', '$', ':', '\\', '/', '*', '[', ']', '{', '}', '@',
        ] {
            let expression = character.to_string();
            assert_eq!(
                tokenize(&expression),
                Err(format!("unsupported character '{character}' in filter")),
                "character: {character:?}"
            );
        }
    }

    #[test]
    fn boolean_precedence_is_not_then_and_then_or() {
        let (sql, aliases) = compiled("NOT a = 1 OR b = 2 AND NOT c = 3");
        assert_eq!(
            sql,
            "((NOT (input.row).\"a\" = 1) OR ((input.row).\"b\" = 2 AND (NOT (input.row).\"c\" = 3)))"
        );
        assert!(aliases.is_empty());
    }

    #[test]
    fn repeated_boolean_operators_are_left_associative() {
        assert_eq!(
            compiled("a = 1 AND b = 2 AND c = 3").0,
            "(((input.row).\"a\" = 1 AND (input.row).\"b\" = 2) AND (input.row).\"c\" = 3)"
        );
        assert_eq!(
            compiled("a = 1 OR b = 2 OR c = 3").0,
            "(((input.row).\"a\" = 1 OR (input.row).\"b\" = 2) OR (input.row).\"c\" = 3)"
        );
    }

    #[test]
    fn parentheses_override_precedence_and_can_be_nested() {
        assert_eq!(
            compiled("((a = 1 OR b = 2) AND c = 3)").0,
            "(((((input.row).\"a\" = 1 OR (input.row).\"b\" = 2)) AND (input.row).\"c\" = 3))"
        );
    }

    #[test]
    fn not_is_right_associative() {
        assert_eq!(
            compiled("NOT NOT a = true").0,
            "(NOT (NOT (input.row).\"a\" = true))"
        );
    }

    #[test]
    fn compiles_every_comparison_operator_and_literal_kind() {
        let cases = [
            ("a = 0", "(input.row).\"a\" = 0"),
            ("a <> -1", "(input.row).\"a\" <> -1"),
            ("a < +1.5", "(input.row).\"a\" < +1.5"),
            ("a <= 0.5", "(input.row).\"a\" <= 0.5"),
            ("a > true", "(input.row).\"a\" > true"),
            ("a >= 'x'", "(input.row).\"a\" >= 'x'"),
            ("a = FALSE", "(input.row).\"a\" = false"),
        ];
        for (expression, expected) in cases {
            assert_eq!(
                compiled(expression).0,
                expected,
                "expression: {expression:?}"
            );
        }
    }

    #[test]
    fn compiles_null_tests_but_not_null_comparisons() {
        assert_eq!(
            compiled("amount IS NULL").0,
            "(input.row).\"amount\" IS NULL"
        );
        assert_eq!(
            compiled("amount is not null").0,
            "(input.row).\"amount\" IS NOT NULL"
        );
        for expression in ["amount = NULL", "amount <> null"] {
            rejected(
                expression,
                "use IS NULL or IS NOT NULL instead of comparing with NULL",
            );
        }
        for expression in ["amount IS 1", "amount IS NOT true", "amount IS"] {
            rejected(expression, "IS only supports NULL in a Shiba filter");
        }
    }

    #[test]
    fn aliases_are_deduplicated_and_returned_in_sorted_order() {
        let (_, aliases) = compiled(
            "z.amount = 1 AND a.id = 2 AND z.other = 3 AND middle.enabled = true AND a.id <> 4",
        );
        assert_eq!(aliases, vec!["a", "middle", "z"]);
        assert!(compiled("unqualified = 1").1.is_empty());
    }

    #[test]
    fn aliases_follow_postgresql_unquoted_case_folding() {
        let (_, aliases) = compiled("Source.a = 1 AND source.b = 2 AND SOURCE.c = 3");
        assert_eq!(aliases, vec!["source"]);
    }

    #[test]
    fn preserves_identifier_case_and_quotes_generated_column_names() {
        assert_eq!(
            compiled("Alias.Mixed_Case = 1").0,
            "(input.row).\"Mixed_Case\" = 1"
        );
        assert_eq!(compiled("_private = 1").0, "(input.row).\"_private\" = 1");
    }

    #[test]
    fn quoted_literal_cannot_escape_into_generated_sql() {
        let payload = "name = '''; DROP TABLE users; --'";
        let (sql, aliases) = compiled(payload);
        assert_eq!(sql, "(input.row).\"name\" = '''; DROP TABLE users; --'");
        assert!(aliases.is_empty());
    }

    #[test]
    fn rejects_common_sql_injection_shapes_outside_a_literal() {
        let cases = [
            (
                "a = 1; DROP TABLE users",
                "unsupported character ';' in filter",
            ),
            ("a = 1 -- comment", "invalid numeric literal in filter"),
            ("a = 1 /* comment */", "unsupported character '/' in filter"),
            ("a = 1 OR 1 = 1", "expected a column reference"),
            ("pg_sleep(1) = 1", "expected a comparison operator"),
            ("a::text = 'x'", "unsupported character ':' in filter"),
            ("a = $1", "unsupported character '$' in filter"),
            (
                "a = 1 UNION SELECT secret",
                "unexpected token after filter expression",
            ),
        ];
        for (expression, message) in cases {
            rejected(expression, message);
        }
    }

    #[test]
    fn rejects_empty_and_missing_operands() {
        let cases = [
            ("", "expected a column reference"),
            ("NOT", "expected a column reference"),
            ("()", "expected a column reference"),
            ("AND a = 1", "expected a column reference"),
            ("OR a = 1", "expected a column reference"),
            ("a", "expected a comparison operator"),
            (
                "a =",
                "expected a numeric, boolean, or quoted string literal",
            ),
            (
                "a = AND",
                "expected a numeric, boolean, or quoted string literal",
            ),
            (
                "a = b",
                "expected a numeric, boolean, or quoted string literal",
            ),
            ("a = 1 AND", "expected a column reference"),
            ("a = 1 OR", "expected a column reference"),
            ("a = 1 AND OR b = 2", "expected a column reference"),
        ];
        for (expression, message) in cases {
            rejected(expression, message);
        }
    }

    #[test]
    fn rejects_unbalanced_parentheses() {
        for expression in ["(a = 1", "((a = 1)", "(a = 1 AND (b = 2)"] {
            rejected(expression, "missing ')' in filter expression");
        }
        for expression in ["a = 1)", "(a = 1))"] {
            rejected(expression, "unexpected token after filter expression");
        }
    }

    #[test]
    fn rejects_residual_tokens_and_malformed_references() {
        let cases = [
            "a = 1 b = 2",
            "a = 1 2",
            "a = 1.2.3",
            "a = 1abc",
            "a.b.c = 1",
            "a..b = 1",
            ".a = 1",
            "a = 1 NOT",
            "a == 1",
            "a <=> 1",
        ];
        for expression in cases {
            assert!(
                compile(expression).is_err(),
                "expected {expression:?} to be rejected"
            );
        }
    }

    #[test]
    fn keywords_and_literals_cannot_be_used_as_column_or_alias_names() {
        for expression in [
            "null = 1",
            "true = 1",
            "and = 1",
            "not = 1",
            "is = 1",
            "null.value = 1",
            "true.value = 1",
            "alias.null = 1",
        ] {
            assert!(
                compile(expression).is_err(),
                "expected {expression:?} to be rejected"
            );
        }
    }
}
