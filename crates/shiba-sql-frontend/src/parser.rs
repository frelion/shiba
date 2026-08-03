use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Location, Token, Tokenizer};

use crate::bounds::{MAX_AST_NODES, MAX_SQL_BYTES, MAX_TOKENS};
use crate::{ErrorCode, FrontendError, Span, UnboundQuery};

/// Parses and normalizes one statement from the deliberately narrow SQL subset.
///
/// # Errors
///
/// Returns a stable parser, unsupported-syntax, or resource-limit diagnostic.
pub fn parse_sql(sql: &str) -> Result<UnboundQuery, FrontendError> {
    if sql.len() > MAX_SQL_BYTES {
        return Err(FrontendError::limit(
            ErrorCode::InputTooLarge,
            Span::whole(sql),
        ));
    }
    let dialect = PostgreSqlDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let tokens = tokenizer.tokenize_with_location().map_err(|error| {
        FrontendError::parser(
            ErrorCode::ParseError,
            SourceMap::new(sql).point(error.location),
        )
    })?;
    let significant = tokens
        .iter()
        .filter(|token| !matches!(token.token, Token::Whitespace(_)))
        .count();
    if significant > MAX_TOKENS {
        return Err(FrontendError::limit(
            ErrorCode::TokenLimit,
            Span::whole(sql),
        ));
    }
    // This conservative pre-parser structural ceiling is unreachable by a
    // valid query under the tighter expression/depth limits. It prevents an
    // adversarial left-associative tree from making AST destruction recursive.
    if significant > MAX_AST_NODES {
        return Err(FrontendError::limit(
            ErrorCode::QueryTooComplex,
            Span::whole(sql),
        ));
    }
    let mut parser = Parser::new(&dialect)
        .with_recursion_limit(64)
        .with_tokens_with_locations(tokens);
    let statements = parser
        .parse_statements()
        .map_err(|_| FrontendError::parser(ErrorCode::ParseError, Span::whole(sql)))?;
    if statements.len() != 1 {
        return Err(FrontendError::parser(
            ErrorCode::MultipleStatements,
            Span::whole(sql),
        ));
    }
    let Some(statement) = statements.into_iter().next() else {
        return Err(FrontendError::parser(
            ErrorCode::ParseError,
            Span::whole(sql),
        ));
    };
    crate::lowering::lower(statement, &SourceMap::new(sql))
}

pub(crate) struct SourceMap<'a> {
    sql: &'a str,
    line_starts: Vec<usize>,
}

impl<'a> SourceMap<'a> {
    fn new(sql: &'a str) -> Self {
        let mut line_starts = vec![0];
        for (index, byte) in sql.bytes().enumerate() {
            if byte == b'\n' {
                line_starts.push(index + 1);
            }
        }
        Self { sql, line_starts }
    }

    pub(crate) fn span(&self, span: sqlparser::tokenizer::Span) -> Span {
        if span.start.line == 0 {
            return Span::whole(self.sql);
        }
        let start = self.offset(span.start);
        let end = self.offset(span.end).max(start).min(self.sql.len());
        Span { start, end }
    }

    fn point(&self, location: Location) -> Span {
        let start = self.offset(location);
        Span { start, end: start }
    }

    fn offset(&self, location: Location) -> usize {
        let line = usize::try_from(location.line.saturating_sub(1)).unwrap_or(usize::MAX);
        let start = self
            .line_starts
            .get(line)
            .copied()
            .unwrap_or(self.sql.len());
        let column = usize::try_from(location.column.saturating_sub(1)).unwrap_or(usize::MAX);
        self.sql[start..]
            .char_indices()
            .nth(column)
            .map_or_else(|| self.sql.len(), |(offset, _)| start + offset)
    }
}
