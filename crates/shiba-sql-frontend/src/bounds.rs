use crate::{ErrorCode, FrontendError, Span};

pub(crate) const MAX_SQL_BYTES: usize = 64 * 1024;
pub(crate) const MAX_TOKENS: usize = 4_096;
pub(crate) const MAX_AST_NODES: usize = 2_048;
pub(crate) const MAX_EXPRESSION_NODES: usize = 256;
pub(crate) const MAX_EXPRESSION_DEPTH: usize = 32;
pub(crate) const MAX_BOOLEAN_TERMS: usize = 64;
pub(crate) const MAX_SOURCES: usize = 2;
pub(crate) const MAX_PROJECTION: usize = 2;

#[derive(Default)]
pub(crate) struct Budget {
    pub(crate) ast_nodes: usize,
    pub(crate) expression_nodes: usize,
    pub(crate) boolean_terms: usize,
}

impl Budget {
    pub(crate) fn ast(&mut self, span: Span) -> Result<(), FrontendError> {
        self.ast_nodes = self.ast_nodes.checked_add(1).ok_or_else(|| limit(span))?;
        if self.ast_nodes > MAX_AST_NODES {
            return Err(limit(span));
        }
        Ok(())
    }

    pub(crate) fn expression(&mut self, depth: usize, span: Span) -> Result<(), FrontendError> {
        self.ast(span)?;
        self.expression_nodes = self
            .expression_nodes
            .checked_add(1)
            .ok_or_else(|| limit(span))?;
        if depth > MAX_EXPRESSION_DEPTH || self.expression_nodes > MAX_EXPRESSION_NODES {
            return Err(limit(span));
        }
        Ok(())
    }

    pub(crate) fn boolean(&mut self, span: Span) -> Result<(), FrontendError> {
        self.boolean_terms = self
            .boolean_terms
            .checked_add(1)
            .ok_or_else(|| limit(span))?;
        if self.boolean_terms > MAX_BOOLEAN_TERMS {
            return Err(limit(span));
        }
        Ok(())
    }
}

fn limit(span: Span) -> FrontendError {
    FrontendError::limit(ErrorCode::QueryTooComplex, span)
}
