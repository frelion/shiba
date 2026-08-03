use sha2::{Digest, Sha256};

use crate::{ErrorCode, FrontendError, Span};

const UNBOUND_DOMAIN: &[u8] = b"shiba.sql.unbound.v1\0";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Identifier {
    pub value: String,
    pub quoted: bool,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QualifiedRelation {
    pub schema: Identifier,
    pub relation: Identifier,
    pub alias: Option<Identifier>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnRef {
    pub source: u8,
    pub name: Identifier,
    pub span: Span,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BinaryOperator {
    Add,
    Subtract,
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
    And,
    Or,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnaryOperator {
    IsNull,
    IsNotNull,
    Not,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UnboundExpression {
    Column(ColumnRef),
    Int8(i64, Span),
    Null(Span),
    Binary {
        operator: BinaryOperator,
        left: Box<Self>,
        right: Box<Self>,
        span: Span,
    },
    Unary {
        operator: UnaryOperator,
        input: Box<Self>,
        span: Span,
    },
}

impl UnboundExpression {
    #[must_use]
    pub const fn span(&self) -> Span {
        match self {
            Self::Column(value) => value.span,
            Self::Int8(_, span)
            | Self::Null(span)
            | Self::Binary { span, .. }
            | Self::Unary { span, .. } => *span,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Aggregate {
    CountStar {
        span: Span,
    },
    Sum {
        input: UnboundExpression,
        span: Span,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SelectExpression {
    Expression(UnboundExpression),
    Aggregate(Aggregate),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnboundSelectItem {
    pub expression: SelectExpression,
    pub presentation_alias: Option<Identifier>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Join {
    pub left: ColumnRef,
    pub right: ColumnRef,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnboundQuery {
    pub sources: Vec<QualifiedRelation>,
    pub join: Option<Join>,
    pub projection: Vec<UnboundSelectItem>,
    pub selection: Option<UnboundExpression>,
    pub group_by: Option<UnboundExpression>,
    pub span: Span,
}

impl UnboundQuery {
    /// Returns the deterministic, span-free ephemeral semantic encoding.
    ///
    /// # Errors
    ///
    /// Rejects manually constructed ASTs that violate any frontend bound.
    pub fn canonical_payload(&self) -> Result<Vec<u8>, FrontendError> {
        crate::ast_validate::validate(self)?;
        let mut out = b"UQ1".to_vec();
        write_len(&mut out, self.sources.len(), self.span)?;
        for source in &self.sources {
            write_ident(&mut out, &source.schema)?;
            write_ident(&mut out, &source.relation)?;
        }
        write_option(&mut out, self.join.as_ref(), write_join)?;
        write_len(&mut out, self.projection.len(), self.span)?;
        for item in &self.projection {
            write_select(&mut out, &item.expression)?;
        }
        write_option(&mut out, self.selection.as_ref(), write_expr)?;
        write_option(&mut out, self.group_by.as_ref(), write_expr)?;
        Ok(out)
    }

    /// Returns the domain-separated canonical unbound-query digest.
    ///
    /// # Errors
    ///
    /// Rejects manually constructed ASTs that violate any frontend bound.
    pub fn canonical_digest(&self) -> Result<[u8; 32], FrontendError> {
        let mut hash = Sha256::new();
        hash.update(UNBOUND_DOMAIN);
        hash.update(self.canonical_payload()?);
        Ok(hash.finalize().into())
    }
}

fn write_select(out: &mut Vec<u8>, value: &SelectExpression) -> Result<(), FrontendError> {
    match value {
        SelectExpression::Expression(expr) => {
            out.push(0);
            write_expr(out, expr)?;
        }
        SelectExpression::Aggregate(Aggregate::CountStar { .. }) => out.push(1),
        SelectExpression::Aggregate(Aggregate::Sum { input, .. }) => {
            out.push(2);
            write_expr(out, input)?;
        }
    }
    Ok(())
}

fn write_join(out: &mut Vec<u8>, join: &Join) -> Result<(), FrontendError> {
    write_column(out, &join.left)?;
    write_column(out, &join.right)
}

fn write_expr(out: &mut Vec<u8>, value: &UnboundExpression) -> Result<(), FrontendError> {
    match value {
        UnboundExpression::Column(column) => {
            out.push(0);
            write_column(out, column)?;
        }
        UnboundExpression::Int8(value, _) => {
            out.push(1);
            out.extend_from_slice(&value.to_be_bytes());
        }
        UnboundExpression::Null(_) => out.push(2),
        UnboundExpression::Binary {
            operator,
            left,
            right,
            ..
        } => {
            out.extend_from_slice(&[3, binary_code(*operator)]);
            write_expr(out, left)?;
            write_expr(out, right)?;
        }
        UnboundExpression::Unary {
            operator, input, ..
        } => {
            out.extend_from_slice(&[4, unary_code(*operator)]);
            write_expr(out, input)?;
        }
    }
    Ok(())
}

fn write_column(out: &mut Vec<u8>, value: &ColumnRef) -> Result<(), FrontendError> {
    out.push(value.source);
    write_ident(out, &value.name)
}

fn write_ident(out: &mut Vec<u8>, value: &Identifier) -> Result<(), FrontendError> {
    out.push(u8::from(value.quoted));
    write_len(out, value.value.len(), value.span)?;
    out.extend_from_slice(value.value.as_bytes());
    Ok(())
}

fn write_len(out: &mut Vec<u8>, value: usize, span: Span) -> Result<(), FrontendError> {
    out.push(u8::try_from(value).map_err(|_| canonical_error(span))?);
    Ok(())
}

fn write_option<T>(
    out: &mut Vec<u8>,
    value: Option<&T>,
    write: fn(&mut Vec<u8>, &T) -> Result<(), FrontendError>,
) -> Result<(), FrontendError> {
    if let Some(value) = value {
        out.push(1);
        write(out, value)?;
    } else {
        out.push(0);
    }
    Ok(())
}

fn canonical_error(span: Span) -> FrontendError {
    FrontendError::unsupported(ErrorCode::CanonicalizationFailed, span)
}

const fn binary_code(operator: BinaryOperator) -> u8 {
    match operator {
        BinaryOperator::Add => 0,
        BinaryOperator::Subtract => 1,
        BinaryOperator::Equal => 2,
        BinaryOperator::NotEqual => 3,
        BinaryOperator::Less => 4,
        BinaryOperator::LessEqual => 5,
        BinaryOperator::Greater => 6,
        BinaryOperator::GreaterEqual => 7,
        BinaryOperator::And => 8,
        BinaryOperator::Or => 9,
    }
}

const fn unary_code(operator: UnaryOperator) -> u8 {
    match operator {
        UnaryOperator::IsNull => 0,
        UnaryOperator::IsNotNull => 1,
        UnaryOperator::Not => 2,
    }
}
