use crate::{
    Aggregate, AggregateArgument, BinaryOperator, ErrorCode, FrontendError, Span, UnaryOperator,
    UnboundExpression,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UnboundHavingExpression {
    Aggregate(Aggregate),
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

impl UnboundHavingExpression {
    #[must_use]
    pub const fn span(&self) -> Span {
        match self {
            Self::Aggregate(value) => value.span,
            Self::Int8(_, span)
            | Self::Null(span)
            | Self::Binary { span, .. }
            | Self::Unary { span, .. } => *span,
        }
    }
}

pub(crate) fn write_canonical(
    out: &mut Vec<u8>,
    value: &UnboundHavingExpression,
) -> Result<(), FrontendError> {
    match value {
        UnboundHavingExpression::Aggregate(aggregate) => {
            out.push(5);
            write_len(out, aggregate.function.len(), aggregate.span)?;
            out.extend_from_slice(aggregate.function.as_bytes());
            match &aggregate.argument {
                AggregateArgument::Star => out.push(0),
                AggregateArgument::Expression(input) => {
                    out.push(1);
                    write_expr(out, input)?;
                }
            }
        }
        UnboundHavingExpression::Int8(value, _) => {
            out.push(1);
            out.extend_from_slice(&value.to_be_bytes());
        }
        UnboundHavingExpression::Null(_) => out.push(2),
        UnboundHavingExpression::Binary {
            operator,
            left,
            right,
            ..
        } => {
            out.extend_from_slice(&[3, binary_code(*operator)]);
            write_canonical(out, left)?;
            write_canonical(out, right)?;
        }
        UnboundHavingExpression::Unary {
            operator, input, ..
        } => {
            out.extend_from_slice(&[4, unary_code(*operator)]);
            write_canonical(out, input)?;
        }
    }
    Ok(())
}

fn write_expr(out: &mut Vec<u8>, value: &UnboundExpression) -> Result<(), FrontendError> {
    match value {
        UnboundExpression::Column(column) => {
            out.push(0);
            out.push(column.source);
            out.push(u8::from(column.name.quoted));
            write_len(out, column.name.value.len(), column.name.span)?;
            out.extend_from_slice(column.name.value.as_bytes());
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

fn write_len(out: &mut Vec<u8>, value: usize, span: Span) -> Result<(), FrontendError> {
    out.push(
        u8::try_from(value)
            .map_err(|_| FrontendError::unsupported(ErrorCode::CanonicalizationFailed, span))?,
    );
    Ok(())
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
