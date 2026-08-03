use sqlparser::ast::{
    BinaryOperator as SqlBinary, Expr, Spanned, UnaryOperator as SqlUnary, Value,
};

use crate::bounds::Budget;
use crate::relation::LoweringContext;
use crate::{BinaryOperator, ErrorCode, FrontendError, UnaryOperator, UnboundExpression};

pub(crate) fn lower_expression(
    expression: &Expr,
    context: &LoweringContext<'_>,
    budget: &mut Budget,
    depth: usize,
) -> Result<UnboundExpression, FrontendError> {
    let span = context.map.span(expression.span());
    if let Expr::Nested(input) = expression {
        budget.ast(span)?;
        return lower_expression(input, context, budget, depth);
    }
    budget.expression(depth, span)?;
    Ok(match expression {
        Expr::Identifier(name) => UnboundExpression::Column(context.column(None, name)?),
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            UnboundExpression::Column(context.column(Some(&parts[0]), &parts[1])?)
        }
        Expr::Value(value) => lower_value(&value.value, value.span, context)?,
        Expr::UnaryOp {
            op: SqlUnary::Minus,
            expr,
        } => lower_negative(expr, context)?,
        Expr::UnaryOp {
            op: SqlUnary::Not,
            expr,
        } => {
            budget.boolean(span)?;
            UnboundExpression::Unary {
                operator: UnaryOperator::Not,
                input: Box::new(lower_expression(expr, context, budget, depth + 1)?),
                span,
            }
        }
        Expr::IsNull(input) | Expr::IsNotNull(input) => {
            budget.boolean(span)?;
            UnboundExpression::Unary {
                operator: if matches!(expression, Expr::IsNull(_)) {
                    UnaryOperator::IsNull
                } else {
                    UnaryOperator::IsNotNull
                },
                input: Box::new(lower_expression(input, context, budget, depth + 1)?),
                span,
            }
        }
        Expr::BinaryOp { left, op, right } => {
            let operator = binary(op, span, budget)?;
            UnboundExpression::Binary {
                operator,
                left: Box::new(lower_expression(left, context, budget, depth + 1)?),
                right: Box::new(lower_expression(right, context, budget, depth + 1)?),
                span,
            }
        }
        _ => {
            return Err(FrontendError::unsupported(
                ErrorCode::UnsupportedSyntax,
                span,
            ));
        }
    })
}

fn lower_value(
    value: &Value,
    sql_span: sqlparser::tokenizer::Span,
    context: &LoweringContext<'_>,
) -> Result<UnboundExpression, FrontendError> {
    let span = context.map.span(sql_span);
    match value {
        Value::Number(value, false) => value
            .parse::<i64>()
            .map(|value| UnboundExpression::Int8(value, span))
            .map_err(|_| FrontendError::unsupported(ErrorCode::UnsupportedSyntax, span)),
        Value::Null => Ok(UnboundExpression::Null(span)),
        _ => Err(FrontendError::unsupported(
            ErrorCode::UnsupportedSyntax,
            span,
        )),
    }
}

fn lower_negative(
    expression: &Expr,
    context: &LoweringContext<'_>,
) -> Result<UnboundExpression, FrontendError> {
    let span = context.map.span(expression.span());
    let Expr::Value(value) = expression else {
        return Err(FrontendError::unsupported(
            ErrorCode::UnsupportedSyntax,
            span,
        ));
    };
    let Value::Number(value, false) = &value.value else {
        return Err(FrontendError::unsupported(
            ErrorCode::UnsupportedSyntax,
            span,
        ));
    };
    let magnitude = value
        .parse::<u64>()
        .map_err(|_| FrontendError::unsupported(ErrorCode::UnsupportedSyntax, span))?;
    let signed = if magnitude == (i64::MAX as u64) + 1 {
        i64::MIN
    } else {
        -i64::try_from(magnitude)
            .map_err(|_| FrontendError::unsupported(ErrorCode::UnsupportedSyntax, span))?
    };
    Ok(UnboundExpression::Int8(signed, span))
}

fn binary(
    operator: &SqlBinary,
    span: crate::Span,
    budget: &mut Budget,
) -> Result<BinaryOperator, FrontendError> {
    let value = match operator {
        SqlBinary::Plus => BinaryOperator::Add,
        SqlBinary::Minus => BinaryOperator::Subtract,
        SqlBinary::Eq => BinaryOperator::Equal,
        SqlBinary::NotEq => BinaryOperator::NotEqual,
        SqlBinary::Lt => BinaryOperator::Less,
        SqlBinary::LtEq => BinaryOperator::LessEqual,
        SqlBinary::Gt => BinaryOperator::Greater,
        SqlBinary::GtEq => BinaryOperator::GreaterEqual,
        SqlBinary::And => BinaryOperator::And,
        SqlBinary::Or => BinaryOperator::Or,
        _ => {
            return Err(FrontendError::unsupported(
                ErrorCode::UnsupportedSyntax,
                span,
            ));
        }
    };
    if !matches!(value, BinaryOperator::Add | BinaryOperator::Subtract) {
        budget.boolean(span)?;
    }
    Ok(value)
}
