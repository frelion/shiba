use shiba_compiler::QueryHavingExpressionV1;

use crate::bind::{ResolvedSource, binding};
use crate::bind_aggregate::{BoundCall, aggregate_call};
use crate::{ErrorCode, FrontendError, UnboundExpression, UnboundHavingExpression};

pub(crate) fn bind(
    expression: &UnboundHavingExpression,
    calls: &[BoundCall<'_>],
    source: &ResolvedSource,
) -> Result<QueryHavingExpressionV1, FrontendError> {
    use UnboundHavingExpression as H;
    Ok(match expression {
        H::Aggregate(aggregate) => {
            let (function, argument) = aggregate_call(aggregate)?;
            let value = argument
                .map(|input| {
                    let UnboundExpression::Column(column) = input else {
                        return Err(binding(ErrorCode::UnsupportedSyntax, input.span()));
                    };
                    crate::bind_expression::resolve_column(&source.descriptor, column)
                })
                .transpose()?;
            let ordinal = calls
                .iter()
                .position(|call| {
                    call.function == function
                        && call.value.map(|v| v.address) == value.as_ref().map(|v| v.address)
                })
                .and_then(|index| u16::try_from(index + 1).ok())
                .ok_or_else(|| binding(ErrorCode::UnsupportedSyntax, aggregate.span))?;
            QueryHavingExpressionV1::Call { ordinal }
        }
        H::Int8(value, _) => QueryHavingExpressionV1::Int8Literal { value: *value },
        H::Null(_) => QueryHavingExpressionV1::NullLiteral,
        H::Binary {
            operator,
            left,
            right,
            ..
        } => {
            let left = Box::new(bind(left, calls, source)?);
            let right = Box::new(bind(right, calls, source)?);
            match operator {
                crate::BinaryOperator::Equal => QueryHavingExpressionV1::Equal { left, right },
                crate::BinaryOperator::NotEqual => {
                    QueryHavingExpressionV1::NotEqual { left, right }
                }
                crate::BinaryOperator::Less => QueryHavingExpressionV1::Less { left, right },
                crate::BinaryOperator::LessEqual => {
                    QueryHavingExpressionV1::LessEqual { left, right }
                }
                crate::BinaryOperator::Greater => QueryHavingExpressionV1::Greater { left, right },
                crate::BinaryOperator::GreaterEqual => {
                    QueryHavingExpressionV1::GreaterEqual { left, right }
                }
                crate::BinaryOperator::And => QueryHavingExpressionV1::And { left, right },
                crate::BinaryOperator::Or => QueryHavingExpressionV1::Or { left, right },
                crate::BinaryOperator::Add | crate::BinaryOperator::Subtract => {
                    return Err(binding(ErrorCode::UnsupportedSyntax, expression.span()));
                }
            }
        }
        H::Unary {
            operator, input, ..
        } => {
            let input = Box::new(bind(input, calls, source)?);
            match operator {
                crate::UnaryOperator::IsNull => QueryHavingExpressionV1::IsNull { input },
                crate::UnaryOperator::IsNotNull => QueryHavingExpressionV1::Not {
                    input: Box::new(QueryHavingExpressionV1::IsNull { input }),
                },
                crate::UnaryOperator::Not => QueryHavingExpressionV1::Not { input },
            }
        }
    })
}
