use crate::{
    BinaryOperator, ColumnRef, ErrorCode, FrontendError, UnaryOperator, UnboundExpression,
};
use shiba_compiler::{
    POSTGRES_INT8_TYPE_OID, QueryExpressionV1, QueryFieldV1, QuerySelectorV1,
    SourceColumnDescriptor, SourceDescriptor,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExpressionType {
    Bool,
    Int8,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BoundExpression {
    pub(crate) expression: QueryExpressionV1,
    pub(crate) value_type: ExpressionType,
    pub(crate) nullable: bool,
}

pub(crate) fn resolve_column<'a>(
    source: &'a SourceDescriptor,
    column: &ColumnRef,
) -> Result<&'a SourceColumnDescriptor, FrontendError> {
    if column.source != 0 {
        return Err(binding(ErrorCode::UnknownColumn, column.span));
    }
    let mut matches = source
        .columns
        .iter()
        .filter(|candidate| candidate.name == column.name.value);
    let resolved = matches
        .next()
        .ok_or_else(|| binding(ErrorCode::UnknownColumn, column.span))?;
    if matches.next().is_some() {
        return Err(binding(ErrorCode::AmbiguousColumn, column.span));
    }
    if resolved.type_oid != POSTGRES_INT8_TYPE_OID {
        return Err(binding(ErrorCode::TypeMismatch, column.span));
    }
    Ok(resolved)
}

pub(crate) fn lower(
    expression: &UnboundExpression,
    source: &SourceDescriptor,
    slots: &[(&SourceColumnDescriptor, u16)],
) -> Result<BoundExpression, FrontendError> {
    match expression {
        UnboundExpression::Column(column) => {
            let descriptor = resolve_column(source, column)?;
            let slot = slots
                .iter()
                .find_map(|(column, slot)| (column.address == descriptor.address).then_some(*slot))
                .ok_or_else(|| binding(ErrorCode::CanonicalizationFailed, column.span))?;
            Ok(BoundExpression {
                expression: QueryExpressionV1::Column {
                    field: QueryFieldV1 {
                        input: 0,
                        selector: QuerySelectorV1::Slot { slot },
                    },
                },
                value_type: ExpressionType::Int8,
                nullable: descriptor.nullable,
            })
        }
        UnboundExpression::Int8(value, _) => Ok(BoundExpression {
            expression: QueryExpressionV1::Int8Literal { value: *value },
            value_type: ExpressionType::Int8,
            nullable: false,
        }),
        UnboundExpression::Null(_) => Ok(BoundExpression {
            expression: QueryExpressionV1::NullInt8,
            value_type: ExpressionType::Int8,
            nullable: true,
        }),
        UnboundExpression::Binary {
            operator,
            left,
            right,
            span,
        } => lower_binary(*operator, left, right, *span, source, slots),
        UnboundExpression::Unary {
            operator,
            input,
            span,
        } => lower_unary(*operator, input, *span, source, slots),
    }
}

fn lower_binary(
    operator: BinaryOperator,
    left: &UnboundExpression,
    right: &UnboundExpression,
    span: crate::Span,
    source: &SourceDescriptor,
    slots: &[(&SourceColumnDescriptor, u16)],
) -> Result<BoundExpression, FrontendError> {
    let left = lower(left, source, slots)?;
    let right = lower(right, source, slots)?;
    let nullable = left.nullable || right.nullable;
    let (expression, value_type, nullable) = match operator {
        BinaryOperator::Add | BinaryOperator::Subtract => {
            require(left.value_type, ExpressionType::Int8, span)?;
            require(right.value_type, ExpressionType::Int8, span)?;
            let expression = match operator {
                BinaryOperator::Add => QueryExpressionV1::CheckedAdd {
                    left: Box::new(left.expression),
                    right: Box::new(right.expression),
                },
                BinaryOperator::Subtract => QueryExpressionV1::CheckedSubtract {
                    left: Box::new(left.expression),
                    right: Box::new(right.expression),
                },
                _ => unreachable!(),
            };
            (expression, ExpressionType::Int8, nullable)
        }
        BinaryOperator::Equal | BinaryOperator::NotEqual => {
            require(left.value_type, ExpressionType::Int8, span)?;
            require(right.value_type, ExpressionType::Int8, span)?;
            let expression = match operator {
                BinaryOperator::Equal => QueryExpressionV1::Equal {
                    left: Box::new(left.expression),
                    right: Box::new(right.expression),
                },
                BinaryOperator::NotEqual => QueryExpressionV1::NotEqual {
                    left: Box::new(left.expression),
                    right: Box::new(right.expression),
                },
                _ => unreachable!(),
            };
            (expression, ExpressionType::Bool, nullable)
        }
        BinaryOperator::Less
        | BinaryOperator::LessEqual
        | BinaryOperator::Greater
        | BinaryOperator::GreaterEqual => {
            require(left.value_type, ExpressionType::Int8, span)?;
            require(right.value_type, ExpressionType::Int8, span)?;
            let left = Box::new(left.expression);
            let right = Box::new(right.expression);
            let expression = match operator {
                BinaryOperator::Less => QueryExpressionV1::Less { left, right },
                BinaryOperator::LessEqual => QueryExpressionV1::LessEqual { left, right },
                BinaryOperator::Greater => QueryExpressionV1::Greater { left, right },
                BinaryOperator::GreaterEqual => QueryExpressionV1::GreaterEqual { left, right },
                _ => unreachable!(),
            };
            (expression, ExpressionType::Bool, nullable)
        }
        BinaryOperator::And | BinaryOperator::Or => {
            require(left.value_type, ExpressionType::Bool, span)?;
            require(right.value_type, ExpressionType::Bool, span)?;
            let left = Box::new(left.expression);
            let right = Box::new(right.expression);
            let expression = match operator {
                BinaryOperator::And => QueryExpressionV1::And { left, right },
                BinaryOperator::Or => QueryExpressionV1::Or { left, right },
                _ => unreachable!(),
            };
            (expression, ExpressionType::Bool, nullable)
        }
    };
    Ok(BoundExpression {
        expression,
        value_type,
        nullable,
    })
}

fn lower_unary(
    operator: UnaryOperator,
    input: &UnboundExpression,
    span: crate::Span,
    source: &SourceDescriptor,
    slots: &[(&SourceColumnDescriptor, u16)],
) -> Result<BoundExpression, FrontendError> {
    let input = lower(input, source, slots)?;
    let (expression, nullable) = match operator {
        UnaryOperator::IsNull => (
            QueryExpressionV1::IsNull {
                input: Box::new(input.expression),
            },
            false,
        ),
        UnaryOperator::IsNotNull => (
            QueryExpressionV1::Not {
                input: Box::new(QueryExpressionV1::IsNull {
                    input: Box::new(input.expression),
                }),
            },
            false,
        ),
        UnaryOperator::Not => {
            require(input.value_type, ExpressionType::Bool, span)?;
            (
                QueryExpressionV1::Not {
                    input: Box::new(input.expression),
                },
                input.nullable,
            )
        }
    };
    Ok(BoundExpression {
        expression,
        value_type: ExpressionType::Bool,
        nullable,
    })
}

fn require(
    actual: ExpressionType,
    expected: ExpressionType,
    span: crate::Span,
) -> Result<(), FrontendError> {
    if actual == expected {
        Ok(())
    } else {
        Err(binding(ErrorCode::TypeMismatch, span))
    }
}

fn binding(code: ErrorCode, span: crate::Span) -> FrontendError {
    FrontendError::binding(code, span)
}
