use shiba_operator::{Expression, TypedLayout, ValueType};

use crate::binding::resolve;
use crate::{CompilerError, QueryExpressionV1, QueryFieldV1, QuerySelectorV1, SourceDescriptor};

#[derive(Clone, Copy)]
pub(crate) enum InputBinding<'a> {
    Source(&'a SourceDescriptor),
    Node,
}

pub(crate) fn compile(
    expression: &QueryExpressionV1,
    binding: InputBinding<'_>,
    layout: &TypedLayout,
) -> Result<(Expression, ValueType), CompilerError> {
    let expression = lower(expression, binding)?;
    let value_type = expression
        .validate(layout)
        .map_err(|_| CompilerError::WrongType)?;
    Ok((expression, value_type))
}

pub(crate) fn slot(field: &QueryFieldV1, binding: InputBinding<'_>) -> Result<u16, CompilerError> {
    if field.input != 0 {
        return Err(CompilerError::InvalidTopology);
    }
    match (binding, &field.selector) {
        (InputBinding::Source(source), QuerySelectorV1::Name { name, .. }) if !name.is_empty() => {
            resolve(source, name).map(|(slot, _)| slot)
        }
        (InputBinding::Node, QuerySelectorV1::Slot { slot }) => Ok(*slot),
        _ => Err(CompilerError::InvalidSpec),
    }
}

fn lower(
    expression: &QueryExpressionV1,
    binding: InputBinding<'_>,
) -> Result<Expression, CompilerError> {
    let binary = |left: &QueryExpressionV1, right: &QueryExpressionV1| {
        Ok::<_, CompilerError>((
            Box::new(lower(left, binding)?),
            Box::new(lower(right, binding)?),
        ))
    };
    Ok(match expression {
        QueryExpressionV1::Column { field } => Expression::Column {
            slot: slot(field, binding)?,
        },
        QueryExpressionV1::Int8Literal { value } => Expression::Int8Literal { value: *value },
        QueryExpressionV1::NullInt8 => Expression::NullLiteral {
            value_type: ValueType::Int8,
        },
        QueryExpressionV1::Equal { left, right } => {
            let (left, right) = binary(left, right)?;
            Expression::Equal { left, right }
        }
        QueryExpressionV1::NotEqual { left, right } => {
            let (left, right) = binary(left, right)?;
            Expression::NotEqual { left, right }
        }
        QueryExpressionV1::Less { left, right } => {
            let (left, right) = binary(left, right)?;
            Expression::Less { left, right }
        }
        QueryExpressionV1::LessEqual { left, right } => {
            let (left, right) = binary(left, right)?;
            Expression::LessEqual { left, right }
        }
        QueryExpressionV1::Greater { left, right } => {
            let (left, right) = binary(left, right)?;
            Expression::Greater { left, right }
        }
        QueryExpressionV1::GreaterEqual { left, right } => {
            let (left, right) = binary(left, right)?;
            Expression::GreaterEqual { left, right }
        }
        QueryExpressionV1::IsNull { input } => Expression::IsNull {
            input: Box::new(lower(input, binding)?),
        },
        QueryExpressionV1::And { left, right } => {
            let (left, right) = binary(left, right)?;
            Expression::And { left, right }
        }
        QueryExpressionV1::Or { left, right } => {
            let (left, right) = binary(left, right)?;
            Expression::Or { left, right }
        }
        QueryExpressionV1::Not { input } => Expression::Not {
            input: Box::new(lower(input, binding)?),
        },
        QueryExpressionV1::CheckedAdd { left, right } => {
            let (left, right) = binary(left, right)?;
            Expression::Add { left, right }
        }
        QueryExpressionV1::CheckedSubtract { left, right } => {
            let (left, right) = binary(left, right)?;
            Expression::Subtract { left, right }
        }
    })
}
