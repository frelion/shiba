use shiba_operator::{
    AggregateCall, AggregateInputContract, HavingExpression, OperatorNodeKind, ValueType,
    aggregate_function_descriptor,
};

use crate::binding::resolve;
use crate::expression::{InputBinding, compile as compile_expression};
use crate::graph::CompiledInput;
use crate::{CompilerError, QueryAggregateCallV1, QueryExpressionV1, QuerySelectorV1};

pub(crate) fn compile(
    group_expressions: &[QueryExpressionV1],
    calls: &[QueryAggregateCallV1],
    input: &CompiledInput<'_>,
) -> Result<(OperatorNodeKind, Vec<ValueType>), CompilerError> {
    let expression = |value| compile_expression(value, input.binding, &input.layout);
    let groups = group_expressions
        .iter()
        .map(expression)
        .collect::<Result<Vec<_>, _>>()?;
    let mut output_types = groups.iter().map(|group| group.1).collect::<Vec<_>>();
    let calls = calls
        .iter()
        .map(|call| compile_call(call, input, &mut output_types))
        .collect::<Result<Vec<_>, _>>()?;
    Ok((
        OperatorNodeKind::Aggregate {
            group_expressions: groups.into_iter().map(|group| group.0).collect(),
            calls,
            having: None,
        },
        output_types,
    ))
}

pub(crate) fn compile_having(
    expression: &crate::QueryHavingExpressionV1,
) -> Result<HavingExpression, CompilerError> {
    use crate::QueryHavingExpressionV1 as H;
    Ok(match expression {
        H::Call { ordinal } => HavingExpression::Call { ordinal: *ordinal },
        H::Int8Literal { value } => HavingExpression::Int8Literal { value: *value },
        H::NullLiteral => HavingExpression::NullLiteral,
        H::Equal { left, right } => HavingExpression::Equal {
            left: Box::new(compile_having(left)?),
            right: Box::new(compile_having(right)?),
        },
        H::NotEqual { left, right } => HavingExpression::NotEqual {
            left: Box::new(compile_having(left)?),
            right: Box::new(compile_having(right)?),
        },
        H::Less { left, right } => HavingExpression::Less {
            left: Box::new(compile_having(left)?),
            right: Box::new(compile_having(right)?),
        },
        H::LessEqual { left, right } => HavingExpression::LessEqual {
            left: Box::new(compile_having(left)?),
            right: Box::new(compile_having(right)?),
        },
        H::Greater { left, right } => HavingExpression::Greater {
            left: Box::new(compile_having(left)?),
            right: Box::new(compile_having(right)?),
        },
        H::GreaterEqual { left, right } => HavingExpression::GreaterEqual {
            left: Box::new(compile_having(left)?),
            right: Box::new(compile_having(right)?),
        },
        H::IsNull { input } => HavingExpression::IsNull {
            input: Box::new(compile_having(input)?),
        },
        H::And { left, right } => HavingExpression::And {
            left: Box::new(compile_having(left)?),
            right: Box::new(compile_having(right)?),
        },
        H::Or { left, right } => HavingExpression::Or {
            left: Box::new(compile_having(left)?),
            right: Box::new(compile_having(right)?),
        },
        H::Not { input } => HavingExpression::Not {
            input: Box::new(compile_having(input)?),
        },
    })
}

fn compile_call(
    call: &QueryAggregateCallV1,
    input: &CompiledInput<'_>,
    output_types: &mut Vec<ValueType>,
) -> Result<AggregateCall, CompilerError> {
    let descriptor = aggregate_function_descriptor(call.function);
    let expression = call
        .expression
        .as_ref()
        .map(|value| compile_expression(value, input.binding, &input.layout))
        .transpose()?;
    match (descriptor.input, expression.as_ref()) {
        (AggregateInputContract::None, None) => {}
        (AggregateInputContract::Nullable(expected), Some((_, actual))) if expected == *actual => {}
        (AggregateInputContract::Nullable(_), Some(_)) => {
            return Err(call
                .expression
                .as_ref()
                .map_or(CompilerError::WrongType, |value| {
                    column_type_error(value, input.binding)
                }));
        }
        _ => return Err(CompilerError::InvalidSpec),
    }
    output_types.push(descriptor.output_type);
    Ok(AggregateCall {
        ordinal: call.ordinal,
        function: call.function,
        function_version: call.function_version,
        expression: expression.map(|value| value.0),
    })
}

fn column_type_error(expression: &QueryExpressionV1, binding: InputBinding<'_>) -> CompilerError {
    if let (QueryExpressionV1::Column { field }, InputBinding::Source(source)) =
        (expression, binding)
        && let QuerySelectorV1::Name { name, .. } = &field.selector
        && let Ok((_, column)) = resolve(source, name)
    {
        return CompilerError::WrongColumnType {
            column: column.name.clone(),
            type_oid: column.type_oid,
        };
    }
    CompilerError::WrongType
}
