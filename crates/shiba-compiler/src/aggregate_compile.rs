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
    let mut nodes = 0;
    let mut boolean_terms = 0;
    compile_having_inner(expression, 0, &mut nodes, &mut boolean_terms)
}

fn compile_having_inner(
    expression: &crate::QueryHavingExpressionV1,
    depth: usize,
    nodes: &mut usize,
    boolean_terms: &mut usize,
) -> Result<HavingExpression, CompilerError> {
    *nodes = nodes.checked_add(1).ok_or(CompilerError::InvalidSpec)?;
    if depth > shiba_operator::MAX_HAVING_DEPTH || *nodes > shiba_operator::MAX_HAVING_NODES {
        return Err(CompilerError::InvalidSpec);
    }
    let boolean = matches!(
        expression,
        crate::QueryHavingExpressionV1::Equal { .. }
            | crate::QueryHavingExpressionV1::NotEqual { .. }
            | crate::QueryHavingExpressionV1::Less { .. }
            | crate::QueryHavingExpressionV1::LessEqual { .. }
            | crate::QueryHavingExpressionV1::Greater { .. }
            | crate::QueryHavingExpressionV1::GreaterEqual { .. }
            | crate::QueryHavingExpressionV1::IsNull { .. }
            | crate::QueryHavingExpressionV1::And { .. }
            | crate::QueryHavingExpressionV1::Or { .. }
            | crate::QueryHavingExpressionV1::Not { .. }
    );
    if boolean {
        *boolean_terms = boolean_terms
            .checked_add(1)
            .ok_or(CompilerError::InvalidSpec)?;
        if *boolean_terms > shiba_operator::MAX_HAVING_BOOLEAN_TERMS {
            return Err(CompilerError::InvalidSpec);
        }
    }
    use crate::QueryHavingExpressionV1 as H;
    Ok(match expression {
        H::Call { ordinal } => HavingExpression::Call { ordinal: *ordinal },
        H::Int8Literal { value } => HavingExpression::Int8Literal { value: *value },
        H::NullLiteral => HavingExpression::NullLiteral,
        H::Equal { left, right } => HavingExpression::Equal {
            left: Box::new(compile_having_inner(left, depth + 1, nodes, boolean_terms)?),
            right: Box::new(compile_having_inner(
                right,
                depth + 1,
                nodes,
                boolean_terms,
            )?),
        },
        H::NotEqual { left, right } => HavingExpression::NotEqual {
            left: Box::new(compile_having_inner(left, depth + 1, nodes, boolean_terms)?),
            right: Box::new(compile_having_inner(
                right,
                depth + 1,
                nodes,
                boolean_terms,
            )?),
        },
        H::Less { left, right } => HavingExpression::Less {
            left: Box::new(compile_having_inner(left, depth + 1, nodes, boolean_terms)?),
            right: Box::new(compile_having_inner(
                right,
                depth + 1,
                nodes,
                boolean_terms,
            )?),
        },
        H::LessEqual { left, right } => HavingExpression::LessEqual {
            left: Box::new(compile_having_inner(left, depth + 1, nodes, boolean_terms)?),
            right: Box::new(compile_having_inner(
                right,
                depth + 1,
                nodes,
                boolean_terms,
            )?),
        },
        H::Greater { left, right } => HavingExpression::Greater {
            left: Box::new(compile_having_inner(left, depth + 1, nodes, boolean_terms)?),
            right: Box::new(compile_having_inner(
                right,
                depth + 1,
                nodes,
                boolean_terms,
            )?),
        },
        H::GreaterEqual { left, right } => HavingExpression::GreaterEqual {
            left: Box::new(compile_having_inner(left, depth + 1, nodes, boolean_terms)?),
            right: Box::new(compile_having_inner(
                right,
                depth + 1,
                nodes,
                boolean_terms,
            )?),
        },
        H::IsNull { input } => HavingExpression::IsNull {
            input: Box::new(compile_having_inner(
                input,
                depth + 1,
                nodes,
                boolean_terms,
            )?),
        },
        H::And { left, right } => HavingExpression::And {
            left: Box::new(compile_having_inner(left, depth + 1, nodes, boolean_terms)?),
            right: Box::new(compile_having_inner(
                right,
                depth + 1,
                nodes,
                boolean_terms,
            )?),
        },
        H::Or { left, right } => HavingExpression::Or {
            left: Box::new(compile_having_inner(left, depth + 1, nodes, boolean_terms)?),
            right: Box::new(compile_having_inner(
                right,
                depth + 1,
                nodes,
                boolean_terms,
            )?),
        },
        H::Not { input } => HavingExpression::Not {
            input: Box::new(compile_having_inner(
                input,
                depth + 1,
                nodes,
                boolean_terms,
            )?),
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
