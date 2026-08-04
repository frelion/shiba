use shiba_operator::{NodeInput, OperatorNodeKind, ValueType};

use crate::expression::compile as compile_expression;
use crate::graph::CompiledInput;
use crate::{CompilerError, IdentityIndexDescriptor, QueryOperationV1, SourceDescriptor};

#[allow(clippy::type_complexity, clippy::too_many_lines)]
pub(crate) fn compile(
    operation: &QueryOperationV1,
    inputs: &[CompiledInput<'_>],
    indexes: &[Option<IdentityIndexDescriptor>],
    descriptors: &[SourceDescriptor],
) -> Result<(NodeInput, OperatorNodeKind, Vec<ValueType>, Vec<bool>, bool), CompilerError> {
    if matches!(operation, QueryOperationV1::InnerJoin { .. }) {
        return crate::join_compile::compile(operation, inputs, indexes, descriptors);
    }
    let [input] = inputs else {
        return Err(CompilerError::InvalidTopology);
    };
    let expression = |value| compile_expression(value, input.binding, &input.layout);
    let (kind, types, nullable, stateful) = match operation {
        QueryOperationV1::Aggregate {
            group_expressions,
            calls,
            having,
        } => {
            let (kind, types) = crate::aggregate_compile::compile(group_expressions, calls, input)?;
            let (kind, nullable) = match kind {
                OperatorNodeKind::Aggregate {
                    group_expressions,
                    calls,
                    ..
                } => {
                    let mut nullable = group_expressions
                        .iter()
                        .map(|expression| expression.nullable(&input.layout))
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|_| CompilerError::WrongType)?;
                    nullable.extend(calls.iter().map(|call| {
                        shiba_operator::aggregate_function_descriptor(call.function).output_nullable
                    }));
                    (
                        OperatorNodeKind::Aggregate {
                            group_expressions,
                            calls,
                            having: having
                                .as_ref()
                                .map(crate::aggregate_compile::compile_having)
                                .transpose()?,
                        },
                        nullable,
                    )
                }
                _ => return Err(CompilerError::InvalidSpec),
            };
            (kind, types, nullable, true)
        }
        QueryOperationV1::Filter { predicate } => {
            let (predicate, ty) = expression(predicate)?;
            if ty != ValueType::Bool {
                return Err(CompilerError::WrongType);
            }
            (
                OperatorNodeKind::Filter { predicate },
                input.layout.value_types.clone(),
                input.layout.nullable.clone(),
                false,
            )
        }
        QueryOperationV1::Project { expressions } => {
            let compiled = expressions
                .iter()
                .map(expression)
                .collect::<Result<Vec<_>, _>>()?;
            let types = compiled.iter().map(|value| value.1).collect();
            let nullable = compiled
                .iter()
                .map(|value| value.0.nullable(&input.layout))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| CompilerError::WrongType)?;
            (
                OperatorNodeKind::Project {
                    expressions: compiled.into_iter().map(|value| value.0).collect(),
                },
                types,
                nullable,
                false,
            )
        }
        QueryOperationV1::Compute { expressions } => {
            let compiled = expressions
                .iter()
                .map(expression)
                .collect::<Result<Vec<_>, _>>()?;
            let mut types = input.layout.value_types.clone();
            types.extend(compiled.iter().map(|value| value.1));
            let computed_nullable = compiled
                .iter()
                .map(|value| value.0.nullable(&input.layout))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| CompilerError::WrongType)?;
            (
                OperatorNodeKind::Compute {
                    expressions: compiled.into_iter().map(|value| value.0).collect(),
                },
                types,
                input
                    .layout
                    .nullable
                    .iter()
                    .copied()
                    .chain(computed_nullable)
                    .collect(),
                false,
            )
        }
        QueryOperationV1::KeyBy { key } => {
            let (key, value_type) = expression(key)?;
            let key_nullable = key
                .nullable(&input.layout)
                .map_err(|_| CompilerError::WrongType)?;
            let mut types = input.layout.value_types.clone();
            types.push(value_type);
            (
                OperatorNodeKind::KeyBy { key },
                types,
                {
                    let mut nullable = input.layout.nullable.clone();
                    nullable.push(key_nullable);
                    nullable
                },
                false,
            )
        }
        QueryOperationV1::InnerJoin { .. } => unreachable!(),
    };
    Ok((input.input, kind, types, nullable, stateful))
}
