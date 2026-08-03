use shiba_operator::{NodeInput, OperatorNodeKind, ValueType};

use crate::binding::resolve;
use crate::expression::{InputBinding, compile as compile_expression, slot};
use crate::graph::CompiledInput;
use crate::{
    CompilerError, IdentityIndexDescriptor, QueryExpressionV1, QueryOperationV1, QuerySelectorV1,
    SourceDescriptor,
};

pub(crate) fn compile(
    operation: &QueryOperationV1,
    inputs: &[CompiledInput<'_>],
    indexes: &[Option<IdentityIndexDescriptor>],
    descriptors: &[SourceDescriptor],
) -> Result<(NodeInput, OperatorNodeKind, Vec<ValueType>, bool), CompilerError> {
    if matches!(operation, QueryOperationV1::InnerJoin { .. }) {
        return crate::join_compile::compile(operation, inputs, indexes, descriptors);
    }
    let [input] = inputs else {
        return Err(CompilerError::InvalidTopology);
    };
    let expression = |value| compile_expression(value, input.binding, &input.layout);
    let (kind, types, stateful) = match operation {
        QueryOperationV1::CountRows => (OperatorNodeKind::CountRows, vec![ValueType::Int8], true),
        QueryOperationV1::SumInt8 { value } => {
            let (compiled, ty) = expression(value)?;
            let shiba_operator::Expression::Column { slot: input_slot } = compiled else {
                return Err(CompilerError::InvalidSpec);
            };
            if ty != ValueType::Int8 {
                return Err(column_type_error(value, input.binding));
            }
            (
                OperatorNodeKind::SumInt8 { input_slot },
                vec![ValueType::Int8],
                true,
            )
        }
        QueryOperationV1::Filter { predicate } => {
            let (predicate, ty) = expression(predicate)?;
            if ty != ValueType::Bool {
                return Err(CompilerError::WrongType);
            }
            (
                OperatorNodeKind::Filter { predicate },
                input.layout.value_types.clone(),
                false,
            )
        }
        QueryOperationV1::Project { expressions } => {
            let compiled = expressions
                .iter()
                .map(expression)
                .collect::<Result<Vec<_>, _>>()?;
            let types = compiled.iter().map(|value| value.1).collect();
            (
                OperatorNodeKind::Project {
                    expressions: compiled.into_iter().map(|value| value.0).collect(),
                },
                types,
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
            (
                OperatorNodeKind::Compute {
                    expressions: compiled.into_iter().map(|value| value.0).collect(),
                },
                types,
                false,
            )
        }
        QueryOperationV1::KeyBy { key } => {
            let (key, value_type) = expression(key)?;
            let mut types = input.layout.value_types.clone();
            types.push(value_type);
            (OperatorNodeKind::KeyBy { key }, types, false)
        }
        QueryOperationV1::GroupedCount { key } => {
            let key_slot = slot(key, input.binding)?;
            let key_type = value_type(input, key_slot)?;
            (
                OperatorNodeKind::GroupedCount { key_slot },
                vec![key_type, ValueType::Int8],
                true,
            )
        }
        QueryOperationV1::GroupedSumInt8 { key, value } => {
            let key_slot = slot(key, input.binding)?;
            let value_slot = slot(value, input.binding)?;
            let key_type = value_type(input, key_slot)?;
            if value_type(input, value_slot)? != ValueType::Int8 {
                return Err(CompilerError::WrongType);
            }
            (
                OperatorNodeKind::GroupedSumInt8 {
                    key_slot,
                    value_slot,
                },
                vec![key_type, ValueType::Int8],
                true,
            )
        }
        QueryOperationV1::InnerJoin { .. } => unreachable!(),
    };
    Ok((input.input, kind, types, stateful))
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

fn value_type(input: &CompiledInput<'_>, slot: u16) -> Result<ValueType, CompilerError> {
    input
        .layout
        .value_types
        .get(usize::from(slot))
        .copied()
        .ok_or(CompilerError::WrongType)
}
