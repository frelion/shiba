use shiba_operator::{NodeInput, OperatorNodeKind, ValueType};

use crate::expression::compile as compile_expression;
use crate::graph::CompiledInput;
use crate::{CompilerError, IdentityIndexDescriptor, QueryOperationV1, SourceDescriptor};

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
        QueryOperationV1::Aggregate {
            group_expressions,
            calls,
        } => {
            let (kind, types) = crate::aggregate_compile::compile(group_expressions, calls, input)?;
            (kind, types, true)
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
        QueryOperationV1::InnerJoin { .. } => unreachable!(),
    };
    Ok((input.input, kind, types, stateful))
}
