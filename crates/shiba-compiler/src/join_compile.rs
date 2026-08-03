use shiba_operator::{NodeInput, OperatorNodeKind, ValueType};

use crate::binding::identity_for;
use crate::expression::{InputBinding, slot};
use crate::graph::CompiledInput;
use crate::{CompilerError, IdentityIndexDescriptor, QueryOperationV1, SourceDescriptor};

pub(crate) fn compile(
    operation: &QueryOperationV1,
    inputs: &[CompiledInput<'_>],
    indexes: &[Option<IdentityIndexDescriptor>],
    descriptors: &[SourceDescriptor],
) -> Result<(NodeInput, OperatorNodeKind, Vec<ValueType>, bool), CompilerError> {
    let QueryOperationV1::InnerJoin {
        left_id,
        left_key,
        right_id,
        right_payload,
    } = operation
    else {
        return Err(CompilerError::InvalidSpec);
    };
    let [left, right] = inputs else {
        return Err(CompilerError::InvalidTopology);
    };
    let (InputBinding::Source(left_source), InputBinding::Source(right_source)) =
        (&left.binding, &right.binding)
    else {
        return Err(CompilerError::InvalidTopology);
    };
    if descriptors.len() != 2 || left_source.source_id == right_source.source_id {
        return Err(CompilerError::InvalidTopology);
    }
    let left_id_slot = join_slot(left_id, 0, left_source)?;
    let left_key_slot = join_slot(left_key, 0, left_source)?;
    let right_id_slot = join_slot(right_id, 1, right_source)?;
    let right_payload_slot = join_slot(right_payload, 1, right_source)?;
    for (layout, position) in [
        (&left.layout, left_id_slot),
        (&left.layout, left_key_slot),
        (&right.layout, right_id_slot),
        (&right.layout, right_payload_slot),
    ] {
        if layout.value_types.get(usize::from(position)) != Some(&ValueType::Int8) {
            return Err(CompilerError::WrongType);
        }
    }
    if left_source.columns[usize::from(left_id_slot)].nullable
        || right_source.columns[usize::from(right_id_slot)].nullable
    {
        return Err(CompilerError::InvalidIdentityIndex);
    }
    let right_index = indexes
        .iter()
        .flatten()
        .find(|index| index.relation == right_source.relation)
        .ok_or(CompilerError::InvalidIdentityIndex)?;
    let exact = identity_for(right_source, core::slice::from_ref(right_index))?;
    if exact.key_column != right_source.columns[usize::from(right_id_slot)].address {
        return Err(CompilerError::InvalidIdentityIndex);
    }
    Ok((
        left.input,
        OperatorNodeKind::InnerJoin {
            left_source_id: left_source.source_id,
            right_source_id: right_source.source_id,
            left_id_slot,
            left_key_slot,
            right_id_slot,
            right_payload_slot,
        },
        vec![ValueType::Int8, ValueType::Int8],
        true,
    ))
}

fn join_slot(
    field: &crate::QueryFieldV1,
    expected: u8,
    source: &SourceDescriptor,
) -> Result<u16, CompilerError> {
    if field.input != expected {
        return Err(CompilerError::InvalidTopology);
    }
    let normalized = crate::QueryFieldV1 {
        input: 0,
        selector: field.selector.clone(),
    };
    slot(&normalized, InputBinding::Source(source))
}
