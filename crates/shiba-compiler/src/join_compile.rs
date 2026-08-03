use shiba_operator::{
    NodeInput, OperatorNode, OperatorNodeKind, OutputContract, SourcePort, StateContract, ValueType,
};

use crate::binding::{int8, source};
use crate::{CompilerError, IdentityIndexDescriptor, SourceDescriptor};

pub(crate) struct JoinArgs<'a> {
    pub(crate) left_source_id: shiba_protocol::SourceId,
    pub(crate) right_source_id: shiba_protocol::SourceId,
    pub(crate) names: [&'a String; 4],
    pub(crate) identity_index: shiba_operator::ObjectAddress,
    pub(crate) node_ids: (shiba_operator::NodeId, shiba_operator::NodeId),
}

pub(crate) fn compile_join(
    sources: &[SourceDescriptor],
    indexes: &[IdentityIndexDescriptor],
    ports: &mut [SourcePort],
    nodes: &mut Vec<OperatorNode>,
    args: &JoinArgs<'_>,
) -> Result<(), CompilerError> {
    let left = source(sources, args.left_source_id)?;
    let right = source(sources, args.right_source_id)?;
    let (left_id_slot, left_id) = int8(left, args.names[0])?;
    let (left_key_slot, _) = int8(left, args.names[1])?;
    let (right_id_slot, right_id) = int8(right, args.names[2])?;
    let (right_payload_slot, _) = int8(right, args.names[3])?;
    let index = indexes
        .iter()
        .find(|index| index.address == args.identity_index)
        .ok_or(CompilerError::InvalidIdentityIndex)?;
    if index.address != args.identity_index
        || left_id.nullable
        || right_id.nullable
        || index.relation != right.relation
        || index.key_column != right_id.address
        || !index.unique
        || !index.valid
        || !index.ready
        || index.has_expression
        || index.has_predicate
        || !index.effective_replica_identity
    {
        return Err(CompilerError::InvalidIdentityIndex);
    }
    let port = ports
        .iter_mut()
        .find(|port| port.source_id == args.right_source_id)
        .ok_or(CompilerError::SourceMismatch)?;
    if port.identity_index != Some(index.address) {
        return Err(CompilerError::InvalidIdentityIndex);
    }
    nodes.push(OperatorNode {
        node_id: args.node_ids.0,
        input: NodeInput::SourcePort(args.left_source_id),
        state_contract: Some(StateContract { codec_version: 1 }),
        kind: OperatorNodeKind::InnerJoin {
            left_source_id: args.left_source_id,
            right_source_id: args.right_source_id,
            left_id_slot,
            left_key_slot,
            right_id_slot,
            right_payload_slot,
        },
    });
    nodes.push(crate::graph::materialize(
        args.node_ids.1,
        args.node_ids.0,
        OutputContract::KeyedRows {
            key_type: ValueType::Int8,
            key_nullable: false,
            value_type: ValueType::Int8,
            nullable: true,
        },
        true,
    ));
    Ok(())
}
