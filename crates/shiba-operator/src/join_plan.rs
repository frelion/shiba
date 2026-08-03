use crate::{
    KernelError, NodeId, NodeInput, OperatorGraph, OperatorNodeKind, OutputContract, TypedLayout,
};
use shiba_protocol::SourceId;

pub(crate) struct JoinSpec {
    pub(crate) node_id: NodeId,
    pub(crate) left_source_id: SourceId,
    pub(crate) right_source_id: SourceId,
    pub(crate) left_id_slot: u16,
    pub(crate) left_key_slot: u16,
    pub(crate) right_id_slot: u16,
    pub(crate) right_payload_slot: u16,
    pub(crate) output_layout: TypedLayout,
    pub(crate) materialize_id: NodeId,
    pub(crate) key_nullable: bool,
    pub(crate) value_nullable: bool,
}

pub(crate) fn join_spec(graph: &OperatorGraph) -> Result<Option<JoinSpec>, KernelError> {
    graph.validate().map_err(|_| KernelError::InvalidGraph)?;
    let Some(join) = graph
        .nodes
        .iter()
        .find(|node| matches!(node.kind, OperatorNodeKind::InnerJoin { .. }))
    else {
        return Ok(None);
    };
    let OperatorNodeKind::InnerJoin {
        left_source_id,
        right_source_id,
        left_id_slot,
        left_key_slot,
        right_id_slot,
        right_payload_slot,
    } = join.kind
    else {
        unreachable!()
    };
    let materialize = graph
        .nodes
        .iter()
        .find(|node| node.input == NodeInput::Node(join.node_id))
        .ok_or(KernelError::InvalidGraph)?;
    let OperatorNodeKind::Materialize { output, .. } = &materialize.kind else {
        return Err(KernelError::InvalidGraph);
    };
    let OutputContract::KeyedRows {
        key_nullable,
        nullable,
        ..
    } = output
    else {
        return Err(KernelError::InvalidGraph);
    };
    let (_, layouts) = graph.layouts().map_err(|_| KernelError::InvalidGraph)?;
    Ok(Some(JoinSpec {
        node_id: join.node_id,
        left_source_id,
        right_source_id,
        left_id_slot,
        left_key_slot,
        right_id_slot,
        right_payload_slot,
        output_layout: layouts
            .get(&join.node_id)
            .ok_or(KernelError::InvalidGraph)?
            .clone(),
        materialize_id: materialize.node_id,
        key_nullable: *key_nullable,
        value_nullable: *nullable,
    }))
}
