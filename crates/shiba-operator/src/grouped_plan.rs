use crate::graph_budget::EvaluationBudget;
use crate::grouped_state::Aggregate;
use crate::{
    DeltaBatch, KernelError, NodeId, NodeInput, OperatorGraph, OperatorNodeKind, TypedLayout,
};

pub(crate) struct GroupSpec {
    pub(crate) node_id: NodeId,
    pub(crate) key_slot: u16,
    pub(crate) aggregate: Aggregate,
    pub(crate) aggregate_layout: TypedLayout,
    pub(crate) materialize_id: NodeId,
    pub(crate) materialize_key_slot: u16,
    pub(crate) materialize_value_slot: u16,
    pub(crate) key_nullable: bool,
    pub(crate) value_nullable: bool,
}

pub(crate) fn prepare(
    graph: &OperatorGraph,
    batch: &DeltaBatch,
) -> Result<Option<(GroupSpec, DeltaBatch, EvaluationBudget, usize)>, KernelError> {
    let Some(spec) = group_spec(graph)? else {
        return Ok(None);
    };
    let (current, _, budget, emitted_rows) =
        crate::graph_eval::apply_prefix(graph, batch, spec.node_id)
            .map_err(|_| KernelError::InvalidGraph)?;
    Ok(Some((spec, current, budget, emitted_rows)))
}

fn group_spec(graph: &OperatorGraph) -> Result<Option<GroupSpec>, KernelError> {
    graph.validate().map_err(|_| KernelError::InvalidGraph)?;
    let aggregate = graph.nodes.iter().find_map(|node| match node.kind {
        OperatorNodeKind::GroupedCount { key_slot } => Some((node, key_slot, Aggregate::Count)),
        OperatorNodeKind::GroupedSumInt8 {
            key_slot,
            value_slot,
        } => Some((node, key_slot, Aggregate::Sum { value_slot })),
        _ => None,
    });
    let Some((aggregate_node, key_slot, aggregate)) = aggregate else {
        return Ok(None);
    };
    let NodeInput::Node(key_node_id) = aggregate_node.input else {
        return Err(KernelError::InvalidGraph);
    };
    let key_node = graph
        .nodes
        .iter()
        .find(|node| node.node_id == key_node_id)
        .ok_or(KernelError::InvalidGraph)?;
    if !matches!(key_node.kind, OperatorNodeKind::KeyBy { .. }) {
        return Err(KernelError::InvalidGraph);
    }
    let materialize = graph
        .nodes
        .iter()
        .find(|node| node.input == NodeInput::Node(aggregate_node.node_id))
        .ok_or(KernelError::InvalidGraph)?;
    let OperatorNodeKind::Materialize {
        key_slot: materialize_key_slot,
        value_slot: materialize_value_slot,
        output,
    } = &materialize.kind
    else {
        return Err(KernelError::InvalidGraph);
    };
    let crate::OutputContract::KeyedRows {
        key_nullable,
        nullable,
        ..
    } = output
    else {
        return Err(KernelError::InvalidGraph);
    };
    let (_, layouts) = graph.layouts().map_err(|_| KernelError::InvalidGraph)?;
    let aggregate_layout = layouts
        .get(&aggregate_node.node_id)
        .ok_or(KernelError::InvalidGraph)?
        .clone();
    Ok(Some(GroupSpec {
        node_id: aggregate_node.node_id,
        key_slot,
        aggregate,
        aggregate_layout,
        materialize_id: materialize.node_id,
        materialize_key_slot: *materialize_key_slot,
        materialize_value_slot: *materialize_value_slot,
        key_nullable: *key_nullable,
        value_nullable: *nullable,
    }))
}
