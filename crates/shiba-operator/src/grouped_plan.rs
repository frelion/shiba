use crate::graph_budget::EvaluationBudget;
use crate::grouped_state::Aggregate;
use crate::{
    DeltaBatch, KernelError, NodeId, NodeInput, OperatorGraph, OperatorNodeKind, TypedLayout,
};

#[derive(Clone)]
pub(crate) struct GroupSpec {
    pub(crate) node_id: NodeId,
    pub(crate) key_node_id: NodeId,
    pub(crate) key_slot: u16,
    pub(crate) aggregate: Aggregate,
    pub(crate) aggregate_layout: TypedLayout,
    pub(crate) materialize_id: NodeId,
    pub(crate) materialize_field_slots: Vec<u16>,
    pub(crate) output: crate::OutputContract,
}

pub(crate) fn specs(graph: &OperatorGraph) -> Result<Vec<GroupSpec>, KernelError> {
    graph.validate().map_err(|_| KernelError::InvalidGraph)?;
    let (_, layouts) = graph.layouts().map_err(|_| KernelError::InvalidGraph)?;
    graph
        .nodes
        .iter()
        .filter_map(|node| {
            let aggregate = match node.kind {
                OperatorNodeKind::GroupedCount { key_slot } => (key_slot, Aggregate::Count),
                OperatorNodeKind::GroupedSumInt8 {
                    key_slot,
                    value_slot,
                } => (key_slot, Aggregate::Sum { value_slot }),
                _ => return None,
            };
            Some(build_spec(graph, &layouts, node, aggregate))
        })
        .collect()
}

fn build_spec(
    graph: &OperatorGraph,
    layouts: &std::collections::BTreeMap<NodeId, TypedLayout>,
    node: &crate::OperatorNode,
    aggregate: (u16, Aggregate),
) -> Result<GroupSpec, KernelError> {
    let NodeInput::Node(key_node_id) = node.input else {
        return Err(KernelError::InvalidGraph);
    };
    let key_node = graph
        .nodes
        .iter()
        .find(|candidate| candidate.node_id == key_node_id)
        .ok_or(KernelError::InvalidGraph)?;
    if !matches!(key_node.kind, OperatorNodeKind::KeyBy { .. }) {
        return Err(KernelError::InvalidGraph);
    }
    let materialize = graph
        .nodes
        .iter()
        .find(|candidate| candidate.input == NodeInput::Node(node.node_id))
        .ok_or(KernelError::InvalidGraph)?;
    let OperatorNodeKind::Materialize {
        field_slots,
        output,
    } = &materialize.kind
    else {
        return Err(KernelError::InvalidGraph);
    };
    if output.schema.is_scalar() {
        return Err(KernelError::InvalidGraph);
    }
    Ok(GroupSpec {
        node_id: node.node_id,
        key_node_id,
        key_slot: aggregate.0,
        aggregate: aggregate.1,
        aggregate_layout: layouts
            .get(&node.node_id)
            .ok_or(KernelError::InvalidGraph)?
            .clone(),
        materialize_id: materialize.node_id,
        materialize_field_slots: field_slots.clone(),
        output: output.clone(),
    })
}

pub(crate) fn prepare(
    graph: &OperatorGraph,
    batch: &DeltaBatch,
    spec: &GroupSpec,
) -> Result<(DeltaBatch, EvaluationBudget, usize), KernelError> {
    let (source_id, prefix) = linear_prefix(graph, spec.key_node_id)?;
    let port = graph
        .sources
        .iter()
        .find(|port| port.source_id == source_id)
        .ok_or(KernelError::InvalidGraph)?;
    let input_layout = crate::source_typed_layout(source_id, &port.layout)
        .map_err(|_| KernelError::InvalidGraph)?;
    let (_, layouts) = graph.layouts().map_err(|_| KernelError::InvalidGraph)?;
    let mut budget = EvaluationBudget::new(batch).map_err(|_| KernelError::InvalidTransition)?;
    let mut output = batch.clone();
    let mut current_layout = input_layout;
    let mut emitted = 0;
    for node in prefix {
        let output_layout = layouts
            .get(&node.node_id)
            .ok_or(KernelError::InvalidGraph)?;
        output =
            crate::graph_eval::transform_node(&node.kind, &output, &current_layout, output_layout)
                .map_err(|_| KernelError::InvalidTransition)?;
        budget
            .charge(&output, &mut emitted)
            .map_err(|_| KernelError::InvalidTransition)?;
        current_layout = output_layout.clone();
    }
    Ok((output, budget, emitted))
}

fn linear_prefix(
    graph: &OperatorGraph,
    terminal: NodeId,
) -> Result<(shiba_protocol::SourceId, Vec<&crate::OperatorNode>), KernelError> {
    let mut current = graph
        .nodes
        .iter()
        .find(|node| node.node_id == terminal)
        .ok_or(KernelError::InvalidGraph)?;
    let mut reversed = Vec::new();
    loop {
        if !matches!(
            current.kind,
            OperatorNodeKind::Filter { .. }
                | OperatorNodeKind::Project { .. }
                | OperatorNodeKind::Compute { .. }
                | OperatorNodeKind::KeyBy { .. }
        ) {
            return Err(KernelError::InvalidGraph);
        }
        reversed.push(current);
        match current.input {
            NodeInput::SourcePort(source_id) => {
                reversed.reverse();
                return Ok((source_id, reversed));
            }
            NodeInput::Node(input) => {
                current = graph
                    .nodes
                    .iter()
                    .find(|node| node.node_id == input)
                    .ok_or(KernelError::InvalidGraph)?;
            }
        }
    }
}
