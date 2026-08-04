use crate::graph_budget::EvaluationBudget;
use crate::{
    AggregateCall, DeltaBatch, Expression, KernelError, NodeId, NodeInput, OperatorGraph,
    OperatorNodeKind, OutputContract, TypedLayout,
};

#[derive(Clone)]
pub(crate) struct AggregateSpec {
    pub node_id: NodeId,
    pub groups: Vec<Expression>,
    pub calls: Vec<AggregateCall>,
    pub input_layout: TypedLayout,
    pub output_layout: TypedLayout,
    pub materialize_id: NodeId,
    pub field_slots: Vec<u16>,
    pub output: OutputContract,
}

pub(crate) fn specs(graph: &OperatorGraph) -> Result<Vec<AggregateSpec>, KernelError> {
    let (_, layouts) = graph.layouts().map_err(|_| KernelError::InvalidGraph)?;
    graph
        .nodes
        .iter()
        .filter_map(|node| {
            let OperatorNodeKind::Aggregate {
                group_expressions,
                calls,
            } = &node.kind
            else {
                return None;
            };
            Some(build(graph, &layouts, node, group_expressions, calls))
        })
        .collect()
}

fn build(
    graph: &OperatorGraph,
    layouts: &std::collections::BTreeMap<NodeId, TypedLayout>,
    node: &crate::OperatorNode,
    groups: &[Expression],
    calls: &[AggregateCall],
) -> Result<AggregateSpec, KernelError> {
    let input_layout = match node.input {
        NodeInput::SourcePort(source_id) => {
            let port = graph
                .sources
                .iter()
                .find(|port| port.source_id == source_id)
                .ok_or(KernelError::InvalidGraph)?;
            crate::source_typed_layout(source_id, &port.layout)
                .map_err(|_| KernelError::InvalidGraph)?
        }
        NodeInput::Node(id) => layouts.get(&id).ok_or(KernelError::InvalidGraph)?.clone(),
    };
    let terminal = graph
        .nodes
        .iter()
        .find(|candidate| candidate.input == NodeInput::Node(node.node_id))
        .ok_or(KernelError::InvalidGraph)?;
    let OperatorNodeKind::Materialize {
        field_slots,
        output,
    } = &terminal.kind
    else {
        return Err(KernelError::InvalidGraph);
    };
    Ok(AggregateSpec {
        node_id: node.node_id,
        groups: groups.to_vec(),
        calls: calls.to_vec(),
        input_layout,
        output_layout: layouts
            .get(&node.node_id)
            .ok_or(KernelError::InvalidGraph)?
            .clone(),
        materialize_id: terminal.node_id,
        field_slots: field_slots.clone(),
        output: output.clone(),
    })
}

pub(crate) fn prepare(
    graph: &OperatorGraph,
    batch: &DeltaBatch,
    spec: &AggregateSpec,
) -> Result<DeltaBatch, KernelError> {
    let mut chain = Vec::new();
    let mut input = graph
        .nodes
        .iter()
        .find(|node| node.node_id == spec.node_id)
        .ok_or(KernelError::InvalidGraph)?
        .input;
    while let NodeInput::Node(id) = input {
        let node = graph
            .nodes
            .iter()
            .find(|node| node.node_id == id)
            .ok_or(KernelError::InvalidGraph)?;
        if !matches!(
            node.kind,
            OperatorNodeKind::Filter { .. }
                | OperatorNodeKind::Project { .. }
                | OperatorNodeKind::Compute { .. }
                | OperatorNodeKind::KeyBy { .. }
        ) {
            return Err(KernelError::InvalidGraph);
        }
        chain.push(node);
        input = node.input;
    }
    chain.reverse();
    let (_, layouts) = graph.layouts().map_err(|_| KernelError::InvalidGraph)?;
    let mut current = batch.clone();
    let mut layout = match input {
        NodeInput::SourcePort(source_id) => {
            let port = graph
                .sources
                .iter()
                .find(|port| port.source_id == source_id)
                .ok_or(KernelError::InvalidGraph)?;
            crate::source_typed_layout(source_id, &port.layout)
                .map_err(|_| KernelError::InvalidGraph)?
        }
        NodeInput::Node(_) => return Err(KernelError::InvalidGraph),
    };
    let mut budget = EvaluationBudget::new(batch).map_err(|_| KernelError::InvalidTransition)?;
    let mut emitted = 0;
    for node in chain {
        let output = layouts
            .get(&node.node_id)
            .ok_or(KernelError::InvalidGraph)?;
        current = crate::graph_eval::transform_node(&node.kind, &current, &layout, output)
            .map_err(|_| KernelError::InvalidTransition)?;
        budget
            .charge(&current, &mut emitted)
            .map_err(|_| KernelError::InvalidTransition)?;
        layout = output.clone();
    }
    if layout != spec.input_layout {
        return Err(KernelError::InvalidGraph);
    }
    Ok(current)
}
