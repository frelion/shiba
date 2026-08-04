use std::collections::BTreeSet;

use crate::{
    GraphError, NodeInput, OperatorNodeKind,
    graph::{CanonicalGraph, GRAPH_FORMAT_VERSION, MAX_GRAPH_NODES},
};

pub(crate) fn validate_topology(graph: &CanonicalGraph) -> Result<(), GraphError> {
    if graph.format_version != GRAPH_FORMAT_VERSION
        || graph.nodes.is_empty()
        || graph.nodes.len() > MAX_GRAPH_NODES
        || !(1..=2).contains(&graph.sources.len())
        || graph
            .sources
            .windows(2)
            .any(|pair| pair[0].source_id >= pair[1].source_id)
        || graph.sources.iter().enumerate().any(|(index, source)| {
            source.identity_index.is_some()
                && graph.sources[index + 1..]
                    .iter()
                    .any(|other| other.identity_index == source.identity_index)
        })
    {
        return Err(GraphError::InvalidTopology);
    }
    validate_identity_free_source(graph)?;
    validate_result_references(graph)?;
    validate_stateful_topology(graph)?;
    validate_join_topology(graph)
}

fn validate_identity_free_source(graph: &CanonicalGraph) -> Result<(), GraphError> {
    if graph.sources.iter().any(|source| {
        source.identity_index.is_none() && (graph.sources.len() != 1 || !source.layout.is_empty())
    }) || graph.sources.iter().any(|source| {
        source.identity_index.is_none()
            && graph.nodes.iter().any(|node| match &node.kind {
                OperatorNodeKind::Aggregate {
                    group_expressions,
                    calls,
                    ..
                } => {
                    !group_expressions.is_empty()
                        || !calls.iter().all(|call| {
                            matches!(
                                crate::aggregate_function_descriptor(call.function).input,
                                crate::AggregateInputContract::None
                            )
                        })
                }
                OperatorNodeKind::Materialize { .. } => false,
                _ => true,
            })
    }) {
        return Err(GraphError::InvalidTopology);
    }
    Ok(())
}

fn validate_result_references(graph: &CanonicalGraph) -> Result<(), GraphError> {
    if !graph
        .nodes
        .iter()
        .any(|node| matches!(node.kind, OperatorNodeKind::Materialize { .. }))
    {
        return Err(GraphError::MissingResult);
    }
    let referenced = graph
        .nodes
        .iter()
        .filter_map(|node| match node.input {
            NodeInput::Node(id) => Some(id),
            NodeInput::SourcePort(_) => None,
        })
        .collect::<BTreeSet<_>>();
    if graph.nodes.iter().any(|node| {
        !matches!(node.kind, OperatorNodeKind::Materialize { .. })
            && !referenced.contains(&node.node_id)
    }) {
        return Err(GraphError::InvalidTopology);
    }
    Ok(())
}

fn validate_join_topology(graph: &CanonicalGraph) -> Result<(), GraphError> {
    let joins = graph
        .nodes
        .iter()
        .filter(|node| matches!(node.kind, OperatorNodeKind::InnerJoin { .. }))
        .collect::<Vec<_>>();
    if joins.is_empty() {
        return if graph.sources.len() == 1 {
            Ok(())
        } else {
            Err(GraphError::InvalidTopology)
        };
    }
    if joins.len() != 1 || graph.nodes.len() != 2 {
        return Err(GraphError::InvalidTopology);
    }
    let join = joins[0];
    let OperatorNodeKind::InnerJoin {
        left_source_id,
        right_source_id,
        ..
    } = join.kind
    else {
        unreachable!()
    };
    if !graph
        .sources
        .iter()
        .any(|source| source.source_id == left_source_id)
        || !graph
            .sources
            .iter()
            .any(|source| source.source_id == right_source_id && source.identity_index.is_some())
        || left_source_id == right_source_id
        || join.input != NodeInput::SourcePort(left_source_id)
        || graph.nodes[1].input != NodeInput::Node(join.node_id)
        || !matches!(graph.nodes[1].kind, OperatorNodeKind::Materialize { .. })
    {
        return Err(GraphError::InvalidTopology);
    }
    Ok(())
}

fn validate_stateful_topology(graph: &CanonicalGraph) -> Result<(), GraphError> {
    for aggregate in &graph.nodes {
        let valid_input = match aggregate.kind {
            OperatorNodeKind::Aggregate { .. } => aggregate_input_is_linear(graph, aggregate),
            _ => continue,
        };
        let materialized = graph
            .nodes
            .iter()
            .filter(|node| {
                node.input == NodeInput::Node(aggregate.node_id)
                    && matches!(node.kind, OperatorNodeKind::Materialize { .. })
            })
            .count();
        let fanout = graph
            .nodes
            .iter()
            .filter(|node| node.input == NodeInput::Node(aggregate.node_id))
            .count();
        if !valid_input || materialized != 1 || fanout != 1 {
            return Err(GraphError::InvalidTopology);
        }
    }
    Ok(())
}

fn aggregate_input_is_linear(graph: &CanonicalGraph, aggregate: &crate::OperatorNode) -> bool {
    let mut input = aggregate.input;
    loop {
        match input {
            NodeInput::SourcePort(source_id) => {
                return graph
                    .sources
                    .iter()
                    .any(|source| source.source_id == source_id);
            }
            NodeInput::Node(id) => {
                let Some(node) = graph.nodes.iter().find(|node| node.node_id == id) else {
                    return false;
                };
                if !matches!(
                    node.kind,
                    OperatorNodeKind::Filter { .. }
                        | OperatorNodeKind::Project { .. }
                        | OperatorNodeKind::Compute { .. }
                        | OperatorNodeKind::KeyBy { .. }
                ) {
                    return false;
                }
                input = node.input;
            }
        }
    }
}
