use std::collections::{BTreeMap, BTreeSet};

use sha2::{Digest, Sha256};
use shiba_protocol::SourceId;

use crate::{
    ColumnBinding, Expression, GraphError, NodeId, NodeInput, OperatorNode, OperatorNodeKind,
    OutputContract, TypedLayout, ValueType,
    graph::{CanonicalGraph, GRAPH_FORMAT_VERSION, MAX_GRAPH_NODES},
};

const LAYOUT_DOMAIN: &[u8] = b"shiba.operator.layout.v1\0";

pub(crate) fn validate_graph(graph: &CanonicalGraph) -> Result<(), GraphError> {
    if graph.format_version != GRAPH_FORMAT_VERSION
        || graph.nodes.is_empty()
        || graph.nodes.len() > MAX_GRAPH_NODES
    {
        return Err(GraphError::InvalidTopology);
    }
    if !(1..=2).contains(&graph.sources.len())
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
    if graph.sources.iter().any(|source| {
        source.identity_index.is_none() && (graph.sources.len() != 1 || !source.layout.is_empty())
    }) || graph.sources.iter().any(|source| {
        source.identity_index.is_none()
            && graph.nodes.iter().any(|node| {
                !matches!(
                    node.kind,
                    OperatorNodeKind::CountRows | OperatorNodeKind::Materialize { .. }
                )
            })
    }) {
        return Err(GraphError::InvalidTopology);
    }
    let (_, layouts) = layout_graph(graph)?;
    let materialized = graph
        .nodes
        .iter()
        .filter(|node| matches!(node.kind, OperatorNodeKind::Materialize { .. }))
        .count();
    if materialized == 0 || layouts.len() + materialized != graph.nodes.len() {
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
    validate_stateful_topology(graph)?;
    validate_join_topology(graph)?;
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
            OperatorNodeKind::CountRows | OperatorNodeKind::SumInt8 { .. } => {
                matches!(aggregate.input, NodeInput::SourcePort(_))
            }
            OperatorNodeKind::GroupedCount { .. } | OperatorNodeKind::GroupedSumInt8 { .. } => {
                matches!(aggregate.input, NodeInput::Node(id) if graph.nodes.iter().any(|node| node.node_id == id && matches!(node.kind, OperatorNodeKind::KeyBy { .. })))
            }
            _ => continue,
        };
        let materialized = graph.nodes.iter().any(|node| {
            node.input == NodeInput::Node(aggregate.node_id)
                && matches!(node.kind, OperatorNodeKind::Materialize { .. })
        });
        if !valid_input || !materialized {
            return Err(GraphError::InvalidTopology);
        }
    }
    Ok(())
}

pub(crate) fn layout_graph(
    graph: &CanonicalGraph,
) -> Result<(TypedLayout, BTreeMap<NodeId, TypedLayout>), GraphError> {
    let primary = graph.sources.first().ok_or(GraphError::InvalidTopology)?;
    let source = source_typed_layout(primary.source_id, &primary.layout)?;
    let source_layouts = graph
        .sources
        .iter()
        .map(|port| {
            source_typed_layout(port.source_id, &port.layout).map(|layout| (port.source_id, layout))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    let mut layouts = BTreeMap::new();
    let mut previous = None;
    for node in &graph.nodes {
        if previous.is_some_and(|id| node.node_id <= id) {
            return Err(GraphError::InvalidTopology);
        }
        previous = Some(node.node_id);
        let stateful = matches!(
            node.kind,
            OperatorNodeKind::CountRows
                | OperatorNodeKind::SumInt8 { .. }
                | OperatorNodeKind::GroupedCount { .. }
                | OperatorNodeKind::GroupedSumInt8 { .. }
                | OperatorNodeKind::InnerJoin { .. }
        );
        if node
            .state_contract
            .is_some_and(|contract| contract.codec_version != 1)
            || (!stateful && node.state_contract.is_some())
        {
            return Err(GraphError::InvalidStateContract);
        }
        let input = match node.input {
            NodeInput::SourcePort(source_id) => source_layouts
                .get(&source_id)
                .ok_or(GraphError::InvalidTopology)?,
            NodeInput::Node(id) => layouts.get(&id).ok_or(GraphError::InvalidTopology)?,
        };
        let node_types = if let OperatorNodeKind::InnerJoin {
            left_source_id,
            right_source_id,
            left_id_slot,
            left_key_slot,
            right_id_slot,
            right_payload_slot,
        } = node.kind
        {
            let left_port = graph
                .sources
                .iter()
                .find(|port| port.source_id == left_source_id)
                .ok_or(GraphError::InvalidTopology)?;
            let input = source_typed_layout(left_port.source_id, &left_port.layout)?;
            let right_port = graph
                .sources
                .iter()
                .find(|port| port.source_id == right_source_id)
                .ok_or(GraphError::InvalidTopology)?;
            let right_layout = source_typed_layout(right_port.source_id, &right_port.layout)?;
            if input.value_types.get(usize::from(left_id_slot)) != Some(&ValueType::Int8)
                || input.value_types.get(usize::from(left_key_slot)) != Some(&ValueType::Int8)
                || right_layout.value_types.get(usize::from(right_id_slot))
                    != Some(&ValueType::Int8)
                || right_layout
                    .value_types
                    .get(usize::from(right_payload_slot))
                    != Some(&ValueType::Int8)
                || right_port.identity_index.is_none()
            {
                return Err(GraphError::WrongType);
            }
            Some(vec![ValueType::Int8, ValueType::Int8])
        } else {
            node_output_types(node, input)?
        };
        if let Some(types) = node_types {
            let output = if matches!(node.kind, OperatorNodeKind::Filter { .. }) {
                input.clone()
            } else {
                let bytes = serde_json::to_vec(&(input.identity, node.node_id, &types))
                    .map_err(|_| GraphError::Codec)?;
                TypedLayout::new(hash(&bytes), types)?
            };
            layouts.insert(node.node_id, output);
        }
    }
    Ok((source, layouts))
}

fn node_output_types(
    node: &OperatorNode,
    input: &TypedLayout,
) -> Result<Option<Vec<ValueType>>, GraphError> {
    Ok(match &node.kind {
        OperatorNodeKind::CountRows => {
            if node.state_contract.is_none() {
                return Err(GraphError::InvalidStateContract);
            }
            Some(vec![ValueType::Int8])
        }
        OperatorNodeKind::SumInt8 { input_slot } => {
            if node.state_contract.is_none()
                || input.value_types.get(usize::from(*input_slot)) != Some(&ValueType::Int8)
            {
                return Err(GraphError::WrongType);
            }
            Some(vec![ValueType::Int8])
        }
        OperatorNodeKind::Filter { predicate } if predicate.validate(input)? == ValueType::Bool => {
            Some(input.value_types.clone())
        }
        OperatorNodeKind::Filter { .. } => return Err(GraphError::WrongType),
        OperatorNodeKind::Project { expressions } => Some(expression_types(expressions, input)?),
        OperatorNodeKind::Compute { expressions } => {
            let mut values = input.value_types.clone();
            values.extend(expression_types(expressions, input)?);
            Some(values)
        }
        OperatorNodeKind::KeyBy { key } => {
            let mut values = input.value_types.clone();
            values.push(key.validate(input)?);
            Some(values)
        }
        OperatorNodeKind::GroupedCount { key_slot } => {
            if node.state_contract.is_none() {
                return Err(GraphError::InvalidStateContract);
            }
            let key = *input
                .value_types
                .get(usize::from(*key_slot))
                .ok_or(GraphError::WrongType)?;
            Some(vec![key, ValueType::Int8])
        }
        OperatorNodeKind::GroupedSumInt8 {
            key_slot,
            value_slot,
        } => {
            if node.state_contract.is_none() {
                return Err(GraphError::InvalidStateContract);
            }
            let key = *input
                .value_types
                .get(usize::from(*key_slot))
                .ok_or(GraphError::WrongType)?;
            if input.value_types.get(usize::from(*value_slot)) != Some(&ValueType::Int8) {
                return Err(GraphError::WrongType);
            }
            Some(vec![key, ValueType::Int8])
        }
        OperatorNodeKind::InnerJoin { .. } => return Err(GraphError::InvalidNode),
        OperatorNodeKind::Materialize {
            key_slot,
            value_slot,
            output,
        } => {
            let valid = match output {
                OutputContract::Scalar {
                    value_type: ValueType::Int8,
                } => input.value_types.get(usize::from(*value_slot)) == Some(&ValueType::Int8),
                OutputContract::KeyedRows {
                    key_type: ValueType::Int8,
                    key_nullable: _,
                    value_type: ValueType::Int8,
                    nullable: _,
                } => {
                    input.value_types.get(usize::from(*key_slot)) == Some(&ValueType::Int8)
                        && input.value_types.get(usize::from(*value_slot)) == Some(&ValueType::Int8)
                }
                _ => false,
            };
            if !valid {
                return Err(GraphError::WrongType);
            }
            None
        }
    })
}

fn expression_types(
    expressions: &[Expression],
    input: &TypedLayout,
) -> Result<Vec<ValueType>, GraphError> {
    if expressions.is_empty() {
        return Err(GraphError::InvalidNode);
    }
    expressions
        .iter()
        .map(|expression| expression.validate(input).map_err(Into::into))
        .collect()
}

/// Derives the source layout identity shared by Source Apply and a graph.
///
/// # Errors
///
/// Rejects an invalid layout identity or a layout wider than the fixed bound.
pub fn source_typed_layout(
    source_id: SourceId,
    bindings: &[ColumnBinding],
) -> Result<TypedLayout, GraphError> {
    let source_bytes = serde_json::to_vec(&(source_id, bindings)).map_err(|_| GraphError::Codec)?;
    TypedLayout::new(
        hash(&source_bytes),
        bindings.iter().map(|binding| binding.value_type).collect(),
    )
    .map_err(Into::into)
}

fn hash(bytes: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(LAYOUT_DOMAIN);
    hash.update(bytes);
    hash.finalize().into()
}
