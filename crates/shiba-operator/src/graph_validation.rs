use std::collections::BTreeMap;

use sha2::{Digest, Sha256};
use shiba_protocol::SourceId;

use crate::{
    ColumnBinding, Expression, GraphError, NodeId, NodeInput, OperatorNode, OperatorNodeKind,
    TypedLayout, ValueType, graph::CanonicalGraph,
};

const LAYOUT_DOMAIN: &[u8] = b"shiba.operator.layout.v1\0";

pub(crate) fn validate_graph(graph: &CanonicalGraph) -> Result<(), GraphError> {
    crate::graph_topology::validate_topology(graph)?;
    let (_, layouts) = layout_graph(graph)?;
    let materialized = graph
        .nodes
        .iter()
        .filter(|node| matches!(node.kind, OperatorNodeKind::Materialize { .. }))
        .count();
    if layouts.len() + materialized != graph.nodes.len() {
        return Err(GraphError::MissingResult);
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
            join_output_types(
                graph,
                left_source_id,
                right_source_id,
                [
                    left_id_slot,
                    left_key_slot,
                    right_id_slot,
                    right_payload_slot,
                ],
            )?
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

fn join_output_types(
    graph: &CanonicalGraph,
    left_source_id: SourceId,
    right_source_id: SourceId,
    slots: [u16; 4],
) -> Result<Option<Vec<ValueType>>, GraphError> {
    let left = graph
        .sources
        .iter()
        .find(|port| port.source_id == left_source_id)
        .ok_or(GraphError::InvalidTopology)?;
    let left_layout = source_typed_layout(left.source_id, &left.layout)?;
    let right = graph
        .sources
        .iter()
        .find(|port| port.source_id == right_source_id)
        .ok_or(GraphError::InvalidTopology)?;
    let right_layout = source_typed_layout(right.source_id, &right.layout)?;
    if left_layout.value_types.get(usize::from(slots[0])) != Some(&ValueType::Int8)
        || left_layout.value_types.get(usize::from(slots[1])) != Some(&ValueType::Int8)
        || right_layout.value_types.get(usize::from(slots[2])) != Some(&ValueType::Int8)
        || right_layout.value_types.get(usize::from(slots[3])) != Some(&ValueType::Int8)
        || right.identity_index.is_none()
    {
        return Err(GraphError::WrongType);
    }
    Ok(Some(vec![ValueType::Int8, ValueType::Int8]))
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
            field_slots,
            output,
        } => {
            output.validate().map_err(|_| GraphError::WrongType)?;
            let valid = field_slots.len() == output.schema.fields.len()
                && field_slots
                    .iter()
                    .zip(&output.schema.fields)
                    .all(|(slot, field)| {
                        input.value_types.get(usize::from(*slot)) == Some(&field.value_type)
                    });
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
