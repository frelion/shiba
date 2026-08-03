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
            NodeInput::Source => None,
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

pub(crate) fn layout_graph(
    graph: &CanonicalGraph,
) -> Result<(TypedLayout, BTreeMap<NodeId, TypedLayout>), GraphError> {
    let source = source_typed_layout(graph.source_id, &graph.source_layout)?;
    let mut layouts = BTreeMap::new();
    let mut previous = None;
    for node in &graph.nodes {
        if previous.is_some_and(|id| node.node_id <= id) {
            return Err(GraphError::InvalidTopology);
        }
        previous = Some(node.node_id);
        if node
            .state_contract
            .is_some_and(|contract| contract.codec_version != 1)
        {
            return Err(GraphError::InvalidStateContract);
        }
        let input = match node.input {
            NodeInput::Source => &source,
            NodeInput::Node(id) => layouts.get(&id).ok_or(GraphError::InvalidTopology)?,
        };
        if let Some(types) = node_output_types(node, input)? {
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
        OperatorNodeKind::Materialize {
            key_slot,
            value_slot,
            output,
        } => {
            if input.value_types.get(usize::from(*key_slot)) != Some(&ValueType::Int8)
                || input.value_types.get(usize::from(*value_slot)) != Some(&ValueType::Int8)
                || !matches!(
                    output,
                    OutputContract::KeyedRows {
                        key_type: ValueType::Int8,
                        value_type: ValueType::Int8,
                        nullable: true
                    }
                )
            {
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
