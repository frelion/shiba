use shiba_operator::{
    Expression, NodeInput, OperatorGraph, OperatorNode, OperatorNodeKind, OutputContract,
    SourcePort, StateContract, ValueType,
};

use crate::binding::{identity_for, int8, source, source_port};
use crate::join_compile::{JoinArgs, compile_join};
use crate::pipeline::compile_pipeline;
use crate::{
    CompilerError, GRAPH_SPEC_VERSION, GraphOutputSpecV1, GraphSpecV1, IdentityIndexDescriptor,
    SourceDescriptor,
};

/// Compiles one strict declaration into its only durable canonical graph.
///
/// # Errors
///
/// Rejects source, column, index, topology, type, or canonical encoding drift.
pub fn compile_graph(
    spec: &GraphSpecV1,
    descriptors: &[SourceDescriptor],
    indexes: &[IdentityIndexDescriptor],
) -> Result<OperatorGraph, CompilerError> {
    let indexes = indexes.iter().cloned().map(Some).collect::<Vec<_>>();
    compile_graph_with_optional_identities(spec, descriptors, &indexes)
}

/// Compiles a graph whose only identity-free shape is the previously proven
/// singleton zero-column `CountRows` source.
///
/// # Errors
///
/// Rejects every other missing, extra, or invalid source identity.
pub fn compile_graph_with_optional_identities(
    spec: &GraphSpecV1,
    descriptors: &[SourceDescriptor],
    indexes: &[Option<IdentityIndexDescriptor>],
) -> Result<OperatorGraph, CompilerError> {
    let canonical_spec = spec
        .to_canonical_json()
        .ok()
        .and_then(|bytes| GraphSpecV1::from_json(&bytes).ok());
    if canonical_spec.as_ref() != Some(spec)
        || spec.version != GRAPH_SPEC_VERSION
        || descriptors
            .iter()
            .map(|source| source.source_id)
            .collect::<Vec<_>>()
            != spec.sources
        || spec.sources.len() == 2
            && (spec.outputs.len() != 1
                || !matches!(spec.outputs[0], GraphOutputSpecV1::InnerJoin { .. }))
        || indexes.len() != descriptors.len()
    {
        return Err(CompilerError::InvalidSpec);
    }
    let mut sources = descriptors
        .iter()
        .zip(indexes)
        .map(|(source, index)| {
            let identity = index
                .as_ref()
                .map(|index| identity_for(source, std::slice::from_ref(index)))
                .transpose()?
                .map(|index| index.address);
            source_port(source, identity)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let exact_indexes = indexes
        .iter()
        .filter_map(Option::as_ref)
        .cloned()
        .collect::<Vec<_>>();
    let mut nodes = Vec::new();
    for output in &spec.outputs {
        compile_output(
            output,
            descriptors,
            &exact_indexes,
            &mut sources,
            &mut nodes,
        )?;
    }
    nodes.sort_by_key(|node| node.node_id);
    OperatorGraph::build(spec.graph_id, sources, nodes).map_err(|_| CompilerError::GraphEncoding)
}

fn compile_output(
    output: &GraphOutputSpecV1,
    sources: &[SourceDescriptor],
    indexes: &[IdentityIndexDescriptor],
    ports: &mut [SourcePort],
    nodes: &mut Vec<OperatorNode>,
) -> Result<(), CompilerError> {
    if let Some(result) = compile_pipeline(output, sources, nodes) {
        return result;
    }
    if let Some(result) = compile_scalar_output(output, sources, nodes) {
        return result;
    }
    match output {
        GraphOutputSpecV1::MaterializedProject {
            source_id,
            key_column,
            value_column,
            project_node_id,
            result_node_id,
        } => compile_project(
            source(sources, *source_id)?,
            key_column,
            value_column,
            (*project_node_id, *result_node_id),
            nodes,
        ),
        GraphOutputSpecV1::GroupedCount {
            source_id,
            key_column,
            key_node_id,
            aggregate_node_id,
            result_node_id,
        } => grouped_nodes(
            source(sources, *source_id)?,
            key_column,
            None,
            (*key_node_id, *aggregate_node_id, *result_node_id),
            nodes,
        ),
        GraphOutputSpecV1::GroupedSumInt8 {
            source_id,
            key_column,
            input_column,
            key_node_id,
            aggregate_node_id,
            result_node_id,
        } => grouped_nodes(
            source(sources, *source_id)?,
            key_column,
            Some(input_column),
            (*key_node_id, *aggregate_node_id, *result_node_id),
            nodes,
        ),
        GraphOutputSpecV1::CountRows { .. }
        | GraphOutputSpecV1::SumInt8 { .. }
        | GraphOutputSpecV1::ComputedProject { .. }
        | GraphOutputSpecV1::FilteredGroupedCount { .. }
        | GraphOutputSpecV1::FilteredGroupedSumInt8 { .. } => unreachable!(),
        GraphOutputSpecV1::InnerJoin {
            left_source_id,
            right_source_id,
            left_id_column,
            left_right_key_column,
            right_id_column,
            right_payload_column,
            right_identity_index,
            join_node_id,
            result_node_id,
        } => compile_join(
            sources,
            indexes,
            ports,
            nodes,
            &JoinArgs {
                left_source_id: *left_source_id,
                right_source_id: *right_source_id,
                names: [
                    left_id_column,
                    left_right_key_column,
                    right_id_column,
                    right_payload_column,
                ],
                identity_index: *right_identity_index,
                node_ids: (*join_node_id, *result_node_id),
            },
        ),
    }
}

fn compile_scalar_output(
    output: &GraphOutputSpecV1,
    sources: &[SourceDescriptor],
    nodes: &mut Vec<OperatorNode>,
) -> Option<Result<(), CompilerError>> {
    let (source_id, aggregate, result, input) = match output {
        GraphOutputSpecV1::CountRows {
            source_id,
            aggregate_node_id,
            result_node_id,
        } => (*source_id, *aggregate_node_id, *result_node_id, None),
        GraphOutputSpecV1::SumInt8 {
            source_id,
            input_column,
            aggregate_node_id,
            result_node_id,
        } => (
            *source_id,
            *aggregate_node_id,
            *result_node_id,
            Some(input_column),
        ),
        _ => return None,
    };
    Some(source(sources, source_id).and_then(|source| {
        let slot = input
            .map(|name| int8(source, name).map(|found| found.0))
            .transpose()?;
        scalar_nodes(source_id, aggregate, result, slot, nodes);
        Ok(())
    }))
}

fn compile_project(
    source: &SourceDescriptor,
    key_name: &str,
    value_name: &str,
    ids: (shiba_operator::NodeId, shiba_operator::NodeId),
    nodes: &mut Vec<OperatorNode>,
) -> Result<(), CompilerError> {
    let (key_slot, key) = int8(source, key_name)?;
    let (value_slot, _) = int8(source, value_name)?;
    if key.nullable {
        return Err(CompilerError::NullableKey(key_name.into()));
    }
    nodes.extend(project_nodes(
        source.source_id,
        ids.0,
        ids.1,
        key_slot,
        value_slot,
    ));
    Ok(())
}

fn scalar_nodes(
    source_id: shiba_protocol::SourceId,
    aggregate_id: shiba_operator::NodeId,
    result_id: shiba_operator::NodeId,
    sum_slot: Option<u16>,
    nodes: &mut Vec<OperatorNode>,
) {
    nodes.push(OperatorNode {
        node_id: aggregate_id,
        input: NodeInput::SourcePort(source_id),
        state_contract: Some(StateContract { codec_version: 1 }),
        kind: sum_slot.map_or(OperatorNodeKind::CountRows, |input_slot| {
            OperatorNodeKind::SumInt8 { input_slot }
        }),
    });
    nodes.push(materialize(
        result_id,
        aggregate_id,
        OutputContract::Scalar {
            value_type: ValueType::Int8,
        },
        false,
    ));
}

fn project_nodes(
    source_id: shiba_protocol::SourceId,
    project_id: shiba_operator::NodeId,
    result_id: shiba_operator::NodeId,
    key_slot: u16,
    value_slot: u16,
) -> Vec<OperatorNode> {
    vec![
        OperatorNode {
            node_id: project_id,
            input: NodeInput::SourcePort(source_id),
            state_contract: None,
            kind: OperatorNodeKind::Project {
                expressions: vec![
                    Expression::Column { slot: key_slot },
                    Expression::Column { slot: value_slot },
                ],
            },
        },
        materialize(
            result_id,
            project_id,
            OutputContract::KeyedRows {
                key_type: ValueType::Int8,
                key_nullable: false,
                value_type: ValueType::Int8,
                nullable: true,
            },
            true,
        ),
    ]
}

fn grouped_nodes(
    source: &SourceDescriptor,
    key_name: &str,
    value_name: Option<&String>,
    ids: (
        shiba_operator::NodeId,
        shiba_operator::NodeId,
        shiba_operator::NodeId,
    ),
    nodes: &mut Vec<OperatorNode>,
) -> Result<(), CompilerError> {
    let (key_slot, key) = int8(source, key_name)?;
    let value_slot = value_name
        .map(|name| int8(source, name).map(|found| found.0))
        .transpose()?;
    let grouped_key_slot =
        u16::try_from(source.columns.len()).map_err(|_| CompilerError::GraphEncoding)?;
    nodes.push(OperatorNode {
        node_id: ids.0,
        input: NodeInput::SourcePort(source.source_id),
        state_contract: None,
        kind: OperatorNodeKind::KeyBy {
            key: Expression::Column { slot: key_slot },
        },
    });
    nodes.push(OperatorNode {
        node_id: ids.1,
        input: NodeInput::Node(ids.0),
        state_contract: Some(StateContract { codec_version: 1 }),
        kind: value_slot.map_or(
            OperatorNodeKind::GroupedCount {
                key_slot: grouped_key_slot,
            },
            |value_slot| OperatorNodeKind::GroupedSumInt8 {
                key_slot: grouped_key_slot,
                value_slot,
            },
        ),
    });
    nodes.push(materialize(
        ids.2,
        ids.1,
        OutputContract::KeyedRows {
            key_type: ValueType::Int8,
            key_nullable: key.nullable,
            value_type: ValueType::Int8,
            nullable: value_slot.is_some(),
        },
        true,
    ));
    Ok(())
}

pub(crate) fn materialize(
    node_id: shiba_operator::NodeId,
    input: shiba_operator::NodeId,
    output: OutputContract,
    keyed: bool,
) -> OperatorNode {
    OperatorNode {
        node_id,
        input: NodeInput::Node(input),
        state_contract: None,
        kind: OperatorNodeKind::Materialize {
            key_slot: 0,
            value_slot: u16::from(keyed),
            output,
        },
    }
}
