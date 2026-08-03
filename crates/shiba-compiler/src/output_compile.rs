use shiba_operator::{OperatorNode, SourcePort};

use crate::binding::{int8, source};
use crate::join_compile::{JoinArgs, compile_join};
use crate::node_builders::{grouped_nodes, project_nodes, scalar_nodes};
use crate::pipeline::compile_pipeline;
use crate::{CompilerError, GraphOutputSpecV1, IdentityIndexDescriptor, SourceDescriptor};

pub(crate) fn compile_output(
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
