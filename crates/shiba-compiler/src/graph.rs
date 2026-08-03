use shiba_operator::OperatorGraph;

use crate::binding::{identity_for, source_port};
use crate::output_compile::compile_output;
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
