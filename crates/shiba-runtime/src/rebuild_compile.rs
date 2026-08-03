use postgres::Transaction;
use shiba_compiler::{CompilerError, QuerySpecV1, compile_query};
use shiba_protocol::GraphId;

use crate::{
    M2Error,
    registration::{RebuildGraphArtifact, RebuildSourceTarget, RegistrationError},
    registration_write::{bigint, result_contracts},
};

pub(super) fn compile_rebuild_graph(
    transaction: &mut Transaction<'_>,
    graph_id: GraphId,
    targets: &[RebuildSourceTarget],
) -> Result<RebuildGraphArtifact, RegistrationError> {
    let graph_key = bigint(graph_id.get())?;
    let payload: Vec<u8> = transaction
        .query_opt(
            "SELECT spec_payload FROM shiba_internal.graph_definition
             WHERE graph_id = $1 FOR UPDATE",
            &[&graph_key],
        )?
        .ok_or(M2Error::InvalidOperatorDefinition)?
        .get(0);
    let spec = QuerySpecV1::from_json(&payload).map_err(|_| CompilerError::InvalidSpec)?;
    if spec.graph_id != graph_id
        || spec
            .to_canonical_json()
            .map_err(|_| CompilerError::GraphEncoding)?
            != payload
    {
        return Err(M2Error::InvalidOperatorDefinition.into());
    }
    if targets
        .iter()
        .map(|target| target.source_id)
        .collect::<Vec<_>>()
        != spec.sources
    {
        return Err(M2Error::InvalidOperatorDefinition.into());
    }
    let mut descriptors = Vec::with_capacity(targets.len());
    let mut indexes = Vec::with_capacity(targets.len());
    for target in targets {
        let (descriptor, identity) =
            crate::registration_descriptor::target_descriptor(transaction, *target)?;
        descriptors.push(descriptor);
        indexes.push(identity);
    }
    let graph = compile_query(&spec, &descriptors, &indexes)?;
    Ok(RebuildGraphArtifact {
        spec_payload: payload,
        graph_payload: graph.canonical_payload.clone(),
        graph_digest: graph.digest,
        results: result_contracts(&graph),
    })
}
