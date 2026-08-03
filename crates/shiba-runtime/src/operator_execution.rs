use std::collections::BTreeMap;

use postgres::Transaction;
use shiba_operator::{
    DeltaBatch, EffectOrigin, GraphEffectOrigin, MultiInputBatch, OperatorGraph, SourceDeltaBatch,
    apply_graph_plan, source_typed_layout,
};
use shiba_protocol::{BootstrapBatchId, BootstrapId, GraphId, SourceId};

use crate::{M2Error, keyed_state, result_sink};

pub(crate) struct LockedGraph {
    pub(crate) graph_id: i64,
    pub(crate) digest: [u8; 32],
    pub(crate) graph: OperatorGraph,
}

pub(crate) fn load_locked_graph(
    transaction: &mut Transaction<'_>,
    graph_id: i64,
    generation: i64,
) -> Result<LockedGraph, M2Error> {
    let row = transaction
        .query_opt(
            "SELECT definition.source_count, definition.graph_format_version,
                    definition.graph_payload, definition.graph_digest,
                    definition.state_codec_version, config.slot_generation
             FROM shiba_internal.graph_definition AS definition
             JOIN shiba_internal.graph_ingress_config AS config USING (graph_id)
             WHERE definition.graph_id = $1
             FOR UPDATE OF definition, config",
            &[&graph_id],
        )?
        .ok_or(M2Error::MissingSourceOperator)?;
    if row.get::<_, i64>(5) != generation || row.get::<_, i32>(4) != 1 {
        return Err(M2Error::SlotGenerationMismatch);
    }
    let digest = digest(row.get(3))?;
    let graph = OperatorGraph::from_canonical_payload(&row.get::<_, Vec<u8>>(2), digest)
        .map_err(|_| M2Error::InvalidOperatorDefinition)?;
    let expected_graph = u64::try_from(graph_id)
        .ok()
        .and_then(|value| GraphId::new(value).ok())
        .ok_or(M2Error::InvalidOperatorDefinition)?;
    if graph.graph_id != expected_graph
        || u32::try_from(row.get::<_, i32>(1)).ok() != Some(graph.format_version)
        || usize::try_from(row.get::<_, i16>(0)).ok() != Some(graph.sources.len())
    {
        return Err(M2Error::InvalidOperatorDefinition);
    }
    validate_members(transaction, graph_id, digest, &graph)?;
    Ok(LockedGraph {
        graph_id,
        digest,
        graph,
    })
}

pub(crate) fn execute(
    transaction: &mut Transaction<'_>,
    locked_graph: &LockedGraph,
    batch: &MultiInputBatch,
    publish: bool,
) -> Result<(), M2Error> {
    execute_mode(transaction, locked_graph, batch, publish, false)
}

fn execute_mode(
    transaction: &mut Transaction<'_>,
    locked_graph: &LockedGraph,
    batch: &MultiInputBatch,
    publish: bool,
    activate: bool,
) -> Result<(), M2Error> {
    validate_batch_inputs(&locked_graph.graph, batch)?;
    let expected_status = if publish { "active" } else { "building" };
    let state = keyed_state::load(
        transaction,
        locked_graph.graph_id,
        &locked_graph.graph,
        batch,
    )?;
    let transition = apply_graph_plan(&locked_graph.graph, &state.snapshot, batch)?;
    let results = result_sink::lock(transaction, locked_graph.graph_id, expected_status)?;
    keyed_state::persist(
        transaction,
        locked_graph.graph_id,
        &state,
        transition.state_deltas,
    )?;
    result_sink::persist(
        transaction,
        locked_graph.graph_id,
        &results,
        transition.results,
        publish,
        activate,
    )
}

pub(crate) fn activate_results(
    transaction: &mut Transaction<'_>,
    locked_graph: &LockedGraph,
    bootstrap_id: i64,
) -> Result<(), M2Error> {
    let bootstrap_id = u64::try_from(bootstrap_id)
        .ok()
        .and_then(|value| BootstrapId::new(value).ok())
        .ok_or(M2Error::BootstrapIdentityConflict)?;
    let batch_id =
        BootstrapBatchId::new(bootstrap_id, 1).map_err(|_| M2Error::BootstrapIdentityConflict)?;
    let sources = locked_graph
        .graph
        .sources
        .iter()
        .map(|source| {
            let layout = source_typed_layout(source.source_id, &source.layout)
                .map_err(|_| M2Error::InvalidOperatorDefinition)?;
            Ok(SourceDeltaBatch {
                source_id: source.source_id,
                delta: DeltaBatch {
                    origin: EffectOrigin::Bootstrap(batch_id),
                    layout_identity: layout.identity,
                    rows: Vec::new(),
                },
            })
        })
        .collect::<Result<Vec<_>, M2Error>>()?;
    let batch = MultiInputBatch {
        origin: GraphEffectOrigin::Bootstrap(batch_id),
        sources,
    };
    execute_mode(transaction, locked_graph, &batch, false, true)
}

fn validate_members(
    transaction: &mut Transaction<'_>,
    graph_id: i64,
    digest: [u8; 32],
    graph: &OperatorGraph,
) -> Result<(), M2Error> {
    let rows = transaction.query(
        "SELECT source_id, input_ordinal, graph_digest
         FROM shiba_internal.graph_source_member WHERE graph_id = $1
         ORDER BY input_ordinal FOR UPDATE",
        &[&graph_id],
    )?;
    if rows.len() != graph.sources.len() {
        return Err(M2Error::InvalidOperatorDefinition);
    }
    for (row, source) in rows.iter().zip(&graph.sources) {
        if row.get::<_, i64>(0) != as_bigint(source.source_id)?
            || usize::try_from(row.get::<_, i16>(1)).ok()
                != Some(
                    graph
                        .sources
                        .iter()
                        .position(|value| value == source)
                        .unwrap(),
                )
            || row.get::<_, Vec<u8>>(2).as_slice() != digest
        {
            return Err(M2Error::InvalidOperatorDefinition);
        }
    }
    Ok(())
}

fn validate_batch_inputs(graph: &OperatorGraph, batch: &MultiInputBatch) -> Result<(), M2Error> {
    if batch.sources.len() != graph.sources.len() {
        return Err(M2Error::InvalidOperatorDefinition);
    }
    let actual: BTreeMap<SourceId, [u8; 32]> = batch
        .sources
        .iter()
        .map(|source| (source.source_id, source.delta.layout_identity))
        .collect();
    for source in &graph.sources {
        let layout = source_typed_layout(source.source_id, &source.layout)
            .map_err(|_| M2Error::InvalidOperatorDefinition)?;
        if actual.get(&source.source_id) != Some(&layout.identity) {
            return Err(M2Error::InvalidOperatorDefinition);
        }
    }
    Ok(())
}

fn as_bigint(source_id: SourceId) -> Result<i64, M2Error> {
    i64::try_from(source_id.get()).map_err(|_| M2Error::InvalidOperatorDefinition)
}

fn digest(value: Vec<u8>) -> Result<[u8; 32], M2Error> {
    value
        .try_into()
        .map_err(|_| M2Error::InvalidOperatorDefinition)
}
