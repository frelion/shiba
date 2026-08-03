use postgres::{Client, Transaction};
use shiba_operator::{
    EffectOrigin, GraphEffectOrigin, MultiInputBatch, RowDelta, SourceDeltaBatch,
};

use crate::bootstrap_model::BootstrapBatch;
use crate::source_batch::SourceLayout;
use crate::transaction::as_bigint;
use crate::{M2Error, SourcePayload, operator_execution, source_preflight};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BootstrapProcessOutcome {
    Applied,
    AlreadyApplied,
}

/// Atomically applies one member snapshot batch and advances its checkpoint.
///
/// # Errors
/// Fails closed on graph/source/digest/phase/order/state or sink drift.
pub fn process_bootstrap_batch(
    client: &mut Client,
    batch: &BootstrapBatch,
) -> Result<BootstrapProcessOutcome, M2Error> {
    let mut transaction = client.transaction()?;
    let outcome = process_batch(&mut transaction, batch)?;
    match outcome {
        BootstrapProcessOutcome::Applied => transaction.commit()?,
        BootstrapProcessOutcome::AlreadyApplied => transaction.rollback()?,
    }
    Ok(outcome)
}

/// Retires partial computation from an abandoned, non-active bootstrap attempt.
///
/// The caller owns the surrounding `PostgreSQL` transaction and must install the
/// exact forward successor lifecycle in that same transaction. This function is
/// the sole generic state/result writer; it never changes graph definition,
/// ingress configuration, checkpoint, or slot authority.
///
/// # Errors
/// Fails closed unless the exact graph/bootstrap is still non-active, building,
/// and has no WAL continuation.
pub fn reset_abandoned_bootstrap_state(
    transaction: &mut Transaction<'_>,
    graph_id: shiba_protocol::GraphId,
    bootstrap_id: shiba_protocol::BootstrapId,
) -> Result<(), M2Error> {
    let graph_key = as_bigint("graph_id", graph_id.get())?;
    let bootstrap_key = as_bigint("bootstrap_id", bootstrap_id.get())?;
    transaction.query_one(
        "SELECT pg_catalog.pg_advisory_xact_lock(
             '-4611686018427387904'::bigint + $1)",
        &[&graph_key],
    )?;
    let generation: i64 = transaction
        .query_opt(
            "SELECT slot_generation FROM shiba_internal.graph_ingress_config
             WHERE graph_id=$1",
            &[&graph_key],
        )?
        .ok_or(M2Error::BootstrapMissing)?
        .get(0);
    let graph = operator_execution::load_locked_graph(transaction, graph_key, generation)?;
    let phase: String = transaction
        .query_opt(
            "SELECT phase FROM shiba_internal.graph_bootstrap
             WHERE graph_id=$1 AND bootstrap_id=$2 FOR UPDATE",
            &[&graph_key, &bootstrap_key],
        )?
        .ok_or(M2Error::BootstrapIdentityConflict)?
        .get(0);
    if !matches!(
        phase.as_str(),
        "creating" | "scanning" | "cleanup_pending" | "failed"
    ) {
        return Err(M2Error::InvalidBootstrapPhase);
    }
    let continuation_exists: bool = transaction
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM shiba_internal.graph_continuation
                            WHERE graph_id=$1)",
            &[&graph_key],
        )?
        .get(0);
    if continuation_exists {
        return Err(M2Error::BootstrapIdentityConflict);
    }
    let results = crate::result_sink::lock(transaction, graph_key, "building")?;
    let source_ids = graph
        .graph
        .sources
        .iter()
        .map(|source| as_bigint("source_id", source.source_id.get()))
        .collect::<Result<Vec<_>, _>>()?;
    transaction.execute(
        "DELETE FROM shiba_internal.source_row_state WHERE source_id=ANY($1)",
        &[&source_ids],
    )?;
    transaction.execute(
        "DELETE FROM shiba_internal.graph_node_state WHERE graph_id=$1",
        &[&graph_key],
    )?;
    transaction.execute(
        "DELETE FROM shiba_internal.graph_result_row WHERE graph_id=$1",
        &[&graph_key],
    )?;
    if transaction.execute(
        "UPDATE shiba.graph_result SET value_payload=NULL,value_bigint=NULL
         WHERE graph_id=$1 AND result_status='building'",
        &[&graph_key],
    )? != u64::try_from(results.contract_count())
        .map_err(|_| M2Error::InvalidOperatorDefinition)?
    {
        return Err(M2Error::InvalidOperatorDefinition);
    }
    Ok(())
}

fn process_batch(
    transaction: &mut Transaction<'_>,
    batch: &BootstrapBatch,
) -> Result<BootstrapProcessOutcome, M2Error> {
    let graph_id = as_bigint("graph_id", batch.graph_id.get())?;
    let source_id = as_bigint("source_id", batch.source_id.get())?;
    let bootstrap_id = as_bigint("bootstrap_id", batch.batch_id.bootstrap_id.get())?;
    let ordinal = as_bigint("bootstrap batch ordinal", batch.batch_id.batch_ordinal())?;
    let generation: i64 = transaction
        .query_opt(
            "SELECT slot_generation FROM shiba_internal.graph_bootstrap
             WHERE graph_id = $1",
            &[&graph_id],
        )?
        .ok_or(M2Error::BootstrapMissing)?
        .get(0);
    let graph = operator_execution::load_locked_graph(transaction, graph_id, generation)?;
    source_preflight::validate_sources(transaction, &graph.graph)?;
    if !graph
        .graph
        .sources
        .iter()
        .any(|port| port.source_id == batch.source_id)
    {
        return Err(M2Error::SourceBindingMissing);
    }
    let row = transaction
        .query_opt(
            "SELECT bootstrap.bootstrap_id, bootstrap.phase,
                    checkpoint.last_batch_ordinal, checkpoint.last_source_row_id,
                    checkpoint.last_batch_digest
             FROM shiba_internal.graph_bootstrap AS bootstrap
             JOIN shiba_internal.graph_bootstrap_checkpoint AS checkpoint USING (graph_id)
             WHERE bootstrap.graph_id = $1 AND checkpoint.source_id = $2
             FOR UPDATE OF bootstrap, checkpoint",
            &[&graph_id, &source_id],
        )?
        .ok_or(M2Error::BootstrapMissing)?;
    if row.get::<_, i64>(0) != bootstrap_id {
        return Err(M2Error::BootstrapIdentityConflict);
    }
    let last_ordinal: i64 = row.get(2);
    let batch_last_key = batch
        .rows
        .last()
        .expect("constructor rejects empty")
        .source_row_id;
    let digest = batch.digest.as_bytes().as_slice();
    if ordinal == last_ordinal {
        if row.get::<_, Option<i64>>(3) == Some(batch_last_key)
            && row.get::<_, Option<Vec<u8>>>(4).as_deref() == Some(digest)
        {
            return Ok(BootstrapProcessOutcome::AlreadyApplied);
        }
        return Err(M2Error::BootstrapIdentityConflict);
    }
    if row.get::<_, &str>(1) != "scanning"
        || ordinal
            != last_ordinal
                .checked_add(1)
                .ok_or(M2Error::BootstrapIdentityConflict)?
        || row
            .get::<_, Option<i64>>(3)
            .is_some_and(|key| batch.rows[0].source_row_id <= key)
    {
        return Err(M2Error::BootstrapBatchOutOfOrder);
    }

    let effects = apply_snapshot_rows(transaction, batch, &graph.graph)?;
    operator_execution::execute(transaction, &graph, &effects, false)?;
    if transaction.execute(
        "UPDATE shiba_internal.graph_bootstrap_checkpoint
         SET last_batch_ordinal=$1,last_source_row_id=$2,last_batch_digest=$3
         WHERE graph_id=$4 AND source_id=$5 AND last_batch_ordinal=$6",
        &[
            &ordinal,
            &batch_last_key,
            &digest,
            &graph_id,
            &source_id,
            &last_ordinal,
        ],
    )? != 1
    {
        return Err(M2Error::BootstrapIdentityConflict);
    }
    Ok(BootstrapProcessOutcome::Applied)
}

fn apply_snapshot_rows(
    transaction: &mut Transaction<'_>,
    batch: &BootstrapBatch,
    graph: &shiba_operator::OperatorGraph,
) -> Result<MultiInputBatch, M2Error> {
    let source_id = as_bigint("source_id", batch.source_id.get())?;
    let row_ids: Vec<i64> = batch.rows.iter().map(|row| row.source_row_id).collect();
    let payloads: Vec<Option<i64>> = batch.rows.iter().map(|row| row.payload).collect();
    let inserted = transaction.execute(
        "INSERT INTO shiba_internal.source_row_state (
             source_id,source_row_id,source_row_sub_id,
             payload_present,payload_int8,payload_text)
         SELECT $1,input.source_row_id,NULL,true,input.payload,NULL
         FROM unnest($2::bigint[],$3::bigint[]) AS input(source_row_id,payload)",
        &[&source_id, &row_ids, &payloads],
    )?;
    if usize::try_from(inserted).ok() != Some(batch.rows.len()) {
        return Err(M2Error::InvalidSourceRowState);
    }
    let mut sources = Vec::with_capacity(graph.sources.len());
    for port in &graph.sources {
        let layout =
            SourceLayout::load(transaction, as_bigint("source_id", port.source_id.get())?)?;
        let rows = if port.source_id == batch.source_id {
            batch
                .rows
                .iter()
                .map(|row| {
                    Ok(RowDelta {
                        before: None,
                        after: Some(layout.row(
                            Some(row.source_row_id),
                            None,
                            &row.payload.map_or(SourcePayload::Null, SourcePayload::Int8),
                        )?),
                    })
                })
                .collect::<Result<Vec<_>, M2Error>>()?
        } else {
            Vec::new()
        };
        sources.push(SourceDeltaBatch {
            source_id: port.source_id,
            delta: layout.batch(EffectOrigin::Bootstrap(batch.batch_id), rows),
        });
    }
    Ok(MultiInputBatch {
        origin: GraphEffectOrigin::Bootstrap(batch.batch_id),
        sources,
    })
}
