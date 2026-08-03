use core::str::FromStr;

use postgres::{Client, Transaction};
use shiba_protocol::PostgresLsn;

use crate::transaction::{as_bigint, check_transaction_change_limit};
use crate::{GraphTransaction, M2Error, operator_execution, source_apply, source_preflight};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessOutcome {
    Applied,
    AlreadyApplied,
}

/// Applies one complete graph transaction under the sole Runtime-owned database transaction.
///
/// # Errors
/// Returns a validation, kernel, sink, or database error; all writes roll back.
pub fn process(client: &mut Client, input: &GraphTransaction) -> Result<ProcessOutcome, M2Error> {
    check_transaction_change_limit(input.changes.len())?;
    let mut transaction = client.transaction()?;
    let outcome = process_in_transaction(&mut transaction, input)?;
    match outcome {
        ProcessOutcome::Applied => transaction.commit()?,
        ProcessOutcome::AlreadyApplied => transaction.rollback()?,
    }
    Ok(outcome)
}

fn process_in_transaction(
    transaction: &mut Transaction<'_>,
    input: &GraphTransaction,
) -> Result<ProcessOutcome, M2Error> {
    let identity = input.identity;
    let graph_id = as_bigint("graph_id", identity.graph_id.get())?;
    let generation = as_bigint("slot_generation", identity.slot_generation.get())?;
    let ingress_id = as_bigint(
        "ingress_transaction_id",
        identity.ingress_transaction_id.get(),
    )?;
    let commit_lsn = identity.commit_lsn.to_string();

    let graph = operator_execution::load_locked_graph(transaction, graph_id, generation)?;
    if exact_replay(
        transaction,
        graph_id,
        generation,
        &commit_lsn,
        ingress_id,
        &graph.digest,
    )? {
        return Ok(ProcessOutcome::AlreadyApplied);
    }
    source_preflight::validate_execution_authority(transaction, graph_id, generation)?;
    reject_out_of_order(transaction, graph_id, generation, identity.commit_lsn)?;
    source_preflight::validate_sources(transaction, &graph.graph)?;

    let batch = source_apply::apply(transaction, &graph.graph, input)?;
    let publish = source_preflight::result_visibility(transaction, graph_id, batch.origin)?;
    operator_execution::execute(transaction, &graph, &batch, publish)?;
    transaction.execute(
        "INSERT INTO shiba_internal.graph_continuation (
             graph_id, slot_generation, commit_lsn,
             ingress_transaction_id, graph_digest
         ) VALUES ($1, $2, $3::text::pg_lsn, $4, $5)",
        &[
            &graph_id,
            &generation,
            &commit_lsn,
            &ingress_id,
            &&graph.digest[..],
        ],
    )?;
    Ok(ProcessOutcome::Applied)
}

fn exact_replay(
    transaction: &mut Transaction<'_>,
    graph_id: i64,
    generation: i64,
    commit_lsn: &str,
    ingress_id: i64,
    digest: &[u8; 32],
) -> Result<bool, M2Error> {
    let Some(row) = transaction.query_opt(
        "SELECT ingress_transaction_id, graph_digest
         FROM shiba_internal.graph_continuation
         WHERE graph_id = $1 AND slot_generation = $2
           AND commit_lsn = $3::text::pg_lsn",
        &[&graph_id, &generation, &commit_lsn],
    )?
    else {
        return Ok(false);
    };
    if row.get::<_, i64>(0) != ingress_id || row.get::<_, Vec<u8>>(1).as_slice() != digest {
        return Err(M2Error::IdentityConflict);
    }
    Ok(true)
}

fn reject_out_of_order(
    transaction: &mut Transaction<'_>,
    graph_id: i64,
    generation: i64,
    commit_lsn: PostgresLsn,
) -> Result<(), M2Error> {
    let Some(row) = transaction.query_opt(
        "SELECT slot_generation, commit_lsn::text
         FROM shiba_internal.graph_continuation
         WHERE graph_id = $1 ORDER BY commit_lsn DESC LIMIT 1 FOR UPDATE",
        &[&graph_id],
    )?
    else {
        return Ok(());
    };
    if row.get::<_, i64>(0) != generation {
        return Err(M2Error::SlotGenerationMismatch);
    }
    let stored =
        PostgresLsn::from_str(row.get::<_, &str>(1)).map_err(|_| M2Error::IdentityConflict)?;
    if commit_lsn <= stored {
        return Err(M2Error::OutOfOrder);
    }
    Ok(())
}
