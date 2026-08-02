use postgres::{Client, Transaction};
use shiba_operator::{EffectBatch, EffectOrigin, RowEffect, RowImage, Value};

use crate::M2Error;
use crate::bootstrap_model::BootstrapBatch;
use crate::operator_execution;
use crate::source_preflight;
use crate::transaction::as_bigint;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BootstrapProcessOutcome {
    Applied,
    AlreadyApplied,
}

/// Atomically applies one snapshot batch and advances its checkpoint.
///
/// # Errors
///
/// Fails closed if source authority, phase, ordering, identity, digest, row
/// state, or operator evaluation differs from the expected bootstrap attempt.
pub fn process_bootstrap_batch(
    client: &mut Client,
    batch: &BootstrapBatch,
) -> Result<BootstrapProcessOutcome, M2Error> {
    let mut transaction = client.transaction()?;
    let outcome = process_batch_in_transaction(&mut transaction, batch)?;
    match outcome {
        BootstrapProcessOutcome::Applied => transaction.commit()?,
        BootstrapProcessOutcome::AlreadyApplied => transaction.rollback()?,
    }
    Ok(outcome)
}

fn process_batch_in_transaction(
    transaction: &mut Transaction<'_>,
    batch: &BootstrapBatch,
) -> Result<BootstrapProcessOutcome, M2Error> {
    let source_id = as_bigint("source_id", batch.source_id.get())?;
    let bootstrap_id = as_bigint("bootstrap_id", batch.batch_id.bootstrap_id.get())?;
    let ordinal = as_bigint("bootstrap batch ordinal", batch.batch_id.batch_ordinal())?;
    source_preflight::lock_binding(transaction, source_id)?;
    source_preflight::validate(transaction, source_id)?;

    let row = transaction
        .query_opt(
            "SELECT bootstrap_id, phase, last_batch_ordinal,
                    last_source_row_id, last_batch_digest
             FROM shiba_internal.source_bootstrap
             WHERE source_id = $1
             FOR UPDATE",
            &[&source_id],
        )?
        .ok_or(M2Error::BootstrapMissing)?;
    if row.get::<_, i64>(0) != bootstrap_id {
        return Err(M2Error::BootstrapIdentityConflict);
    }
    let phase: &str = row.get(1);
    let last_ordinal: i64 = row.get(2);
    let last_key: Option<i64> = row.get(3);
    let last_digest: Option<Vec<u8>> = row.get(4);
    let batch_last_key = batch
        .rows
        .last()
        .expect("constructor rejects empty")
        .source_row_id;
    let digest = batch.digest.as_bytes().as_slice();

    if ordinal == last_ordinal {
        if last_key == Some(batch_last_key) && last_digest.as_deref() == Some(digest) {
            return Ok(BootstrapProcessOutcome::AlreadyApplied);
        }
        return Err(M2Error::BootstrapIdentityConflict);
    }
    if phase != "scanning" {
        return Err(M2Error::InvalidBootstrapPhase);
    }
    if ordinal
        != last_ordinal
            .checked_add(1)
            .ok_or(M2Error::BootstrapIdentityConflict)?
    {
        return Err(M2Error::BootstrapBatchOutOfOrder);
    }
    if last_key.is_some_and(|key| batch.rows[0].source_row_id <= key) {
        return Err(M2Error::BootstrapRowsOutOfOrder);
    }

    let row_ids: Vec<i64> = batch.rows.iter().map(|row| row.source_row_id).collect();
    let payloads: Vec<Option<i64>> = batch.rows.iter().map(|row| row.payload).collect();
    let inserted = transaction.execute(
        "INSERT INTO shiba_internal.source_row_state (
             source_id, source_row_id, source_row_sub_id,
             payload_present, payload_int8, payload_text
         )
         SELECT $1, input.source_row_id, NULL, true, input.payload, NULL
         FROM unnest($2::bigint[], $3::bigint[]) AS input(source_row_id, payload)",
        &[&source_id, &row_ids, &payloads],
    )?;
    if usize::try_from(inserted).ok() != Some(batch.rows.len()) {
        return Err(M2Error::InvalidSourceRowState);
    }

    let effects = batch
        .rows
        .iter()
        .map(|row| RowEffect {
            before: None,
            after: Some(RowImage {
                source_row_id: Some(row.source_row_id),
                source_row_sub_id: None,
                payload: row.payload.map_or(Value::Null, Value::Int8),
            }),
        })
        .collect();
    operator_execution::apply_all(
        transaction,
        source_id,
        &EffectBatch {
            origin: EffectOrigin::Bootstrap(batch.batch_id),
            effects,
        },
    )?;
    if transaction.execute(
        "UPDATE shiba_internal.source_bootstrap
         SET last_batch_ordinal = $1, last_source_row_id = $2,
             last_batch_digest = $3
         WHERE source_id = $4 AND bootstrap_id = $5 AND phase = 'scanning'",
        &[
            &ordinal,
            &batch_last_key,
            &digest,
            &source_id,
            &bootstrap_id,
        ],
    )? != 1
    {
        return Err(M2Error::BootstrapIdentityConflict);
    }
    Ok(BootstrapProcessOutcome::Applied)
}
