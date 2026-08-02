use core::str::FromStr;

use postgres::{Client, Transaction};
use shiba_protocol::PostgresLsn;

use crate::count;
use crate::source_preflight;
use crate::transaction::as_bigint;
use crate::{M2Error, SourceChange, SourcePayload, SourceTransaction, SourceUpdatePayload};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessOutcome {
    Applied,
    AlreadyApplied,
}

/// # Errors
/// Returns a validation or database error; the transaction is rolled back.
pub fn process(client: &mut Client, input: &SourceTransaction) -> Result<ProcessOutcome, M2Error> {
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
    input: &SourceTransaction,
) -> Result<ProcessOutcome, M2Error> {
    let identity = input.identity;
    let source_id = as_bigint("source_id", identity.source_id.get())?;
    let generation = as_bigint("slot_generation", identity.slot_generation.get())?;
    let ingress_id = as_bigint(
        "ingress_transaction_id",
        identity.ingress_transaction_id.get(),
    )?;
    let commit_lsn = identity.commit_lsn.to_string();

    if exact_replay(transaction, source_id, generation, &commit_lsn, ingress_id)? {
        return Ok(ProcessOutcome::AlreadyApplied);
    }

    source_preflight::lock_binding(transaction, source_id)?;

    if exact_replay(transaction, source_id, generation, &commit_lsn, ingress_id)? {
        return Ok(ProcessOutcome::AlreadyApplied);
    }

    source_preflight::validate(transaction, source_id)?;

    if let Some(row) = transaction.query_opt(
        "SELECT slot_generation, commit_lsn::text
         FROM shiba_internal.source_continuation
         WHERE source_id = $1
         ORDER BY commit_lsn DESC
         LIMIT 1
         FOR UPDATE",
        &[&source_id],
    )? {
        let stored_generation: i64 = row.get(0);
        if stored_generation != generation {
            return Err(M2Error::SlotGenerationMismatch);
        }
        let stored_lsn =
            PostgresLsn::from_str(row.get::<_, &str>(1)).map_err(|_| M2Error::IdentityConflict)?;
        if identity.commit_lsn <= stored_lsn {
            return Err(M2Error::OutOfOrder);
        }
    }

    let row_delta = apply_changes(
        transaction,
        input,
        source_id,
        generation,
        ingress_id,
        &commit_lsn,
    )?;

    let row = transaction.query_one(
        "SELECT row_count
         FROM shiba_internal.count_state
         WHERE singleton = 1
         FOR UPDATE",
        &[],
    )?;
    let next_count = count::advance(row.get(0), row_delta)?;
    transaction.execute(
        "UPDATE shiba_internal.count_state SET row_count = $1 WHERE singleton = 1",
        &[&next_count],
    )?;
    transaction.execute(
        "UPDATE shiba.count_result SET row_count = $1 WHERE singleton = 1",
        &[&next_count],
    )?;
    transaction.execute(
        "INSERT INTO shiba_internal.source_continuation (
             source_id, slot_generation, commit_lsn, ingress_transaction_id
         ) VALUES ($1, $2, $3::text::pg_lsn, $4)",
        &[&source_id, &generation, &commit_lsn, &ingress_id],
    )?;

    Ok(ProcessOutcome::Applied)
}

fn exact_replay(
    transaction: &mut Transaction<'_>,
    source_id: i64,
    generation: i64,
    commit_lsn: &str,
    ingress_id: i64,
) -> Result<bool, M2Error> {
    let Some(row) = transaction.query_opt(
        "SELECT ingress_transaction_id
         FROM shiba_internal.source_continuation
         WHERE source_id = $1 AND slot_generation = $2
           AND commit_lsn = $3::text::pg_lsn",
        &[&source_id, &generation, &commit_lsn],
    )?
    else {
        return Ok(false);
    };
    if row.get::<_, i64>(0) != ingress_id {
        return Err(M2Error::IdentityConflict);
    }
    Ok(true)
}

fn apply_changes(
    transaction: &mut Transaction<'_>,
    input: &SourceTransaction,
    source_id: i64,
    generation: i64,
    ingress_id: i64,
    commit_lsn: &str,
) -> Result<i64, M2Error> {
    let mut row_delta = 0;
    for change in &input.changes {
        match change {
            SourceChange::Insert(insert) => {
                row_delta += 1;
                let sequence = as_bigint("input_sequence", insert.input_sequence.get())?;
                let (payload_present, payload_int8, payload_text) = match &insert.source_payload {
                    SourcePayload::Absent => (false, None, None),
                    SourcePayload::Null => (true, None, None),
                    SourcePayload::Int8(value) => (true, Some(*value), None),
                    SourcePayload::Text(value) => (true, None, Some(value.as_str())),
                };
                transaction.execute(
                    "INSERT INTO shiba_internal.applied_insert (
                         source_id, slot_generation, commit_lsn,
                         ingress_transaction_id, input_sequence, source_row_id,
                         source_row_sub_id, payload_present, payload_int8, payload_text
                     ) VALUES ($1, $2, $3::text::pg_lsn, $4, $5, $6, $7, $8, $9, $10)",
                    &[
                        &source_id,
                        &generation,
                        &commit_lsn,
                        &ingress_id,
                        &sequence,
                        &insert.source_row_id,
                        &insert.source_row_sub_id,
                        &payload_present,
                        &payload_int8,
                        &payload_text,
                    ],
                )?;
            }
            SourceChange::Update(update) => {
                let changed = match &update.source_payload {
                    SourceUpdatePayload::Int8(payload) => transaction.execute(
                        "UPDATE shiba_internal.applied_insert
                         SET payload_present = true, payload_int8 = $1, payload_text = NULL
                         WHERE source_id = $2 AND source_row_id = $3
                           AND source_row_sub_id IS NULL AND payload_text IS NULL",
                        &[payload, &source_id, &update.source_row_id],
                    )?,
                    SourceUpdatePayload::UnchangedText => transaction.execute(
                        "UPDATE shiba_internal.applied_insert
                         SET payload_text = payload_text
                         WHERE source_id = $1 AND source_row_id = $2
                           AND source_row_sub_id IS NULL
                           AND payload_text IS NOT NULL AND payload_int8 IS NULL",
                        &[&source_id, &update.source_row_id],
                    )?,
                    SourceUpdatePayload::Text(payload) => transaction.execute(
                        "UPDATE shiba_internal.applied_insert
                         SET payload_text = $1
                         WHERE source_id = $2 AND source_row_id = $3
                           AND source_row_sub_id IS NULL
                           AND payload_text IS NOT NULL AND payload_int8 IS NULL",
                        &[payload, &source_id, &update.source_row_id],
                    )?,
                };
                if changed != 1 {
                    return Err(M2Error::MissingSourceRow);
                }
            }
            SourceChange::Delete {
                source_row_id,
                source_row_sub_id,
                ..
            } => {
                let changed = transaction.execute(
                    "DELETE FROM shiba_internal.applied_insert
                     WHERE source_id = $1 AND source_row_id = $2
                       AND source_row_sub_id IS NOT DISTINCT FROM $3",
                    &[&source_id, source_row_id, source_row_sub_id],
                )?;
                if changed != 1 {
                    return Err(M2Error::MissingSourceRow);
                }
                row_delta -= 1;
            }
        }
    }
    Ok(row_delta)
}
