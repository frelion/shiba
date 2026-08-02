use core::str::FromStr;

use postgres::{Client, Transaction};
use shiba_protocol::PostgresLsn;

use crate::count;
use crate::transaction::as_bigint;
use crate::{M2Error, SourcePayload, SourceTransaction};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessOutcome {
    Applied,
    AlreadyApplied,
}

/// Atomically applies one committed source transaction and advances its result.
/// # Errors
/// Fails on identity/order, overflow, or `PostgreSQL` errors; all facts roll back.
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

    if let Some(row) = transaction.query_opt(
        "SELECT ingress_transaction_id
         FROM shiba_internal.source_continuation
         WHERE source_id = $1 AND slot_generation = $2
           AND commit_lsn = $3::text::pg_lsn",
        &[&source_id, &generation, &commit_lsn],
    )? {
        let stored_ingress: i64 = row.get(0);
        return if stored_ingress == ingress_id {
            Ok(ProcessOutcome::AlreadyApplied)
        } else {
            Err(M2Error::IdentityConflict)
        };
    }

    if let Some(row) = transaction.query_opt(
        "SELECT source_id, slot_generation, commit_lsn::text
         FROM shiba_internal.source_continuation
         ORDER BY commit_lsn DESC
         LIMIT 1
         FOR UPDATE",
        &[],
    )? {
        let stored_source: i64 = row.get(0);
        let stored_generation: i64 = row.get(1);
        if stored_source != source_id || stored_generation != generation {
            return Err(M2Error::SourceScopeMismatch);
        }
        let stored_lsn =
            PostgresLsn::from_str(row.get::<_, &str>(2)).map_err(|_| M2Error::IdentityConflict)?;
        if identity.commit_lsn <= stored_lsn {
            return Err(M2Error::OutOfOrder);
        }
    }

    for insert in &input.inserts {
        let sequence = as_bigint("input_sequence", insert.input_sequence.get())?;
        let (payload_present, payload_int8) = match insert.source_payload {
            SourcePayload::Absent => (false, None),
            SourcePayload::Null => (true, None),
            SourcePayload::Int8(value) => (true, Some(value)),
        };
        transaction.execute(
            "INSERT INTO shiba_internal.applied_insert (
                 source_id, slot_generation, commit_lsn,
                 ingress_transaction_id, input_sequence, source_row_id,
                 payload_present, payload_int8
             ) VALUES ($1, $2, $3::text::pg_lsn, $4, $5, $6, $7, $8)",
            &[
                &source_id,
                &generation,
                &commit_lsn,
                &ingress_id,
                &sequence,
                &insert.source_row_id,
                &payload_present,
                &payload_int8,
            ],
        )?;
    }

    let row = transaction.query_one(
        "SELECT row_count
         FROM shiba_internal.count_state
         WHERE singleton = 1
         FOR UPDATE",
        &[],
    )?;
    let next_count = count::advance(row.get(0), input.inserts.len())?;
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
