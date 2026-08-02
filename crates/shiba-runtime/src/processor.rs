use core::str::FromStr;

use postgres::{Client, Transaction};
use shiba_protocol::PostgresLsn;

use crate::operator_execution;
use crate::source_apply;
use crate::source_preflight;
use crate::transaction::{as_bigint, check_transaction_change_limit};
use crate::{M2Error, SourceTransaction};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessOutcome {
    Applied,
    AlreadyApplied,
}

/// # Errors
/// Returns a validation or database error; any opened transaction is rolled back.
pub fn process(client: &mut Client, input: &SourceTransaction) -> Result<ProcessOutcome, M2Error> {
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

    let effects = source_apply::apply(transaction, input)?;
    operator_execution::apply_all(transaction, source_id, &effects)?;
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
