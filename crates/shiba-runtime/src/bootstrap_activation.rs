use postgres::Client;
use shiba_protocol::{BootstrapId, PostgresLsn, SourceId};

use crate::{M2Error, source_preflight, transaction::as_bigint};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BootstrapTransitionOutcome {
    Advanced,
    AlreadyAdvanced,
}

/// Atomically records that every row in the exported snapshot was scanned.
///
/// # Errors
/// Fails closed unless the exact source/bootstrap attempt is scanning.
pub fn complete_bootstrap_scan(
    client: &mut Client,
    source_id: SourceId,
    bootstrap_id: BootstrapId,
) -> Result<BootstrapTransitionOutcome, M2Error> {
    transition_phase(client, source_id, bootstrap_id, "scanning", "scan_complete")
}

/// Atomically publishes the fully caught-up private operator state.
///
/// # Errors
/// Fails closed unless the exact attempt is in `catching_up` and every result
/// row is still unavailable.
pub fn activate_bootstrap(
    client: &mut Client,
    source_id: SourceId,
    bootstrap_id: BootstrapId,
    marker_lsn: u64,
    terminal_end_lsn: u64,
) -> Result<BootstrapTransitionOutcome, M2Error> {
    let mut transaction = client.transaction()?;
    let source_id_raw = as_bigint("source_id", source_id.get())?;
    let bootstrap_id_raw = as_bigint("bootstrap_id", bootstrap_id.get())?;
    if marker_lsn == 0 || terminal_end_lsn == 0 {
        return Err(M2Error::InvalidBootstrapFence);
    }
    let marker_lsn = PostgresLsn::from_u64(marker_lsn).to_string();
    let terminal_end_lsn = PostgresLsn::from_u64(terminal_end_lsn).to_string();
    source_preflight::lock_binding(&mut transaction, source_id_raw)?;
    source_preflight::validate(&mut transaction, source_id_raw)?;
    let row = transaction
        .query_opt(
            "SELECT bootstrap_id, phase,
                    catchup_fence_lsn = $2::text::pg_lsn,
                    catchup_fence_lsn <= $3::text::pg_lsn,
                    activation_end_lsn = $3::text::pg_lsn
             FROM shiba_internal.source_bootstrap
             WHERE source_id = $1
             FOR UPDATE",
            &[&source_id_raw, &marker_lsn, &terminal_end_lsn],
        )?
        .ok_or(M2Error::BootstrapMissing)?;
    if row.get::<_, i64>(0) != bootstrap_id_raw {
        return Err(M2Error::BootstrapIdentityConflict);
    }
    let phase: &str = row.get(1);
    let already_active =
        validate_activation_coordinates(phase, row.get(2), row.get(3), row.get(4))?;
    if already_active {
        transaction.rollback()?;
        return Ok(BootstrapTransitionOutcome::AlreadyAdvanced);
    }
    let expected: i64 = transaction
        .query_one(
            "SELECT count(*) FROM shiba_internal.operator_definition
             WHERE source_id = $1",
            &[&source_id_raw],
        )?
        .get(0);
    if expected <= 0 {
        return Err(M2Error::MissingSourceOperator);
    }
    let updated = transaction.execute(
        "UPDATE shiba.operator_result AS result
         SET result_status = 'active', value_bigint = state.value_bigint
         FROM shiba_internal.operator_state AS state
         JOIN shiba_internal.operator_definition AS definition USING (operator_id)
         WHERE result.operator_id = state.operator_id
           AND definition.source_id = $1 AND result.result_status = 'building'",
        &[&source_id_raw],
    )?;
    if i64::try_from(updated).ok() != Some(expected) {
        return Err(M2Error::InvalidOperatorDefinition);
    }
    if transaction.execute(
        "UPDATE shiba_internal.source_bootstrap
         SET phase = 'active', activation_end_lsn = $3::text::pg_lsn
         WHERE source_id = $1 AND bootstrap_id = $2 AND phase = 'catching_up'
           AND catchup_fence_lsn = $4::text::pg_lsn
           AND catchup_fence_lsn <= $3::text::pg_lsn",
        &[
            &source_id_raw,
            &bootstrap_id_raw,
            &terminal_end_lsn,
            &marker_lsn,
        ],
    )? != 1
    {
        return Err(M2Error::BootstrapIdentityConflict);
    }
    transaction.commit()?;
    Ok(BootstrapTransitionOutcome::Advanced)
}

fn validate_activation_coordinates(
    phase: &str,
    exact_marker: bool,
    terminal_covers_marker: bool,
    exact_activation_end: Option<bool>,
) -> Result<bool, M2Error> {
    match phase {
        "catching_up" if exact_marker && terminal_covers_marker => Ok(false),
        "active" if exact_marker && exact_activation_end == Some(true) => Ok(true),
        "catching_up" | "active" => Err(M2Error::BootstrapIdentityConflict),
        _ => Err(M2Error::InvalidBootstrapPhase),
    }
}

fn transition_phase(
    client: &mut Client,
    source_id: SourceId,
    bootstrap_id: BootstrapId,
    from: &str,
    to: &str,
) -> Result<BootstrapTransitionOutcome, M2Error> {
    let mut transaction = client.transaction()?;
    let source_id = as_bigint("source_id", source_id.get())?;
    let bootstrap_id = as_bigint("bootstrap_id", bootstrap_id.get())?;
    source_preflight::lock_binding(&mut transaction, source_id)?;
    source_preflight::validate(&mut transaction, source_id)?;
    let row = transaction
        .query_opt(
            "SELECT bootstrap_id, phase FROM shiba_internal.source_bootstrap
             WHERE source_id = $1 FOR UPDATE",
            &[&source_id],
        )?
        .ok_or(M2Error::BootstrapMissing)?;
    if row.get::<_, i64>(0) != bootstrap_id {
        return Err(M2Error::BootstrapIdentityConflict);
    }
    let phase: &str = row.get(1);
    if phase == to {
        transaction.rollback()?;
        return Ok(BootstrapTransitionOutcome::AlreadyAdvanced);
    }
    if phase != from {
        return Err(M2Error::InvalidBootstrapPhase);
    }
    if transaction.execute(
        "UPDATE shiba_internal.source_bootstrap SET phase = $1
         WHERE source_id = $2 AND bootstrap_id = $3 AND phase = $4",
        &[&to, &source_id, &bootstrap_id, &from],
    )? != 1
    {
        return Err(M2Error::BootstrapIdentityConflict);
    }
    transaction.commit()?;
    Ok(BootstrapTransitionOutcome::Advanced)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn activation_requires_exact_marker_and_exact_active_replay() {
        assert!(!validate_activation_coordinates("catching_up", true, true, None).unwrap());
        assert!(matches!(
            validate_activation_coordinates("catching_up", false, true, None),
            Err(M2Error::BootstrapIdentityConflict)
        ));
        assert!(matches!(
            validate_activation_coordinates("catching_up", true, false, None),
            Err(M2Error::BootstrapIdentityConflict)
        ));
        assert!(validate_activation_coordinates("active", true, true, Some(true)).unwrap());
        assert!(matches!(
            validate_activation_coordinates("active", true, true, Some(false)),
            Err(M2Error::BootstrapIdentityConflict)
        ));
        assert!(matches!(
            validate_activation_coordinates("scan_complete", true, true, None),
            Err(M2Error::InvalidBootstrapPhase)
        ));
    }
}
