use postgres::Transaction;

use crate::{
    BootstrapSpec, IngressError, bootstrap::as_bigint, rebuild_resume::load_rebuild_authority,
    rebuild_validation::verify_rebuild_target,
};

/// Validates the M12 identity retained across a lost exported snapshot.
///
/// Legacy M11 attempts have an all-NULL retired marker and retain their proven
/// recovery behavior. A full marker selects the strict M12 path; partial
/// markers and every caller/catalog mismatch fail closed.
pub(crate) fn validate_m12_abandoned(
    transaction: &mut Transaction<'_>,
    abandoned: &BootstrapSpec,
    replacement: &BootstrapSpec,
    phase: &str,
) -> Result<bool, IngressError> {
    let graph_id = as_bigint(abandoned.graph_id.get())?;
    let marker = transaction.query_one(
        "SELECT retired_bootstrap_id, retired_slot_name::text,
                    retired_slot_generation
             FROM shiba_internal.graph_bootstrap
             WHERE graph_id = $1 AND bootstrap_id = $2",
        &[&graph_id, &as_bigint(abandoned.bootstrap_id.get())?],
    )?;
    let retired_bootstrap: Option<i64> = marker.get(0);
    let retired_slot: Option<String> = marker.get(1);
    let retired_generation: Option<i64> = marker.get(2);
    match (&retired_bootstrap, &retired_slot, &retired_generation) {
        (None, None, None) => return Ok(false),
        (Some(bootstrap), Some(slot), Some(generation))
            if *bootstrap > 0 && !slot.is_empty() && *generation > 0 => {}
        _ => return Err(IngressError::Governance("rebuild marker is partial")),
    }
    if !matches!(
        phase,
        "creating" | "scanning" | "cleanup_pending" | "failed"
    ) {
        return Err(IngressError::Governance(
            "M12 snapshot attempt is not replaceable",
        ));
    }
    if abandoned.slot_name == replacement.slot_name
        || abandoned.slot_generation.get().checked_add(1) != Some(replacement.slot_generation.get())
    {
        return Err(IngressError::Governance(
            "M12 replacement transport identity is not fresh and exact",
        ));
    }
    let (authority, durable_phase) = load_rebuild_authority(
        transaction,
        abandoned.graph_id,
        abandoned.bootstrap_id,
        abandoned.slot_generation,
    )?;
    if durable_phase != phase
        || authority.target.bootstrap_id != abandoned.bootstrap_id
        || authority.target.publication_oid != abandoned.publication_oid
        || authority.target.slot_name != abandoned.slot_name
        || authority.target.slot_generation != abandoned.slot_generation
    {
        return Err(IngressError::Governance(
            "abandoned M12 authority differs from durable catalog",
        ));
    }
    verify_rebuild_target(transaction, &authority)?;
    Ok(true)
}
