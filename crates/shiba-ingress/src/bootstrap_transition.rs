use core::str::FromStr;

use postgres::Client;
use shiba_protocol::PostgresLsn;

use crate::{
    IngressError, bootstrap::BootstrapSpec, fence::FENCE_PREFIX, governance::GovernedConfig,
};

/// Moves a completed scan to catch-up, or reconstructs an existing catch-up.
///
/// Fence emission and its catalog coordinate share one `PostgreSQL` transaction;
/// a resumed active attempt is complete only after slot feedback covers its
/// exact activation terminal.
pub(crate) fn prepare_catchup(
    apply: &mut Client,
    mut config: GovernedConfig,
    spec: &BootstrapSpec,
    phase: &str,
) -> Result<(GovernedConfig, String, u64, bool), IngressError> {
    config.revalidate(apply, false)?;
    let graph_id = as_bigint(spec.graph_id.get())?;
    let bootstrap_id = as_bigint(spec.bootstrap_id.get())?;
    let fence_token: String = apply
        .query_one(
            "SELECT fence_token::text FROM shiba_internal.graph_bootstrap
             WHERE graph_id = $1 AND bootstrap_id = $2",
            &[&graph_id, &bootstrap_id],
        )?
        .get(0);
    let content = format!(
        "{}:{}:{fence_token}",
        spec.graph_id.get(),
        spec.bootstrap_id.get()
    );
    if phase == "scan_complete" {
        emit_fence_and_enter_catchup(apply, graph_id, bootstrap_id, &content)?;
    } else if !matches!(phase, "catching_up" | "active") {
        return Err(IngressError::Governance("bootstrap scan is not complete"));
    }
    let (fresh, confirmed_lsn) =
        GovernedConfig::load(apply, spec.graph_id, spec.slot_generation, false)?;
    if fresh != config {
        return Err(IngressError::Governance("source configuration drifted"));
    }
    config = fresh;
    let active =
        phase == "active" && confirmed_lsn >= activation_end_lsn(apply, graph_id, bootstrap_id)?;
    Ok((config, content, confirmed_lsn, active))
}

fn emit_fence_and_enter_catchup(
    apply: &mut Client,
    graph_id: i64,
    bootstrap_id: i64,
    content: &str,
) -> Result<(), IngressError> {
    let mut transaction = apply.transaction()?;
    let marker_lsn: String = transaction
        .query_one(
            "SELECT pg_catalog.pg_logical_emit_message(true, $1, $2)::text",
            &[&FENCE_PREFIX, &content],
        )?
        .get(0);
    let marker = PostgresLsn::from_str(&marker_lsn)
        .map_err(|_| IngressError::InvalidEnvelope("invalid fence LSN"))?;
    if marker.is_zero()
        || transaction.execute(
            "UPDATE shiba_internal.graph_bootstrap
             SET phase = 'catching_up', catchup_fence_lsn = $1::text::pg_lsn
             WHERE graph_id = $2 AND bootstrap_id = $3 AND phase = 'scan_complete'",
            &[&marker_lsn, &graph_id, &bootstrap_id],
        )? != 1
    {
        return Err(IngressError::Governance(
            "bootstrap catch-up transition failed",
        ));
    }
    transaction.commit()?;
    Ok(())
}

fn activation_end_lsn(
    apply: &mut Client,
    graph_id: i64,
    bootstrap_id: i64,
) -> Result<u64, IngressError> {
    let value: String = apply
        .query_one(
            "SELECT activation_end_lsn::text
             FROM shiba_internal.graph_bootstrap
             WHERE graph_id = $1 AND bootstrap_id = $2 AND phase = 'active'",
            &[&graph_id, &bootstrap_id],
        )?
        .get(0);
    let lsn = PostgresLsn::from_str(&value)
        .map_err(|_| IngressError::InvalidEnvelope("invalid activation end LSN"))?;
    Ok(lsn.as_u64())
}

fn as_bigint(value: u64) -> Result<i64, IngressError> {
    i64::try_from(value).map_err(|_| IngressError::Governance("identity exceeds bigint"))
}
