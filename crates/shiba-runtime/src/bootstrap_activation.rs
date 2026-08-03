use postgres::Client;
use shiba_protocol::{BootstrapId, GraphId, PostgresLsn};

use crate::{M2Error, operator_execution, source_preflight, transaction::as_bigint};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BootstrapTransitionOutcome {
    Advanced,
    AlreadyAdvanced,
}

/// Records that every ordered graph member has completed its snapshot scan.
///
/// # Errors
/// Fails unless the exact graph/bootstrap lifecycle is scanning.
pub fn complete_bootstrap_scan(
    client: &mut Client,
    graph_id: GraphId,
    bootstrap_id: BootstrapId,
) -> Result<BootstrapTransitionOutcome, M2Error> {
    transition_phase(client, graph_id, bootstrap_id, "scanning", "scan_complete")
}

/// Atomically publishes all graph results after the exact catch-up fence.
///
/// # Errors
/// Fails closed unless the exact graph attempt is caught up and still building.
pub fn activate_bootstrap(
    client: &mut Client,
    graph_id: GraphId,
    bootstrap_id: BootstrapId,
    marker_lsn: u64,
    terminal_end_lsn: u64,
) -> Result<BootstrapTransitionOutcome, M2Error> {
    let mut transaction = client.transaction()?;
    let graph_key = as_bigint("graph_id", graph_id.get())?;
    let bootstrap_key = as_bigint("bootstrap_id", bootstrap_id.get())?;
    if marker_lsn == 0 || terminal_end_lsn == 0 {
        return Err(M2Error::InvalidBootstrapFence);
    }
    let marker = PostgresLsn::from_u64(marker_lsn).to_string();
    let terminal = PostgresLsn::from_u64(terminal_end_lsn).to_string();
    let generation: i64 = transaction
        .query_opt(
            "SELECT slot_generation FROM shiba_internal.graph_bootstrap WHERE graph_id=$1",
            &[&graph_key],
        )?
        .ok_or(M2Error::BootstrapMissing)?
        .get(0);
    let graph = operator_execution::load_locked_graph(&mut transaction, graph_key, generation)?;
    source_preflight::validate_sources(&mut transaction, &graph.graph)?;
    let row = transaction
        .query_opt(
            "SELECT bootstrap_id,phase,catchup_fence_lsn=$2::text::pg_lsn,
                catchup_fence_lsn<=$3::text::pg_lsn,
                activation_end_lsn=$3::text::pg_lsn
         FROM shiba_internal.graph_bootstrap WHERE graph_id=$1 FOR UPDATE",
            &[&graph_key, &marker, &terminal],
        )?
        .ok_or(M2Error::BootstrapMissing)?;
    if row.get::<_, i64>(0) != bootstrap_key {
        return Err(M2Error::BootstrapIdentityConflict);
    }
    let phase: &str = row.get(1);
    if phase == "active" && row.get::<_, bool>(2) && row.get::<_, Option<bool>>(4) == Some(true) {
        transaction.rollback()?;
        return Ok(BootstrapTransitionOutcome::AlreadyAdvanced);
    }
    if phase != "catching_up" || !row.get::<_, bool>(2) || !row.get::<_, bool>(3) {
        return Err(M2Error::InvalidBootstrapFence);
    }
    operator_execution::activate_results(&mut transaction, &graph, bootstrap_key)?;
    if transaction.execute(
        "UPDATE shiba_internal.graph_bootstrap
         SET phase='active',activation_end_lsn=$3::text::pg_lsn
         WHERE graph_id=$1 AND bootstrap_id=$2 AND phase='catching_up'
           AND catchup_fence_lsn=$4::text::pg_lsn AND catchup_fence_lsn<=$3::text::pg_lsn",
        &[&graph_key, &bootstrap_key, &terminal, &marker],
    )? != 1
    {
        return Err(M2Error::BootstrapIdentityConflict);
    }
    transaction.commit()?;
    Ok(BootstrapTransitionOutcome::Advanced)
}

fn transition_phase(
    client: &mut Client,
    graph_id: GraphId,
    bootstrap_id: BootstrapId,
    from: &str,
    to: &str,
) -> Result<BootstrapTransitionOutcome, M2Error> {
    let mut transaction = client.transaction()?;
    let graph_key = as_bigint("graph_id", graph_id.get())?;
    let bootstrap_key = as_bigint("bootstrap_id", bootstrap_id.get())?;
    let generation: i64 = transaction
        .query_opt(
            "SELECT slot_generation FROM shiba_internal.graph_bootstrap WHERE graph_id=$1",
            &[&graph_key],
        )?
        .ok_or(M2Error::BootstrapMissing)?
        .get(0);
    let graph = operator_execution::load_locked_graph(&mut transaction, graph_key, generation)?;
    source_preflight::validate_sources(&mut transaction, &graph.graph)?;
    let row = transaction
        .query_opt(
            "SELECT bootstrap_id,phase FROM shiba_internal.graph_bootstrap
         WHERE graph_id=$1 FOR UPDATE",
            &[&graph_key],
        )?
        .ok_or(M2Error::BootstrapMissing)?;
    if row.get::<_, i64>(0) != bootstrap_key {
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
        "UPDATE shiba_internal.graph_bootstrap SET phase=$1
         WHERE graph_id=$2 AND bootstrap_id=$3 AND phase=$4",
        &[&to, &graph_key, &bootstrap_key, &from],
    )? != 1
    {
        return Err(M2Error::BootstrapIdentityConflict);
    }
    transaction.commit()?;
    Ok(BootstrapTransitionOutcome::Advanced)
}
