use postgres::Client;

use crate::{
    BootstrapSession, IngressError,
    bootstrap::{BootstrapOptions, BootstrapSpec, ReservedBootstrap, as_bigint, validate_spec},
    connection_config::{open_apply, replication_database},
    governed::advisory_key,
    limits::ActivePermit,
    rebuild_abandoned::validate_m12_abandoned,
    transport::ReplicationTransport,
};

impl BootstrapSession {
    /// Replaces one abandoned pre-scan attempt with a fresh exact attempt.
    ///
    /// This is the only lifecycle that drops a bootstrap slot. It first owns
    /// the source advisory lock, reconciles the exact catalog attempt and
    /// physical inactive pgoutput slot, persists `cleanup_pending`, drops that
    /// exact slot, and invokes the atomic catalog replacement writer. Ordinary
    /// receiver startup never performs cleanup or adoption.
    ///
    /// # Errors
    /// Refuses active/post-scan attempts, identity or publication drift, active
    /// and foreign slots, reused bootstrap/generation identities, and partial
    /// catalog replacement.
    pub fn restart_abandoned(
        apply_conninfo: &str,
        replication_conninfo: &str,
        abandoned: &BootstrapSpec,
        replacement: BootstrapSpec,
        options: BootstrapOptions,
    ) -> Result<Self, IngressError> {
        validate_replacement(abandoned, &replacement)?;
        let permit = ActivePermit::acquire()?;
        let (mut apply, database) = open_apply(apply_conninfo, options.statement_timeout())?;
        if database != replication_database(replication_conninfo)? {
            return Err(IngressError::Governance("connection databases differ"));
        }
        let (scanner, scanner_database) = open_apply(apply_conninfo, options.statement_timeout())?;
        if scanner_database != database {
            return Err(IngressError::Governance("scanner database differs"));
        }
        let advisory_key = advisory_key(abandoned.graph_id)?;
        let acquired: bool = apply
            .query_one(
                "SELECT pg_catalog.pg_try_advisory_lock($1)",
                &[&advisory_key],
            )?
            .get(0);
        if !acquired {
            return Err(IngressError::Governance(
                "source already has an active session",
            ));
        }

        reject_foreign_replacement_slot(
            &mut apply,
            &database,
            &abandoned.slot_name,
            &replacement.slot_name,
        )?;
        let m12 = reconcile_abandoned(&mut apply, &database, abandoned, &replacement)?;
        let old_slot_exists = if m12 {
            exact_m12_slot_exists(&mut apply, &database, &abandoned.slot_name)?
        } else {
            exact_slot_exists(&mut apply, &database, &abandoned.slot_name)?
        };
        if old_slot_exists {
            ReplicationTransport::connect(replication_conninfo)?.drop_slot(&abandoned.slot_name)?;
        }
        replace_catalog_attempt(&mut apply, abandoned, &replacement)?;
        Self::finish_reserved(ReservedBootstrap {
            apply,
            scanner,
            spec: replacement,
            options,
            apply_conninfo: apply_conninfo.to_owned(),
            replication_conninfo: replication_conninfo.to_owned(),
            advisory_key,
            permit,
        })
    }
}

fn validate_replacement(
    abandoned: &BootstrapSpec,
    replacement: &BootstrapSpec,
) -> Result<(), IngressError> {
    validate_spec(abandoned)?;
    validate_spec(replacement)?;
    if abandoned.graph_id != replacement.graph_id
        || abandoned.publication_oid != replacement.publication_oid
        || abandoned.bootstrap_id == replacement.bootstrap_id
        || replacement.slot_generation.get() <= abandoned.slot_generation.get()
    {
        return Err(IngressError::Governance(
            "bootstrap replacement identity is not fresh and exact",
        ));
    }
    Ok(())
}

fn reject_foreign_replacement_slot(
    apply: &mut Client,
    database: &str,
    old_slot: &str,
    new_slot: &str,
) -> Result<(), IngressError> {
    if new_slot != old_slot && exact_slot_exists(apply, database, new_slot)? {
        return Err(IngressError::Governance(
            "replacement bootstrap slot already exists",
        ));
    }
    Ok(())
}

fn reconcile_abandoned(
    apply: &mut Client,
    database: &str,
    spec: &BootstrapSpec,
    replacement: &BootstrapSpec,
) -> Result<bool, IngressError> {
    let graph_id = as_bigint(spec.graph_id.get())?;
    let bootstrap_id = as_bigint(spec.bootstrap_id.get())?;
    let generation = as_bigint(spec.slot_generation.get())?;
    let publication_oid = i64::from(spec.publication_oid);
    let mut transaction = apply.transaction()?;
    let row = transaction
        .query_opt(
            "SELECT bootstrap.slot_name::text, bootstrap.slot_generation,
                    bootstrap.phase, config.publication_objid::bigint,
                    database.datname::text
             FROM shiba_internal.graph_bootstrap AS bootstrap
             JOIN shiba_internal.graph_ingress_config AS config
               ON config.graph_id = bootstrap.graph_id
              AND config.slot_name = bootstrap.slot_name
              AND config.slot_generation = bootstrap.slot_generation
             JOIN pg_catalog.pg_database AS database
               ON database.oid = config.database_oid
             WHERE bootstrap.graph_id = $1 AND bootstrap.bootstrap_id = $2
             FOR UPDATE OF bootstrap, config",
            &[&graph_id, &bootstrap_id],
        )?
        .ok_or(IngressError::Governance("bootstrap attempt is missing"))?;
    if row.get::<_, &str>(0) != spec.slot_name
        || row.get::<_, i64>(1) != generation
        || row.get::<_, i64>(3) != publication_oid
        || row.get::<_, &str>(4) != database
    {
        return Err(IngressError::Governance(
            "abandoned bootstrap identity drifted",
        ));
    }
    if !matches!(
        row.get::<_, &str>(2),
        "creating" | "scanning" | "cleanup_pending" | "failed"
    ) {
        return Err(IngressError::Governance(
            "only a pre-scan attempt can be replaced",
        ));
    }
    let m12 = validate_m12_abandoned(&mut transaction, spec, replacement, row.get(2))?;
    if transaction.execute(
        "UPDATE shiba_internal.graph_bootstrap
         SET phase = 'cleanup_pending'
         WHERE graph_id = $1 AND bootstrap_id = $2
           AND slot_name = $3::text::name AND slot_generation = $4
           AND phase IN ('creating', 'scanning', 'cleanup_pending', 'failed')",
        &[&graph_id, &bootstrap_id, &spec.slot_name, &generation],
    )? != 1
    {
        return Err(IngressError::Governance(
            "abandoned bootstrap ownership changed",
        ));
    }
    transaction.commit()?;
    Ok(m12)
}

fn exact_slot_exists(
    apply: &mut Client,
    database: &str,
    slot_name: &str,
) -> Result<bool, IngressError> {
    let Some(row) = apply.query_opt(
        "SELECT slot_type, plugin, database, active
         FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
        &[&slot_name],
    )?
    else {
        return Ok(false);
    };
    if row.get::<_, &str>(0) != "logical"
        || row.get::<_, Option<&str>>(1) != Some("pgoutput")
        || row.get::<_, Option<&str>>(2) != Some(database)
        || row.get::<_, bool>(3)
    {
        return Err(IngressError::Governance(
            "bootstrap slot is active or has foreign identity",
        ));
    }
    Ok(true)
}

fn exact_m12_slot_exists(
    apply: &mut Client,
    database: &str,
    slot_name: &str,
) -> Result<bool, IngressError> {
    let Some(row) = apply.query_opt(
        "SELECT slot_type, plugin, database, temporary, active,
                two_phase, failover, synced
         FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
        &[&slot_name],
    )?
    else {
        return Ok(false);
    };
    if row.get::<_, &str>(0) != "logical"
        || row.get::<_, Option<&str>>(1) != Some("pgoutput")
        || row.get::<_, Option<&str>>(2) != Some(database)
        || (3..=7).any(|index| row.get::<_, bool>(index))
    {
        return Err(IngressError::Governance(
            "M12 bootstrap slot has foreign observable identity",
        ));
    }
    Ok(true)
}

fn replace_catalog_attempt(
    apply: &mut Client,
    abandoned: &BootstrapSpec,
    replacement: &BootstrapSpec,
) -> Result<(), IngressError> {
    let mut transaction = apply.transaction()?;
    shiba_runtime::reset_abandoned_bootstrap_state(
        &mut transaction,
        abandoned.graph_id,
        abandoned.bootstrap_id,
    )?;
    transaction.query_one(
        "SELECT shiba_internal.replace_pristine_graph_bootstrap(
             $1, $2, $3::text::name, $4, $5, $6::bigint::oid,
             $7::text::name, $8
         )",
        &[
            &as_bigint(abandoned.bootstrap_id.get())?,
            &as_bigint(abandoned.graph_id.get())?,
            &abandoned.slot_name,
            &as_bigint(abandoned.slot_generation.get())?,
            &as_bigint(replacement.bootstrap_id.get())?,
            &i64::from(replacement.publication_oid),
            &replacement.slot_name,
            &as_bigint(replacement.slot_generation.get())?,
        ],
    )?;
    transaction.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use shiba_protocol::{BootstrapId, GraphId, SlotGeneration};

    use super::{BootstrapSpec, validate_replacement};

    fn spec(source: u64, bootstrap: u64, publication: u32, generation: u64) -> BootstrapSpec {
        BootstrapSpec {
            graph_id: GraphId::new(source).expect("graph ID"),
            bootstrap_id: BootstrapId::new(bootstrap).expect("bootstrap ID"),
            publication_oid: publication,
            slot_name: format!("slot_{generation}"),
            slot_generation: SlotGeneration::new(generation).expect("slot generation"),
        }
    }

    #[test]
    fn replacement_requires_exact_identity_and_strictly_newer_generation() {
        let old = spec(1, 10, 42, 7);
        assert!(validate_replacement(&old, &spec(1, 11, 42, 8)).is_ok());
        for replacement in [
            spec(1, 11, 42, 7),
            spec(1, 11, 42, 6),
            spec(2, 11, 42, 8),
            spec(1, 11, 43, 8),
            spec(1, 10, 42, 8),
        ] {
            assert!(validate_replacement(&old, &replacement).is_err());
        }
    }
}
