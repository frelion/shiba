use postgres::GenericClient;

use crate::{
    BootstrapSession, IngressError,
    bootstrap::{BootstrapSpec, ReservedBootstrap, as_bigint},
    connection_config::{open_apply, replication_database},
    rebuild::PreparedRebuild,
    rebuild_model::PreparedAuthority,
    rebuild_validation::verify_rebuild_target,
    transport::ReplicationTransport,
};

impl PreparedRebuild {
    /// Retires the exact old physical slot and enters the existing bootstrap path.
    ///
    /// The target catalog identity is already the sole building authority. This
    /// handoff only reconciles the non-transactional old-slot operation, changes
    /// the same lifecycle row from `rebuild_prepared` to `creating`, and delegates
    /// exported-snapshot creation to `BootstrapSession::finish_reserved`.
    ///
    /// # Errors
    /// Fails closed on any catalog, database, operator, row-state, or observable
    /// slot drift. A missing old slot is accepted as the idempotent post-drop
    /// state; a present target slot is never adopted.
    pub fn into_bootstrap(self) -> Result<BootstrapSession, IngressError> {
        let Self {
            mut apply,
            authority,
            options,
            apply_conninfo,
            replication_conninfo,
            advisory_key,
            permit,
        } = self;
        let (scanner, scanner_database) = open_apply(&apply_conninfo, options.statement_timeout())?;
        let replication_database = replication_database(&replication_conninfo)?;
        let apply_database: String = apply
            .query_one("SELECT pg_catalog.current_database()::text", &[])?
            .get(0);
        if scanner_database != apply_database || replication_database != apply_database {
            return Err(IngressError::Governance("connection databases differ"));
        }
        let replication = ReplicationTransport::connect(&replication_conninfo)?;

        let old_exists = {
            let mut transaction = apply.transaction()?;
            verify_prepared_catalog(&mut transaction, &authority)?;
            verify_rebuild_target(&mut transaction, &authority)?;
            reject_target_slot(&mut transaction, &authority.target.slot_name)?;
            let exists = exact_old_slot(
                &mut transaction,
                &apply_database,
                &authority.retired_slot_name,
            )?;
            transaction.commit()?;
            exists
        };
        if old_exists {
            replication.drop_slot(&authority.retired_slot_name)?;
        }
        drop(replication);

        let mut transaction = apply.transaction()?;
        verify_prepared_catalog(&mut transaction, &authority)?;
        verify_rebuild_target(&mut transaction, &authority)?;
        if exact_old_slot(
            &mut transaction,
            &apply_database,
            &authority.retired_slot_name,
        )? {
            return Err(IngressError::Governance("retired slot still exists"));
        }
        reject_target_slot(&mut transaction, &authority.target.slot_name)?;
        transition_to_creating(&mut transaction, &authority)?;
        transaction.commit()?;

        let bootstrap_spec = BootstrapSpec {
            source_id: authority.source_id,
            bootstrap_id: authority.target.bootstrap_id,
            publication_oid: authority.target.publication_oid,
            slot_name: authority.target.slot_name.clone(),
            slot_generation: authority.target.slot_generation,
        };
        BootstrapSession::finish_reserved(ReservedBootstrap {
            apply,
            scanner,
            spec: bootstrap_spec,
            options,
            apply_conninfo,
            replication_conninfo,
            advisory_key,
            permit,
        })
    }
}

fn verify_prepared_catalog(
    client: &mut impl GenericClient,
    authority: &PreparedAuthority,
) -> Result<(), IngressError> {
    let source_id = as_bigint(authority.source_id.get())?;
    let target_bootstrap = as_bigint(authority.target.bootstrap_id.get())?;
    let target_generation = as_bigint(authority.target.slot_generation.get())?;
    let retired_bootstrap = as_bigint(authority.retired_bootstrap_id.get())?;
    let retired_generation = as_bigint(authority.retired_slot_generation.get())?;
    let count_operator = as_bigint(authority.count_operator_id.get())?;
    let sum_operator = as_bigint(authority.sum_operator_id.get())?;
    let exact: bool = client
        .query_opt(
            "SELECT
           bootstrap.phase = 'rebuild_prepared'
           AND bootstrap.bootstrap_id = $2
           AND bootstrap.slot_name = $3::text::name
           AND bootstrap.slot_generation = $4
           AND bootstrap.retired_bootstrap_id = $5
           AND bootstrap.retired_slot_name = $6::text::name
           AND bootstrap.retired_slot_generation = $7
           AND bootstrap.consistent_point IS NULL
           AND bootstrap.catchup_fence_lsn IS NULL
           AND bootstrap.activation_end_lsn IS NULL
           AND config.database_oid = (SELECT oid FROM pg_catalog.pg_database
                                      WHERE datname = pg_catalog.current_database())
           AND config.publication_objid = $8::bigint::oid
           AND config.slot_name = $3::text::name
           AND config.slot_generation = $4
           AND config.source_binding_kind = 'relation'
           AND config.source_binding_objsubid = 0
           AND 4 = (SELECT count(*) FROM shiba_internal.source_binding
                    WHERE source_id = $1)
           AND 3 = (SELECT count(*) FROM shiba_internal.source_binding
                    WHERE source_id = $1 AND address_objid = $9::bigint::oid
                      AND ((binding_kind = 'relation' AND address_objsubid = 0)
                           OR (binding_kind = 'column' AND address_objsubid IN (1, 2))))
           AND 1 = (SELECT count(*) FROM shiba_internal.source_binding
                    WHERE source_id = $1 AND binding_kind = 'identity_index'
                      AND address_objid = $12::bigint::oid AND address_objsubid = 0)
           AND 0 = (SELECT count(*) FROM shiba_internal.source_row_state
                    WHERE source_id = $1)
           AND 0 = (SELECT count(*) FROM shiba_internal.source_continuation
                    WHERE source_id = $1)
           AND 2 = (SELECT count(*) FROM shiba_internal.operator_definition
                    WHERE source_id = $1)
           AND 1 = (SELECT count(*) FROM shiba_internal.operator_definition AS definition
                    JOIN shiba_internal.operator_state AS state USING (operator_id)
                    JOIN shiba.operator_result AS result USING (operator_id, operator_kind)
                    WHERE definition.source_id = $1 AND definition.operator_id = $10
                      AND definition.operator_kind = 'count_rows'
                      AND definition.input_objid IS NULL
                      AND state.value_bigint = 0
                      AND result.result_status = 'building'
                      AND result.value_bigint IS NULL)
           AND 1 = (SELECT count(*) FROM shiba_internal.operator_definition AS definition
                    JOIN shiba_internal.operator_state AS state USING (operator_id)
                    JOIN shiba.operator_result AS result USING (operator_id, operator_kind)
                    WHERE definition.source_id = $1 AND definition.operator_id = $11
                      AND definition.operator_kind = 'sum_int8'
                      AND definition.input_classid = 'pg_class'::regclass
                      AND definition.input_objid = $9::bigint::oid
                      AND definition.input_objsubid = 2
                      AND state.value_bigint = 0
                      AND result.result_status = 'building'
                      AND result.value_bigint IS NULL)
         FROM shiba_internal.source_bootstrap AS bootstrap
         JOIN shiba_internal.source_ingress_config AS config USING (source_id)
         WHERE bootstrap.source_id = $1
         FOR UPDATE OF bootstrap",
            &[
                &source_id,
                &target_bootstrap,
                &authority.target.slot_name,
                &target_generation,
                &retired_bootstrap,
                &authority.retired_slot_name,
                &retired_generation,
                &i64::from(authority.target.publication_oid),
                &i64::from(authority.target.relation_oid),
                &count_operator,
                &sum_operator,
                &i64::from(authority.target.identity_index_oid),
            ],
        )?
        .is_some_and(|row| row.get(0));
    if !exact {
        return Err(IngressError::Governance("prepared rebuild catalog drifted"));
    }
    Ok(())
}

fn exact_old_slot(
    client: &mut impl GenericClient,
    database: &str,
    slot_name: &str,
) -> Result<bool, IngressError> {
    let Some(row) = client.query_opt(
        "SELECT slot_type, plugin, database, temporary, active,
                two_phase, failover, synced
         FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
        &[&slot_name],
    )?
    else {
        return Ok(false);
    };
    let exact = row.get::<_, &str>(0) == "logical"
        && row.get::<_, Option<&str>>(1) == Some("pgoutput")
        && row.get::<_, Option<&str>>(2) == Some(database)
        && !(3..=7).any(|index| row.get::<_, bool>(index));
    if !exact {
        return Err(IngressError::Governance("retired slot identity drifted"));
    }
    Ok(true)
}

fn reject_target_slot(
    client: &mut impl GenericClient,
    slot_name: &str,
) -> Result<(), IngressError> {
    if client
        .query_opt(
            "SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )?
        .is_some()
    {
        return Err(IngressError::Governance(
            "target rebuild slot already exists",
        ));
    }
    Ok(())
}

fn transition_to_creating(
    client: &mut impl GenericClient,
    authority: &PreparedAuthority,
) -> Result<(), IngressError> {
    if client.execute(
        "UPDATE shiba_internal.source_bootstrap
         SET phase = 'creating'
         WHERE source_id = $1 AND bootstrap_id = $2
           AND slot_name = $3::text::name AND slot_generation = $4
           AND phase = 'rebuild_prepared'
           AND retired_bootstrap_id = $5
           AND retired_slot_name = $6::text::name
           AND retired_slot_generation = $7",
        &[
            &as_bigint(authority.source_id.get())?,
            &as_bigint(authority.target.bootstrap_id.get())?,
            &authority.target.slot_name,
            &as_bigint(authority.target.slot_generation.get())?,
            &as_bigint(authority.retired_bootstrap_id.get())?,
            &authority.retired_slot_name,
            &as_bigint(authority.retired_slot_generation.get())?,
        ],
    )? != 1
    {
        return Err(IngressError::Governance(
            "prepared rebuild ownership changed",
        ));
    }
    Ok(())
}
