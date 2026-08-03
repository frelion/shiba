use postgres::GenericClient;

use crate::{
    BootstrapSession, IngressError,
    bootstrap::{BootstrapSpec, ReservedBootstrap, as_bigint},
    connection_config::{open_apply, replication_database},
    operator_authority::validate_prepared_graph,
    rebuild::PreparedRebuild,
    rebuild_model::PreparedAuthority,
    rebuild_validation::verify_rebuild_target,
    transport::ReplicationTransport,
};

impl PreparedRebuild {
    /// Drops the exact retired slot and starts the target exported snapshot.
    ///
    /// # Errors
    /// Fails closed on any durable authority, live slot, member, or graph drift.
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
        let database: String = apply
            .query_one("SELECT pg_catalog.current_database()::text", &[])?
            .get(0);
        if scanner_database != database || replication_database(&replication_conninfo)? != database
        {
            return Err(IngressError::Governance("connection databases differ"));
        }
        let transport = ReplicationTransport::connect(&replication_conninfo)?;
        let old_exists = {
            let mut transaction = apply.transaction()?;
            verify_prepared_catalog(&mut transaction, &authority)?;
            verify_rebuild_target(&mut transaction, &authority)?;
            reject_target_slot(&mut transaction, &authority.target.slot_name)?;
            let exists = exact_old_slot(&mut transaction, &database, &authority.retired_slot_name)?;
            transaction.commit()?;
            exists
        };
        if old_exists {
            transport.drop_slot(&authority.retired_slot_name)?;
        }
        drop(transport);
        let mut transaction = apply.transaction()?;
        verify_prepared_catalog(&mut transaction, &authority)?;
        verify_rebuild_target(&mut transaction, &authority)?;
        if exact_old_slot(&mut transaction, &database, &authority.retired_slot_name)? {
            return Err(IngressError::Governance("retired slot still exists"));
        }
        reject_target_slot(&mut transaction, &authority.target.slot_name)?;
        transition_to_creating(&mut transaction, &authority)?;
        transaction.commit()?;
        let spec = BootstrapSpec {
            graph_id: authority.graph_id,
            bootstrap_id: authority.target.bootstrap_id,
            publication_oid: authority.target.publication_oid,
            slot_name: authority.target.slot_name.clone(),
            slot_generation: authority.target.slot_generation,
        };
        BootstrapSession::finish_reserved(ReservedBootstrap {
            apply,
            scanner,
            spec,
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
    let graph = as_bigint(authority.graph_id.get())?;
    let exact: bool = client
        .query_opt(
            "SELECT bootstrap.phase = 'rebuild_prepared'
           AND bootstrap.bootstrap_id = $2
           AND bootstrap.slot_name = $3::text::name
           AND bootstrap.slot_generation = $4
           AND bootstrap.retired_bootstrap_id = $5
           AND bootstrap.retired_slot_name = $6::text::name
           AND bootstrap.retired_slot_generation = $7
           AND bootstrap.graph_digest = $8
           AND config.graph_digest = $8
           AND config.publication_objid = $9::bigint::oid
           AND config.slot_name = $3::text::name
           AND config.slot_generation = $4
           AND 0 = (SELECT count(*) FROM shiba_internal.source_row_state AS row_state
                    JOIN shiba_internal.graph_source_member AS member USING (source_id)
                    WHERE member.graph_id = $1)
           AND 0 = (SELECT count(*) FROM shiba_internal.graph_continuation WHERE graph_id = $1)
         FROM shiba_internal.graph_bootstrap AS bootstrap
         JOIN shiba_internal.graph_ingress_config AS config USING (graph_id)
         WHERE bootstrap.graph_id = $1 FOR UPDATE OF bootstrap, config",
            &[
                &graph,
                &as_bigint(authority.target.bootstrap_id.get())?,
                &authority.target.slot_name,
                &as_bigint(authority.target.slot_generation.get())?,
                &as_bigint(authority.retired_bootstrap_id.get())?,
                &authority.retired_slot_name,
                &as_bigint(authority.retired_slot_generation.get())?,
                &authority.target.graph_digest.as_slice(),
                &i64::from(authority.target.publication_oid),
            ],
        )?
        .is_some_and(|row| row.get(0));
    if !exact {
        return Err(IngressError::Governance("prepared rebuild catalog drifted"));
    }
    validate_prepared_graph(client, authority.graph_id, &authority.graph)
}

fn exact_old_slot(
    client: &mut impl GenericClient,
    database: &str,
    slot_name: &str,
) -> Result<bool, IngressError> {
    let Some(row) = client.query_opt(
        "SELECT slot_type, plugin, database, temporary, active, two_phase, failover, synced
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
        return Err(IngressError::Governance("retired slot identity drifted"));
    }
    Ok(true)
}

fn reject_target_slot(client: &mut impl GenericClient, slot: &str) -> Result<(), IngressError> {
    if client
        .query_opt(
            "SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
            &[&slot],
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
        "UPDATE shiba_internal.graph_bootstrap SET phase = 'creating'
         WHERE graph_id = $1 AND bootstrap_id = $2 AND slot_name = $3::text::name
           AND slot_generation = $4 AND phase = 'rebuild_prepared'
           AND retired_bootstrap_id = $5 AND retired_slot_name = $6::text::name
           AND retired_slot_generation = $7",
        &[
            &as_bigint(authority.graph_id.get())?,
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
