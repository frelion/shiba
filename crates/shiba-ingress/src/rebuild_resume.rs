use postgres::Transaction;
use shiba_protocol::{BootstrapId, GraphId, SlotGeneration, SourceId};

use crate::{
    BootstrapOptions, IngressError,
    bootstrap::as_bigint,
    connection_config::{open_apply, replication_database},
    governed::advisory_key,
    limits::ActivePermit,
    operator_authority::load_graph_fingerprint,
    rebuild::PreparedRebuild,
    rebuild_model::{PreparedAuthority, RebuildIdentity, RebuildMemberIdentity},
    rebuild_validation::verify_rebuild_target,
};

impl PreparedRebuild {
    /// Recovers the sole durable `rebuild_prepared` graph authority.
    ///
    /// # Errors
    /// Fails closed on lifecycle, graph digest, member, privilege, or slot drift.
    pub fn resume_prepared(
        apply_conninfo: &str,
        replication_conninfo: &str,
        graph_id: GraphId,
        target_bootstrap_id: BootstrapId,
        target_generation: SlotGeneration,
        options: BootstrapOptions,
    ) -> Result<Self, IngressError> {
        let permit = ActivePermit::acquire()?;
        let (mut apply, database) = open_apply(apply_conninfo, options.statement_timeout())?;
        if database != replication_database(replication_conninfo)? {
            return Err(IngressError::Governance("connection databases differ"));
        }
        let advisory_key = advisory_key(graph_id)?;
        let acquired: bool = apply
            .query_one(
                "SELECT pg_catalog.pg_try_advisory_lock($1)",
                &[&advisory_key],
            )?
            .get(0);
        if !acquired {
            return Err(IngressError::Governance(
                "graph already has an active session",
            ));
        }
        let mut transaction = apply.transaction()?;
        let authority = load_prepared_authority(
            &mut transaction,
            graph_id,
            target_bootstrap_id,
            target_generation,
        )?;
        verify_rebuild_target(&mut transaction, &authority)?;
        if transaction
            .query_opt(
                "SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
                &[&authority.target.slot_name],
            )?
            .is_some()
        {
            return Err(IngressError::Governance(
                "target rebuild slot already exists",
            ));
        }
        transaction.commit()?;
        Ok(Self {
            apply,
            authority,
            options,
            apply_conninfo: apply_conninfo.to_owned(),
            replication_conninfo: replication_conninfo.to_owned(),
            advisory_key,
            permit,
        })
    }
}

pub(crate) fn load_prepared_authority(
    transaction: &mut Transaction<'_>,
    graph_id: GraphId,
    bootstrap_id: BootstrapId,
    generation: SlotGeneration,
) -> Result<PreparedAuthority, IngressError> {
    let (authority, phase) =
        load_rebuild_authority(transaction, graph_id, bootstrap_id, generation)?;
    if phase != "rebuild_prepared" {
        return Err(IngressError::Governance(
            "prepared rebuild authority is missing",
        ));
    }
    Ok(authority)
}

pub(crate) fn load_rebuild_authority(
    transaction: &mut Transaction<'_>,
    graph_id: GraphId,
    bootstrap_id: BootstrapId,
    generation: SlotGeneration,
) -> Result<(PreparedAuthority, String), IngressError> {
    let graph_key = as_bigint(graph_id.get())?;
    let row = transaction
        .query_opt(
            "SELECT bootstrap.slot_name::text, bootstrap.retired_bootstrap_id,
                bootstrap.retired_slot_name::text, bootstrap.retired_slot_generation,
                config.publication_objid::bigint, bootstrap.phase,
                definition.graph_digest
         FROM shiba_internal.graph_bootstrap AS bootstrap
         JOIN shiba_internal.graph_ingress_config AS config USING (graph_id)
         JOIN shiba_internal.graph_definition AS definition USING (graph_id)
         WHERE bootstrap.graph_id = $1 AND bootstrap.bootstrap_id = $2
           AND bootstrap.slot_generation = $3
           AND config.slot_name = bootstrap.slot_name
           AND config.slot_generation = bootstrap.slot_generation
           AND config.graph_digest = bootstrap.graph_digest
           AND definition.graph_digest = bootstrap.graph_digest
         FOR UPDATE OF bootstrap, config, definition",
            &[
                &graph_key,
                &as_bigint(bootstrap_id.get())?,
                &as_bigint(generation.get())?,
            ],
        )?
        .ok_or(IngressError::Governance(
            "prepared rebuild authority is missing",
        ))?;
    let digest = exact_digest(row.get(6))?;
    let members = transaction
        .query(
            "SELECT member.source_id, relation.address_objid::bigint,
                identity.address_objid::bigint
         FROM shiba_internal.graph_source_member AS member
         JOIN shiba_internal.source_binding AS relation
           ON relation.source_id = member.source_id AND relation.binding_kind = 'relation'
          AND relation.address_objsubid = 0
         JOIN shiba_internal.source_binding AS identity
           ON identity.source_id = member.source_id AND identity.binding_kind = 'identity_index'
          AND identity.address_objsubid = 0
         WHERE member.graph_id = $1 ORDER BY member.input_ordinal",
            &[&graph_key],
        )?
        .into_iter()
        .map(|member| {
            Ok(RebuildMemberIdentity {
                source_id: source_id(member.get(0))?,
                relation_oid: as_oid(member.get(1), "target relation OID")?,
                identity_index_oid: as_oid(member.get(2), "target identity index OID")?,
            })
        })
        .collect::<Result<Vec<_>, IngressError>>()?;
    if members.is_empty() || members.len() > 2 {
        return Err(IngressError::Governance(
            "prepared graph members are incomplete",
        ));
    }
    let graph = load_graph_fingerprint(transaction, graph_id)?;
    let authority = PreparedAuthority {
        graph_id,
        target: RebuildIdentity {
            bootstrap_id,
            graph_digest: digest,
            members,
            publication_oid: as_oid(row.get(4), "target publication OID")?,
            slot_name: row.get(0),
            slot_generation: generation,
        },
        retired_bootstrap_id: parsed_bootstrap(row.get(1))?,
        retired_slot_name: row.get(2),
        retired_slot_generation: parsed_generation(row.get(3))?,
        graph,
    };
    Ok((authority, row.get(5)))
}

fn exact_digest(value: Vec<u8>) -> Result<[u8; 32], IngressError> {
    value
        .try_into()
        .map_err(|_| IngressError::Governance("graph digest is invalid"))
}
fn as_oid(value: i64, label: &'static str) -> Result<u32, IngressError> {
    u32::try_from(value)
        .ok()
        .filter(|value| *value != 0)
        .ok_or(IngressError::InvalidIdentifier(label))
}
fn source_id(value: i64) -> Result<SourceId, IngressError> {
    u64::try_from(value)
        .ok()
        .and_then(|value| SourceId::new(value).ok())
        .ok_or(IngressError::Governance("source identity is invalid"))
}
fn parsed_bootstrap(value: i64) -> Result<BootstrapId, IngressError> {
    u64::try_from(value)
        .ok()
        .and_then(|value| BootstrapId::new(value).ok())
        .ok_or(IngressError::Governance(
            "retired bootstrap identity is invalid",
        ))
}
fn parsed_generation(value: i64) -> Result<SlotGeneration, IngressError> {
    u64::try_from(value)
        .ok()
        .and_then(|value| SlotGeneration::new(value).ok())
        .ok_or(IngressError::Governance("retired generation is invalid"))
}
