use core::num::NonZeroU64;

use shiba_operator::OperatorId;
use shiba_protocol::{BootstrapId, SlotGeneration, SourceId};

use crate::{
    BootstrapOptions, IngressError,
    bootstrap::as_bigint,
    connection_config::{open_apply, replication_database},
    governed::advisory_key,
    limits::ActivePermit,
    rebuild::PreparedRebuild,
    rebuild_model::{PreparedAuthority, RebuildIdentity},
    rebuild_validation::verify_rebuild_target,
    source_shape::validate_bindings,
};

impl PreparedRebuild {
    /// Resumes the sole durable `rebuild_prepared` authority after a crash.
    ///
    /// Caller input is only the expected lifecycle CAS. Relation, primary-index,
    /// publication, slot, retired transport identity, and operator identities
    /// are recovered from the exact catalog rows and cannot be overridden.
    ///
    /// # Errors
    /// Fails closed without creating or dropping a slot when catalog identity,
    /// target shape, privileges, invalidation, or target-slot absence drifted.
    pub fn resume_prepared(
        apply_conninfo: &str,
        replication_conninfo: &str,
        source_id: SourceId,
        target_bootstrap_id: BootstrapId,
        target_generation: SlotGeneration,
        options: BootstrapOptions,
    ) -> Result<Self, IngressError> {
        let permit = ActivePermit::acquire()?;
        let (mut apply, apply_database) = open_apply(apply_conninfo, options.statement_timeout())?;
        if apply_database != replication_database(replication_conninfo)? {
            return Err(IngressError::Governance("connection databases differ"));
        }
        let advisory_key = advisory_key(source_id)?;
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
        let mut transaction = apply.transaction()?;
        let authority = load_prepared_authority(
            &mut transaction,
            source_id,
            target_bootstrap_id,
            target_generation,
        )?;
        verify_rebuild_target(&mut transaction, &authority)?;
        let target_exists: bool = transaction
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_replication_slots
                                WHERE slot_name = $1)",
                &[&authority.target.slot_name],
            )?
            .get(0);
        if target_exists {
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
    transaction: &mut postgres::Transaction<'_>,
    source_id: SourceId,
    target_bootstrap_id: BootstrapId,
    target_generation: SlotGeneration,
) -> Result<PreparedAuthority, IngressError> {
    let (authority, phase) = load_rebuild_authority(
        transaction,
        source_id,
        target_bootstrap_id,
        target_generation,
    )?;
    if phase != "rebuild_prepared" {
        return Err(IngressError::Governance(
            "prepared rebuild authority is missing",
        ));
    }
    Ok(authority)
}

pub(crate) fn load_rebuild_authority(
    transaction: &mut postgres::Transaction<'_>,
    source_id: SourceId,
    target_bootstrap_id: BootstrapId,
    target_generation: SlotGeneration,
) -> Result<(PreparedAuthority, String), IngressError> {
    let source_key = as_bigint(source_id.get())?;
    let row = transaction
        .query_opt(
            "SELECT bootstrap.slot_name::text, bootstrap.retired_bootstrap_id,
                    bootstrap.retired_slot_name::text,
                    bootstrap.retired_slot_generation,
                    config.publication_objid::bigint,
                    binding.address_classid::bigint, binding.address_objid::bigint,
                    bootstrap.phase
             FROM shiba_internal.source_bootstrap AS bootstrap
             JOIN shiba_internal.source_ingress_config AS config USING (source_id)
             JOIN shiba_internal.source_binding AS binding
               ON binding.source_id = bootstrap.source_id
              AND binding.binding_kind = 'relation'
              AND binding.address_objsubid = 0
             WHERE bootstrap.source_id = $1 AND bootstrap.bootstrap_id = $2
               AND bootstrap.slot_generation = $3
               AND config.slot_name = bootstrap.slot_name
               AND config.slot_generation = bootstrap.slot_generation
             FOR UPDATE OF bootstrap, config, binding",
            &[
                &source_key,
                &as_bigint(target_bootstrap_id.get())?,
                &as_bigint(target_generation.get())?,
            ],
        )?
        .ok_or(IngressError::Governance(
            "prepared rebuild authority is missing",
        ))?;
    let relation_class: i64 = row.get(5);
    let relation_oid: i64 = row.get(6);
    let identity_oid = validate_bindings(transaction, source_id, relation_class, relation_oid, 2)?
        .ok_or(IngressError::Governance(
            "prepared rebuild identity binding is missing",
        ))?;
    let (count_operator_id, sum_operator_id) =
        load_operator_ids(transaction, source_key, relation_class, relation_oid)?;
    let authority = PreparedAuthority {
        source_id,
        target: RebuildIdentity {
            bootstrap_id: target_bootstrap_id,
            relation_oid: as_oid(relation_oid, "target relation OID")?,
            identity_index_oid: as_oid(identity_oid, "target identity index OID")?,
            publication_oid: as_oid(row.get(4), "target publication OID")?,
            slot_name: row.get(0),
            slot_generation: target_generation,
        },
        retired_bootstrap_id: bootstrap_id(row.get(1))?,
        retired_slot_name: row.get(2),
        retired_slot_generation: slot_generation(row.get(3))?,
        count_operator_id,
        sum_operator_id,
    };
    Ok((authority, row.get(7)))
}

fn load_operator_ids(
    transaction: &mut postgres::Transaction<'_>,
    source_id: i64,
    relation_class: i64,
    relation_oid: i64,
) -> Result<(OperatorId, OperatorId), IngressError> {
    let rows = transaction.query(
        "SELECT operator_id, operator_kind, input_classid::bigint,
                input_objid::bigint, input_objsubid
         FROM shiba_internal.operator_definition
         WHERE source_id = $1 ORDER BY operator_id FOR UPDATE",
        &[&source_id],
    )?;
    if rows.len() != 2 {
        return Err(IngressError::Governance("prepared operator plan drifted"));
    }
    let mut count = None;
    let mut sum = None;
    for row in rows {
        let id = operator_id(row.get(0))?;
        match row.get::<_, &str>(1) {
            "count_rows"
                if count.is_none()
                    && row.get::<_, Option<i64>>(2).is_none()
                    && row.get::<_, Option<i64>>(3).is_none()
                    && row.get::<_, Option<i32>>(4).is_none() =>
            {
                count = Some(id);
            }
            "sum_int8"
                if sum.is_none()
                    && row.get::<_, Option<i64>>(2) == Some(relation_class)
                    && row.get::<_, Option<i64>>(3) == Some(relation_oid)
                    && row.get::<_, Option<i32>>(4) == Some(2) =>
            {
                sum = Some(id);
            }
            _ => return Err(IngressError::Governance("prepared operator plan drifted")),
        }
    }
    count
        .zip(sum)
        .ok_or(IngressError::Governance("prepared operator plan drifted"))
}

fn as_oid(value: i64, label: &'static str) -> Result<u32, IngressError> {
    u32::try_from(value)
        .ok()
        .filter(|value| *value != 0)
        .ok_or(IngressError::InvalidIdentifier(label))
}

fn operator_id(value: i64) -> Result<OperatorId, IngressError> {
    u64::try_from(value)
        .ok()
        .and_then(NonZeroU64::new)
        .map(OperatorId::new)
        .ok_or(IngressError::Governance("operator identity is invalid"))
}

fn bootstrap_id(value: i64) -> Result<BootstrapId, IngressError> {
    u64::try_from(value)
        .ok()
        .and_then(|value| BootstrapId::new(value).ok())
        .ok_or(IngressError::Governance(
            "retired bootstrap identity is invalid",
        ))
}

fn slot_generation(value: i64) -> Result<SlotGeneration, IngressError> {
    u64::try_from(value)
        .ok()
        .and_then(|value| SlotGeneration::new(value).ok())
        .ok_or(IngressError::Governance(
            "retired slot generation is invalid",
        ))
}
