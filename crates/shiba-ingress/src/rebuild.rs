use postgres::Client;
use shiba_protocol::{BootstrapId, SlotGeneration, SourceId};

use crate::{
    BootstrapOptions, IngressError,
    bootstrap::as_bigint,
    connection_config::{open_apply, replication_database},
    governed::advisory_key,
    limits::ActivePermit,
    operator_authority::load_plan_fingerprints,
    rebuild_model::{PreparedAuthority, RebuildSpec},
    rebuild_resume::load_prepared_authority,
    rebuild_validation::verify_rebuild_target,
    transport::{ReplicationTransport, validate_slot},
};

/// Exclusive owner of a durably prepared, forward-only rebuild.
///
/// It owns the source advisory lock and process admission permit, but performs
/// no replication-slot operation. The target catalog identity is already the
/// sole `building` authority when this value is returned.
pub struct PreparedRebuild {
    pub(crate) apply: Client,
    pub(crate) authority: PreparedAuthority,
    pub(crate) options: BootstrapOptions,
    pub(crate) apply_conninfo: String,
    pub(crate) replication_conninfo: String,
    pub(crate) advisory_key: i64,
    pub(crate) permit: ActivePermit,
}

impl PreparedRebuild {
    /// Performs all local validation, locks the target relation, and invokes
    /// the sole SQL lifecycle writer in one transaction.
    ///
    /// # Errors
    /// Every error before commit leaves the old active authority untouched.
    /// No slot is created, dropped, attached, adopted, or acknowledged here.
    pub fn prepare(
        apply_conninfo: &str,
        replication_conninfo: &str,
        spec: RebuildSpec,
        options: BootstrapOptions,
    ) -> Result<Self, IngressError> {
        validate_spec(&spec)?;
        let permit = ActivePermit::acquire()?;
        let (mut apply, apply_database) = open_apply(apply_conninfo, options.statement_timeout())?;
        if apply_database != replication_database(replication_conninfo)? {
            return Err(IngressError::Governance("connection databases differ"));
        }
        let replication = ReplicationTransport::connect(replication_conninfo)?;
        let replication_principal = replication.preflight(&apply_database)?;
        let receiver_can_read: Option<bool> = apply
            .query_one(
                "SELECT pg_catalog.has_table_privilege($1, $2::bigint::oid, 'SELECT')",
                &[&replication_principal, &i64::from(spec.target.relation_oid)],
            )?
            .get(0);
        if receiver_can_read != Some(true) {
            return Err(IngressError::Governance(
                "replication credential lacks SELECT on target relation",
            ));
        }
        drop(replication);
        let advisory_key = advisory_key(spec.source_id)?;
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
        lock_target_relation(&mut transaction, spec.target.relation_oid)?;
        // Destructive prepare must never repair a corrupt old authority. Decode
        // and validate the complete durable plan set while the old binding is
        // still the sole active authority.
        load_plan_fingerprints(&mut transaction, spec.source_id)?;
        invoke_prepare_writer(&mut transaction, &spec)?;
        transaction.commit()?;

        let mut transaction = apply.transaction()?;
        let authority = load_prepared_authority(
            &mut transaction,
            spec.source_id,
            spec.target.bootstrap_id,
            spec.target.slot_generation,
        )?;
        if !authority.matches_spec(&spec) {
            return Err(IngressError::Governance(
                "durable prepared rebuild differs from request",
            ));
        }
        verify_rebuild_target(&mut transaction, &authority)?;
        transaction.commit()?;
        drop(spec);
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

    #[must_use]
    pub const fn source_id(&self) -> SourceId {
        self.authority.source_id
    }

    #[must_use]
    pub const fn target_bootstrap_id(&self) -> BootstrapId {
        self.authority.target.bootstrap_id
    }

    #[must_use]
    pub const fn target_generation(&self) -> SlotGeneration {
        self.authority.target.slot_generation
    }

    /// Explicitly releases the source owner after a prepared handoff.
    ///
    /// # Errors
    /// Fails if this session no longer owns the exact advisory lock.
    pub fn detach(mut self) -> Result<(), IngressError> {
        let released: bool = self
            .apply
            .query_one(
                "SELECT pg_catalog.pg_advisory_unlock($1)",
                &[&self.advisory_key],
            )?
            .get(0);
        if !released {
            return Err(IngressError::Governance(
                "source advisory lock was not held",
            ));
        }
        Ok(())
    }
}

fn validate_spec(spec: &RebuildSpec) -> Result<(), IngressError> {
    for (value, name) in [
        (spec.expected.relation_oid, "expected relation OID"),
        (
            spec.expected.identity_index_oid,
            "expected identity index OID",
        ),
        (spec.expected.publication_oid, "expected publication OID"),
        (spec.target.relation_oid, "target relation OID"),
        (spec.target.identity_index_oid, "target identity index OID"),
        (spec.target.publication_oid, "target publication OID"),
    ] {
        if value == 0 {
            return Err(IngressError::InvalidIdentifier(name));
        }
    }
    validate_slot(&spec.expected.slot_name)?;
    validate_slot(&spec.target.slot_name)?;
    if spec.expected.slot_name == spec.target.slot_name {
        return Err(IngressError::Governance("target slot name is not fresh"));
    }
    if spec.expected.bootstrap_id == spec.target.bootstrap_id {
        return Err(IngressError::Governance("target bootstrap ID is not fresh"));
    }
    if spec.expected.slot_generation.get().checked_add(1) != Some(spec.target.slot_generation.get())
    {
        return Err(IngressError::Governance(
            "target generation is not the exact successor",
        ));
    }
    Ok(())
}

fn lock_target_relation(
    transaction: &mut postgres::Transaction<'_>,
    relation_oid: u32,
) -> Result<(), IngressError> {
    let row = transaction
        .query_opt(
            "SELECT namespace.nspname::text, class.relname::text
             FROM pg_catalog.pg_class AS class
             JOIN pg_catalog.pg_namespace AS namespace
               ON namespace.oid = class.relnamespace
             WHERE class.oid = $1::bigint::oid",
            &[&i64::from(relation_oid)],
        )?
        .ok_or(IngressError::Governance("target relation is missing"))?;
    let namespace: &str = row.get(0);
    let relation: &str = row.get(1);
    let qualified = format!(
        "{}.{}",
        quote_identifier(namespace),
        quote_identifier(relation)
    );
    transaction.batch_execute(&format!("LOCK TABLE {qualified} IN ACCESS SHARE MODE"))?;
    let locked_oid: i64 = transaction
        .query_one(
            "SELECT pg_catalog.to_regclass($1)::oid::bigint",
            &[&qualified],
        )?
        .get(0);
    if locked_oid != i64::from(relation_oid) {
        return Err(IngressError::Governance("target relation identity drifted"));
    }
    Ok(())
}

fn invoke_prepare_writer(
    transaction: &mut postgres::Transaction<'_>,
    spec: &RebuildSpec,
) -> Result<(), IngressError> {
    let source_id = as_bigint(spec.source_id.get())?;
    let old_bootstrap = as_bigint(spec.expected.bootstrap_id.get())?;
    let old_generation = as_bigint(spec.expected.slot_generation.get())?;
    let new_bootstrap = as_bigint(spec.target.bootstrap_id.get())?;
    let new_generation = as_bigint(spec.target.slot_generation.get())?;
    transaction.query_one(
        "SELECT shiba_internal.prepare_source_rebuild(
             $1, $2, $3::bigint::oid, $4::bigint::oid, $5::bigint::oid,
             $6::text::name, $7, $8, $9::bigint::oid::regclass,
             $10::bigint::oid::regclass, $11::bigint::oid, $12::text::name, $13
         )",
        &[
            &source_id,
            &old_bootstrap,
            &i64::from(spec.expected.relation_oid),
            &i64::from(spec.expected.identity_index_oid),
            &i64::from(spec.expected.publication_oid),
            &spec.expected.slot_name,
            &old_generation,
            &new_bootstrap,
            &i64::from(spec.target.relation_oid),
            &i64::from(spec.target.identity_index_oid),
            &i64::from(spec.target.publication_oid),
            &spec.target.slot_name,
            &new_generation,
        ],
    )?;
    shiba_runtime::recompile_registered_plans(transaction, spec.source_id)?;
    Ok(())
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RebuildIdentity;

    fn identity(bootstrap: u64, generation: u64, slot: &str) -> RebuildIdentity {
        RebuildIdentity {
            bootstrap_id: BootstrapId::new(bootstrap).unwrap(),
            relation_oid: 10,
            identity_index_oid: 11,
            publication_oid: 12,
            slot_name: slot.to_owned(),
            slot_generation: SlotGeneration::new(generation).unwrap(),
        }
    }

    fn spec() -> RebuildSpec {
        RebuildSpec {
            source_id: SourceId::new(1).unwrap(),
            expected: identity(1, 1, "old_slot"),
            target: identity(2, 2, "new_slot"),
        }
    }

    #[test]
    fn rebuild_identity_requires_exact_successor_and_fresh_attempt() {
        assert!(validate_spec(&spec()).is_ok());
        let mut wrong = spec();
        wrong.target.slot_generation = SlotGeneration::new(3).unwrap();
        assert!(validate_spec(&wrong).is_err());
        let mut reused = spec();
        reused.target.slot_name = reused.expected.slot_name.clone();
        assert!(validate_spec(&reused).is_err());
        let mut zero_oid = spec();
        zero_oid.target.relation_oid = 0;
        assert!(validate_spec(&zero_oid).is_err());
    }

    #[test]
    fn quoted_lock_identifier_escapes_each_identifier() {
        assert_eq!(quote_identifier("odd\"name"), "\"odd\"\"name\"");
    }
}
