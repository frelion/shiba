use postgres::Client;
use shiba_protocol::{BootstrapId, GraphId, SlotGeneration};
use shiba_runtime::{RebuildSourceTarget, compile_rebuild_graph};

use crate::{
    BootstrapOptions, IngressError,
    connection_config::{open_apply, replication_database},
    governed::advisory_key,
    limits::ActivePermit,
    rebuild_model::{PreparedAuthority, RebuildSpec},
    rebuild_prepare::{invoke_prepare_writer, lock_target_relations, validate_spec},
    rebuild_resume::load_prepared_authority,
    rebuild_validation::verify_rebuild_target,
    transport::ReplicationTransport,
};

/// Exclusive owner of one durably prepared forward-only graph rebuild.
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
    /// Preflights and atomically installs one forward-only building authority.
    ///
    /// # Errors
    /// Fails before the destructive boundary on identity, privilege, slot,
    /// compilation, or authority drift; post-commit validation also fails closed.
    pub fn prepare(
        apply_conninfo: &str,
        replication_conninfo: &str,
        spec: &RebuildSpec,
        options: BootstrapOptions,
    ) -> Result<Self, IngressError> {
        validate_spec(spec)?;
        let permit = ActivePermit::acquire()?;
        let (mut apply, database) = open_apply(apply_conninfo, options.statement_timeout())?;
        if database != replication_database(replication_conninfo)? {
            return Err(IngressError::Governance("connection databases differ"));
        }
        let replication = ReplicationTransport::connect(replication_conninfo)?;
        let principal = replication.preflight(&database)?;
        for member in &spec.target.members {
            let can_read: Option<bool> = apply
                .query_one(
                    "SELECT pg_catalog.has_table_privilege($1, $2::bigint::oid, 'SELECT')",
                    &[&principal, &i64::from(member.relation_oid)],
                )?
                .get(0);
            if can_read != Some(true) {
                return Err(IngressError::Governance(
                    "replication credential lacks SELECT on target graph member",
                ));
            }
        }
        drop(replication);
        let advisory_key = advisory_key(spec.graph_id)?;
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
        lock_target_relations(&mut transaction, spec)?;
        let targets = spec
            .target
            .members
            .iter()
            .map(|member| RebuildSourceTarget {
                source_id: member.source_id,
                relation_id: member.relation_oid,
                identity_index_id: member.identity_index_oid,
            })
            .collect::<Vec<_>>();
        let artifact = compile_rebuild_graph(&mut transaction, spec.graph_id, &targets)
            .map_err(|_| IngressError::Governance("target graph compilation failed"))?;
        if artifact.graph_digest != spec.target.graph_digest {
            return Err(IngressError::Governance(
                "target graph digest differs from request",
            ));
        }
        invoke_prepare_writer(&mut transaction, spec, &artifact)?;
        transaction.commit()?;
        let mut transaction = apply.transaction()?;
        let authority = load_prepared_authority(
            &mut transaction,
            spec.graph_id,
            spec.target.bootstrap_id,
            spec.target.slot_generation,
        )?;
        if !authority.matches_spec(spec) {
            return Err(IngressError::Governance(
                "durable prepared graph differs from request",
            ));
        }
        verify_rebuild_target(&mut transaction, &authority)?;
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

    #[must_use]
    pub const fn graph_id(&self) -> GraphId {
        self.authority.graph_id
    }
    #[must_use]
    pub const fn target_bootstrap_id(&self) -> BootstrapId {
        self.authority.target.bootstrap_id
    }
    #[must_use]
    pub const fn target_generation(&self) -> SlotGeneration {
        self.authority.target.slot_generation
    }

    /// Releases this process's graph ownership without changing durable state.
    ///
    /// # Errors
    /// Fails if the exact graph advisory lock is no longer held.
    pub fn detach(mut self) -> Result<(), IngressError> {
        let released: bool = self
            .apply
            .query_one(
                "SELECT pg_catalog.pg_advisory_unlock($1)",
                &[&self.advisory_key],
            )?
            .get(0);
        if !released {
            return Err(IngressError::Governance("graph advisory lock was not held"));
        }
        Ok(())
    }
}
