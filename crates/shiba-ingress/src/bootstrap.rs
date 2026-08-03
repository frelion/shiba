use core::str::FromStr;
use std::{num::NonZeroUsize, time::Duration};

use postgres::Client;
use shiba_protocol::{BootstrapId, GraphId, PostgresLsn, SlotGeneration, SourceId};
use shiba_runtime::MAX_BOOTSTRAP_BATCH_ROWS;

use crate::{
    IngressError,
    bootstrap_catchup::BootstrapCatchupSession,
    bootstrap_locator::ScanLocator,
    connection_config::{open_apply, replication_database},
    governance::GovernedConfig,
    governed::{AttachOptions, advisory_key},
    limits::ActivePermit,
    transport::{ReplicationTransport, validate_slot},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BootstrapSpec {
    pub graph_id: GraphId,
    pub bootstrap_id: BootstrapId,
    pub publication_oid: u32,
    pub slot_name: String,
    pub slot_generation: SlotGeneration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BootstrapOptions {
    batch_rows: NonZeroUsize,
    statement_timeout: Duration,
}

impl BootstrapOptions {
    /// # Errors
    /// Rejects batches above the shared Runtime bound and invalid timeouts.
    pub fn new(batch_rows: usize, statement_timeout: Duration) -> Result<Self, IngressError> {
        let batch_rows = NonZeroUsize::new(batch_rows)
            .filter(|value| value.get() <= MAX_BOOTSTRAP_BATCH_ROWS)
            .ok_or(IngressError::LimitExceeded)?;
        AttachOptions::new(crate::ReplicationMode::Committed, statement_timeout)?;
        Ok(Self {
            batch_rows,
            statement_timeout,
        })
    }

    #[must_use]
    pub const fn batch_rows(self) -> usize {
        self.batch_rows.get()
    }

    #[must_use]
    pub const fn statement_timeout(self) -> Duration {
        self.statement_timeout
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotProgress {
    BatchApplied { ordinal: u64, rows: usize },
    ScanComplete,
}

pub struct BootstrapSession {
    pub(crate) apply: Client,
    pub(crate) scanner: Client,
    pub(crate) exporter: ReplicationTransport,
    pub(crate) config: GovernedConfig,
    pub(crate) spec: BootstrapSpec,
    pub(crate) options: BootstrapOptions,
    pub(crate) snapshot_name: String,
    pub(crate) members: Vec<MemberScan>,
    pub(crate) current_member: usize,
    pub(crate) apply_conninfo: String,
    pub(crate) replication_conninfo: String,
    pub(crate) advisory_key: i64,
    pub(crate) permit: ActivePermit,
}

impl BootstrapSession {
    /// Reserves one pristine attempt and creates its exact exported-snapshot slot.
    ///
    /// # Errors
    /// Fails closed on duplicate ownership, drift, existing slot, or malformed
    /// replication response. A post-reservation failure remains recoverable as
    /// the exact `creating` attempt; it is never silently replaced.
    pub fn begin(
        apply_conninfo: &str,
        replication_conninfo: &str,
        spec: BootstrapSpec,
        options: BootstrapOptions,
    ) -> Result<Self, IngressError> {
        validate_spec(&spec)?;
        let permit = ActivePermit::acquire()?;
        let (mut apply, apply_database) = open_apply(apply_conninfo, options.statement_timeout)?;
        let replication_database = replication_database(replication_conninfo)?;
        if apply_database != replication_database {
            return Err(IngressError::Governance("connection databases differ"));
        }
        let (scanner, scanner_database) = open_apply(apply_conninfo, options.statement_timeout)?;
        if scanner_database != apply_database {
            return Err(IngressError::Governance("scanner database differs"));
        }
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
        let graph_id = as_bigint(spec.graph_id.get())?;
        let bootstrap_id = as_bigint(spec.bootstrap_id.get())?;
        let generation = as_bigint(spec.slot_generation.get())?;
        let publication_oid = i64::from(spec.publication_oid);
        apply.query_one(
            "SELECT shiba_internal.reserve_graph_bootstrap(
                 $1, $2, $3::bigint::oid, $4::text::name, $5
             )",
            &[
                &bootstrap_id,
                &graph_id,
                &publication_oid,
                &spec.slot_name,
                &generation,
            ],
        )?;

        Self::finish_reserved(ReservedBootstrap {
            apply,
            scanner,
            spec,
            options,
            apply_conninfo: apply_conninfo.to_owned(),
            replication_conninfo: replication_conninfo.to_owned(),
            advisory_key,
            permit,
        })
    }

    pub(crate) fn finish_reserved(reserved: ReservedBootstrap) -> Result<Self, IngressError> {
        let ReservedBootstrap {
            mut apply,
            scanner,
            spec,
            options,
            apply_conninfo,
            replication_conninfo,
            advisory_key,
            permit,
        } = reserved;
        let graph_id = as_bigint(spec.graph_id.get())?;
        let bootstrap_id = as_bigint(spec.bootstrap_id.get())?;
        let replication = ReplicationTransport::connect(&replication_conninfo)?;
        let boundary = match replication.create_exported_slot(&spec.slot_name) {
            Ok(boundary) => boundary,
            Err(error) => {
                apply.execute(
                    "UPDATE shiba_internal.graph_bootstrap
                     SET phase = 'cleanup_pending'
                     WHERE graph_id = $1 AND bootstrap_id = $2 AND phase = 'creating'",
                    &[&graph_id, &bootstrap_id],
                )?;
                return Err(error);
            }
        };
        let consistent_point = PostgresLsn::from_str(&boundary.consistent_point)
            .map_err(|_| IngressError::InvalidEnvelope("invalid consistent point"))?;
        if consistent_point.is_zero() {
            return Err(IngressError::InvalidEnvelope("zero consistent point"));
        }
        if apply.execute(
            "UPDATE shiba_internal.graph_bootstrap
             SET consistent_point = $1::text::pg_lsn, phase = 'scanning'
             WHERE graph_id = $2 AND bootstrap_id = $3 AND phase = 'creating'",
            &[&boundary.consistent_point, &graph_id, &bootstrap_id],
        )? != 1
        {
            return Err(IngressError::Governance("bootstrap reservation drifted"));
        }
        let (config, confirmed_lsn) =
            GovernedConfig::load(&mut apply, spec.graph_id, spec.slot_generation, false)?;
        if confirmed_lsn != consistent_point.as_u64() {
            return Err(IngressError::Governance(
                "slot confirmed LSN differs from consistent point",
            ));
        }
        let member_rows = apply.query(
            "SELECT member.source_id, checkpoint.last_batch_ordinal,
                    checkpoint.last_source_row_id
             FROM shiba_internal.graph_source_member AS member
             JOIN shiba_internal.graph_bootstrap_checkpoint AS checkpoint
               ON (checkpoint.graph_id, checkpoint.source_id) =
                  (member.graph_id, member.source_id)
             WHERE member.graph_id = $1 ORDER BY member.input_ordinal",
            &[&graph_id],
        )?;
        if member_rows.is_empty() || member_rows.len() > 2 {
            return Err(IngressError::Governance(
                "graph bootstrap members are incomplete",
            ));
        }
        let members = member_rows
            .into_iter()
            .map(|row| {
                let raw: i64 = row.get(0);
                let source_id = u64::try_from(raw)
                    .ok()
                    .and_then(|value| SourceId::new(value).ok())
                    .ok_or(IngressError::Governance("source identity is invalid"))?;
                let ordinal: i64 = row.get(1);
                Ok(MemberScan {
                    source_id,
                    locator: ScanLocator::load(&mut apply, raw)?,
                    next_ordinal: u64::try_from(ordinal)
                        .ok()
                        .and_then(|value| value.checked_add(1))
                        .ok_or(IngressError::Governance("bootstrap ordinal is invalid"))?,
                    last_key: row.get(2),
                })
            })
            .collect::<Result<Vec<_>, IngressError>>()?;
        Ok(Self {
            apply,
            scanner,
            exporter: replication,
            config,
            spec,
            options,
            snapshot_name: boundary.snapshot_name,
            members,
            current_member: 0,
            apply_conninfo,
            replication_conninfo,
            advisory_key,
            permit,
        })
    }

    /// Releases the ephemeral snapshot and enters bounded M10 catch-up.
    ///
    /// # Errors
    /// Requires an atomically recorded `scan_complete` phase.
    pub fn into_catchup(self) -> Result<BootstrapCatchupSession, IngressError> {
        BootstrapCatchupSession::from_scanned(self)
    }

    pub(crate) fn into_parts(self) -> BootstrapParts {
        BootstrapParts {
            apply: self.apply,
            config: self.config,
            spec: self.spec,
            options: self.options,
            apply_conninfo: self.apply_conninfo,
            replication_conninfo: self.replication_conninfo,
            advisory_key: self.advisory_key,
            permit: self.permit,
            exporter: self.exporter,
            scanner: self.scanner,
        }
    }
}

pub(crate) struct MemberScan {
    pub(crate) source_id: SourceId,
    pub(crate) locator: ScanLocator,
    pub(crate) next_ordinal: u64,
    pub(crate) last_key: Option<i64>,
}

pub(crate) struct ReservedBootstrap {
    pub(crate) apply: Client,
    pub(crate) scanner: Client,
    pub(crate) spec: BootstrapSpec,
    pub(crate) options: BootstrapOptions,
    pub(crate) apply_conninfo: String,
    pub(crate) replication_conninfo: String,
    pub(crate) advisory_key: i64,
    pub(crate) permit: ActivePermit,
}

pub(crate) struct BootstrapParts {
    pub(crate) apply: Client,
    pub(crate) config: GovernedConfig,
    pub(crate) spec: BootstrapSpec,
    pub(crate) options: BootstrapOptions,
    pub(crate) apply_conninfo: String,
    pub(crate) replication_conninfo: String,
    pub(crate) advisory_key: i64,
    pub(crate) permit: ActivePermit,
    pub(crate) exporter: ReplicationTransport,
    pub(crate) scanner: Client,
}

pub(crate) fn validate_spec(spec: &BootstrapSpec) -> Result<(), IngressError> {
    if spec.publication_oid == 0 {
        return Err(IngressError::InvalidIdentifier("publication OID"));
    }
    validate_slot(&spec.slot_name)
}

pub(crate) fn as_bigint(value: u64) -> Result<i64, IngressError> {
    i64::try_from(value).map_err(|_| IngressError::Governance("identity exceeds bigint"))
}
