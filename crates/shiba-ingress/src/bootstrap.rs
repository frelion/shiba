use core::str::FromStr;
use std::{num::NonZeroUsize, time::Duration};

use postgres::Client;
use shiba_protocol::{BootstrapId, PostgresLsn, SlotGeneration, SourceId};
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
    pub source_id: SourceId,
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
    pub(crate) locator: ScanLocator,
    pub(crate) next_ordinal: u64,
    pub(crate) last_key: Option<i64>,
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
        let source_id = as_bigint(spec.source_id.get())?;
        let bootstrap_id = as_bigint(spec.bootstrap_id.get())?;
        let generation = as_bigint(spec.slot_generation.get())?;
        let publication_oid = i64::from(spec.publication_oid);
        apply.query_one(
            "SELECT shiba_internal.reserve_source_bootstrap(
                 $1, $2, $3::bigint::oid, $4::text::name, $5
             )",
            &[
                &bootstrap_id,
                &source_id,
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
        let source_id = as_bigint(spec.source_id.get())?;
        let bootstrap_id = as_bigint(spec.bootstrap_id.get())?;
        let replication = ReplicationTransport::connect(&replication_conninfo)?;
        let boundary = match replication.create_exported_slot(&spec.slot_name) {
            Ok(boundary) => boundary,
            Err(error) => {
                apply.execute(
                    "UPDATE shiba_internal.source_bootstrap
                     SET phase = 'cleanup_pending'
                     WHERE source_id = $1 AND bootstrap_id = $2 AND phase = 'creating'",
                    &[&source_id, &bootstrap_id],
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
            "UPDATE shiba_internal.source_bootstrap
             SET consistent_point = $1::text::pg_lsn, phase = 'scanning'
             WHERE source_id = $2 AND bootstrap_id = $3 AND phase = 'creating'",
            &[&boundary.consistent_point, &source_id, &bootstrap_id],
        )? != 1
        {
            return Err(IngressError::Governance("bootstrap reservation drifted"));
        }
        let (config, confirmed_lsn) =
            GovernedConfig::load(&mut apply, spec.source_id, spec.slot_generation, false)?;
        if confirmed_lsn != consistent_point.as_u64() {
            return Err(IngressError::Governance(
                "slot confirmed LSN differs from consistent point",
            ));
        }
        let locator = ScanLocator::load(&mut apply, source_id)?;
        Ok(Self {
            apply,
            scanner,
            exporter: replication,
            config,
            spec,
            options,
            snapshot_name: boundary.snapshot_name,
            locator,
            next_ordinal: 1,
            last_key: None,
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
