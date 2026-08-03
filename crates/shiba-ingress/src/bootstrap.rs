use std::{num::NonZeroUsize, time::Duration};

use postgres::Client;
use shiba_protocol::{BootstrapId, GraphId, SlotGeneration, SourceId};
use shiba_runtime::MAX_BOOTSTRAP_BATCH_ROWS;

use crate::{
    IngressError,
    bootstrap_catchup::BootstrapCatchupSession,
    bootstrap_locator::ScanLocator,
    governance::GovernedConfig,
    governed::AttachOptions,
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
