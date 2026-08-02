use core::str::FromStr;
use std::{num::NonZeroUsize, time::Duration};

use postgres::{Client, IsolationLevel};
use shiba_protocol::{BootstrapBatchId, BootstrapId, PostgresLsn, SlotGeneration, SourceId};
use shiba_runtime::{
    BootstrapBatch, MAX_BOOTSTRAP_BATCH_ROWS, SnapshotRow, complete_bootstrap_scan,
    process_bootstrap_batch,
};

use crate::{
    IngressError,
    bootstrap_catchup::BootstrapCatchupSession,
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
    scanner: Client,
    exporter: ReplicationTransport,
    config: GovernedConfig,
    spec: BootstrapSpec,
    options: BootstrapOptions,
    snapshot_name: String,
    locator: ScanLocator,
    next_ordinal: u64,
    last_key: Option<i64>,
    apply_conninfo: String,
    replication_conninfo: String,
    advisory_key: i64,
    permit: ActivePermit,
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

    /// Scans and durably applies one bounded batch from the exported snapshot.
    ///
    /// # Errors
    /// Any snapshot, authority, ordering, Apply, or operator failure rolls back
    /// the current Apply transaction and leaves the checkpoint retryable.
    pub fn scan_next(&mut self) -> Result<SnapshotProgress, IngressError> {
        self.config.revalidate(&mut self.apply, false)?;
        let mut scan = self
            .scanner
            .build_transaction()
            .isolation_level(IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()?;
        scan.batch_execute(&format!(
            "SET TRANSACTION SNAPSHOT '{}'",
            self.snapshot_name
        ))?;
        let batch_limit =
            i64::try_from(self.options.batch_rows()).map_err(|_| IngressError::LimitExceeded)?;
        let rows = scan.query(&self.locator.query, &[&self.last_key, &batch_limit])?;
        let snapshot_rows: Vec<SnapshotRow> = rows
            .into_iter()
            .map(|row| SnapshotRow {
                source_row_id: row.get(0),
                payload: row.get(1),
            })
            .collect();
        scan.commit()?;
        if snapshot_rows.is_empty() {
            complete_bootstrap_scan(&mut self.apply, self.spec.source_id, self.spec.bootstrap_id)?;
            return Ok(SnapshotProgress::ScanComplete);
        }
        let count = snapshot_rows.len();
        let last_key = snapshot_rows
            .last()
            .ok_or(IngressError::InvalidEnvelope("empty snapshot batch"))?
            .source_row_id;
        let batch_id = BootstrapBatchId::new(self.spec.bootstrap_id, self.next_ordinal)
            .map_err(|_| IngressError::LimitExceeded)?;
        let batch = BootstrapBatch::new(self.spec.source_id, batch_id, snapshot_rows)?;
        process_bootstrap_batch(&mut self.apply, &batch)?;
        let ordinal = self.next_ordinal;
        self.next_ordinal = ordinal.checked_add(1).ok_or(IngressError::LimitExceeded)?;
        self.last_key = Some(last_key);
        Ok(SnapshotProgress::BatchApplied {
            ordinal,
            rows: count,
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

struct ScanLocator {
    query: String,
}

impl ScanLocator {
    fn load(client: &mut Client, source_id: i64) -> Result<Self, IngressError> {
        let row = client.query_one(
            "SELECT namespace.nspname::text, class.relname::text,
                    key.attname::text, payload.attname::text,
                    key.atttypid::bigint, key.attnotnull,
                    payload.atttypid::bigint, payload.attnotnull
             FROM shiba_internal.source_binding AS binding
             JOIN pg_catalog.pg_class AS class ON class.oid = binding.address_objid
             JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace
             JOIN pg_catalog.pg_attribute AS key
               ON key.attrelid = class.oid AND key.attnum = 1
             JOIN pg_catalog.pg_attribute AS payload
               ON payload.attrelid = class.oid AND payload.attnum = 2
             WHERE binding.source_id = $1 AND binding.binding_kind = 'relation'",
            &[&source_id],
        )?;
        if row.get::<_, i64>(4) != 20
            || !row.get::<_, bool>(5)
            || row.get::<_, i64>(6) != 20
            || row.get::<_, bool>(7)
        {
            return Err(IngressError::Governance(
                "bootstrap requires int8 key and nullable int8 payload",
            ));
        }
        let relation = format!(
            "{}.{}",
            quote_identifier(row.get(0)),
            quote_identifier(row.get(1))
        );
        let key = quote_identifier(row.get(2));
        let payload = quote_identifier(row.get(3));
        Ok(Self {
            query: format!(
                "SELECT {key}, {payload} FROM {relation}
                 WHERE ($1::bigint IS NULL OR {key} > $1)
                 ORDER BY {key} LIMIT $2"
            ),
        })
    }
}

pub(crate) fn validate_spec(spec: &BootstrapSpec) -> Result<(), IngressError> {
    if spec.publication_oid == 0 {
        return Err(IngressError::InvalidIdentifier("publication OID"));
    }
    validate_slot(&spec.slot_name)
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

pub(crate) fn as_bigint(value: u64) -> Result<i64, IngressError> {
    i64::try_from(value).map_err(|_| IngressError::Governance("identity exceeds bigint"))
}
