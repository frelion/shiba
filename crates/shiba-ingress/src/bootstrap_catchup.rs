use postgres::Client;
use shiba_protocol::{BootstrapId, SlotGeneration, SourceId};
use shiba_runtime::activate_bootstrap;

use crate::{
    BootstrapFence, DurableTransaction, GovernedSourceSession, IngressError, ReplicationMode,
    ShutdownHandle, SourceReceiver,
    bootstrap::{BootstrapOptions, BootstrapParts, BootstrapSession, BootstrapSpec},
    bootstrap_transition::prepare_catchup,
    connection_config::{open_apply, replication_database},
    governance::GovernedConfig,
    governed::{AttachOptions, advisory_key},
    limits::ActivePermit,
    tokens::BootstrapInput,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BootstrapCatchupProgress {
    TransactionApplied,
    Active,
}

pub struct BootstrapCatchupSession {
    receiver: Option<SourceReceiver>,
    apply: Client,
    config: GovernedConfig,
    spec: BootstrapSpec,
    options: BootstrapOptions,
    expected_content: String,
    shutdown: ShutdownHandle,
    pending_durable: Option<DurableTransaction>,
    pending_fence: Option<BootstrapFence>,
    active: bool,
    apply_conninfo: String,
    replication_conninfo: String,
    advisory_key: i64,
    permit: ActivePermit,
}

impl BootstrapCatchupSession {
    pub(crate) fn from_scanned(session: BootstrapSession) -> Result<Self, IngressError> {
        let BootstrapParts {
            mut apply,
            config,
            spec,
            options,
            apply_conninfo,
            replication_conninfo,
            advisory_key,
            permit,
            exporter,
            scanner,
        } = session.into_parts();
        drop(scanner);
        drop(exporter);
        let (config, expected_content, start_lsn, active) =
            prepare_catchup(&mut apply, config, &spec, "scan_complete")?;
        Self::connect(
            apply,
            config,
            spec,
            options,
            expected_content,
            start_lsn,
            active,
            apply_conninfo,
            replication_conninfo,
            advisory_key,
            permit,
        )
    }

    /// Resumes the same post-scan slot after a Shiba or `PostgreSQL` restart.
    ///
    /// # Errors
    /// Refuses scanning/pre-active reset states, a different attempt, or drift.
    pub fn resume(
        apply_conninfo: &str,
        replication_conninfo: &str,
        source_id: SourceId,
        bootstrap_id: BootstrapId,
        slot_generation: SlotGeneration,
        options: BootstrapOptions,
    ) -> Result<Self, IngressError> {
        let permit = ActivePermit::acquire()?;
        let (mut apply, database) = open_apply(apply_conninfo, options.statement_timeout())?;
        if database != replication_database(replication_conninfo)? {
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
        let source_key = as_bigint(source_id.get())?;
        let bootstrap_key = as_bigint(bootstrap_id.get())?;
        let row = apply
            .query_opt(
                "SELECT slot_name::text, slot_generation, phase
                 FROM shiba_internal.source_bootstrap
                 WHERE source_id = $1 AND bootstrap_id = $2",
                &[&source_key, &bootstrap_key],
            )?
            .ok_or(IngressError::Governance("bootstrap attempt is missing"))?;
        if row.get::<_, i64>(1) != as_bigint(slot_generation.get())? {
            return Err(IngressError::Governance("bootstrap generation mismatch"));
        }
        let phase: &str = row.get(2);
        if !matches!(phase, "scan_complete" | "catching_up" | "active") {
            return Err(IngressError::Governance(
                "bootstrap is not resumable catch-up",
            ));
        }
        let spec = BootstrapSpec {
            source_id,
            bootstrap_id,
            publication_oid: 1,
            slot_name: row.get(0),
            slot_generation,
        };
        let (config, _) = GovernedConfig::load(&mut apply, source_id, slot_generation, false)?;
        let (config, expected_content, start_lsn, active) =
            prepare_catchup(&mut apply, config, &spec, phase)?;
        Self::connect(
            apply,
            config,
            spec,
            options,
            expected_content,
            start_lsn,
            active,
            apply_conninfo.to_owned(),
            replication_conninfo.to_owned(),
            advisory_key,
            permit,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn connect(
        apply: Client,
        config: GovernedConfig,
        spec: BootstrapSpec,
        options: BootstrapOptions,
        expected_content: String,
        start_lsn: u64,
        active: bool,
        apply_conninfo: String,
        replication_conninfo: String,
        advisory_key: i64,
        permit: ActivePermit,
    ) -> Result<Self, IngressError> {
        let receiver = if active {
            None
        } else {
            Some(SourceReceiver::connect_with_messages(
                &replication_conninfo,
                &config.slot_name,
                &config.publication_name,
                ReplicationMode::Committed,
                start_lsn,
                start_lsn,
                true,
            )?)
        };
        Ok(Self {
            receiver,
            apply,
            config,
            spec,
            options,
            expected_content,
            shutdown: ShutdownHandle::new(),
            pending_durable: None,
            pending_fence: None,
            active,
            apply_conninfo,
            replication_conninfo,
            advisory_key,
            permit,
        })
    }

    /// Applies and acknowledges at most one terminal outcome in stream order.
    ///
    /// # Errors
    /// Decoder/Runtime/governance failures never advance feedback. Failed ACKs
    /// retain the exact in-memory token for a same-session retry.
    pub fn catch_up_next(&mut self) -> Result<BootstrapCatchupProgress, IngressError> {
        if self.active {
            return Ok(BootstrapCatchupProgress::Active);
        }
        self.config.revalidate(&mut self.apply, true)?;
        if self.pending_durable.is_some() {
            return self.retry_durable_ack();
        }
        if self.pending_fence.is_some() {
            return self.retry_fence_activation();
        }
        let mut receiver = self
            .receiver
            .take()
            .ok_or(IngressError::Governance("bootstrap receiver is detached"))?;
        let outcome = receiver.receive_bootstrap_one(
            self.config.source,
            self.spec.source_id,
            self.spec.bootstrap_id,
            &self.expected_content,
            &self.shutdown,
        );
        self.receiver = Some(receiver);
        let input = outcome?;
        self.config.revalidate(&mut self.apply, true)?;
        match input {
            BootstrapInput::Transaction(input) => {
                let mut receiver = self
                    .receiver
                    .take()
                    .ok_or(IngressError::Governance("bootstrap receiver is detached"))?;
                let applied = receiver.apply_received(&mut self.apply, &input);
                self.receiver = Some(receiver);
                let token = applied?;
                self.pending_durable = Some(token);
                self.retry_durable_ack()
            }
            BootstrapInput::Fence(token) => {
                self.pending_fence = Some(token);
                self.retry_fence_activation()
            }
        }
    }

    fn retry_durable_ack(&mut self) -> Result<BootstrapCatchupProgress, IngressError> {
        self.config.revalidate(&mut self.apply, true)?;
        let token = self
            .pending_durable
            .take()
            .ok_or(IngressError::FeedbackMismatch)?;
        if let Err(error) = self.receiver_mut()?.acknowledge(&token) {
            self.pending_durable = Some(token);
            return Err(error);
        }
        Ok(BootstrapCatchupProgress::TransactionApplied)
    }

    fn retry_fence_activation(&mut self) -> Result<BootstrapCatchupProgress, IngressError> {
        self.config.revalidate(&mut self.apply, true)?;
        let token = self
            .pending_fence
            .take()
            .ok_or(IngressError::FeedbackMismatch)?;
        if token.source_id() != self.spec.source_id
            || token.bootstrap_id() != self.spec.bootstrap_id
        {
            return Err(IngressError::FeedbackMismatch);
        }
        activate_bootstrap(
            &mut self.apply,
            self.spec.source_id,
            self.spec.bootstrap_id,
            token.message_lsn(),
            token.end_lsn(),
        )?;
        self.config.revalidate(&mut self.apply, true)?;
        if let Err(error) = self.receiver_mut()?.acknowledge_fence(&token) {
            self.pending_fence = Some(token);
            return Err(error);
        }
        self.active = true;
        Ok(BootstrapCatchupProgress::Active)
    }

    /// Converts an activated bootstrap into the ordinary governed M10 session.
    ///
    /// # Errors
    /// Fails if activation has not durably committed or advisory ownership was
    /// lost before the normal receiver reattaches.
    pub fn into_live(self) -> Result<GovernedSourceSession, IngressError> {
        if !self.active {
            return Err(IngressError::Governance("bootstrap is not active"));
        }
        let Self {
            receiver,
            mut apply,
            spec,
            options,
            apply_conninfo,
            replication_conninfo,
            advisory_key,
            permit,
            ..
        } = self;
        drop(receiver);
        let released: bool = apply
            .query_one("SELECT pg_catalog.pg_advisory_unlock($1)", &[&advisory_key])?
            .get(0);
        if !released {
            return Err(IngressError::Governance(
                "source advisory lock was not held",
            ));
        }
        drop(apply);
        drop(permit);
        GovernedSourceSession::attach(
            &apply_conninfo,
            &replication_conninfo,
            spec.source_id,
            spec.slot_generation,
            AttachOptions::new(ReplicationMode::Committed, options.statement_timeout())?,
        )
    }

    #[must_use]
    pub fn shutdown_handle(&self) -> ShutdownHandle {
        self.shutdown.clone()
    }

    fn receiver_mut(&mut self) -> Result<&mut SourceReceiver, IngressError> {
        self.receiver
            .as_mut()
            .ok_or(IngressError::Governance("bootstrap receiver is detached"))
    }
}

fn as_bigint(value: u64) -> Result<i64, IngressError> {
    i64::try_from(value).map_err(|_| IngressError::Governance("identity exceeds bigint"))
}
