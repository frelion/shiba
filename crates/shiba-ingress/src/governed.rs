use std::time::Duration;

use postgres::Client;
use shiba_protocol::{SlotGeneration, SourceId};

use crate::{
    AbortedTransaction, DurableTransaction, EmptyCommitted, IngressError, ReceivedInput,
    ReplicationMode, ShutdownHandle, SourceReceiver, StreamedInput,
    connection_config::{open_apply, replication_database},
    governance::GovernedConfig,
    limits::ActivePermit,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AttachOptions {
    mode: ReplicationMode,
    statement_timeout: Duration,
}

impl AttachOptions {
    /// Creates explicit synchronous Apply and replication options.
    ///
    /// # Errors
    /// Rejects zero or server-out-of-range statement timeouts.
    pub fn new(mode: ReplicationMode, statement_timeout: Duration) -> Result<Self, IngressError> {
        let millis = statement_timeout.as_millis();
        if millis == 0 || millis > i32::MAX as u128 {
            return Err(IngressError::Governance(
                "statement timeout is out of range",
            ));
        }
        Ok(Self {
            mode,
            statement_timeout,
        })
    }

    #[must_use]
    pub const fn mode(self) -> ReplicationMode {
        self.mode
    }

    #[must_use]
    pub const fn statement_timeout(self) -> Duration {
        self.statement_timeout
    }
}

/// Governed owner of exactly one Apply and one replication connection.
pub struct GovernedSourceSession {
    receiver: Option<SourceReceiver>,
    apply: Client,
    config: GovernedConfig,
    advisory_key: i64,
    shutdown: ShutdownHandle,
    _permit: ActivePermit,
}

impl GovernedSourceSession {
    /// Attaches to existing catalog, publication, and slot authorities.
    ///
    /// # Errors
    /// Fails closed on unbounded conninfo, duplicate ownership, catalog/live
    /// drift, unsupported source shape, or replication startup failure.
    pub fn attach(
        apply_conninfo: &str,
        replication_conninfo: &str,
        source_id: SourceId,
        expected_slot_generation: SlotGeneration,
        options: AttachOptions,
    ) -> Result<Self, IngressError> {
        let permit = ActivePermit::acquire()?;
        let (mut apply, apply_database) = open_apply(apply_conninfo, options.statement_timeout)?;
        let replication_database = replication_database(replication_conninfo)?;
        if apply_database != replication_database {
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

        let (config, confirmed_lsn) =
            GovernedConfig::load(&mut apply, source_id, expected_slot_generation, false)?;
        if config.database_name != apply_database {
            return Err(IngressError::Governance(
                "conninfo database does not match catalog",
            ));
        }
        if options.mode == ReplicationMode::Streamed && !config.streamed_admitted {
            return Err(IngressError::Governance(
                "streamed mode requires key-only publication shape",
            ));
        }
        let receiver = SourceReceiver::connect(
            replication_conninfo,
            &config.slot_name,
            &config.publication_name,
            options.mode,
            confirmed_lsn,
            confirmed_lsn,
        )?;
        config.revalidate(&mut apply, true)?;
        let shutdown = ShutdownHandle::new();
        Ok(Self {
            receiver: Some(receiver),
            apply,
            config,
            advisory_key,
            shutdown,
            _permit: permit,
        })
    }

    /// Receives one governed protocol-v1 transaction.
    ///
    /// # Errors
    /// Revalidates all durable/live authority before entering receive.
    pub fn receive_one(&mut self) -> Result<ReceivedInput, IngressError> {
        self.revalidate()?;
        let source = self.config.source;
        let shutdown = self.shutdown.clone();
        self.receiver_mut()?.receive_one(source, &shutdown)
    }

    /// Receives one governed protocol-v2 terminal.
    ///
    /// # Errors
    /// Revalidates all durable/live authority before entering receive.
    pub fn receive_streamed_one(&mut self) -> Result<StreamedInput, IngressError> {
        self.revalidate()?;
        let source = self.config.source;
        let shutdown = self.shutdown.clone();
        self.receiver_mut()?.receive_streamed_one(source, &shutdown)
    }

    /// Applies an exact received input with the session-owned Apply client.
    ///
    /// # Errors
    /// Revalidates governance, then returns Runtime's atomic result.
    pub fn apply_received(
        &mut self,
        input: &ReceivedInput,
    ) -> Result<DurableTransaction, IngressError> {
        self.revalidate()?;
        let receiver = self
            .receiver
            .as_mut()
            .ok_or(IngressError::Governance("source session is detached"))?;
        receiver.apply_received(&mut self.apply, input)
    }

    /// Receives and applies one v1 transaction, revalidating between the two.
    ///
    /// # Errors
    /// Returns governance, receive/decode, or Runtime failure.
    pub fn receive_and_apply_one(&mut self) -> Result<DurableTransaction, IngressError> {
        let input = self.receive_one()?;
        self.apply_received(&input)
    }

    /// Acknowledges an exact durably applied transaction after revalidation.
    ///
    /// # Errors
    /// Returns governance, token, or transport failure.
    pub fn acknowledge(&mut self, token: &DurableTransaction) -> Result<(), IngressError> {
        self.revalidate()?;
        self.receiver_mut()?.acknowledge(token)
    }

    /// Acknowledges a strict empty commit only after current revalidation.
    ///
    /// # Errors
    /// Returns governance, token, or transport failure.
    pub fn acknowledge_empty(&mut self, token: &EmptyCommitted) -> Result<(), IngressError> {
        self.revalidate()?;
        self.receiver_mut()?.acknowledge_empty(token)
    }

    /// Acknowledges a stream abort only after current revalidation.
    ///
    /// # Errors
    /// Returns governance, token, or transport failure.
    pub fn acknowledge_abort(&mut self, token: &AbortedTransaction) -> Result<(), IngressError> {
        self.revalidate()?;
        self.receiver_mut()?.acknowledge_abort(token)
    }

    /// Detaches between receive calls without creating or dropping any slot.
    ///
    /// # Errors
    /// Returns an error if the owned advisory lock was unexpectedly absent.
    pub fn detach(mut self) -> Result<(), IngressError> {
        drop(self.receiver.take());
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

    #[must_use]
    pub const fn source_id(&self) -> SourceId {
        self.config.source_id
    }

    #[must_use]
    pub const fn slot_generation(&self) -> SlotGeneration {
        self.config.generation
    }

    /// Returns a signal that can interrupt a blocking receive from another thread.
    #[must_use]
    pub fn shutdown_handle(&self) -> ShutdownHandle {
        self.shutdown.clone()
    }

    fn revalidate(&mut self) -> Result<(), IngressError> {
        self.config.revalidate(&mut self.apply, true)
    }

    fn receiver_mut(&mut self) -> Result<&mut SourceReceiver, IngressError> {
        self.receiver
            .as_mut()
            .ok_or(IngressError::Governance("source session is detached"))
    }
}

fn advisory_key(source_id: SourceId) -> Result<i64, IngressError> {
    // Positive SQL bigint source IDs map bijectively into the reserved negative
    // session-lock range (MIN+1..=-1), disjoint from positive application keys.
    let source = i64::try_from(source_id.get())
        .map_err(|_| IngressError::Governance("source ID exceeds bigint"))?;
    i64::MIN
        .checked_add(source)
        .ok_or(IngressError::Governance("source advisory key overflow"))
}

#[cfg(test)]
#[path = "governed_tests.rs"]
mod tests;
