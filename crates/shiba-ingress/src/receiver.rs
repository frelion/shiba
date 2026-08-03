use postgres::Client;
use shiba_protocol::{BootstrapId, GraphId};
use shiba_runtime::{
    GraphTransaction, PgoutputGraph, PgoutputRelationState, decode_committed_changes_in_session,
    decode_streamed_changes_in_session, process,
};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_RECEIVER_AUTHORIZATION: AtomicU64 = AtomicU64::new(1);

use crate::{
    CommittedAssembler, IngressError, ReplicationMode, ReplicationTransport, ShutdownHandle,
    feedback::FeedbackState,
    fence,
    streamed::{StreamTerminal, StreamedAssembler},
    tokens::{
        AbortedTransaction, BootstrapFence, BootstrapInput, DurableTransaction, EmptyCommitted,
        ReceivedInput, StreamedInput,
    },
};

pub(super) enum Assembly {
    Committed(CommittedAssembler),
    Streamed(StreamedAssembler),
}

/// Exclusive, synchronous owner of one logical-replication connection.
pub(crate) struct SourceReceiver {
    pub(super) transport: ReplicationTransport,
    pub(super) assembly: Assembly,
    pub(super) feedback: FeedbackState,
    authorization: u64,
    relation_state: PgoutputRelationState,
    pub(super) outstanding_lsn: Option<u64>,
    pub(super) failed: bool,
}

impl SourceReceiver {
    /// Opens an explicit protocol-v1 committed or protocol-v2 streamed session.
    ///
    /// # Errors
    /// Rejects invalid configuration, connection failure, or non-COPY-BOTH.
    pub(crate) fn connect(
        conninfo: &str,
        slot: &str,
        publication: &str,
        mode: ReplicationMode,
        start_lsn: u64,
        last_acknowledged_lsn: u64,
    ) -> Result<Self, IngressError> {
        Self::connect_with_messages(
            conninfo,
            slot,
            publication,
            mode,
            start_lsn,
            last_acknowledged_lsn,
            false,
        )
    }

    pub(crate) fn connect_with_messages(
        conninfo: &str,
        slot: &str,
        publication: &str,
        mode: ReplicationMode,
        start_lsn: u64,
        last_acknowledged_lsn: u64,
        messages: bool,
    ) -> Result<Self, IngressError> {
        let authorization = NEXT_RECEIVER_AUTHORIZATION
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .map_err(|_| IngressError::LimitExceeded)?;
        let transport = ReplicationTransport::connect(conninfo)?;
        transport.start_with_messages(slot, publication, start_lsn, mode, messages)?;
        let assembly = match mode {
            ReplicationMode::Committed => Assembly::Committed(CommittedAssembler::new()),
            ReplicationMode::Streamed => Assembly::Streamed(StreamedAssembler::new()),
        };
        Ok(Self {
            transport,
            assembly,
            feedback: FeedbackState::new(last_acknowledged_lsn),
            authorization,
            relation_state: PgoutputRelationState::new(),
            outstanding_lsn: None,
            failed: false,
        })
    }

    pub(crate) fn receive_bootstrap_one(
        &mut self,
        graph: &PgoutputGraph,
        graph_id: GraphId,
        bootstrap_id: BootstrapId,
        expected_content: &str,
        shutdown: &ShutdownHandle,
    ) -> Result<BootstrapInput, IngressError> {
        self.ready()?;
        if !matches!(self.assembly, Assembly::Committed(_)) {
            return Err(IngressError::InvalidEnvelope(
                "bootstrap catch-up requires committed mode",
            ));
        }
        let result = self.receive_committed_wire(shutdown).and_then(|assembled| {
            if let Some(fence) = fence::classify(&assembled.bytes, expected_content)? {
                if fence.end_lsn != assembled.end_lsn {
                    return Err(IngressError::FeedbackMismatch);
                }
                self.feedback.mark_fence(fence.end_lsn, self.authorization);
                return Ok(BootstrapInput::Fence(BootstrapFence::new(
                    graph_id,
                    bootstrap_id,
                    fence.message_lsn,
                    fence.end_lsn,
                    self.authorization,
                )));
            }
            let transaction = decode_committed_changes_in_session(
                &assembled.bytes,
                graph,
                &mut self.relation_state,
            )?;
            Ok(BootstrapInput::Transaction(
                self.set_outstanding(transaction, assembled.end_lsn),
            ))
        });
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    /// Receives one protocol-v1 committed transaction without applying it.
    ///
    /// # Errors
    /// Fails closed on mode, state, transport, framing, or decode errors.
    pub fn receive_one(
        &mut self,
        graph: &PgoutputGraph,
        shutdown: &ShutdownHandle,
    ) -> Result<ReceivedInput, IngressError> {
        self.ready()?;
        if !matches!(self.assembly, Assembly::Committed(_)) {
            return Err(IngressError::InvalidEnvelope(
                "receiver is in streamed mode",
            ));
        }
        let result = self.receive_committed_wire(shutdown).and_then(|assembled| {
            let transaction = decode_committed_changes_in_session(
                &assembled.bytes,
                graph,
                &mut self.relation_state,
            )?;
            Ok(self.set_outstanding(transaction, assembled.end_lsn))
        });
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    /// Receives one protocol-v2 committed stream or terminal stream abort.
    ///
    /// # Errors
    /// Partial segments remain internal. Invalid framing/order/XID, limits, and
    /// semantic decode errors poison the receiver.
    pub fn receive_streamed_one(
        &mut self,
        graph: &PgoutputGraph,
        shutdown: &ShutdownHandle,
    ) -> Result<StreamedInput, IngressError> {
        self.ready()?;
        if !matches!(self.assembly, Assembly::Streamed(_)) {
            return Err(IngressError::InvalidEnvelope(
                "receiver is in committed mode",
            ));
        }
        let result = self
            .receive_stream_terminal(shutdown)
            .and_then(|terminal| self.handle_stream_terminal(graph, terminal));
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    /// Applies one exact outstanding committed input through Runtime.
    ///
    /// # Errors
    /// Mismatch or Runtime failure fails closed without advancing feedback.
    pub fn apply_received(
        &mut self,
        apply: &mut Client,
        input: &ReceivedInput,
    ) -> Result<DurableTransaction, IngressError> {
        if self.failed {
            return Err(IngressError::ReceiverFailed);
        }
        if self.outstanding_lsn != Some(input.end_lsn())
            || input.authorization() != self.authorization
            || self.feedback.has_pending()
        {
            return Err(IngressError::FeedbackMismatch);
        }
        let outcome = match process(apply, input.raw_transaction()) {
            Ok(outcome) => outcome,
            Err(error) => {
                self.failed = true;
                return Err(error.into());
            }
        };
        self.outstanding_lsn = None;
        self.feedback
            .mark_applied(input.end_lsn(), self.authorization);
        Ok(DurableTransaction::new(
            outcome,
            input.end_lsn(),
            self.authorization,
        ))
    }

    fn ready(&self) -> Result<(), IngressError> {
        if self.failed {
            return Err(IngressError::ReceiverFailed);
        }
        if self.outstanding_lsn.is_some() || self.feedback.has_pending() {
            return Err(IngressError::FeedbackPending);
        }
        Ok(())
    }

    fn handle_stream_terminal(
        &mut self,
        graph: &PgoutputGraph,
        terminal: StreamTerminal,
    ) -> Result<StreamedInput, IngressError> {
        match terminal {
            StreamTerminal::Committed(assembled) => {
                let transaction = decode_streamed_changes_in_session(
                    &assembled.bytes,
                    graph,
                    &mut self.relation_state,
                )?;
                Ok(StreamedInput::Transaction(
                    self.set_outstanding(transaction, assembled.end_lsn),
                ))
            }
            StreamTerminal::EmptyCommitted {
                xid,
                commit_lsn,
                end_lsn,
                segment_count,
            } => {
                self.feedback.mark_empty(end_lsn, self.authorization);
                Ok(StreamedInput::EmptyCommitted(EmptyCommitted::new(
                    xid,
                    commit_lsn,
                    end_lsn,
                    segment_count,
                    self.authorization,
                )))
            }
            StreamTerminal::Aborted { acknowledgment_lsn } => {
                self.feedback
                    .mark_aborted(acknowledgment_lsn, self.authorization);
                Ok(StreamedInput::Aborted(AbortedTransaction::new(
                    acknowledgment_lsn,
                    self.authorization,
                )))
            }
        }
    }

    fn set_outstanding(&mut self, transaction: GraphTransaction, end_lsn: u64) -> ReceivedInput {
        self.outstanding_lsn = Some(end_lsn);
        ReceivedInput::new(transaction, end_lsn, self.authorization)
    }
}
