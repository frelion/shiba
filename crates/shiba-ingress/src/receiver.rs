use postgres::Client;
use shiba_runtime::{
    PgoutputSource, ProcessOutcome, SourceTransaction, decode_committed_changes, process,
};

use crate::{
    CommittedAssembler, IngressError, ReplicationMessage, ReplicationTransport,
    parse_replication_message,
};

/// A decoded committed input held in the receive-before-Apply crash window.
///
/// Private fields prevent callers from manufacturing an input that bypasses
/// this receiver's outstanding-LSN check.
#[derive(Debug)]
pub struct ReceivedInput {
    transaction: SourceTransaction,
    end_lsn: u64,
}

impl ReceivedInput {
    #[must_use]
    pub const fn transaction(&self) -> &SourceTransaction {
        &self.transaction
    }

    #[must_use]
    pub const fn end_lsn(&self) -> u64 {
        self.end_lsn
    }
}

/// Proof that Runtime durably handled one exact terminal commit LSN.
///
/// Creating this token never sends replication feedback; acknowledgment is a
/// separate explicit state transition.
#[derive(Debug)]
pub struct DurableTransaction {
    outcome: ProcessOutcome,
    end_lsn: u64,
}

impl DurableTransaction {
    #[must_use]
    pub const fn outcome(&self) -> ProcessOutcome {
        self.outcome
    }

    #[must_use]
    pub const fn end_lsn(&self) -> u64 {
        self.end_lsn
    }
}

/// Exclusive, synchronous owner of one logical-replication connection.
pub struct SourceReceiver {
    transport: ReplicationTransport,
    assembler: CommittedAssembler,
    last_acknowledged_lsn: u64,
    outstanding_lsn: Option<u64>,
    pending_feedback: Option<u64>,
    failed: bool,
}

impl SourceReceiver {
    /// Opens one exclusive replication connection and enters COPY BOTH.
    ///
    /// `last_acknowledged_lsn` is the already confirmed position read from the
    /// validated `PostgreSQL` slot at startup. It is not a Shiba cursor. It is
    /// the only position reported until a later explicit [`Self::acknowledge`]
    /// succeeds.
    ///
    /// # Errors
    /// Rejects invalid configuration, connection failure, or a non-COPY-BOTH
    /// replication response.
    pub fn connect(
        conninfo: &str,
        slot: &str,
        publication: &str,
        start_lsn: u64,
        last_acknowledged_lsn: u64,
    ) -> Result<Self, IngressError> {
        let transport = ReplicationTransport::connect(conninfo)?;
        transport.start(slot, publication, start_lsn)?;
        Ok(Self {
            transport,
            assembler: CommittedAssembler::new(),
            last_acknowledged_lsn,
            outstanding_lsn: None,
            pending_feedback: None,
            failed: false,
        })
    }

    /// Receives and decodes one committed source transaction without applying
    /// or acknowledging it.
    ///
    /// # Errors
    /// Refuses a second receive while an input or durable feedback is pending,
    /// and fails closed on transport, framing, or semantic decode errors.
    pub fn receive_one(&mut self, source: PgoutputSource) -> Result<ReceivedInput, IngressError> {
        if self.failed {
            return Err(IngressError::ReceiverFailed);
        }
        if self.outstanding_lsn.is_some() || self.pending_feedback.is_some() {
            return Err(IngressError::FeedbackPending);
        }

        let result = self.receive_one_inner(source);
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn receive_one_inner(&mut self, source: PgoutputSource) -> Result<ReceivedInput, IngressError> {
        loop {
            if let Some(assembled) = self.assembler.push(&[])? {
                return self.decode_received(source, &assembled);
            }

            let copy_data = self.transport.receive()?;
            match parse_replication_message(&copy_data)? {
                ReplicationMessage::XLogData { data, .. } => {
                    if let Some(assembled) = self.assembler.push(data)? {
                        return self.decode_received(source, &assembled);
                    }
                }
                ReplicationMessage::Keepalive {
                    reply_requested: true,
                    ..
                } => self.transport.send_feedback(self.last_acknowledged_lsn)?,
                ReplicationMessage::Keepalive {
                    reply_requested: false,
                    ..
                } => {}
            }
        }
    }

    /// Applies one exact outstanding input through Runtime.
    ///
    /// On Runtime failure the outstanding receive is consumed without changing
    /// acknowledged or pending feedback state.
    ///
    /// # Errors
    /// Rejects inputs that do not match the receiver's exact outstanding LSN,
    /// or returns Runtime's atomic apply failure.
    pub fn apply_received(
        &mut self,
        apply: &mut Client,
        input: &ReceivedInput,
    ) -> Result<DurableTransaction, IngressError> {
        if self.failed {
            return Err(IngressError::ReceiverFailed);
        }
        if self.outstanding_lsn != Some(input.end_lsn) || self.pending_feedback.is_some() {
            return Err(IngressError::FeedbackMismatch);
        }

        let outcome = match process(apply, &input.transaction) {
            Ok(outcome) => outcome,
            Err(error) => {
                self.failed = true;
                return Err(error.into());
            }
        };
        self.outstanding_lsn = None;
        self.pending_feedback = Some(input.end_lsn);
        Ok(DurableTransaction {
            outcome,
            end_lsn: input.end_lsn,
        })
    }

    /// Convenience composition of [`Self::receive_one`] and
    /// [`Self::apply_received`]. It still does not acknowledge feedback.
    ///
    /// # Errors
    /// Returns either receive/decode or Runtime apply failure.
    pub fn receive_and_apply_one(
        &mut self,
        apply: &mut Client,
        source: PgoutputSource,
    ) -> Result<DurableTransaction, IngressError> {
        let input = self.receive_one(source)?;
        self.apply_received(apply, &input)
    }

    /// Sends feedback for one exact durable token, then advances in-memory ACK
    /// state. A transport error leaves the pending token unchanged for retry.
    ///
    /// # Errors
    /// Rejects stale or foreign tokens and propagates feedback transport errors.
    pub fn acknowledge(&mut self, token: &DurableTransaction) -> Result<(), IngressError> {
        if self.failed {
            return Err(IngressError::ReceiverFailed);
        }
        if self.pending_feedback != Some(token.end_lsn) || self.outstanding_lsn.is_some() {
            return Err(IngressError::FeedbackMismatch);
        }
        self.transport.send_feedback(token.end_lsn)?;
        self.last_acknowledged_lsn = token.end_lsn;
        self.pending_feedback = None;
        Ok(())
    }

    #[must_use]
    pub const fn last_acknowledged_lsn(&self) -> u64 {
        self.last_acknowledged_lsn
    }

    #[must_use]
    pub const fn outstanding_lsn(&self) -> Option<u64> {
        self.outstanding_lsn
    }

    #[must_use]
    pub const fn pending_feedback_lsn(&self) -> Option<u64> {
        self.pending_feedback
    }

    fn decode_received(
        &mut self,
        source: PgoutputSource,
        assembled: &crate::AssembledTransaction,
    ) -> Result<ReceivedInput, IngressError> {
        let transaction = decode_committed_changes(&assembled.bytes, source)?;
        self.outstanding_lsn = Some(assembled.end_lsn);
        Ok(ReceivedInput {
            transaction,
            end_lsn: assembled.end_lsn,
        })
    }
}
