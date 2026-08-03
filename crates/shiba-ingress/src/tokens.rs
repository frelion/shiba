use shiba_protocol::{BootstrapId, GraphId};
use shiba_runtime::{GraphTransaction, ProcessOutcome};

/// A decoded committed input held before Runtime Apply.
#[derive(Debug)]
pub struct ReceivedInput {
    transaction: GraphTransaction,
    end_lsn: u64,
    authorization: u64,
}

impl ReceivedInput {
    pub(crate) const fn new(
        transaction: GraphTransaction,
        end_lsn: u64,
        authorization: u64,
    ) -> Self {
        Self {
            transaction,
            end_lsn,
            authorization,
        }
    }

    pub(crate) const fn raw_transaction(&self) -> &GraphTransaction {
        &self.transaction
    }

    #[must_use]
    pub const fn transaction(&self) -> &GraphTransaction {
        &self.transaction
    }

    #[must_use]
    pub const fn end_lsn(&self) -> u64 {
        self.end_lsn
    }

    pub(crate) const fn authorization(&self) -> u64 {
        self.authorization
    }
}

/// Proof that Runtime durably handled one exact terminal commit LSN.
#[derive(Debug)]
pub struct DurableTransaction {
    outcome: ProcessOutcome,
    end_lsn: u64,
    authorization: u64,
}

impl DurableTransaction {
    pub(crate) const fn new(outcome: ProcessOutcome, end_lsn: u64, authorization: u64) -> Self {
        Self {
            outcome,
            end_lsn,
            authorization,
        }
    }

    #[must_use]
    pub const fn outcome(&self) -> ProcessOutcome {
        self.outcome
    }

    #[must_use]
    pub const fn end_lsn(&self) -> u64 {
        self.end_lsn
    }

    pub(crate) const fn authorization(&self) -> u64 {
        self.authorization
    }
}

/// Exact safe feedback coordinate for a terminal stream abort.
#[derive(Debug)]
pub struct AbortedTransaction {
    acknowledgment_lsn: u64,
    authorization: u64,
}

/// A strict empty streamed commit that performs no Runtime work.
#[derive(Debug)]
pub struct EmptyCommitted {
    xid: u32,
    commit_lsn: u64,
    end_lsn: u64,
    segment_count: usize,
    authorization: u64,
}

/// Exact attempt-bound terminal fence awaiting durable activation.
#[derive(Debug)]
pub struct BootstrapFence {
    graph_id: GraphId,
    bootstrap_id: BootstrapId,
    message_lsn: u64,
    end_lsn: u64,
    authorization: u64,
}

impl BootstrapFence {
    pub(crate) const fn new(
        graph_id: GraphId,
        bootstrap_id: BootstrapId,
        message_lsn: u64,
        end_lsn: u64,
        authorization: u64,
    ) -> Self {
        Self {
            graph_id,
            bootstrap_id,
            message_lsn,
            end_lsn,
            authorization,
        }
    }

    pub(crate) const fn authorization(&self) -> u64 {
        self.authorization
    }

    #[must_use]
    pub const fn graph_id(&self) -> GraphId {
        self.graph_id
    }

    #[must_use]
    pub const fn bootstrap_id(&self) -> BootstrapId {
        self.bootstrap_id
    }

    #[must_use]
    pub const fn end_lsn(&self) -> u64 {
        self.end_lsn
    }

    #[must_use]
    pub const fn message_lsn(&self) -> u64 {
        self.message_lsn
    }
}

#[derive(Debug)]
pub(crate) enum BootstrapInput {
    Transaction(ReceivedInput),
    Fence(BootstrapFence),
}

impl EmptyCommitted {
    pub(crate) const fn new(
        xid: u32,
        commit_lsn: u64,
        end_lsn: u64,
        segment_count: usize,
        authorization: u64,
    ) -> Self {
        Self {
            xid,
            commit_lsn,
            end_lsn,
            segment_count,
            authorization,
        }
    }

    pub(crate) const fn authorization(&self) -> u64 {
        self.authorization
    }

    #[must_use]
    pub const fn xid(&self) -> u32 {
        self.xid
    }

    #[must_use]
    pub const fn commit_lsn(&self) -> u64 {
        self.commit_lsn
    }

    #[must_use]
    pub const fn end_lsn(&self) -> u64 {
        self.end_lsn
    }

    #[must_use]
    pub const fn segment_count(&self) -> usize {
        self.segment_count
    }
}

impl AbortedTransaction {
    pub(crate) const fn new(acknowledgment_lsn: u64, authorization: u64) -> Self {
        Self {
            acknowledgment_lsn,
            authorization,
        }
    }

    #[must_use]
    pub const fn acknowledgment_lsn(&self) -> u64 {
        self.acknowledgment_lsn
    }

    pub(crate) const fn authorization(&self) -> u64 {
        self.authorization
    }
}

#[derive(Debug)]
pub enum StreamedInput {
    Transaction(ReceivedInput),
    EmptyCommitted(EmptyCommitted),
    Aborted(AbortedTransaction),
}
