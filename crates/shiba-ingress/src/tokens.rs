use shiba_runtime::{ProcessOutcome, SourceTransaction};

/// A decoded committed input held before Runtime Apply.
#[derive(Debug)]
pub struct ReceivedInput {
    transaction: SourceTransaction,
    end_lsn: u64,
}

impl ReceivedInput {
    pub(crate) const fn new(transaction: SourceTransaction, end_lsn: u64) -> Self {
        Self {
            transaction,
            end_lsn,
        }
    }

    pub(crate) const fn raw_transaction(&self) -> &SourceTransaction {
        &self.transaction
    }

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
#[derive(Debug)]
pub struct DurableTransaction {
    outcome: ProcessOutcome,
    end_lsn: u64,
}

impl DurableTransaction {
    pub(crate) const fn new(outcome: ProcessOutcome, end_lsn: u64) -> Self {
        Self { outcome, end_lsn }
    }

    #[must_use]
    pub const fn outcome(&self) -> ProcessOutcome {
        self.outcome
    }

    #[must_use]
    pub const fn end_lsn(&self) -> u64 {
        self.end_lsn
    }
}

/// Exact safe feedback coordinate for a terminal stream abort.
#[derive(Debug)]
pub struct AbortedTransaction {
    acknowledgment_lsn: u64,
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
    pub(crate) const fn new(acknowledgment_lsn: u64) -> Self {
        Self { acknowledgment_lsn }
    }

    #[must_use]
    pub const fn acknowledgment_lsn(&self) -> u64 {
        self.acknowledgment_lsn
    }
}

#[derive(Debug)]
pub enum StreamedInput {
    Transaction(ReceivedInput),
    EmptyCommitted(EmptyCommitted),
    Aborted(AbortedTransaction),
}
