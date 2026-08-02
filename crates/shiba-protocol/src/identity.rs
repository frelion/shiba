use core::num::NonZeroU64;

use serde::{Deserialize, Deserializer, Serialize, de};

use crate::{PostgresLsn, ProtocolError, ScopeMismatch};

macro_rules! id_type {
    ($name:ident, $label:literal) => {
        #[doc = concat!("A non-zero ", $label, ".")]
        #[derive(
            Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(NonZeroU64);

        impl $name {
            #[doc = concat!(
                "Creates a non-zero ",
                $label,
                ".\n\n# Errors\n\nReturns [`ProtocolError::ZeroValue`] when `value` is zero."
            )]
            pub const fn new(value: u64) -> Result<Self, ProtocolError> {
                match NonZeroU64::new(value) {
                    Some(value) => Ok(Self(value)),
                    None => Err(ProtocolError::ZeroValue($label)),
                }
            }

            pub const fn get(self) -> u64 {
                self.0.get()
            }
        }
    };
}

id_type!(SourceId, "source ID");
id_type!(SlotGeneration, "slot generation");
id_type!(IngressTransactionId, "ingress transaction ID");
id_type!(InputSequence, "input sequence");

/// Durable identity of one committed transaction from one source generation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct SourceTransactionId {
    pub source_id: SourceId,
    pub slot_generation: SlotGeneration,
    pub commit_lsn: PostgresLsn,
    pub ingress_transaction_id: IngressTransactionId,
}

impl SourceTransactionId {
    /// Creates a durable source transaction identity.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::ZeroCommitLsn`] when `commit_lsn` is zero.
    pub const fn new(
        source_id: SourceId,
        slot_generation: SlotGeneration,
        commit_lsn: PostgresLsn,
        ingress_transaction_id: IngressTransactionId,
    ) -> Result<Self, ProtocolError> {
        if commit_lsn.is_zero() {
            return Err(ProtocolError::ZeroCommitLsn);
        }
        Ok(Self {
            source_id,
            slot_generation,
            commit_lsn,
            ingress_transaction_id,
        })
    }
}

impl<'de> Deserialize<'de> for SourceTransactionId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Raw {
            source_id: SourceId,
            slot_generation: SlotGeneration,
            commit_lsn: PostgresLsn,
            ingress_transaction_id: IngressTransactionId,
        }

        let raw = Raw::deserialize(deserializer)?;
        Self::new(
            raw.source_id,
            raw.slot_generation,
            raw.commit_lsn,
            raw.ingress_transaction_id,
        )
        .map_err(de::Error::custom)
    }
}

/// Identity of one source input within a committed transaction.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CauseId {
    pub transaction: SourceTransactionId,
    pub input_sequence: InputSequence,
}

impl CauseId {
    #[must_use]
    pub const fn new(transaction: SourceTransactionId, input_sequence: InputSequence) -> Self {
        Self {
            transaction,
            input_sequence,
        }
    }

    /// Tests whether this cause belongs to a transaction covered by `frontier`.
    ///
    /// # Errors
    ///
    /// Returns [`ScopeMismatch`] when the source or slot generation differs.
    pub fn is_at_or_before(self, frontier: CommitFrontier) -> Result<bool, ScopeMismatch> {
        frontier.covers(self.transaction)
    }
}

/// Highest fully committed LSN applied for exactly one source generation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct CommitFrontier {
    pub source_id: SourceId,
    pub slot_generation: SlotGeneration,
    pub commit_lsn: PostgresLsn,
}

impl CommitFrontier {
    /// Creates a frontier for exactly one source generation.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::ZeroCommitLsn`] when `commit_lsn` is zero.
    pub const fn new(
        source_id: SourceId,
        slot_generation: SlotGeneration,
        commit_lsn: PostgresLsn,
    ) -> Result<Self, ProtocolError> {
        if commit_lsn.is_zero() {
            return Err(ProtocolError::ZeroCommitLsn);
        }
        Ok(Self {
            source_id,
            slot_generation,
            commit_lsn,
        })
    }

    /// Tests whether `transaction` is in this frontier's source generation and
    /// at or before its commit LSN.
    ///
    /// # Errors
    ///
    /// Returns [`ScopeMismatch`] when the source or slot generation differs.
    pub fn covers(self, transaction: SourceTransactionId) -> Result<bool, ScopeMismatch> {
        if self.source_id != transaction.source_id {
            return Err(ScopeMismatch::Source);
        }
        if self.slot_generation != transaction.slot_generation {
            return Err(ScopeMismatch::SlotGeneration);
        }
        Ok(transaction.commit_lsn <= self.commit_lsn)
    }
}

impl<'de> Deserialize<'de> for CommitFrontier {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Raw {
            source_id: SourceId,
            slot_generation: SlotGeneration,
            commit_lsn: PostgresLsn,
        }

        let raw = Raw::deserialize(deserializer)?;
        Self::new(raw.source_id, raw.slot_generation, raw.commit_lsn).map_err(de::Error::custom)
    }
}
