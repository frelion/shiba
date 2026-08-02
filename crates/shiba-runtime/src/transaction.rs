use std::collections::HashSet;

use shiba_protocol::{InputSequence, SourceTransactionId};

use crate::M2Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SourceInsert {
    pub input_sequence: InputSequence,
    pub source_row_id: i64,
}

impl SourceInsert {
    #[must_use]
    pub const fn new(input_sequence: InputSequence, source_row_id: i64) -> Self {
        Self {
            input_sequence,
            source_row_id,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceTransaction {
    pub identity: SourceTransactionId,
    pub inserts: Vec<SourceInsert>,
}

impl SourceTransaction {
    /// Constructs the only M2 production input.
    ///
    /// # Errors
    ///
    /// Rejects an empty transaction, duplicate cause sequence, duplicate source
    /// row, or an identity coordinate `PostgreSQL` `bigint` cannot represent.
    pub fn new(identity: SourceTransactionId, inserts: Vec<SourceInsert>) -> Result<Self, M2Error> {
        validate_coordinate("source_id", identity.source_id.get())?;
        validate_coordinate("slot_generation", identity.slot_generation.get())?;
        validate_coordinate(
            "ingress_transaction_id",
            identity.ingress_transaction_id.get(),
        )?;
        if inserts.is_empty() {
            return Err(M2Error::EmptyTransaction);
        }

        let mut sequences = HashSet::with_capacity(inserts.len());
        let mut rows = HashSet::with_capacity(inserts.len());
        for insert in &inserts {
            let sequence = insert.input_sequence.get();
            validate_coordinate("input_sequence", sequence)?;
            if !sequences.insert(sequence) {
                return Err(M2Error::DuplicateInputSequence(sequence));
            }
            if !rows.insert(insert.source_row_id) {
                return Err(M2Error::DuplicateSourceRow(insert.source_row_id));
            }
        }
        Ok(Self { identity, inserts })
    }
}

pub(crate) fn as_bigint(field: &'static str, value: u64) -> Result<i64, M2Error> {
    i64::try_from(value).map_err(|_| M2Error::CoordinateOutOfRange(field))
}

fn validate_coordinate(field: &'static str, value: u64) -> Result<(), M2Error> {
    as_bigint(field, value).map(|_| ())
}
