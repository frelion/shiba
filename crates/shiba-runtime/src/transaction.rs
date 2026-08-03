use std::collections::HashSet;

use shiba_protocol::{GraphTransactionId, InputSequence, SourceId};

use crate::M2Error;

pub(crate) const MAX_TRANSACTION_CHANGES: usize = 10_000;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SourcePayload {
    Absent,
    Null,
    Int8(i64),
    Text(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceInsert {
    pub input_sequence: InputSequence,
    pub source_row_id: Option<i64>,
    pub source_row_sub_id: Option<i64>,
    pub source_payload: SourcePayload,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SourceUpdatePayload {
    Int8(Option<i64>),
    UnchangedText,
    Text(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceUpdate {
    pub input_sequence: InputSequence,
    pub source_row_id: i64,
    pub new_source_row_id: i64,
    pub source_payload: SourceUpdatePayload,
}

impl SourceUpdate {
    #[must_use]
    pub const fn new(
        input_sequence: InputSequence,
        source_row_id: i64,
        source_payload: Option<i64>,
    ) -> Self {
        Self {
            input_sequence,
            source_row_id,
            new_source_row_id: source_row_id,
            source_payload: SourceUpdatePayload::Int8(source_payload),
        }
    }

    #[must_use]
    pub const fn key_change(
        input_sequence: InputSequence,
        old_source_row_id: i64,
        new_source_row_id: i64,
        source_payload: Option<i64>,
    ) -> Self {
        Self {
            input_sequence,
            source_row_id: old_source_row_id,
            new_source_row_id,
            source_payload: SourceUpdatePayload::Int8(source_payload),
        }
    }

    #[must_use]
    pub const fn unchanged_text(input_sequence: InputSequence, source_row_id: i64) -> Self {
        Self {
            input_sequence,
            source_row_id,
            new_source_row_id: source_row_id,
            source_payload: SourceUpdatePayload::UnchangedText,
        }
    }

    #[must_use]
    pub fn text(input_sequence: InputSequence, source_row_id: i64, source_payload: String) -> Self {
        Self {
            input_sequence,
            source_row_id,
            new_source_row_id: source_row_id,
            source_payload: SourceUpdatePayload::Text(source_payload),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SourceChange {
    Insert(SourceInsert),
    Update(SourceUpdate),
    Delete {
        input_sequence: InputSequence,
        source_row_id: i64,
        source_row_sub_id: Option<i64>,
    },
}

impl SourceInsert {
    #[must_use]
    pub const fn new(input_sequence: InputSequence, source_row_id: i64) -> Self {
        Self {
            input_sequence,
            source_row_id: Some(source_row_id),
            source_row_sub_id: None,
            source_payload: SourcePayload::Absent,
        }
    }

    #[must_use]
    pub const fn with_payload(
        input_sequence: InputSequence,
        source_row_id: i64,
        source_payload: SourcePayload,
    ) -> Self {
        Self {
            input_sequence,
            source_row_id: Some(source_row_id),
            source_row_sub_id: None,
            source_payload,
        }
    }

    #[must_use]
    pub const fn empty(input_sequence: InputSequence) -> Self {
        Self {
            input_sequence,
            source_row_id: None,
            source_row_sub_id: None,
            source_payload: SourcePayload::Absent,
        }
    }

    #[must_use]
    pub const fn composite(input_sequence: InputSequence, first: i64, second: i64) -> Self {
        Self {
            input_sequence,
            source_row_id: Some(first),
            source_row_sub_id: Some(second),
            source_payload: SourcePayload::Absent,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphSourceChange {
    pub source_id: SourceId,
    pub change: SourceChange,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphTransaction {
    pub identity: GraphTransactionId,
    pub changes: Vec<GraphSourceChange>,
}

impl GraphTransaction {
    /// # Errors
    /// Rejects empty, duplicate, or out-of-range relation-tagged changes.
    pub fn new(
        identity: GraphTransactionId,
        changes: Vec<GraphSourceChange>,
    ) -> Result<Self, M2Error> {
        check_transaction_change_limit(changes.len())?;
        validate_coordinate("graph_id", identity.graph_id.get())?;
        validate_coordinate("slot_generation", identity.slot_generation.get())?;
        validate_coordinate(
            "ingress_transaction_id",
            identity.ingress_transaction_id.get(),
        )?;
        if changes.is_empty() {
            return Err(M2Error::EmptyTransaction);
        }

        let mut sequences = HashSet::with_capacity(changes.len());
        let mut rows = HashSet::with_capacity(changes.len());
        for tagged in &changes {
            validate_coordinate("source_id", tagged.source_id.get())?;
            let (sequence, row) = match &tagged.change {
                SourceChange::Insert(insert) => (
                    insert.input_sequence.get(),
                    insert
                        .source_row_id
                        .map(|key| (key, insert.source_row_sub_id)),
                ),
                SourceChange::Update(update) => (
                    update.input_sequence.get(),
                    Some((update.source_row_id, None)),
                ),
                SourceChange::Delete {
                    input_sequence,
                    source_row_id,
                    source_row_sub_id,
                } => (
                    input_sequence.get(),
                    Some((*source_row_id, *source_row_sub_id)),
                ),
            };
            validate_coordinate("input_sequence", sequence)?;
            if !sequences.insert(sequence) {
                return Err(M2Error::DuplicateInputSequence(sequence));
            }
            if let Some((source_row_id, sub_id)) = row
                && !rows.insert((tagged.source_id, source_row_id, sub_id))
            {
                return Err(M2Error::DuplicateSourceRow(source_row_id));
            }
        }
        Ok(Self { identity, changes })
    }
}

pub(crate) fn check_transaction_change_limit(count: usize) -> Result<(), M2Error> {
    if count > MAX_TRANSACTION_CHANGES {
        return Err(M2Error::TransactionLimitExceeded);
    }
    Ok(())
}

pub(crate) fn as_bigint(field: &'static str, value: u64) -> Result<i64, M2Error> {
    i64::try_from(value).map_err(|_| M2Error::CoordinateOutOfRange(field))
}

fn validate_coordinate(field: &'static str, value: u64) -> Result<(), M2Error> {
    as_bigint(field, value).map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transaction_change_limit_is_exact() {
        assert!(check_transaction_change_limit(MAX_TRANSACTION_CHANGES).is_ok());
        assert!(matches!(
            check_transaction_change_limit(MAX_TRANSACTION_CHANGES + 1),
            Err(M2Error::TransactionLimitExceeded)
        ));
    }
}
