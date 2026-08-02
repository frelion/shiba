use std::collections::HashSet;

use shiba_protocol::{InputSequence, SourceTransactionId};

use crate::M2Error;

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
            source_payload: SourceUpdatePayload::Int8(source_payload),
        }
    }

    #[must_use]
    pub const fn unchanged_text(input_sequence: InputSequence, source_row_id: i64) -> Self {
        Self {
            input_sequence,
            source_row_id,
            source_payload: SourceUpdatePayload::UnchangedText,
        }
    }

    #[must_use]
    pub fn text(input_sequence: InputSequence, source_row_id: i64, source_payload: String) -> Self {
        Self {
            input_sequence,
            source_row_id,
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
pub struct SourceTransaction {
    pub identity: SourceTransactionId,
    pub changes: Vec<SourceChange>,
}

impl SourceTransaction {
    /// # Errors
    /// Rejects invalid identity coordinates or INSERT facts.
    pub fn new(identity: SourceTransactionId, inserts: Vec<SourceInsert>) -> Result<Self, M2Error> {
        Self::from_changes(
            identity,
            inserts.into_iter().map(SourceChange::Insert).collect(),
        )
    }

    /// # Errors
    /// Rejects empty, duplicate, or out-of-range changes.
    pub fn from_changes(
        identity: SourceTransactionId,
        changes: Vec<SourceChange>,
    ) -> Result<Self, M2Error> {
        validate_coordinate("source_id", identity.source_id.get())?;
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
        for change in &changes {
            let (sequence, row) = match change {
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
                && !rows.insert((source_row_id, sub_id))
            {
                return Err(M2Error::DuplicateSourceRow(source_row_id));
            }
        }
        Ok(Self { identity, changes })
    }
}

pub(crate) fn as_bigint(field: &'static str, value: u64) -> Result<i64, M2Error> {
    i64::try_from(value).map_err(|_| M2Error::CoordinateOutOfRange(field))
}

fn validate_coordinate(field: &'static str, value: u64) -> Result<(), M2Error> {
    as_bigint(field, value).map(|_| ())
}
