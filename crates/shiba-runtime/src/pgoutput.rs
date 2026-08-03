use core::fmt;

use shiba_protocol::{GraphTransactionId, IngressTransactionId, InputSequence, PostgresLsn};

use crate::{
    GraphSourceChange, GraphTransaction, SourceChange, SourceInsert, SourceUpdate,
    SourceUpdatePayload,
    pgoutput_source::{PgoutputGraph, PgoutputSource, SourceShape},
    pgoutput_tuple::{DecodedChange, decode_delete, decode_insert, decode_update},
    pgoutput_wire::Cursor,
    transaction::MAX_TRANSACTION_CHANGES,
};

const INT8_OID: u32 = 20;
const TEXT_OID: u32 = 25;
pub(crate) const MAX_PGOUTPUT_INPUT_BYTES: usize = 16 * 1024 * 1024;

/// Bounded connection-local proof for the exact graph relation descriptors.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PgoutputRelationState {
    graph: Option<(shiba_protocol::GraphId, shiba_protocol::SlotGeneration)>,
    sources: Vec<PgoutputSource>,
}

impl PgoutputRelationState {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            graph: None,
            sources: Vec::new(),
        }
    }

    pub(crate) fn begin(&mut self, graph: &PgoutputGraph) -> Result<(), PgoutputError> {
        match self.graph {
            None => self.graph = Some((graph.graph_id, graph.slot_generation)),
            Some(identity) if identity == (graph.graph_id, graph.slot_generation) => {}
            Some(_) => return Err(PgoutputError::RelationMismatch),
        }
        Ok(())
    }

    pub(crate) fn validated_for(&self, source: PgoutputSource) -> bool {
        self.sources.contains(&source)
    }

    pub(crate) fn mark_validated(&mut self, source: PgoutputSource) {
        if !self.sources.contains(&source) {
            self.sources.push(source);
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PgoutputError {
    Truncated,
    UnknownMessage(u8),
    MessageOrder,
    RelationMismatch,
    RelationShape,
    RelationType,
    TupleTag(u8),
    TupleShape,
    TupleValue,
    InvalidIdentity,
    InvalidLsn,
    LimitExceeded,
}

impl fmt::Display for PgoutputError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated => formatter.write_str("truncated pgoutput message"),
            Self::UnknownMessage(tag) => write!(formatter, "unsupported pgoutput message {tag:#x}"),
            Self::MessageOrder => formatter.write_str("invalid pgoutput message order"),
            Self::RelationMismatch => formatter.write_str("pgoutput relation does not match"),
            Self::RelationShape => formatter.write_str("pgoutput relation shape does not match"),
            Self::RelationType => formatter.write_str("pgoutput relation column is not int8"),
            Self::TupleTag(tag) => write!(formatter, "unsupported pgoutput tuple tag {tag:#x}"),
            Self::TupleShape => formatter.write_str("pgoutput tuple shape does not match"),
            Self::TupleValue => formatter.write_str("invalid pgoutput int8 text value"),
            Self::InvalidIdentity => formatter.write_str("invalid pgoutput transaction identity"),
            Self::InvalidLsn => formatter.write_str("inconsistent pgoutput commit LSN"),
            Self::LimitExceeded => formatter.write_str("pgoutput decoder limit exceeded"),
        }
    }
}

impl std::error::Error for PgoutputError {}

/// # Errors
/// Rejects input that is not one complete admitted M4.6 transaction.
pub fn decode_committed_changes(
    input: &[u8],
    graph: &PgoutputGraph,
) -> Result<GraphTransaction, PgoutputError> {
    decode_committed_changes_in_session(input, graph, &mut PgoutputRelationState::new())
}

/// Decodes a committed transaction using relation metadata validated earlier on this connection.
///
/// # Errors
/// The first transaction must contain an exact `RELATION`; later transactions
/// may omit it, while every repeated descriptor is validated again.
pub fn decode_committed_changes_in_session(
    input: &[u8],
    graph: &PgoutputGraph,
    relation_state: &mut PgoutputRelationState,
) -> Result<GraphTransaction, PgoutputError> {
    check_input_limit(input)?;
    relation_state.begin(graph)?;
    let mut cursor = Cursor::new(input);
    if cursor.byte()? != b'B' {
        return Err(PgoutputError::MessageOrder);
    }
    let final_lsn = cursor.u64()?;
    let commit_time = cursor.u64()?;
    let xid = cursor.u32()?;
    if final_lsn == 0 || xid == 0 {
        return Err(PgoutputError::InvalidIdentity);
    }

    let mut values = Vec::new();
    let mut relation_updates = Vec::new();
    loop {
        let tag = cursor.byte()?;
        if matches!(tag, b'I' | b'U' | b'D') && values.len() >= MAX_TRANSACTION_CHANGES {
            return Err(PgoutputError::LimitExceeded);
        }
        match tag {
            b'R' => {
                let source = graph.source_for_relation(peek_u32(&cursor)?)?;
                decode_relation(&mut cursor, source)?;
                relation_updates.push(source);
            }
            b'I' | b'U' | b'D' => {
                let source = graph.source_for_relation(peek_u32(&cursor)?)?;
                if !relation_state.validated_for(source) && !relation_updates.contains(&source) {
                    return Err(PgoutputError::MessageOrder);
                }
                let value = match tag {
                    b'I' => decode_insert(&mut cursor, source)?,
                    b'U' => decode_update(&mut cursor, source)?,
                    b'D' => decode_delete(&mut cursor, source)?,
                    _ => unreachable!(),
                };
                values.push((source, value));
            }
            b'C' if !values.is_empty() => break,
            b'C' => return Err(PgoutputError::MessageOrder),
            other => return Err(PgoutputError::UnknownMessage(other)),
        }
    }

    let flags = cursor.byte()?;
    let commit_lsn = cursor.u64()?;
    let end_lsn = cursor.u64()?;
    let commit_commit_time = cursor.u64()?;
    if flags != 0 || commit_lsn == 0 || final_lsn != commit_lsn || end_lsn < commit_lsn {
        return Err(PgoutputError::InvalidLsn);
    }
    if commit_time != commit_commit_time || !cursor.finished() {
        return Err(PgoutputError::InvalidIdentity);
    }

    build_transaction(
        graph,
        xid,
        commit_lsn,
        values,
        relation_updates,
        relation_state,
    )
}

fn build_transaction(
    graph: &PgoutputGraph,
    xid: u32,
    commit_lsn: u64,
    values: Vec<(PgoutputSource, DecodedChange)>,
    relation_updates: Vec<PgoutputSource>,
    relation_state: &mut PgoutputRelationState,
) -> Result<GraphTransaction, PgoutputError> {
    let identity = GraphTransactionId::new(
        graph.graph_id,
        graph.slot_generation,
        PostgresLsn::from_u64(commit_lsn),
        IngressTransactionId::new(u64::from(xid)).map_err(|_| PgoutputError::InvalidIdentity)?,
    )
    .map_err(|_| PgoutputError::InvalidIdentity)?;
    let changes = values
        .into_iter()
        .enumerate()
        .map(|(index, (source, row))| {
            let sequence = u64::try_from(index)
                .ok()
                .and_then(|value| value.checked_add(1))
                .and_then(|value| InputSequence::new(value).ok())
                .ok_or(PgoutputError::InvalidIdentity)?;
            let change = match row {
                DecodedChange::EmptyInsert => SourceChange::Insert(SourceInsert::empty(sequence)),
                DecodedChange::RowInsert(row_id, payload) => {
                    SourceChange::Insert(SourceInsert::with_payload(sequence, row_id, payload))
                }
                DecodedChange::CompositeInsert(key1, key2) => {
                    SourceChange::Insert(SourceInsert::composite(sequence, key1, key2))
                }
                DecodedChange::Update(old_row_id, new_row_id, payload) => {
                    let update = match payload {
                        SourceUpdatePayload::Int8(value) => {
                            SourceUpdate::key_change(sequence, old_row_id, new_row_id, value)
                        }
                        SourceUpdatePayload::UnchangedText => {
                            SourceUpdate::unchanged_text(sequence, new_row_id)
                        }
                        SourceUpdatePayload::Text(value) => {
                            SourceUpdate::text(sequence, new_row_id, value)
                        }
                    };
                    SourceChange::Update(update)
                }
                DecodedChange::Delete(source_row_id, source_row_sub_id) => SourceChange::Delete {
                    input_sequence: sequence,
                    source_row_id,
                    source_row_sub_id,
                },
            };
            Ok(GraphSourceChange {
                source_id: source.source_id,
                change,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let transaction =
        GraphTransaction::new(identity, changes).map_err(|_| PgoutputError::TupleValue)?;
    for source in relation_updates {
        relation_state.mark_validated(source);
    }
    Ok(transaction)
}

fn peek_u32(cursor: &Cursor<'_>) -> Result<u32, PgoutputError> {
    let mut copy = *cursor;
    copy.u32()
}

pub(crate) fn check_input_limit(input: &[u8]) -> Result<(), PgoutputError> {
    if input.len() > MAX_PGOUTPUT_INPUT_BYTES {
        return Err(PgoutputError::LimitExceeded);
    }
    Ok(())
}

pub(crate) fn decode_relation(
    cursor: &mut Cursor<'_>,
    source: PgoutputSource,
) -> Result<(), PgoutputError> {
    if source.relation_id == 0 || cursor.u32()? != source.relation_id {
        return Err(PgoutputError::RelationMismatch);
    }
    cursor.string()?;
    cursor.string()?;
    let columns: &[(u8, u32)] = match source.shape {
        SourceShape::Empty => &[],
        SourceShape::KeyOnly => &[(1, INT8_OID)],
        SourceShape::NullableInt8Payload => &[(1, INT8_OID), (0, INT8_OID)],
        SourceShape::CompositeInt8 => &[(1, INT8_OID), (1, INT8_OID)],
        SourceShape::TextPayload => &[(1, INT8_OID), (0, TEXT_OID)],
    };
    if cursor.byte()? != source.relation_identity || usize::from(cursor.u16()?) != columns.len() {
        return Err(PgoutputError::RelationShape);
    }
    for (expected_key_flag, expected_oid) in columns {
        if cursor.byte()? != *expected_key_flag {
            return Err(PgoutputError::RelationShape);
        }
        cursor.string()?;
        if cursor.u32()? != *expected_oid {
            return Err(PgoutputError::RelationType);
        }
        cursor.u32()?;
    }
    Ok(())
}
