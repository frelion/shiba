use shiba_protocol::{GraphTransactionId, IngressTransactionId, InputSequence, PostgresLsn};

use crate::{
    GraphSourceChange, GraphTransaction, PgoutputError, PgoutputGraph, PgoutputRelationState,
    SourceChange, SourceInsert, SourceUpdate, SourceUpdatePayload,
    pgoutput::{check_input_limit, decode_relation},
    pgoutput_tuple::{DecodedChange, decode_delete, decode_insert, decode_update},
    pgoutput_wire::Cursor,
    transaction::MAX_TRANSACTION_CHANGES,
};

/// Decodes one complete protocol-v2 graph transaction.
///
/// # Errors
/// Rejects partial, mixed-XID, unknown-relation, malformed, or oversized input.
pub fn decode_streamed_changes(
    input: &[u8],
    graph: &PgoutputGraph,
) -> Result<GraphTransaction, PgoutputError> {
    decode_streamed_changes_in_session(input, graph, &mut PgoutputRelationState::new())
}

/// Decodes protocol-v2 using the bounded connection-local relation proof.
///
/// # Errors
/// Every changed relation must have an exact descriptor in this or an earlier transaction.
pub fn decode_streamed_changes_in_session(
    input: &[u8],
    graph: &PgoutputGraph,
    relation_state: &mut PgoutputRelationState,
) -> Result<GraphTransaction, PgoutputError> {
    check_input_limit(input)?;
    relation_state.begin(graph)?;
    let mut cursor = Cursor::new(input);
    if cursor.byte()? != b'S' {
        return Err(PgoutputError::MessageOrder);
    }
    let xid = cursor.u32()?;
    if xid == 0 || cursor.byte()? != 1 {
        return Err(PgoutputError::InvalidIdentity);
    }
    let mut values = Vec::new();
    let mut relation_updates = Vec::new();
    loop {
        loop {
            let tag = cursor.byte()?;
            match tag {
                b'R' => {
                    require_xid(&mut cursor, xid)?;
                    let source = graph.source_for_relation(peek_u32(&cursor)?)?;
                    decode_relation(&mut cursor, source)?;
                    relation_updates.push(source);
                }
                b'I' | b'U' | b'D' => {
                    if values.len() >= MAX_TRANSACTION_CHANGES {
                        return Err(PgoutputError::LimitExceeded);
                    }
                    require_xid(&mut cursor, xid)?;
                    let source = graph.source_for_relation(peek_u32(&cursor)?)?;
                    if !relation_state.validated_for(source) && !relation_updates.contains(&source)
                    {
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
                b'E' => break,
                other => return Err(PgoutputError::UnknownMessage(other)),
            }
        }
        match cursor.byte()? {
            b'S' => {
                require_xid(&mut cursor, xid)?;
                if cursor.byte()? != 0 {
                    return Err(PgoutputError::MessageOrder);
                }
            }
            b'c' if !values.is_empty() => break,
            b'c' => return Err(PgoutputError::MessageOrder),
            other => return Err(PgoutputError::UnknownMessage(other)),
        }
    }
    require_xid(&mut cursor, xid)?;
    let flags = cursor.byte()?;
    let commit_lsn = cursor.u64()?;
    let end_lsn = cursor.u64()?;
    cursor.u64()?;
    if flags != 0 || commit_lsn == 0 || end_lsn < commit_lsn {
        return Err(PgoutputError::InvalidLsn);
    }
    if !cursor.finished() {
        return Err(PgoutputError::InvalidIdentity);
    }
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
            Ok(GraphSourceChange {
                source_id: source.source_id,
                change: decoded_change(sequence, row),
            })
        })
        .collect::<Result<Vec<_>, PgoutputError>>()?;
    let transaction =
        GraphTransaction::new(identity, changes).map_err(|_| PgoutputError::TupleValue)?;
    for source in relation_updates {
        relation_state.mark_validated(source);
    }
    Ok(transaction)
}

fn decoded_change(sequence: InputSequence, row: DecodedChange) -> SourceChange {
    match row {
        DecodedChange::EmptyInsert => SourceChange::Insert(SourceInsert::empty(sequence)),
        DecodedChange::RowInsert(id, payload) => {
            SourceChange::Insert(SourceInsert::with_payload(sequence, id, payload))
        }
        DecodedChange::CompositeInsert(a, b) => {
            SourceChange::Insert(SourceInsert::composite(sequence, a, b))
        }
        DecodedChange::Update(old, new, payload) => SourceChange::Update(match payload {
            SourceUpdatePayload::Int8(value) => SourceUpdate::key_change(sequence, old, new, value),
            SourceUpdatePayload::UnchangedText => SourceUpdate::unchanged_text(sequence, new),
            SourceUpdatePayload::Text(value) => SourceUpdate::text(sequence, new, value),
        }),
        DecodedChange::Delete(source_row_id, source_row_sub_id) => SourceChange::Delete {
            input_sequence: sequence,
            source_row_id,
            source_row_sub_id,
        },
    }
}

fn require_xid(cursor: &mut Cursor<'_>, expected: u32) -> Result<(), PgoutputError> {
    if cursor.u32()? != expected {
        return Err(PgoutputError::InvalidIdentity);
    }
    Ok(())
}
fn peek_u32(cursor: &Cursor<'_>) -> Result<u32, PgoutputError> {
    let mut copy = *cursor;
    copy.u32()
}
