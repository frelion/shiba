use shiba_protocol::{IngressTransactionId, InputSequence, PostgresLsn, SourceTransactionId};

use crate::{
    PgoutputError, SourceInsert, SourcePayload, SourceTransaction,
    pgoutput::{MAX_PGOUTPUT_CHANGES, check_input_limit, decode_relation},
    pgoutput_source::{PgoutputSource, SourceShape},
    pgoutput_tuple::{DecodedChange, decode_insert},
    pgoutput_wire::Cursor,
};

/// # Errors
/// Rejects input that is not one complete admitted protocol-v2 streamed transaction.
pub fn decode_streamed_changes(
    input: &[u8],
    source: PgoutputSource,
) -> Result<SourceTransaction, PgoutputError> {
    check_input_limit(input)?;
    if source.shape != SourceShape::KeyOnly {
        return Err(PgoutputError::RelationShape);
    }

    let mut cursor = Cursor::new(input);
    if cursor.byte()? != b'S' {
        return Err(PgoutputError::MessageOrder);
    }
    let xid = cursor.u32()?;
    if xid == 0 || cursor.byte()? != 1 {
        return Err(PgoutputError::InvalidIdentity);
    }

    let mut relation_seen = false;
    let mut row_ids = Vec::new();
    loop {
        loop {
            match cursor.byte()? {
                b'R' => {
                    require_xid(&mut cursor, xid)?;
                    decode_relation(&mut cursor, source)?;
                    relation_seen = true;
                }
                b'I' if relation_seen => {
                    if row_ids.len() >= MAX_PGOUTPUT_CHANGES {
                        return Err(PgoutputError::LimitExceeded);
                    }
                    require_xid(&mut cursor, xid)?;
                    match decode_insert(&mut cursor, source)? {
                        DecodedChange::RowInsert(row_id, SourcePayload::Absent) => {
                            row_ids.push(row_id);
                        }
                        _ => return Err(PgoutputError::TupleShape),
                    }
                }
                b'I' => return Err(PgoutputError::MessageOrder),
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
            b'c' => break,
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

    let identity = SourceTransactionId::new(
        source.source_id,
        source.slot_generation,
        PostgresLsn::from_u64(commit_lsn),
        IngressTransactionId::new(u64::from(xid)).map_err(|_| PgoutputError::InvalidIdentity)?,
    )
    .map_err(|_| PgoutputError::InvalidIdentity)?;
    let inserts = row_ids
        .into_iter()
        .enumerate()
        .map(|(index, row_id)| {
            let sequence = u64::try_from(index)
                .ok()
                .and_then(|value| value.checked_add(1))
                .and_then(|value| InputSequence::new(value).ok())
                .ok_or(PgoutputError::InvalidIdentity)?;
            Ok(SourceInsert::new(sequence, row_id))
        })
        .collect::<Result<Vec<_>, PgoutputError>>()?;
    SourceTransaction::new(identity, inserts).map_err(|_| PgoutputError::TupleValue)
}

fn require_xid(cursor: &mut Cursor<'_>, expected: u32) -> Result<(), PgoutputError> {
    if cursor.u32()? != expected {
        return Err(PgoutputError::InvalidIdentity);
    }
    Ok(())
}
