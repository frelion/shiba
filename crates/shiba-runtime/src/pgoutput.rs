use core::fmt;

use shiba_protocol::{IngressTransactionId, InputSequence, PostgresLsn, SourceTransactionId};

use crate::{
    SourceChange, SourceInsert, SourcePayload, SourceTransaction, SourceUpdate,
    pgoutput_source::{PgoutputSource, SourceShape},
    pgoutput_wire::Cursor,
};

const INT8_OID: u32 = 20;

enum DecodedChange {
    EmptyInsert,
    RowInsert(i64, SourcePayload),
    CompositeInsert(i64, i64),
    Update(i64, Option<i64>),
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
        }
    }
}

impl std::error::Error for PgoutputError {}

/// # Errors
/// Rejects input that is not one complete admitted M4.4 transaction.
pub fn decode_committed_changes(
    input: &[u8],
    source: PgoutputSource,
) -> Result<SourceTransaction, PgoutputError> {
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

    if cursor.byte()? != b'R' {
        return Err(PgoutputError::MessageOrder);
    }
    decode_relation(&mut cursor, source)?;

    let mut values = Vec::new();
    loop {
        let tag = cursor.byte()?;
        match tag {
            b'I' => values.push(decode_insert(&mut cursor, source)?),
            b'U' => values.push(decode_update(&mut cursor, source)?),
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

    let identity = SourceTransactionId::new(
        source.source_id,
        source.slot_generation,
        PostgresLsn::from_u64(commit_lsn),
        IngressTransactionId::new(u64::from(xid)).map_err(|_| PgoutputError::InvalidIdentity)?,
    )
    .map_err(|_| PgoutputError::InvalidIdentity)?;
    let changes = values
        .into_iter()
        .enumerate()
        .map(|(index, row)| {
            let sequence = u64::try_from(index)
                .ok()
                .and_then(|value| value.checked_add(1))
                .and_then(|value| InputSequence::new(value).ok())
                .ok_or(PgoutputError::InvalidIdentity)?;
            Ok(match row {
                DecodedChange::EmptyInsert => SourceChange::Insert(SourceInsert::empty(sequence)),
                DecodedChange::RowInsert(row_id, payload) => {
                    SourceChange::Insert(SourceInsert::with_payload(sequence, row_id, payload))
                }
                DecodedChange::CompositeInsert(key1, key2) => {
                    SourceChange::Insert(SourceInsert::composite(sequence, key1, key2))
                }
                DecodedChange::Update(row_id, payload) => {
                    SourceChange::Update(SourceUpdate::new(sequence, row_id, payload))
                }
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    SourceTransaction::from_changes(identity, changes).map_err(|_| PgoutputError::TupleValue)
}

fn decode_relation(cursor: &mut Cursor<'_>, source: PgoutputSource) -> Result<(), PgoutputError> {
    if source.relation_id == 0 || cursor.u32()? != source.relation_id {
        return Err(PgoutputError::RelationMismatch);
    }
    cursor.string()?;
    cursor.string()?;
    let columns = source.shape.columns();
    if !matches!(cursor.byte()?, b'd' | b'n' | b'f' | b'i') || cursor.u16()? != columns {
        return Err(PgoutputError::RelationShape);
    }
    for _ in 0..columns {
        if cursor.byte()? > 1 {
            return Err(PgoutputError::RelationShape);
        }
        cursor.string()?;
        if cursor.u32()? != INT8_OID {
            return Err(PgoutputError::RelationType);
        }
        cursor.u32()?;
    }
    Ok(())
}

fn decode_insert(
    cursor: &mut Cursor<'_>,
    source: PgoutputSource,
) -> Result<DecodedChange, PgoutputError> {
    if cursor.u32()? != source.relation_id {
        return Err(PgoutputError::RelationMismatch);
    }
    let columns = source.shape.columns();
    if cursor.byte()? != b'N' || cursor.u16()? != columns {
        return Err(PgoutputError::TupleShape);
    }
    match source.shape {
        SourceShape::Empty => Ok(DecodedChange::EmptyInsert),
        SourceShape::KeyOnly => Ok(DecodedChange::RowInsert(
            decode_int8(cursor)?,
            SourcePayload::Absent,
        )),
        SourceShape::NullableInt8Payload => {
            let key = decode_int8(cursor)?;
            let payload = match decode_optional_int8(cursor)? {
                None => SourcePayload::Null,
                Some(value) => SourcePayload::Int8(value),
            };
            Ok(DecodedChange::RowInsert(key, payload))
        }
        SourceShape::CompositeInt8 => Ok(DecodedChange::CompositeInsert(
            decode_int8(cursor)?,
            decode_int8(cursor)?,
        )),
    }
}

fn decode_update(
    cursor: &mut Cursor<'_>,
    source: PgoutputSource,
) -> Result<DecodedChange, PgoutputError> {
    if source.shape != SourceShape::NullableInt8Payload {
        return Err(PgoutputError::TupleShape);
    }
    if cursor.u32()? != source.relation_id {
        return Err(PgoutputError::RelationMismatch);
    }
    if cursor.byte()? != b'N' || cursor.u16()? != 2 {
        return Err(PgoutputError::TupleShape);
    }
    let row_id = decode_int8(cursor)?;
    let payload = decode_optional_int8(cursor)?;
    Ok(DecodedChange::Update(row_id, payload))
}

fn decode_optional_int8(cursor: &mut Cursor<'_>) -> Result<Option<i64>, PgoutputError> {
    match cursor.byte()? {
        b'n' => Ok(None),
        b't' => Ok(Some(cursor.int8_text()?)),
        tag => Err(PgoutputError::TupleTag(tag)),
    }
}

fn decode_int8(cursor: &mut Cursor<'_>) -> Result<i64, PgoutputError> {
    let format = cursor.byte()?;
    if format != b't' {
        return Err(PgoutputError::TupleTag(format));
    }
    cursor.int8_text()
}
