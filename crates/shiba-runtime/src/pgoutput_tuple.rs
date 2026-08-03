use crate::{
    PgoutputError, SourcePayload, SourceUpdatePayload,
    pgoutput_source::{PgoutputSource, SourceShape},
    pgoutput_wire::Cursor,
};

pub(crate) enum DecodedChange {
    EmptyInsert,
    RowInsert(i64, SourcePayload),
    CompositeInsert(i64, i64),
    Update(i64, i64, SourceUpdatePayload),
    Delete(i64, Option<i64>),
}

pub(crate) fn decode_insert(
    cursor: &mut Cursor<'_>,
    source: PgoutputSource,
) -> Result<DecodedChange, PgoutputError> {
    tuple_header(cursor, source, b'N', source.shape.columns())?;
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
        SourceShape::TextPayload => {
            let key = decode_int8(cursor)?;
            let format = cursor.byte()?;
            Ok(DecodedChange::RowInsert(
                key,
                SourcePayload::Text(decode_text(cursor, format)?),
            ))
        }
    }
}

pub(crate) fn decode_update(
    cursor: &mut Cursor<'_>,
    source: PgoutputSource,
) -> Result<DecodedChange, PgoutputError> {
    match source.shape {
        SourceShape::NullableInt8Payload => {
            if cursor.u32()? != source.relation_id {
                return Err(PgoutputError::RelationMismatch);
            }
            let tag = cursor.byte()?;
            let old_key = if tag == b'K' {
                tuple_column_count(cursor, 2)?;
                let key = decode_int8(cursor)?;
                if cursor.byte()? != b'n' || cursor.byte()? != b'N' {
                    return Err(PgoutputError::TupleShape);
                }
                Some(key)
            } else if tag == b'N' {
                None
            } else {
                return Err(PgoutputError::TupleTag(tag));
            };
            tuple_column_count(cursor, 2)?;
            let new_key = decode_int8(cursor)?;
            Ok(DecodedChange::Update(
                old_key.unwrap_or(new_key),
                new_key,
                SourceUpdatePayload::Int8(decode_optional_int8(cursor)?),
            ))
        }
        SourceShape::TextPayload => {
            tuple_header(cursor, source, b'N', 2)?;
            let key = decode_int8(cursor)?;
            let payload = match cursor.byte()? {
                b'u' => SourceUpdatePayload::UnchangedText,
                format => SourceUpdatePayload::Text(decode_text(cursor, format)?),
            };
            Ok(DecodedChange::Update(key, key, payload))
        }
        _ => Err(PgoutputError::TupleShape),
    }
}

pub(crate) fn decode_delete(
    cursor: &mut Cursor<'_>,
    source: PgoutputSource,
) -> Result<DecodedChange, PgoutputError> {
    match source.shape {
        SourceShape::KeyOnly => {
            tuple_header(cursor, source, b'K', 1)?;
            Ok(DecodedChange::Delete(decode_int8(cursor)?, None))
        }
        SourceShape::NullableInt8Payload => {
            tuple_header(cursor, source, b'K', 2)?;
            let key = decode_int8(cursor)?;
            if cursor.byte()? != b'n' {
                return Err(PgoutputError::TupleShape);
            }
            Ok(DecodedChange::Delete(key, None))
        }
        SourceShape::CompositeInt8 => {
            tuple_header(cursor, source, b'K', 2)?;
            Ok(DecodedChange::Delete(
                decode_int8(cursor)?,
                Some(decode_int8(cursor)?),
            ))
        }
        _ => Err(PgoutputError::TupleShape),
    }
}

fn tuple_header(
    cursor: &mut Cursor<'_>,
    source: PgoutputSource,
    tuple_tag: u8,
    columns: u16,
) -> Result<(), PgoutputError> {
    if cursor.u32()? != source.relation_id {
        return Err(PgoutputError::RelationMismatch);
    }
    if cursor.byte()? != tuple_tag {
        return Err(PgoutputError::TupleShape);
    }
    tuple_column_count(cursor, columns)
}

fn tuple_column_count(cursor: &mut Cursor<'_>, columns: u16) -> Result<(), PgoutputError> {
    if cursor.u16()? != columns {
        return Err(PgoutputError::TupleShape);
    }
    Ok(())
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

fn decode_text(cursor: &mut Cursor<'_>, format: u8) -> Result<String, PgoutputError> {
    if format != b't' {
        return Err(PgoutputError::TupleTag(format));
    }
    let length = usize::try_from(cursor.u32()?).map_err(|_| PgoutputError::Truncated)?;
    Ok(std::str::from_utf8(cursor.take(length)?)
        .map_err(|_| PgoutputError::TupleValue)?
        .to_owned())
}
