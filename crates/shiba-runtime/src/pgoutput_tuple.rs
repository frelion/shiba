use crate::{
    PgoutputError, SourcePayload,
    pgoutput_source::{PgoutputSource, SourceShape},
    pgoutput_wire::Cursor,
};

pub(crate) enum DecodedChange {
    EmptyInsert,
    RowInsert(i64, SourcePayload),
    CompositeInsert(i64, i64),
    Update(i64, Option<i64>),
    Delete(i64),
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
    }
}

pub(crate) fn decode_update(
    cursor: &mut Cursor<'_>,
    source: PgoutputSource,
) -> Result<DecodedChange, PgoutputError> {
    if source.shape != SourceShape::NullableInt8Payload {
        return Err(PgoutputError::TupleShape);
    }
    tuple_header(cursor, source, b'N', 2)?;
    Ok(DecodedChange::Update(
        decode_int8(cursor)?,
        decode_optional_int8(cursor)?,
    ))
}

pub(crate) fn decode_delete(
    cursor: &mut Cursor<'_>,
    source: PgoutputSource,
) -> Result<DecodedChange, PgoutputError> {
    if source.shape != SourceShape::KeyOnly {
        return Err(PgoutputError::TupleShape);
    }
    tuple_header(cursor, source, b'K', 1)?;
    Ok(DecodedChange::Delete(decode_int8(cursor)?))
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
    if cursor.byte()? != tuple_tag || cursor.u16()? != columns {
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
