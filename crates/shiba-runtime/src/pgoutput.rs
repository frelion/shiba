use core::fmt;

use shiba_protocol::{
    IngressTransactionId, InputSequence, PostgresLsn, SlotGeneration, SourceId, SourceTransactionId,
};

use crate::{SourceInsert, SourceTransaction};

const INT8_OID: u32 = 20;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgoutputSource {
    source_id: SourceId,
    slot_generation: SlotGeneration,
    relation_id: u32,
}

impl PgoutputSource {
    #[must_use]
    pub const fn new(
        source_id: SourceId,
        slot_generation: SlotGeneration,
        relation_id: u32,
    ) -> Self {
        Self {
            source_id,
            slot_generation,
            relation_id,
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
}

impl fmt::Display for PgoutputError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated => formatter.write_str("truncated pgoutput message"),
            Self::UnknownMessage(tag) => write!(formatter, "unsupported pgoutput message {tag:#x}"),
            Self::MessageOrder => formatter.write_str("invalid pgoutput message order"),
            Self::RelationMismatch => {
                formatter.write_str("pgoutput relation does not match source")
            }
            Self::RelationShape => formatter.write_str("pgoutput relation must have one column"),
            Self::RelationType => formatter.write_str("pgoutput relation column is not int8"),
            Self::TupleTag(tag) => write!(formatter, "unsupported pgoutput tuple tag {tag:#x}"),
            Self::TupleShape => formatter.write_str("pgoutput tuple must have one column"),
            Self::TupleValue => formatter.write_str("invalid pgoutput int8 text value"),
            Self::InvalidIdentity => formatter.write_str("invalid pgoutput transaction identity"),
            Self::InvalidLsn => formatter.write_str("inconsistent pgoutput commit LSN"),
        }
    }
}

impl std::error::Error for PgoutputError {}

/// Decodes exactly one complete, non-streaming pgoutput protocol-v1 transaction.
///
/// # Errors
///
/// Returns [`PgoutputError`] unless the input is one complete transaction with
/// the exact `BEGIN`, target `RELATION`, `INSERT`+, `COMMIT` shape supported by
/// M3.1.
pub fn decode_committed_insert(
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
    decode_relation(&mut cursor, source.relation_id)?;

    let mut values = Vec::new();
    loop {
        let tag = cursor.byte()?;
        match tag {
            b'I' => values.push(decode_insert(&mut cursor, source.relation_id)?),
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
    let inserts = values
        .into_iter()
        .enumerate()
        .map(|(index, source_row_id)| {
            let sequence = u64::try_from(index)
                .ok()
                .and_then(|value| value.checked_add(1))
                .and_then(|value| InputSequence::new(value).ok())
                .ok_or(PgoutputError::InvalidIdentity)?;
            Ok(SourceInsert::new(sequence, source_row_id))
        })
        .collect::<Result<Vec<_>, _>>()?;
    SourceTransaction::new(identity, inserts).map_err(|_| PgoutputError::TupleValue)
}

fn decode_relation(cursor: &mut Cursor<'_>, relation_id: u32) -> Result<(), PgoutputError> {
    if relation_id == 0 || cursor.u32()? != relation_id {
        return Err(PgoutputError::RelationMismatch);
    }
    cursor.string()?;
    cursor.string()?;
    if !matches!(cursor.byte()?, b'd' | b'n' | b'f' | b'i') || cursor.u16()? != 1 {
        return Err(PgoutputError::RelationShape);
    }
    if cursor.byte()? > 1 {
        return Err(PgoutputError::RelationShape);
    }
    cursor.string()?;
    if cursor.u32()? != INT8_OID {
        return Err(PgoutputError::RelationType);
    }
    cursor.u32()?;
    Ok(())
}

fn decode_insert(cursor: &mut Cursor<'_>, relation_id: u32) -> Result<i64, PgoutputError> {
    if cursor.u32()? != relation_id {
        return Err(PgoutputError::RelationMismatch);
    }
    if cursor.byte()? != b'N' || cursor.u16()? != 1 {
        return Err(PgoutputError::TupleShape);
    }
    let format = cursor.byte()?;
    if format != b't' {
        return Err(PgoutputError::TupleTag(format));
    }
    let length = usize::try_from(cursor.u32()?).map_err(|_| PgoutputError::Truncated)?;
    let encoded =
        std::str::from_utf8(cursor.take(length)?).map_err(|_| PgoutputError::TupleValue)?;
    let value = encoded
        .parse::<i64>()
        .map_err(|_| PgoutputError::TupleValue)?;
    if value.to_string() != encoded {
        return Err(PgoutputError::TupleValue);
    }
    Ok(value)
}

struct Cursor<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    const fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], PgoutputError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(PgoutputError::Truncated)?;
        let value = self
            .input
            .get(self.offset..end)
            .ok_or(PgoutputError::Truncated)?;
        self.offset = end;
        Ok(value)
    }

    fn byte(&mut self) -> Result<u8, PgoutputError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, PgoutputError> {
        Ok(u16::from_be_bytes(
            self.take(2)?.try_into().expect("fixed width"),
        ))
    }

    fn u32(&mut self) -> Result<u32, PgoutputError> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().expect("fixed width"),
        ))
    }

    fn u64(&mut self) -> Result<u64, PgoutputError> {
        Ok(u64::from_be_bytes(
            self.take(8)?.try_into().expect("fixed width"),
        ))
    }

    fn string(&mut self) -> Result<&'a str, PgoutputError> {
        let rest = self
            .input
            .get(self.offset..)
            .ok_or(PgoutputError::Truncated)?;
        let length = rest
            .iter()
            .position(|byte| *byte == 0)
            .ok_or(PgoutputError::Truncated)?;
        let value =
            std::str::from_utf8(self.take(length)?).map_err(|_| PgoutputError::RelationShape)?;
        self.byte()?;
        Ok(value)
    }

    fn finished(&self) -> bool {
        self.offset == self.input.len()
    }
}
