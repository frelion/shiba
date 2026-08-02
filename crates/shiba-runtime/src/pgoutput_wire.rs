use crate::pgoutput::PgoutputError;

pub(crate) struct Cursor<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    pub(crate) const fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    pub(crate) fn take(&mut self, length: usize) -> Result<&'a [u8], PgoutputError> {
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

    pub(crate) fn byte(&mut self) -> Result<u8, PgoutputError> {
        Ok(self.take(1)?[0])
    }

    pub(crate) fn u16(&mut self) -> Result<u16, PgoutputError> {
        Ok(u16::from_be_bytes(
            self.take(2)?.try_into().expect("fixed width"),
        ))
    }

    pub(crate) fn u32(&mut self) -> Result<u32, PgoutputError> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().expect("fixed width"),
        ))
    }

    pub(crate) fn u64(&mut self) -> Result<u64, PgoutputError> {
        Ok(u64::from_be_bytes(
            self.take(8)?.try_into().expect("fixed width"),
        ))
    }

    pub(crate) fn string(&mut self) -> Result<&'a str, PgoutputError> {
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

    pub(crate) fn int8_text(&mut self) -> Result<i64, PgoutputError> {
        let length = usize::try_from(self.u32()?).map_err(|_| PgoutputError::Truncated)?;
        let encoded =
            std::str::from_utf8(self.take(length)?).map_err(|_| PgoutputError::TupleValue)?;
        let value = encoded
            .parse::<i64>()
            .map_err(|_| PgoutputError::TupleValue)?;
        if value.to_string() != encoded {
            return Err(PgoutputError::TupleValue);
        }
        Ok(value)
    }

    pub(crate) fn finished(&self) -> bool {
        self.offset == self.input.len()
    }
}
