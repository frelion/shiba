use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputContract {
    pub schema: crate::ResultSchemaV1,
    pub initial_row: Option<crate::TypedResultRowV1>,
}

impl OutputContract {
    /// Builds and validates one generic result contract.
    ///
    /// # Errors
    ///
    /// Rejects an invalid schema or an initial row that does not match it.
    pub fn new(
        schema: crate::ResultSchemaV1,
        initial_row: Option<crate::TypedResultRowV1>,
    ) -> Result<Self, crate::ResultError> {
        schema.validate()?;
        match (schema.is_scalar(), &initial_row) {
            (true, Some(row)) => row.validate(&schema)?,
            (false, None) => {}
            _ => return Err(crate::ResultError::InvalidSchema),
        }
        Ok(Self {
            schema,
            initial_row,
        })
    }

    /// Validates the schema and optional initial row as one exact contract.
    ///
    /// # Errors
    ///
    /// Rejects malformed schema bytes or a mismatched initial row.
    pub fn validate(&self) -> Result<(), crate::ResultError> {
        let rebuilt = Self::new(self.schema.clone(), self.initial_row.clone())?;
        if rebuilt != *self {
            return Err(crate::ResultError::Codec);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateContract {
    pub codec_version: u32,
}
