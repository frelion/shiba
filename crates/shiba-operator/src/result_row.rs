use serde::{Deserialize, Serialize};

use crate::{ResultError, ResultSchemaV1, TypedValue};

pub const RESULT_ROW_FORMAT_VERSION: u32 = 1;
pub const MAX_RESULT_ROW_BYTES: usize = 4 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TypedResultRowV1 {
    pub format_version: u32,
    pub schema_digest: [u8; 32],
    pub values: Vec<TypedValue>,
}

impl TypedResultRowV1 {
    /// Creates a complete typed row for one exact result schema.
    ///
    /// # Errors
    ///
    /// Rejects absent, mistyped, incorrectly nullable, or oversized values.
    pub fn new(schema: &ResultSchemaV1, values: Vec<TypedValue>) -> Result<Self, ResultError> {
        schema.validate()?;
        validate_values(schema, &values)?;
        let row = Self {
            format_version: RESULT_ROW_FORMAT_VERSION,
            schema_digest: schema.digest,
            values,
        };
        if row.to_canonical_payload()?.len() > MAX_RESULT_ROW_BYTES {
            return Err(ResultError::RowLimit);
        }
        Ok(row)
    }

    /// Encodes this row deterministically.
    ///
    /// # Errors
    ///
    /// Rejects an unsupported version or row-size overflow.
    pub fn to_canonical_payload(&self) -> Result<Vec<u8>, ResultError> {
        if self.format_version != RESULT_ROW_FORMAT_VERSION {
            return Err(ResultError::Version);
        }
        let payload = serde_json::to_vec(self).map_err(|_| ResultError::Codec)?;
        if payload.len() > MAX_RESULT_ROW_BYTES {
            return Err(ResultError::RowLimit);
        }
        Ok(payload)
    }

    /// Decodes and validates an exact canonical row.
    ///
    /// # Errors
    ///
    /// Rejects malformed, noncanonical, oversized, or schema-mismatched input.
    pub fn from_canonical_payload(
        schema: &ResultSchemaV1,
        payload: &[u8],
    ) -> Result<Self, ResultError> {
        if payload.len() > MAX_RESULT_ROW_BYTES {
            return Err(ResultError::RowLimit);
        }
        let row: Self = serde_json::from_slice(payload).map_err(|_| ResultError::Codec)?;
        row.validate(schema)?;
        if row.to_canonical_payload()? != payload {
            return Err(ResultError::Codec);
        }
        Ok(row)
    }

    /// Validates this row against the supplied schema.
    ///
    /// # Errors
    ///
    /// Rejects a version, digest, field count, type, or nullability mismatch.
    pub fn validate(&self, schema: &ResultSchemaV1) -> Result<(), ResultError> {
        if self.format_version != RESULT_ROW_FORMAT_VERSION || self.schema_digest != schema.digest {
            return Err(ResultError::SchemaMismatch);
        }
        validate_values(schema, &self.values)
    }
}

fn validate_values(schema: &ResultSchemaV1, values: &[TypedValue]) -> Result<(), ResultError> {
    if values.len() != schema.fields.len() {
        return Err(ResultError::SchemaMismatch);
    }
    for (value, field) in values.iter().zip(&schema.fields) {
        if matches!(value, TypedValue::Absent) {
            return Err(ResultError::Absent);
        }
        if value.value_type() != Some(field.value_type)
            || matches!(value, TypedValue::Null(_)) && !field.nullable
        {
            return Err(ResultError::WrongType);
        }
        value
            .to_canonical_json()
            .map_err(|_| ResultError::WrongType)?;
    }
    Ok(())
}
