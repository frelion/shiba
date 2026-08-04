use serde::{Deserialize, Serialize};

use crate::{NodeId, ResultError, ResultSchemaV1, TypedResultRowV1, TypedValue};

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResultRowKey {
    pub schema_digest: [u8; 32],
    pub values: Vec<TypedValue>,
}

impl ResultRowKey {
    /// Creates the sole singleton identity for a scalar schema.
    ///
    /// # Errors
    ///
    /// Rejects an invalid or keyed schema.
    pub fn scalar(schema: &ResultSchemaV1) -> Result<Self, ResultError> {
        schema.validate()?;
        if !schema.is_scalar() {
            return Err(ResultError::InvalidSchema);
        }
        Ok(Self {
            schema_digest: schema.digest,
            values: Vec::new(),
        })
    }

    /// Projects a canonical keyed identity from a complete result row.
    ///
    /// # Errors
    ///
    /// Rejects scalar schemas and invalid or mismatched rows.
    pub fn from_row(schema: &ResultSchemaV1, row: &TypedResultRowV1) -> Result<Self, ResultError> {
        row.validate(schema)?;
        if schema.is_scalar() {
            return Err(ResultError::InvalidSchema);
        }
        let values = schema
            .key_ordinals
            .iter()
            .map(|ordinal| row.values[usize::from(*ordinal) - 1].clone())
            .collect();
        Ok(Self {
            schema_digest: schema.digest,
            values,
        })
    }

    /// Validates the key against its exact schema.
    ///
    /// # Errors
    ///
    /// Rejects digest, arity, type, nullability, or absent-value mismatch.
    pub fn validate(&self, schema: &ResultSchemaV1) -> Result<(), ResultError> {
        schema.validate()?;
        if self.schema_digest != schema.digest || self.values.len() != schema.key_ordinals.len() {
            return Err(ResultError::SchemaMismatch);
        }
        for (value, ordinal) in self.values.iter().zip(&schema.key_ordinals) {
            let field = &schema.fields[usize::from(*ordinal) - 1];
            if matches!(value, TypedValue::Absent)
                || value.value_type() != Some(field.value_type)
                || matches!(value, TypedValue::Null(_)) && !field.nullable
            {
                return Err(ResultError::WrongType);
            }
        }
        Ok(())
    }

    /// Encodes the key deterministically.
    ///
    /// # Errors
    ///
    /// Rejects encoding failure or size overflow.
    pub fn to_canonical_payload(&self) -> Result<Vec<u8>, ResultError> {
        let payload = serde_json::to_vec(self).map_err(|_| ResultError::Codec)?;
        if payload.len() > crate::MAX_RESULT_ROW_BYTES {
            return Err(ResultError::RowLimit);
        }
        Ok(payload)
    }

    /// Decodes and validates an exact canonical key.
    ///
    /// # Errors
    ///
    /// Rejects malformed, noncanonical, oversized, or schema-mismatched input.
    pub fn from_canonical_payload(
        schema: &ResultSchemaV1,
        payload: &[u8],
    ) -> Result<Self, ResultError> {
        if payload.len() > crate::MAX_RESULT_ROW_BYTES {
            return Err(ResultError::RowLimit);
        }
        let key: Self = serde_json::from_slice(payload).map_err(|_| ResultError::Codec)?;
        key.validate(schema)?;
        if key.to_canonical_payload()? != payload {
            return Err(ResultError::Codec);
        }
        Ok(key)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "mutation", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResultMutation {
    ReplaceScalar {
        row: TypedResultRowV1,
    },
    Delete {
        key: ResultRowKey,
    },
    Upsert {
        key: ResultRowKey,
        row: TypedResultRowV1,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResultDelta {
    pub node_id: NodeId,
    pub mutations: Vec<ResultMutation>,
}
