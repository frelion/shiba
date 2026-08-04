use core::fmt;
use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::ValueType;

pub const RESULT_SCHEMA_FORMAT_VERSION: u32 = 1;
pub const MAX_RESULT_FIELDS: usize = 16;
pub const MAX_RESULT_SCHEMA_BYTES: usize = 16 * 1024;
pub const MAX_RESULT_IDENTIFIER_BYTES: usize = 63;
const RESULT_SCHEMA_DOMAIN: &[u8] = b"shiba.result.schema.v1\0";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResultField {
    pub ordinal: u16,
    pub name: String,
    pub value_type: ValueType,
    pub nullable: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CanonicalResultSchema {
    format_version: u32,
    fields: Vec<ResultField>,
    key_ordinals: Vec<u16>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResultSchemaV1 {
    pub format_version: u32,
    pub fields: Vec<ResultField>,
    pub key_ordinals: Vec<u16>,
    pub canonical_payload: Vec<u8>,
    pub digest: [u8; 32],
}

impl ResultSchemaV1 {
    /// Creates the canonical version-1 result schema.
    ///
    /// # Errors
    ///
    /// Rejects invalid fields, key ordinals, or encoded-size overflow.
    pub fn new(fields: Vec<ResultField>, key_ordinals: Vec<u16>) -> Result<Self, ResultError> {
        validate_fields(&fields, &key_ordinals)?;
        let canonical = CanonicalResultSchema {
            format_version: RESULT_SCHEMA_FORMAT_VERSION,
            fields,
            key_ordinals,
        };
        let canonical_payload = serde_json::to_vec(&canonical).map_err(|_| ResultError::Codec)?;
        if canonical_payload.len() > MAX_RESULT_SCHEMA_BYTES {
            return Err(ResultError::SchemaLimit);
        }
        let digest = hash(&canonical_payload);
        Ok(Self {
            format_version: canonical.format_version,
            fields: canonical.fields,
            key_ordinals: canonical.key_ordinals,
            canonical_payload,
            digest,
        })
    }

    /// Decodes one exact canonical schema payload and digest.
    ///
    /// # Errors
    ///
    /// Rejects malformed, noncanonical, unsupported, or digest-mismatched input.
    pub fn from_canonical_payload(payload: &[u8], digest: [u8; 32]) -> Result<Self, ResultError> {
        if payload.len() > MAX_RESULT_SCHEMA_BYTES {
            return Err(ResultError::SchemaLimit);
        }
        let canonical: CanonicalResultSchema =
            serde_json::from_slice(payload).map_err(|_| ResultError::Codec)?;
        if canonical.format_version != RESULT_SCHEMA_FORMAT_VERSION {
            return Err(ResultError::Version);
        }
        let rebuilt = Self::new(canonical.fields, canonical.key_ordinals)?;
        if rebuilt.canonical_payload != payload || rebuilt.digest != digest {
            return Err(ResultError::Codec);
        }
        Ok(rebuilt)
    }

    /// Revalidates every materialized and canonical field of this schema.
    ///
    /// # Errors
    ///
    /// Rejects any internal or canonical encoding mismatch.
    pub fn validate(&self) -> Result<(), ResultError> {
        let rebuilt = Self::from_canonical_payload(&self.canonical_payload, self.digest)?;
        if rebuilt != *self {
            return Err(ResultError::Codec);
        }
        Ok(())
    }

    #[must_use]
    pub fn is_scalar(&self) -> bool {
        self.key_ordinals.is_empty()
    }
}

fn validate_fields(fields: &[ResultField], key_ordinals: &[u16]) -> Result<(), ResultError> {
    if fields.is_empty() || fields.len() > MAX_RESULT_FIELDS {
        return Err(ResultError::FieldLimit);
    }
    for (index, field) in fields.iter().enumerate() {
        if usize::from(field.ordinal) != index + 1
            || field.name.is_empty()
            || field.name.len() > MAX_RESULT_IDENTIFIER_BYTES
            || field.name.contains('\0')
        {
            return Err(ResultError::InvalidSchema);
        }
    }
    if fields
        .iter()
        .enumerate()
        .any(|(index, field)| fields[..index].iter().any(|other| other.name == field.name))
    {
        return Err(ResultError::InvalidSchema);
    }
    let mut keys = BTreeSet::new();
    for (index, ordinal) in key_ordinals.iter().enumerate() {
        let expected = u16::try_from(index + 1).map_err(|_| ResultError::InvalidSchema)?;
        if *ordinal == 0 || usize::from(*ordinal) > fields.len() || !keys.insert(*ordinal) {
            return Err(ResultError::InvalidSchema);
        }
        if *ordinal != expected {
            return Err(ResultError::InvalidSchema);
        }
    }
    Ok(())
}

fn hash(payload: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(RESULT_SCHEMA_DOMAIN);
    hash.update(payload);
    hash.finalize().into()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResultError {
    Absent,
    Codec,
    FieldLimit,
    InvalidSchema,
    RowLimit,
    SchemaLimit,
    SchemaMismatch,
    Version,
    WrongType,
}

impl fmt::Display for ResultError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "result contract rejected: {self:?}")
    }
}

impl std::error::Error for ResultError {}
