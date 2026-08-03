//! Strict, database-independent compilation of versioned operator specs.

#![forbid(unsafe_code)]

mod plan;

use core::fmt;

use serde::{Deserialize, Deserializer, Serialize, de};
use shiba_operator::{ObjectAddress, OperatorId};
use shiba_protocol::SourceId;

pub use plan::compile_plan;

pub const OPERATOR_SPEC_VERSION: u32 = 1;
/// `PostgreSQL`'s built-in `int8` type OID.
pub const POSTGRES_INT8_TYPE_OID: u32 = 20;

/// Strict version-1 declarative operator definition.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct OperatorSpecV1 {
    pub version: u32,
    pub operator_id: OperatorId,
    pub source_id: SourceId,
    pub operation: OperatorOperationV1,
}

impl OperatorSpecV1 {
    /// Encodes the unique compact JSON representation of this IR.
    ///
    /// # Errors
    ///
    /// Returns a serialization error if encoding fails.
    pub fn to_canonical_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Decodes exactly one strict version-1 spec.
    ///
    /// # Errors
    ///
    /// Rejects malformed JSON, trailing data, unknown fields, unsupported
    /// versions, zero identities, and blank `SumInt8` column names.
    pub fn from_json(input: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(input)
    }
}

impl<'de> Deserialize<'de> for OperatorSpecV1 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Raw {
            version: u32,
            operator_id: OperatorId,
            source_id: SourceId,
            operation: OperatorOperationV1,
        }

        let raw = Raw::deserialize(deserializer)?;
        if raw.version != OPERATOR_SPEC_VERSION {
            return Err(de::Error::custom(format_args!(
                "unsupported operator spec version {}",
                raw.version
            )));
        }
        match &raw.operation {
            OperatorOperationV1::CountRows => {}
            OperatorOperationV1::SumInt8 { input_column } => {
                reject_blank::<D::Error>(input_column, "sum_int8 input_column")?;
            }
            OperatorOperationV1::ProjectRows {
                key_column,
                input_column,
            } => {
                reject_blank::<D::Error>(key_column, "project_rows key_column")?;
                reject_blank::<D::Error>(input_column, "project_rows input_column")?;
            }
        }
        Ok(Self {
            version: raw.version,
            operator_id: raw.operator_id,
            source_id: raw.source_id,
            operation: raw.operation,
        })
    }
}

/// Closed version-1 operation set. It intentionally accepts no SQL text.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperatorOperationV1 {
    CountRows,
    SumInt8 {
        input_column: String,
    },
    ProjectRows {
        key_column: String,
        input_column: String,
    },
}

fn reject_blank<E: de::Error>(value: &str, field: &str) -> Result<(), E> {
    if value.trim().is_empty() {
        return Err(E::custom(format_args!("{field} cannot be blank")));
    }
    Ok(())
}

/// Live source metadata supplied to the pure compiler by Runtime.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceDescriptor {
    pub source_id: SourceId,
    pub relation: ObjectAddress,
    pub columns: Vec<SourceColumnDescriptor>,
}

/// One live bound column available during compilation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceColumnDescriptor {
    pub name: String,
    pub address: ObjectAddress,
    pub type_oid: u32,
    pub nullable: bool,
}

/// Fail-closed compilation errors with no database behavior.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CompilerError {
    UnsupportedVersion(u32),
    BlankInputColumn,
    SourceMismatch,
    MissingColumn(String),
    DuplicateColumn(String),
    WrongColumnType { column: String, type_oid: u32 },
    NullableKey(String),
    PlanEncoding,
}

impl fmt::Display for CompilerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported operator spec version {version}")
            }
            Self::BlankInputColumn => formatter.write_str("sum_int8 input column is blank"),
            Self::SourceMismatch => formatter.write_str("spec and descriptor source differ"),
            Self::MissingColumn(column) => write!(formatter, "source column {column:?} is missing"),
            Self::DuplicateColumn(column) => {
                write!(formatter, "source column {column:?} is ambiguous")
            }
            Self::WrongColumnType { column, type_oid } => write!(
                formatter,
                "source column {column:?} has type OID {type_oid}, expected 20"
            ),
            Self::NullableKey(column) => {
                write!(formatter, "project key column {column:?} must be non-null")
            }
            Self::PlanEncoding => formatter.write_str("compiled plan encoding failed"),
        }
    }
}

impl std::error::Error for CompilerError {}
