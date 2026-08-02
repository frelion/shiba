use core::num::NonZeroU64;

use serde::{Deserialize, Serialize};
use shiba_protocol::{SourceId, SourceTransactionId};

/// Stable identity of one registered operator.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OperatorId(NonZeroU64);

impl OperatorId {
    #[must_use]
    pub const fn new(value: NonZeroU64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Database-independent form of a `PostgreSQL` object address.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectAddress {
    pub class_id: u32,
    pub object_id: u32,
    pub sub_id: i32,
}

/// Value carried by the first proven row-effect contract.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Value {
    Absent,
    Null,
    Int8(i64),
    Text(String),
}

/// One source row image visible inside the processor-owned transaction.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RowImage {
    pub source_row_id: Option<i64>,
    pub source_row_sub_id: Option<i64>,
    pub payload: Value,
}

/// Before/after images for exactly one applied source-row mutation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RowEffect {
    pub before: Option<RowImage>,
    pub after: Option<RowImage>,
}

/// Transaction-local effects produced by Source Apply.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectBatch {
    pub source_transaction: SourceTransactionId,
    pub effects: Vec<RowEffect>,
}

/// Closed set of operator implementations proven by the compiler.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CompiledOperatorKind {
    CountRows,
    SumInt8 { input: ObjectAddress },
}

/// Database-independent output of compilation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompiledOperator {
    pub operator_id: OperatorId,
    pub source_id: SourceId,
    pub kind: CompiledOperatorKind,
}
