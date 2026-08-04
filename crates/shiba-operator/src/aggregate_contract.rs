use core::fmt;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{Expression, ValueType};

pub const AGGREGATE_FUNCTION_SEMANTIC_VERSION: u32 = 1;
pub const AGGREGATE_STATE_CODEC_VERSION: u32 = 1;
pub const MAX_AGGREGATE_CALLS: usize = 16;
pub const MAX_GROUP_EXPRESSIONS: usize = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggregateFunctionV1 {
    CountStar,
    Count,
    SumInt8,
    MinInt8,
    MaxInt8,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AggregateInputContract {
    None,
    Nullable(ValueType),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "value_type", rename_all = "snake_case")]
pub enum EmptyResultV1 {
    Int8Zero,
    Null(ValueType),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AggregateFunctionDescriptor {
    pub function: AggregateFunctionV1,
    pub semantic_version: u32,
    pub input: AggregateInputContract,
    pub output_type: ValueType,
    pub output_nullable: bool,
    pub state_codec_version: u32,
    pub empty_result: EmptyResultV1,
    pub supports_retraction: bool,
}

const COUNT_STAR: AggregateFunctionDescriptor = descriptor(
    AggregateFunctionV1::CountStar,
    AggregateInputContract::None,
    false,
    EmptyResultV1::Int8Zero,
);
const COUNT: AggregateFunctionDescriptor = descriptor(
    AggregateFunctionV1::Count,
    AggregateInputContract::Nullable(ValueType::Int8),
    false,
    EmptyResultV1::Int8Zero,
);
const SUM: AggregateFunctionDescriptor = descriptor(
    AggregateFunctionV1::SumInt8,
    AggregateInputContract::Nullable(ValueType::Int8),
    true,
    EmptyResultV1::Null(ValueType::Int8),
);
const MIN: AggregateFunctionDescriptor = descriptor(
    AggregateFunctionV1::MinInt8,
    AggregateInputContract::Nullable(ValueType::Int8),
    true,
    EmptyResultV1::Null(ValueType::Int8),
);
const MAX: AggregateFunctionDescriptor = descriptor(
    AggregateFunctionV1::MaxInt8,
    AggregateInputContract::Nullable(ValueType::Int8),
    true,
    EmptyResultV1::Null(ValueType::Int8),
);

const fn descriptor(
    function: AggregateFunctionV1,
    input: AggregateInputContract,
    output_nullable: bool,
    empty_result: EmptyResultV1,
) -> AggregateFunctionDescriptor {
    AggregateFunctionDescriptor {
        function,
        semantic_version: AGGREGATE_FUNCTION_SEMANTIC_VERSION,
        input,
        output_type: ValueType::Int8,
        output_nullable,
        state_codec_version: AGGREGATE_STATE_CODEC_VERSION,
        empty_result,
        supports_retraction: true,
    }
}

#[must_use]
pub const fn aggregate_function_descriptor(
    function: AggregateFunctionV1,
) -> &'static AggregateFunctionDescriptor {
    match function {
        AggregateFunctionV1::CountStar => &COUNT_STAR,
        AggregateFunctionV1::Count => &COUNT,
        AggregateFunctionV1::SumInt8 => &SUM,
        AggregateFunctionV1::MinInt8 => &MIN,
        AggregateFunctionV1::MaxInt8 => &MAX,
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AggregateCall {
    pub ordinal: u16,
    pub function_version: u32,
    pub function: AggregateFunctionV1,
    pub expression: Option<Expression>,
}

const DESCRIPTOR_DOMAIN: &[u8] = b"shiba.aggregate.function.descriptor.v1\0";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AggregateCodecError {
    Codec,
}

impl fmt::Display for AggregateCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "aggregate descriptor codec rejected: {self:?}")
    }
}

impl std::error::Error for AggregateCodecError {}

/// Returns the stable canonical descriptor payload for one closed ABI function.
///
/// # Errors
///
/// Rejects serialization failure without substituting an unknown function.
pub fn aggregate_function_canonical_payload(
    function: AggregateFunctionV1,
) -> Result<Vec<u8>, AggregateCodecError> {
    serde_json::to_vec(aggregate_function_descriptor(function))
        .map_err(|_| AggregateCodecError::Codec)
}

/// Returns the domain-separated identity of one exact function ABI descriptor.
///
/// # Errors
///
/// Rejects canonical encoding failure.
pub fn aggregate_function_digest(
    function: AggregateFunctionV1,
) -> Result<[u8; 32], AggregateCodecError> {
    let payload = aggregate_function_canonical_payload(function)?;
    let mut hash = Sha256::new();
    hash.update(DESCRIPTOR_DOMAIN);
    hash.update(payload);
    Ok(hash.finalize().into())
}
