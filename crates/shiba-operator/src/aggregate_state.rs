use crate::{
    AGGREGATE_STATE_CODEC_VERSION, AggregateFunctionV1, EncodedOperatorState, KernelError,
    StateEntry, TypedValue, ValueType,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CallState {
    Count(i64),
    Sum { non_null: i64, value: i64 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CallDelta {
    Count(i64),
    Sum { non_null: i64, value: i128 },
}

pub(crate) fn decode(
    function: AggregateFunctionV1,
    entry: &StateEntry,
) -> Result<CallState, KernelError> {
    let Some(encoded) = &entry.state else {
        return Ok(empty(function));
    };
    if encoded.codec_version != AGGREGATE_STATE_CODEC_VERSION {
        return Err(KernelError::InvalidState);
    }
    let state = match function {
        AggregateFunctionV1::CountStar | AggregateFunctionV1::Count => {
            CallState::Count(read_i64(&encoded.payload)?)
        }
        AggregateFunctionV1::SumInt8 if encoded.payload.len() == 16 => CallState::Sum {
            non_null: i64::from_be_bytes(
                encoded.payload[..8]
                    .try_into()
                    .map_err(|_| KernelError::InvalidState)?,
            ),
            value: i64::from_be_bytes(
                encoded.payload[8..]
                    .try_into()
                    .map_err(|_| KernelError::InvalidState)?,
            ),
        },
        AggregateFunctionV1::SumInt8 => return Err(KernelError::InvalidState),
    };
    validate(state)?;
    Ok(state)
}

pub(crate) const fn empty(function: AggregateFunctionV1) -> CallState {
    match function {
        AggregateFunctionV1::CountStar | AggregateFunctionV1::Count => CallState::Count(0),
        AggregateFunctionV1::SumInt8 => CallState::Sum {
            non_null: 0,
            value: 0,
        },
    }
}

pub(crate) fn encode(state: CallState) -> EncodedOperatorState {
    let payload = match state {
        CallState::Count(value) => value.to_be_bytes().to_vec(),
        CallState::Sum { non_null, value } => {
            let mut bytes = non_null.to_be_bytes().to_vec();
            bytes.extend_from_slice(&value.to_be_bytes());
            bytes
        }
    };
    EncodedOperatorState {
        codec_version: AGGREGATE_STATE_CODEC_VERSION,
        payload,
    }
}

pub(crate) fn output(state: CallState) -> TypedValue {
    match state {
        CallState::Sum { non_null: 0, .. } => TypedValue::Null(ValueType::Int8),
        CallState::Count(value) | CallState::Sum { value, .. } => TypedValue::Int8(value),
    }
}

pub(crate) const fn empty_delta(function: AggregateFunctionV1) -> CallDelta {
    match function {
        AggregateFunctionV1::CountStar | AggregateFunctionV1::Count => CallDelta::Count(0),
        AggregateFunctionV1::SumInt8 => CallDelta::Sum {
            non_null: 0,
            value: 0,
        },
    }
}

pub(crate) fn accumulate(
    change: &mut CallDelta,
    function: AggregateFunctionV1,
    value: Option<&TypedValue>,
    delta: i64,
) -> Result<(), KernelError> {
    match (function, change, value) {
        (AggregateFunctionV1::CountStar, CallDelta::Count(count), None)
        | (AggregateFunctionV1::Count, CallDelta::Count(count), Some(TypedValue::Int8(_))) => {
            *count = count.checked_add(delta).ok_or(KernelError::Overflow)?;
        }
        (
            AggregateFunctionV1::Count,
            CallDelta::Count(_),
            Some(TypedValue::Null(ValueType::Int8)),
        )
        | (
            AggregateFunctionV1::SumInt8,
            CallDelta::Sum { .. },
            Some(TypedValue::Null(ValueType::Int8)),
        ) => {}
        (
            AggregateFunctionV1::SumInt8,
            CallDelta::Sum { non_null, value },
            Some(TypedValue::Int8(input)),
        ) => {
            *non_null = non_null.checked_add(delta).ok_or(KernelError::Overflow)?;
            let contribution = i128::from(*input) * i128::from(delta);
            *value = value
                .checked_add(contribution)
                .ok_or(KernelError::Overflow)?;
        }
        _ => return Err(KernelError::WrongType),
    }
    Ok(())
}

pub(crate) fn apply_delta(state: &mut CallState, delta: CallDelta) -> Result<(), KernelError> {
    match (state, delta) {
        (CallState::Count(value), CallDelta::Count(change)) => {
            *value = checked_nonnegative(*value, change)?;
        }
        (
            CallState::Sum { non_null, value },
            CallDelta::Sum {
                non_null: count_change,
                value: value_change,
            },
        ) => {
            *non_null = checked_nonnegative(*non_null, count_change)?;
            let next = i128::from(*value)
                .checked_add(value_change)
                .ok_or(KernelError::Overflow)?;
            *value = i64::try_from(next).map_err(|_| KernelError::Overflow)?;
            if *non_null == 0 {
                *value = 0;
            }
        }
        _ => return Err(KernelError::InvalidState),
    }
    Ok(())
}

fn read_i64(payload: &[u8]) -> Result<i64, KernelError> {
    payload
        .try_into()
        .map(i64::from_be_bytes)
        .map_err(|_| KernelError::InvalidState)
}

fn validate(state: CallState) -> Result<(), KernelError> {
    match state {
        CallState::Count(value) if value < 0 => Err(KernelError::InvalidState),
        CallState::Sum { non_null, .. } if non_null < 0 => Err(KernelError::InvalidState),
        CallState::Sum { non_null: 0, value } if value != 0 => Err(KernelError::InvalidState),
        _ => Ok(()),
    }
}

fn checked_nonnegative(value: i64, delta: i64) -> Result<i64, KernelError> {
    let next = value.checked_add(delta).ok_or(KernelError::Overflow)?;
    if next < 0 {
        Err(KernelError::Underflow)
    } else {
        Ok(next)
    }
}
