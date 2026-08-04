use std::collections::BTreeMap;

use crate::{
    AGGREGATE_STATE_CODEC_VERSION, AggregateFunctionV1, EncodedOperatorState, KernelError,
    StateEntry, TypedValue, ValueType,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CallState {
    Count(i64),
    Sum { non_null: i64, value: i64 },
    Min(BTreeMap<i64, i64>),
    Max(BTreeMap<i64, i64>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CallDelta {
    Count(i64),
    Sum { non_null: i64, value: i128 },
    Min(BTreeMap<i64, i64>),
    Max(BTreeMap<i64, i64>),
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
        AggregateFunctionV1::SumInt8
        | AggregateFunctionV1::MinInt8
        | AggregateFunctionV1::MaxInt8 => return Err(KernelError::InvalidState),
    };
    validate(&state)?;
    Ok(state)
}

pub(crate) const fn empty(function: AggregateFunctionV1) -> CallState {
    match function {
        AggregateFunctionV1::CountStar | AggregateFunctionV1::Count => CallState::Count(0),
        AggregateFunctionV1::SumInt8 => CallState::Sum {
            non_null: 0,
            value: 0,
        },
        AggregateFunctionV1::MinInt8 => CallState::Min(BTreeMap::new()),
        AggregateFunctionV1::MaxInt8 => CallState::Max(BTreeMap::new()),
    }
}

pub(crate) fn encode(state: &CallState) -> EncodedOperatorState {
    let payload = match state {
        CallState::Count(value) => value.to_be_bytes().to_vec(),
        CallState::Sum { non_null, value } => {
            let mut bytes = non_null.to_be_bytes().to_vec();
            bytes.extend_from_slice(&value.to_be_bytes());
            bytes
        }
        CallState::Min(_) | CallState::Max(_) => unreachable!("extrema use keyed state"),
    };
    EncodedOperatorState {
        codec_version: AGGREGATE_STATE_CODEC_VERSION,
        payload,
    }
}

pub(crate) fn output(state: &CallState) -> TypedValue {
    match state {
        CallState::Sum { non_null: 0, .. } => TypedValue::Null(ValueType::Int8),
        CallState::Count(value) | CallState::Sum { value, .. } => TypedValue::Int8(*value),
        CallState::Min(values) => values
            .first_key_value()
            .map_or(TypedValue::Null(ValueType::Int8), |(value, _)| {
                TypedValue::Int8(*value)
            }),
        CallState::Max(values) => values
            .last_key_value()
            .map_or(TypedValue::Null(ValueType::Int8), |(value, _)| {
                TypedValue::Int8(*value)
            }),
    }
}

pub(crate) const fn empty_delta(function: AggregateFunctionV1) -> CallDelta {
    match function {
        AggregateFunctionV1::CountStar | AggregateFunctionV1::Count => CallDelta::Count(0),
        AggregateFunctionV1::SumInt8 => CallDelta::Sum {
            non_null: 0,
            value: 0,
        },
        AggregateFunctionV1::MinInt8 => CallDelta::Min(BTreeMap::new()),
        AggregateFunctionV1::MaxInt8 => CallDelta::Max(BTreeMap::new()),
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
        (function, change, Some(TypedValue::Null(ValueType::Int8)))
            if accepts_null(function, &*change) => {}
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
        (AggregateFunctionV1::MinInt8, CallDelta::Min(values), Some(TypedValue::Int8(input)))
        | (AggregateFunctionV1::MaxInt8, CallDelta::Max(values), Some(TypedValue::Int8(input))) => {
            let current = values.get(input).copied().unwrap_or(0);
            let next = current.checked_add(delta).ok_or(KernelError::Overflow)?;
            if next == 0 {
                values.remove(input);
            } else {
                if !values.contains_key(input) && values.len() >= crate::MAX_EXTREMA_VALUES {
                    return Err(KernelError::InvalidTransition);
                }
                values.insert(*input, next);
            }
        }
        (
            AggregateFunctionV1::MinInt8 | AggregateFunctionV1::MaxInt8,
            CallDelta::Min(_) | CallDelta::Max(_),
            Some(TypedValue::Null(ValueType::Int8)),
        ) => {}
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
        (CallState::Min(values), CallDelta::Min(delta))
        | (CallState::Max(values), CallDelta::Max(delta)) => {
            for (candidate, change) in delta {
                let current = values.get(&candidate).copied().unwrap_or(0);
                let next = checked_nonnegative(current, change)?;
                if next == 0 {
                    values.remove(&candidate);
                } else {
                    values.insert(candidate, next);
                }
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

fn validate(state: &CallState) -> Result<(), KernelError> {
    match state {
        CallState::Count(value) if *value < 0 => Err(KernelError::InvalidState),
        CallState::Sum { non_null, .. } if *non_null < 0 => Err(KernelError::InvalidState),
        CallState::Sum { non_null: 0, value } if *value != 0 => Err(KernelError::InvalidState),
        CallState::Min(values) | CallState::Max(values)
            if values.values().any(|count| *count <= 0) =>
        {
            Err(KernelError::InvalidState)
        }
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

fn accepts_null(function: AggregateFunctionV1, change: &CallDelta) -> bool {
    matches!(
        (function, change),
        (AggregateFunctionV1::Count, CallDelta::Count(_))
            | (AggregateFunctionV1::SumInt8, CallDelta::Sum { .. })
            | (AggregateFunctionV1::MinInt8, CallDelta::Min(_))
            | (AggregateFunctionV1::MaxInt8, CallDelta::Max(_))
    )
}

pub(crate) fn decode_extrema<'a>(
    function: AggregateFunctionV1,
    entries: impl Iterator<Item = &'a crate::StateEntry>,
) -> Result<CallState, KernelError> {
    let mut values = BTreeMap::new();
    for entry in entries {
        if values.len() >= crate::MAX_EXTREMA_VALUES {
            return Err(KernelError::InvalidState);
        }
        let Some(TypedValue::Int8(candidate)) = entry.key.item_key.as_ref() else {
            return Err(KernelError::InvalidState);
        };
        let Some(state) = &entry.state else {
            return Err(KernelError::InvalidState);
        };
        if state.codec_version != AGGREGATE_STATE_CODEC_VERSION {
            return Err(KernelError::InvalidState);
        }
        let multiplicity = i64::from_be_bytes(
            state
                .payload
                .as_slice()
                .try_into()
                .map_err(|_| KernelError::InvalidState)?,
        );
        if multiplicity <= 0 || values.insert(*candidate, multiplicity).is_some() {
            return Err(KernelError::InvalidState);
        }
    }
    match function {
        AggregateFunctionV1::MinInt8 => Ok(CallState::Min(values)),
        AggregateFunctionV1::MaxInt8 => Ok(CallState::Max(values)),
        _ => Err(KernelError::InvalidState),
    }
}

pub(crate) fn encode_extreme_value(multiplicity: i64) -> EncodedOperatorState {
    EncodedOperatorState {
        codec_version: AGGREGATE_STATE_CODEC_VERSION,
        payload: multiplicity.to_be_bytes().to_vec(),
    }
}
