use crate::{EncodedOperatorState, KernelError, StateEntry, TypedRow, TypedValue, ValueType};

#[derive(Clone, Copy)]
pub(crate) enum Aggregate {
    Count,
    Sum { value_slot: u16 },
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) struct GroupState {
    pub(crate) count: i64,
    pub(crate) non_null_count: i64,
    pub(crate) sum: i64,
}

pub(crate) fn decode(aggregate: Aggregate, entry: &StateEntry) -> Result<GroupState, KernelError> {
    let Some(state) = &entry.state else {
        return Ok(GroupState {
            count: 0,
            non_null_count: 0,
            sum: 0,
        });
    };
    if state.codec_version != 1 {
        return Err(KernelError::InvalidState);
    }
    let decoded = match aggregate {
        Aggregate::Count => GroupState {
            count: i64::from_be_bytes(
                state
                    .payload
                    .as_slice()
                    .try_into()
                    .map_err(|_| KernelError::InvalidState)?,
            ),
            non_null_count: 0,
            sum: 0,
        },
        Aggregate::Sum { .. } if state.payload.len() == 24 => GroupState {
            count: i64::from_be_bytes(state.payload[..8].try_into().expect("length checked")),
            non_null_count: i64::from_be_bytes(
                state.payload[8..16].try_into().expect("length checked"),
            ),
            sum: i64::from_be_bytes(state.payload[16..].try_into().expect("length checked")),
        },
        Aggregate::Sum { .. } => return Err(KernelError::InvalidState),
    };
    if decoded.count <= 0 || decoded.non_null_count < 0 || decoded.non_null_count > decoded.count {
        return Err(KernelError::InvalidState);
    }
    Ok(decoded)
}

pub(crate) fn encode(aggregate: Aggregate, state: GroupState) -> EncodedOperatorState {
    let mut payload = state.count.to_be_bytes().to_vec();
    if matches!(aggregate, Aggregate::Sum { .. }) {
        payload.extend_from_slice(&state.non_null_count.to_be_bytes());
        payload.extend_from_slice(&state.sum.to_be_bytes());
    }
    EncodedOperatorState {
        codec_version: 1,
        payload,
    }
}

pub(crate) fn contribution(row: &TypedRow, slot: u16) -> Result<Option<i64>, KernelError> {
    match row.values.get(usize::from(slot)) {
        Some(TypedValue::Int8(value)) => Ok(Some(*value)),
        Some(TypedValue::Null(ValueType::Int8)) => Ok(None),
        Some(TypedValue::Absent) | None => Err(KernelError::AbsentInput),
        _ => Err(KernelError::WrongType),
    }
}
