use crate::aggregate_plan::AggregateSpec;
use crate::aggregate_state::{self, CallState};
use crate::{
    KernelError, StateDelta, StateKey, StateMutation, StatePartition, StateReadSet, TypedValue,
};

pub(crate) const MEMBERSHIP_NAMESPACE: u16 = 0;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GroupState {
    pub values: Vec<TypedValue>,
    pub membership: i64,
    pub calls: Vec<CallState>,
}

pub(crate) fn read_set(
    spec: &AggregateSpec,
    groups: impl IntoIterator<Item = Vec<TypedValue>>,
) -> Result<StateReadSet, KernelError> {
    let mut keys = Vec::new();
    let mut partitions = Vec::new();
    for (index, values) in groups.into_iter().enumerate() {
        if index >= crate::MAX_TOUCHED_GROUPS {
            return Err(KernelError::InvalidTransition);
        }
        let partition_key = values.first().cloned().unwrap_or(TypedValue::Bool(true));
        if keys.len() >= crate::MAX_STATE_KEYS {
            return Err(KernelError::InvalidTransition);
        }
        keys.push(state_key(spec, partition_key.clone(), MEMBERSHIP_NAMESPACE));
        for call in &spec.calls {
            if is_extrema(call.function) {
                if partitions.len() >= crate::MAX_PARTITION_ENTRIES {
                    return Err(KernelError::InvalidTransition);
                }
                partitions.push(StatePartition {
                    node_id: spec.node_id,
                    namespace: call.ordinal,
                    partition_key: partition_key.clone(),
                });
            } else {
                if keys.len() >= crate::MAX_STATE_KEYS {
                    return Err(KernelError::InvalidTransition);
                }
                keys.push(state_key(spec, partition_key.clone(), call.ordinal));
            }
        }
    }
    StateReadSet::with_partitions(keys, partitions).map_err(|_| KernelError::InvalidState)
}

pub(crate) fn decode(
    spec: &AggregateSpec,
    snapshot: &crate::StateSnapshot,
    values: Vec<TypedValue>,
) -> Result<GroupState, KernelError> {
    let partition_key = values.first().cloned().unwrap_or(TypedValue::Bool(true));
    let membership = snapshot
        .entries
        .iter()
        .find(|entry| entry.key == state_key(spec, partition_key.clone(), MEMBERSHIP_NAMESPACE))
        .ok_or(KernelError::InvalidState)
        .and_then(decode_membership)?;
    let mut calls = Vec::with_capacity(spec.calls.len());
    for call in &spec.calls {
        if is_extrema(call.function) {
            calls.push(aggregate_state::decode_extrema(
                call.function,
                snapshot.entries.iter().filter(|entry| {
                    entry.key.node_id == spec.node_id
                        && entry.key.namespace == call.ordinal
                        && entry.key.partition_key == partition_key
                }),
            )?);
        } else {
            let key = state_key(spec, partition_key.clone(), call.ordinal);
            let entry = snapshot
                .entries
                .iter()
                .find(|entry| entry.key == key)
                .ok_or(KernelError::InvalidState)?;
            calls.push(aggregate_state::decode(call.function, entry)?);
        }
    }
    for (call, state) in spec.calls.iter().zip(&calls) {
        if matches!(call.function, crate::AggregateFunctionV1::CountStar)
            && *state != CallState::Count(membership)
        {
            return Err(KernelError::InvalidState);
        }
    }
    if membership == 0 && calls.iter().any(|state| !is_empty(state)) {
        return Err(KernelError::InvalidState);
    }
    Ok(GroupState {
        values,
        membership,
        calls,
    })
}

pub(crate) fn deltas(
    spec: &AggregateSpec,
    key: &TypedValue,
    before: &GroupState,
    after: &GroupState,
) -> Result<Vec<StateDelta>, KernelError> {
    let mut deltas = Vec::new();
    if before.membership != after.membership {
        deltas.push(delta(
            spec,
            key,
            MEMBERSHIP_NAMESPACE,
            after.membership == 0,
            crate::EncodedOperatorState {
                codec_version: crate::AGGREGATE_STATE_CODEC_VERSION,
                payload: after.membership.to_be_bytes().to_vec(),
            },
        ));
    }
    let group_created = before.membership == 0 && after.membership != 0;
    let group_deleted = before.membership != 0 && after.membership == 0;
    for ((call, old), new) in spec.calls.iter().zip(&before.calls).zip(&after.calls) {
        if is_extrema(call.function) {
            let ((CallState::Min(old_values), CallState::Min(new_values))
            | (CallState::Max(old_values), CallState::Max(new_values))) = (old, new)
            else {
                continue;
            };
            let mut candidates = std::collections::BTreeSet::new();
            candidates.extend(old_values.keys().copied());
            candidates.extend(new_values.keys().copied());
            if candidates.len() > crate::MAX_EXTREMA_VALUES {
                return Err(KernelError::InvalidTransition);
            }
            for candidate in candidates {
                let old_count = old_values.get(&candidate).copied();
                let new_count = new_values.get(&candidate).copied();
                if old_count == new_count {
                    continue;
                }
                let state = new_count.map_or_else(
                    || crate::EncodedOperatorState {
                        codec_version: crate::AGGREGATE_STATE_CODEC_VERSION,
                        payload: Vec::new(),
                    },
                    aggregate_state::encode_extreme_value,
                );
                deltas.push(StateDelta {
                    key: state_key_with_item(spec, key.clone(), call.ordinal, candidate),
                    mutation: if new_count.is_some() {
                        StateMutation::Upsert { state }
                    } else {
                        StateMutation::Delete
                    },
                });
            }
            continue;
        }
        if old != new || group_created || group_deleted {
            deltas.push(delta(
                spec,
                key,
                call.ordinal,
                group_deleted,
                aggregate_state::encode(new),
            ));
        }
    }
    if deltas.len() > crate::MAX_STATE_MUTATIONS {
        return Err(KernelError::InvalidTransition);
    }
    Ok(deltas)
}

pub(crate) fn state_key(
    spec: &AggregateSpec,
    partition_key: TypedValue,
    namespace: u16,
) -> StateKey {
    StateKey {
        node_id: spec.node_id,
        namespace,
        partition_key,
        item_key: None,
    }
}

fn state_key_with_item(
    spec: &AggregateSpec,
    partition_key: TypedValue,
    namespace: u16,
    item_key: i64,
) -> StateKey {
    StateKey {
        node_id: spec.node_id,
        namespace,
        partition_key,
        item_key: Some(TypedValue::Int8(item_key)),
    }
}

fn is_extrema(function: crate::AggregateFunctionV1) -> bool {
    matches!(
        function,
        crate::AggregateFunctionV1::MinInt8 | crate::AggregateFunctionV1::MaxInt8
    )
}

fn is_empty(state: &CallState) -> bool {
    match state {
        CallState::Count(value) => *value == 0,
        CallState::Sum { non_null, value } => *non_null == 0 && *value == 0,
        CallState::Min(values) | CallState::Max(values) => values.is_empty(),
    }
}

fn decode_membership(entry: &crate::StateEntry) -> Result<i64, KernelError> {
    let Some(state) = &entry.state else {
        return Ok(0);
    };
    if state.codec_version != crate::AGGREGATE_STATE_CODEC_VERSION {
        return Err(KernelError::InvalidState);
    }
    let value = i64::from_be_bytes(
        state
            .payload
            .as_slice()
            .try_into()
            .map_err(|_| KernelError::InvalidState)?,
    );
    if value < 0 {
        Err(KernelError::InvalidState)
    } else {
        Ok(value)
    }
}

fn delta(
    spec: &AggregateSpec,
    key: &TypedValue,
    namespace: u16,
    delete: bool,
    state: crate::EncodedOperatorState,
) -> StateDelta {
    StateDelta {
        key: state_key(spec, key.clone(), namespace),
        mutation: if delete {
            StateMutation::Delete
        } else {
            StateMutation::Upsert { state }
        },
    }
}
