use crate::aggregate_plan::AggregateSpec;
use crate::aggregate_state::{self, CallState};
use crate::{KernelError, StateDelta, StateKey, StateMutation, StateReadSet, TypedValue};

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
    for values in groups {
        keys.extend(group_keys(spec, &values));
    }
    StateReadSet::canonical(keys).map_err(|_| KernelError::InvalidState)
}

pub(crate) fn decode(
    spec: &AggregateSpec,
    snapshot: &crate::StateSnapshot,
    values: Vec<TypedValue>,
) -> Result<GroupState, KernelError> {
    let keys = group_keys(spec, &values);
    let entries = keys
        .iter()
        .map(|key| {
            snapshot
                .entries
                .iter()
                .find(|entry| &entry.key == key)
                .ok_or(KernelError::InvalidState)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let membership = decode_membership(entries[0])?;
    let calls = spec
        .calls
        .iter()
        .zip(&entries[1..])
        .map(|(call, entry)| aggregate_state::decode(call.function, entry))
        .collect::<Result<Vec<_>, _>>()?;
    for (call, state) in spec.calls.iter().zip(&calls) {
        if matches!(call.function, crate::AggregateFunctionV1::CountStar)
            && *state != CallState::Count(membership)
        {
            return Err(KernelError::InvalidState);
        }
    }
    if membership == 0 && entries[1..].iter().any(|entry| entry.state.is_some()) {
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
) -> Vec<StateDelta> {
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
        if old != new || group_created || group_deleted {
            deltas.push(delta(
                spec,
                key,
                call.ordinal,
                group_deleted,
                aggregate_state::encode(*new),
            ));
        }
    }
    deltas
}

fn group_keys(spec: &AggregateSpec, values: &[TypedValue]) -> Vec<StateKey> {
    let partition_key = values.first().cloned().unwrap_or(TypedValue::Bool(true));
    let mut keys = vec![state_key(spec, partition_key.clone(), MEMBERSHIP_NAMESPACE)];
    keys.extend(
        spec.calls
            .iter()
            .map(|call| state_key(spec, partition_key.clone(), call.ordinal)),
    );
    keys
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
