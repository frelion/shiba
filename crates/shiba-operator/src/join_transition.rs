use std::collections::{BTreeMap, BTreeSet};

use crate::join::{LEFT_NAMESPACE, RIGHT_NAMESPACE};
use crate::join_plan::JoinSpec;
use crate::{
    DeltaBatch, KernelError, StateDelta, StateKey, StateMutation, StateSnapshot, TypedRow,
    TypedValue, ValueType,
};

pub(crate) type LeftMap = BTreeSet<(TypedValue, i64)>;
pub(crate) type RightMap = BTreeMap<TypedValue, TypedValue>;
pub(crate) type LeftIndex = BTreeMap<i64, TypedValue>;

pub(crate) fn decode_snapshot(
    spec: &JoinSpec,
    snapshot: &StateSnapshot,
) -> Result<(LeftMap, RightMap), KernelError> {
    let mut left = LeftMap::new();
    let mut right = RightMap::new();
    for entry in &snapshot.entries {
        if entry.key.node_id != spec.node_id {
            return Err(KernelError::InvalidState);
        }
        match entry.key.namespace {
            LEFT_NAMESPACE => {
                let id = match &entry.key.item_key {
                    Some(TypedValue::Int8(id)) => *id,
                    _ => return Err(KernelError::InvalidState),
                };
                if let Some(state) = &entry.state {
                    crate::join_codec::decode_left(state)?;
                    if !left.insert((entry.key.partition_key.clone(), id)) {
                        return Err(KernelError::InvalidState);
                    }
                }
            }
            RIGHT_NAMESPACE => {
                if entry.key.item_key.is_some() {
                    return Err(KernelError::InvalidState);
                }
                if let Some(state) = &entry.state
                    && right
                        .insert(
                            entry.key.partition_key.clone(),
                            crate::join_codec::decode_right(state)?,
                        )
                        .is_some()
                {
                    return Err(KernelError::InvalidState);
                }
            }
            _ => return Err(KernelError::InvalidState),
        }
    }
    Ok((left, right))
}

pub(crate) fn index_left(state: &LeftMap) -> Result<LeftIndex, KernelError> {
    let mut by_id = LeftIndex::new();
    for (key, id) in state {
        if by_id.insert(*id, key.clone()).is_some() {
            return Err(KernelError::InvalidState);
        }
    }
    Ok(by_id)
}

pub(crate) fn apply_left(
    spec: &JoinSpec,
    batch: &DeltaBatch,
    state: &mut LeftMap,
) -> Result<(), KernelError> {
    for delta in &batch.rows {
        if let Some(before) = &delta.before
            && let Some(key) = left_coordinate(spec, before)?
            && !state.remove(&key)
        {
            return Err(KernelError::InvalidState);
        }
        if let Some(after) = &delta.after
            && let Some(key) = left_coordinate(spec, after)?
            && !state.insert(key)
        {
            return Err(KernelError::InvalidState);
        }
    }
    Ok(())
}

pub(crate) fn apply_right(
    spec: &JoinSpec,
    batch: &DeltaBatch,
    state: &mut RightMap,
) -> Result<(), KernelError> {
    for delta in &batch.rows {
        if let Some(before) = &delta.before {
            let (key, value) = right_coordinate(spec, before)?;
            if state.remove(&key) != Some(value) {
                return Err(KernelError::InvalidState);
            }
        }
        if let Some(after) = &delta.after {
            let (key, value) = right_coordinate(spec, after)?;
            if state.insert(key, value).is_some() {
                return Err(KernelError::InvalidState);
            }
        }
    }
    Ok(())
}

pub(crate) fn affected_ids(
    spec: &JoinSpec,
    left_batch: &DeltaBatch,
    right_batch: &DeltaBatch,
    pre: &LeftMap,
    post: &LeftMap,
) -> Result<BTreeSet<i64>, KernelError> {
    let mut ids = BTreeSet::new();
    for row in super::join::rows(left_batch) {
        ids.insert(super::join::int8(row, spec.left_id_slot)?);
    }
    let right_keys = super::join::rows(right_batch)
        .map(|row| super::join::int8(row, spec.right_id_slot).map(TypedValue::Int8))
        .collect::<Result<BTreeSet<_>, _>>()?;
    for (key, id) in pre.iter().chain(post) {
        if right_keys.contains(key) {
            ids.insert(*id);
        }
    }
    Ok(ids)
}

pub(crate) fn joined_row(
    spec: &JoinSpec,
    id: i64,
    left: &LeftIndex,
    right: &RightMap,
) -> Result<Option<TypedRow>, KernelError> {
    let Some(key) = left.get(&id) else {
        return Ok(None);
    };
    let Some(payload) = right.get(key) else {
        return Ok(None);
    };
    TypedRow::new(
        &spec.output_layout,
        vec![TypedValue::Int8(id), payload.clone()],
    )
    .map(Some)
    .map_err(|_| KernelError::InvalidTransition)
}

pub(crate) fn state_deltas(
    spec: &JoinSpec,
    pre_left: &LeftMap,
    post_left: &LeftMap,
    pre_right: &RightMap,
    post_right: &RightMap,
) -> Result<Vec<StateDelta>, KernelError> {
    let mut deltas = Vec::new();
    let left_keys = pre_left
        .iter()
        .chain(post_left)
        .cloned()
        .collect::<BTreeSet<_>>();
    for (partition_key, id) in left_keys {
        let before = pre_left.contains(&(partition_key.clone(), id));
        let after = post_left.contains(&(partition_key.clone(), id));
        if before != after {
            let key = StateKey {
                node_id: spec.node_id,
                namespace: LEFT_NAMESPACE,
                partition_key,
                item_key: Some(TypedValue::Int8(id)),
            };
            deltas.push(StateDelta {
                key,
                mutation: if after {
                    StateMutation::Upsert {
                        state: crate::join_codec::left_state(),
                    }
                } else {
                    StateMutation::Delete
                },
            });
        }
    }
    let right_keys = pre_right
        .keys()
        .chain(post_right.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    for partition_key in right_keys {
        let before = pre_right.get(&partition_key);
        let after = post_right.get(&partition_key);
        if before != after {
            let key = StateKey {
                node_id: spec.node_id,
                namespace: RIGHT_NAMESPACE,
                partition_key,
                item_key: None,
            };
            deltas.push(StateDelta {
                key,
                mutation: match after {
                    Some(value) => StateMutation::Upsert {
                        state: crate::join_codec::encode_right(value)?,
                    },
                    None => StateMutation::Delete,
                },
            });
        }
    }
    deltas.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(deltas)
}

fn left_coordinate(
    spec: &JoinSpec,
    row: &TypedRow,
) -> Result<Option<(TypedValue, i64)>, KernelError> {
    let id = super::join::int8(row, spec.left_id_slot)?;
    Ok(super::join::nullable_int8(row, spec.left_key_slot)?.map(|key| (key, id)))
}

fn right_coordinate(
    spec: &JoinSpec,
    row: &TypedRow,
) -> Result<(TypedValue, TypedValue), KernelError> {
    let key = TypedValue::Int8(super::join::int8(row, spec.right_id_slot)?);
    let value = match row.values.get(usize::from(spec.right_payload_slot)) {
        Some(value @ (TypedValue::Int8(_) | TypedValue::Null(ValueType::Int8))) => value.clone(),
        Some(TypedValue::Absent) | None => return Err(KernelError::AbsentInput),
        _ => return Err(KernelError::WrongType),
    };
    Ok((key, value))
}
