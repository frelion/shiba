use std::collections::BTreeMap;

use crate::grouped_plan::{GroupSpec, prepare, specs};
use crate::grouped_state::{Aggregate, GroupState};
use crate::{
    DeltaBatch, GraphTransition, KernelError, StateDelta, StateKey, StateMutation, StateReadSet,
    StateSnapshot, TypedRow, TypedValue, ValueType,
};

const GROUP_NAMESPACE: u16 = 1;

pub(crate) fn state_read_set(
    graph: &crate::OperatorGraph,
    batch: &DeltaBatch,
) -> Result<StateReadSet, KernelError> {
    let mut keys = Vec::new();
    for spec in specs(graph)? {
        let (grouped, _, _) = prepare(graph, batch, &spec)?;
        keys.extend(keys_for(&spec, &grouped)?);
    }
    StateReadSet::canonical(keys).map_err(|_| KernelError::InvalidState)
}

pub(crate) fn apply(
    graph: &crate::OperatorGraph,
    snapshot: &StateSnapshot,
    batch: &DeltaBatch,
) -> Result<GraphTransition, KernelError> {
    let mut transition = GraphTransition {
        state_deltas: Vec::new(),
        results: Vec::new(),
    };
    for spec in specs(graph)? {
        let (grouped, mut budget, mut emitted) = prepare(graph, batch, &spec)?;
        let read_set = StateReadSet::canonical(keys_for(&spec, &grouped)?)
            .map_err(|_| KernelError::InvalidState)?;
        let entries = snapshot
            .entries
            .iter()
            .filter(|entry| read_set.keys.contains(&entry.key))
            .cloned()
            .collect();
        let local =
            StateSnapshot::new(&read_set, entries).map_err(|_| KernelError::InvalidState)?;
        let mut groups = BTreeMap::new();
        for entry in &local.entries {
            groups.insert(
                entry.key.clone(),
                crate::grouped_state::decode(spec.aggregate, entry)?,
            );
        }
        let initial = groups.clone();
        for delta in &grouped.rows {
            if let Some(before) = &delta.before {
                mutate(&spec, &mut groups, before, false)?;
            }
            if let Some(after) = &delta.after {
                mutate(&spec, &mut groups, after, true)?;
            }
        }
        let (state, rows) = finish_groups(&spec, &initial, groups)?;
        let aggregate_batch = DeltaBatch {
            origin: batch.origin,
            layout_identity: spec.aggregate_layout.identity,
            rows,
        };
        budget
            .charge(&aggregate_batch, &mut emitted)
            .map_err(|_| KernelError::InvalidTransition)?;
        let result = crate::materialize::materialize(
            spec.materialize_id,
            &aggregate_batch,
            &spec.aggregate_layout,
            spec.materialize_key_slot,
            spec.materialize_value_slot,
            spec.key_nullable,
            spec.value_nullable,
        )
        .map_err(|_| KernelError::InvalidTransition)?;
        transition.state_deltas.extend(state);
        transition.results.push(result);
    }
    Ok(transition)
}

fn keys_for(spec: &GroupSpec, batch: &DeltaBatch) -> Result<Vec<StateKey>, KernelError> {
    let mut keys = Vec::with_capacity(batch.rows.len().saturating_mul(2));
    for delta in &batch.rows {
        for row in [delta.before.as_ref(), delta.after.as_ref()]
            .into_iter()
            .flatten()
        {
            keys.push(state_key(spec, row)?);
        }
    }
    Ok(keys)
}

fn finish_groups(
    spec: &GroupSpec,
    initial: &BTreeMap<StateKey, GroupState>,
    groups: BTreeMap<StateKey, GroupState>,
) -> Result<(Vec<StateDelta>, Vec<crate::RowDelta>), KernelError> {
    let mut states = Vec::new();
    let mut rows = Vec::new();
    for (key, state) in groups {
        let old = *initial.get(&key).ok_or(KernelError::InvalidState)?;
        if old == state {
            continue;
        }
        let before = group_row(spec, &key.partition_key, old)?;
        let after = group_row(spec, &key.partition_key, state)?;
        states.push(StateDelta {
            key: key.clone(),
            mutation: if state.count == 0 {
                StateMutation::Delete
            } else {
                StateMutation::Upsert {
                    state: crate::grouped_state::encode(spec.aggregate, state),
                }
            },
        });
        rows.push(crate::RowDelta { before, after });
    }
    Ok((states, rows))
}

fn state_key(spec: &GroupSpec, row: &TypedRow) -> Result<StateKey, KernelError> {
    let partition_key = row
        .values
        .get(usize::from(spec.key_slot))
        .ok_or(KernelError::WrongType)?
        .clone();
    if matches!(partition_key, TypedValue::Absent) {
        return Err(KernelError::AbsentInput);
    }
    Ok(StateKey {
        node_id: spec.node_id,
        namespace: GROUP_NAMESPACE,
        partition_key,
        item_key: None,
    })
}

fn group_row(
    spec: &GroupSpec,
    key: &TypedValue,
    state: GroupState,
) -> Result<Option<TypedRow>, KernelError> {
    if state.count == 0 {
        return Ok(None);
    }
    let value = match spec.aggregate {
        Aggregate::Count => TypedValue::Int8(state.count),
        Aggregate::Sum { .. } if state.non_null_count == 0 => TypedValue::Null(ValueType::Int8),
        Aggregate::Sum { .. } => TypedValue::Int8(state.sum),
    };
    TypedRow::new(&spec.aggregate_layout, vec![key.clone(), value])
        .map(Some)
        .map_err(|_| KernelError::InvalidTransition)
}

fn mutate(
    spec: &GroupSpec,
    groups: &mut BTreeMap<StateKey, GroupState>,
    row: &TypedRow,
    add: bool,
) -> Result<(), KernelError> {
    let state = groups
        .get_mut(&state_key(spec, row)?)
        .ok_or(KernelError::InvalidState)?;
    state.count = if add {
        state.count.checked_add(1)
    } else {
        state.count.checked_sub(1)
    }
    .ok_or(KernelError::Overflow)?;
    if state.count < 0 {
        return Err(KernelError::Underflow);
    }
    if let Aggregate::Sum { value_slot } = spec.aggregate
        && let Some(value) = crate::grouped_state::contribution(row, value_slot)?
    {
        state.non_null_count = if add {
            state.non_null_count.checked_add(1)
        } else {
            state.non_null_count.checked_sub(1)
        }
        .ok_or(KernelError::Overflow)?;
        state.sum = if add {
            state.sum.checked_add(value)
        } else {
            state.sum.checked_sub(value)
        }
        .ok_or(KernelError::Overflow)?;
        if state.non_null_count < 0 || state.non_null_count > state.count {
            return Err(KernelError::Underflow);
        }
    }
    Ok(())
}
