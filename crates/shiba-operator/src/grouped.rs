use std::collections::BTreeMap;

use crate::grouped_plan::{GroupSpec, prepare};
use crate::grouped_state::{Aggregate, GroupState};
use crate::{
    CompiledPlan, DeltaBatch, EncodedOperatorState, KernelError, KeyedMutation, OperatorTransition,
    OutputDelta, PlanImplementation, StateDelta, StateKey, StateMutation, StateReadSet,
    StateSnapshot, TypedRow, TypedValue, ValueType,
};

const GROUP_NAMESPACE: u16 = 1;

pub(crate) fn state_read_set(
    plan: &CompiledPlan,
    batch: &DeltaBatch,
) -> Result<StateReadSet, KernelError> {
    plan.validate()?;
    let PlanImplementation::Graph { graph } = &plan.implementation else {
        return StateReadSet::canonical(Vec::new()).map_err(|_| KernelError::InvalidState);
    };
    let Some((spec, grouped_batch, _, _)) = prepare(graph, batch)? else {
        return StateReadSet::canonical(Vec::new()).map_err(|_| KernelError::InvalidState);
    };
    read_set_for(&spec, &grouped_batch)
}

fn read_set_for(spec: &GroupSpec, batch: &DeltaBatch) -> Result<StateReadSet, KernelError> {
    let mut keys = Vec::with_capacity(batch.rows.len().saturating_mul(2));
    for delta in &batch.rows {
        for row in [delta.before.as_ref(), delta.after.as_ref()]
            .into_iter()
            .flatten()
        {
            keys.push(state_key(spec, row)?);
        }
    }
    StateReadSet::canonical(keys).map_err(|_| KernelError::InvalidState)
}

pub(crate) fn apply(
    plan: &CompiledPlan,
    scalar_state: &EncodedOperatorState,
    snapshot: &StateSnapshot,
    batch: &DeltaBatch,
) -> Result<Option<OperatorTransition>, KernelError> {
    let PlanImplementation::Graph { graph } = &plan.implementation else {
        return Ok(None);
    };
    let Some((spec, grouped_batch, mut budget, mut emitted_rows)) = prepare(graph, batch)? else {
        return Ok(None);
    };
    if scalar_state.codec_version != crate::plan::STATE_CODEC_VERSION
        || !scalar_state.payload.is_empty()
    {
        return Err(KernelError::InvalidState);
    }
    let read_set = read_set_for(&spec, &grouped_batch)?;
    snapshot
        .validate_exact(&read_set)
        .map_err(|_| KernelError::InvalidState)?;
    let mut groups = BTreeMap::new();
    for entry in &snapshot.entries {
        groups.insert(
            entry.key.clone(),
            crate::grouped_state::decode(spec.aggregate, entry)?,
        );
    }
    let initial = groups.clone();
    for delta in &grouped_batch.rows {
        if let Some(before) = &delta.before {
            mutate(&spec, &mut groups, before, false)?;
        }
        if let Some(after) = &delta.after {
            mutate(&spec, &mut groups, after, true)?;
        }
    }
    let mut state_deltas = Vec::with_capacity(groups.len());
    let mut result_rows = Vec::with_capacity(groups.len());
    for (key, state) in groups {
        let old = *initial.get(&key).ok_or(KernelError::InvalidState)?;
        if old == state {
            continue;
        }
        let before = group_row(&spec, &key.partition_key, old)?;
        let after = group_row(&spec, &key.partition_key, state)?;
        if state.count == 0 {
            state_deltas.push(StateDelta {
                key: key.clone(),
                mutation: StateMutation::Delete,
            });
        } else {
            state_deltas.push(StateDelta {
                key: key.clone(),
                mutation: StateMutation::Upsert {
                    state: crate::grouped_state::encode(spec.aggregate, state),
                },
            });
        }
        result_rows.push(crate::RowDelta { before, after });
    }
    let aggregate_batch = DeltaBatch {
        origin: batch.origin,
        layout_identity: spec.aggregate_layout.identity,
        rows: result_rows,
    };
    budget
        .charge(&aggregate_batch, &mut emitted_rows)
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
    let crate::ResultDelta::Keyed { mutations, .. } = result;
    let mutations = mutations
        .into_iter()
        .map(|mutation| match mutation {
            crate::ResultMutation::Delete { key } => KeyedMutation::Delete { key },
            crate::ResultMutation::Upsert { key, value } => KeyedMutation::Upsert { key, value },
        })
        .collect();
    Ok(Some(OperatorTransition {
        next_state: scalar_state.clone(),
        state_deltas,
        output_delta: OutputDelta::KeyedMutations { mutations },
    }))
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
    let key = StateKey {
        node_id: spec.node_id,
        namespace: GROUP_NAMESPACE,
        partition_key,
        item_key: None,
    };
    key.validate().map_err(|_| KernelError::InvalidState)?;
    Ok(key)
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
    let key = state_key(spec, row)?;
    let state = groups.get_mut(&key).ok_or(KernelError::InvalidState)?;
    if add {
        state.count = state.count.checked_add(1).ok_or(KernelError::Overflow)?;
    } else {
        state.count = state.count.checked_sub(1).ok_or(KernelError::Underflow)?;
    }
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
        if state.non_null_count < 0 || state.non_null_count > state.count {
            return Err(KernelError::Underflow);
        }
        state.sum = if add {
            state.sum.checked_add(value)
        } else {
            state.sum.checked_sub(value)
        }
        .ok_or(KernelError::Overflow)?;
    }
    Ok(())
}
