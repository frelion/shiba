use std::collections::BTreeMap;

use crate::aggregate_group::GroupState;
use crate::aggregate_plan::{AggregateSpec, prepare, specs};
use crate::aggregate_state;
use crate::{
    DeltaBatch, GraphTransition, KernelError, ResultDelta, ResultMutation, StateDelta,
    StateReadSet, StateSnapshot, TypedResultRowV1, TypedRow, TypedValue,
};

#[derive(Clone)]
struct GroupDelta {
    membership: i64,
    calls: Vec<aggregate_state::CallDelta>,
}

pub(crate) fn state_read_set(
    graph: &crate::OperatorGraph,
    batch: &DeltaBatch,
    budget: &mut crate::graph_budget::GraphBudget,
) -> Result<StateReadSet, KernelError> {
    let mut keys = Vec::new();
    let mut partitions = Vec::new();
    for spec in specs(graph)? {
        let prepared = prepare(graph, batch, &spec)?;
        crate::graph_budget::EvaluationBudget::check_batch(&prepared)
            .map_err(|_| KernelError::InvalidTransition)?;
        let prepared_bytes = crate::graph_budget::estimated_batch_bytes(&prepared)
            .map_err(|_| KernelError::InvalidTransition)?;
        budget
            .charge_work_bytes(prepared_bytes)
            .map_err(|_| KernelError::InvalidTransition)?;
        let touched = touched_groups(&spec, &prepared, budget)?;
        let local = crate::aggregate_group::read_set(&spec, touched.into_values(), budget)?;
        budget
            .charge_read_set_work(&local)
            .map_err(|_| KernelError::InvalidTransition)?;
        keys.extend(local.keys);
        partitions.extend(local.partitions);
    }
    StateReadSet::with_partitions(keys, partitions).map_err(|_| KernelError::InvalidState)
}

pub(crate) fn apply(
    graph: &crate::OperatorGraph,
    snapshot: &StateSnapshot,
    batch: &DeltaBatch,
    budget: &mut crate::graph_budget::GraphBudget,
) -> Result<GraphTransition, KernelError> {
    let mut transition = GraphTransition {
        state_deltas: Vec::new(),
        results: Vec::new(),
    };
    for spec in specs(graph)? {
        let prepared = prepare(graph, batch, &spec)?;
        let prepared_bytes = crate::graph_budget::estimated_batch_bytes(&prepared)
            .map_err(|_| KernelError::InvalidTransition)?;
        budget
            .charge_work_bytes(prepared_bytes)
            .map_err(|_| KernelError::InvalidTransition)?;
        let touched = touched_groups(&spec, &prepared, budget)?;
        let estimated_mutations = touched
            .len()
            .checked_mul(spec.calls.len().saturating_add(1))
            .ok_or(KernelError::InvalidTransition)?;
        if estimated_mutations > crate::MAX_STATE_MUTATIONS {
            return Err(KernelError::InvalidTransition);
        }
        let mut initial = BTreeMap::new();
        for (key, values) in &touched {
            initial.insert(
                key.clone(),
                crate::aggregate_group::decode(&spec, snapshot, values.clone())?,
            );
        }
        let mut normalized = touched
            .keys()
            .cloned()
            .map(|key| {
                (
                    key,
                    GroupDelta {
                        membership: 0,
                        calls: spec
                            .calls
                            .iter()
                            .map(|call| aggregate_state::empty_delta(call.function))
                            .collect(),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        for delta in &prepared.rows {
            if let Some(before) = &delta.before {
                accumulate(&spec, &mut normalized, before, -1)?;
            }
            if let Some(after) = &delta.after {
                accumulate(&spec, &mut normalized, after, 1)?;
            }
        }
        let mut final_state = initial.clone();
        for (key, delta) in normalized {
            apply_delta(
                final_state.get_mut(&key).ok_or(KernelError::InvalidState)?,
                delta,
            )?;
        }
        let (states, result) = finish(&spec, prepared.origin, &initial, &final_state, budget)?;
        transition.state_deltas.extend(states);
        transition.results.push(result);
    }
    Ok(transition)
}

fn touched_groups(
    spec: &AggregateSpec,
    batch: &DeltaBatch,
    budget: &mut crate::graph_budget::GraphBudget,
) -> Result<BTreeMap<TypedValue, Vec<TypedValue>>, KernelError> {
    let mut groups = BTreeMap::new();
    if spec.groups.is_empty() {
        budget
            .charge_touched_groups(1)
            .map_err(|_| KernelError::InvalidTransition)?;
        groups.insert(TypedValue::Bool(true), Vec::new());
    }
    for row in batch
        .rows
        .iter()
        .flat_map(|delta| delta.before.iter().chain(delta.after.iter()))
    {
        let (key, values) = group_identity(spec, row)?;
        if !groups.contains_key(&key) {
            budget
                .charge_touched_groups(1)
                .map_err(|_| KernelError::InvalidTransition)?;
        }
        groups.insert(key, values);
    }
    Ok(groups)
}

fn group_identity(
    spec: &AggregateSpec,
    row: &TypedRow,
) -> Result<(TypedValue, Vec<TypedValue>), KernelError> {
    if spec.groups.is_empty() {
        return Ok((TypedValue::Bool(true), Vec::new()));
    }
    let values = spec
        .groups
        .iter()
        .map(|expression| expression.evaluate(&spec.input_layout, row))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| KernelError::WrongType)?;
    if values
        .iter()
        .any(|value| matches!(value, TypedValue::Absent))
    {
        return Err(KernelError::AbsentInput);
    }
    Ok((values[0].clone(), values))
}

fn accumulate(
    spec: &AggregateSpec,
    groups: &mut BTreeMap<TypedValue, GroupDelta>,
    row: &TypedRow,
    delta: i64,
) -> Result<(), KernelError> {
    let (key, _) = group_identity(spec, row)?;
    let group = groups.get_mut(&key).ok_or(KernelError::InvalidState)?;
    group.membership = group
        .membership
        .checked_add(delta)
        .ok_or(KernelError::Overflow)?;
    for (call, state) in spec.calls.iter().zip(&mut group.calls) {
        let value = call
            .expression
            .as_ref()
            .map(|expression| expression.evaluate(&spec.input_layout, row))
            .transpose()
            .map_err(|_| KernelError::WrongType)?;
        aggregate_state::accumulate(state, call.function, value.as_ref(), delta)?;
    }
    Ok(())
}

fn apply_delta(group: &mut GroupState, delta: GroupDelta) -> Result<(), KernelError> {
    group.membership = group
        .membership
        .checked_add(delta.membership)
        .ok_or(KernelError::Overflow)?;
    if group.membership < 0 {
        return Err(KernelError::Underflow);
    }
    for (state, change) in group.calls.iter_mut().zip(delta.calls) {
        aggregate_state::apply_delta(state, change)?;
    }
    Ok(())
}

fn finish(
    spec: &AggregateSpec,
    origin: crate::EffectOrigin,
    initial: &BTreeMap<TypedValue, GroupState>,
    final_state: &BTreeMap<TypedValue, GroupState>,
    budget: &mut crate::graph_budget::GraphBudget,
) -> Result<(Vec<StateDelta>, ResultDelta), KernelError> {
    let mut states = Vec::new();
    let mut rows = Vec::new();
    for (key, after) in final_state {
        let before = initial.get(key).ok_or(KernelError::InvalidState)?;
        if before == after {
            continue;
        }
        states.extend(crate::aggregate_group::deltas(
            spec, key, before, after, budget,
        )?);
        rows.push(crate::RowDelta {
            before: output_row(spec, before)?,
            after: output_row(spec, after)?,
        });
    }
    if states.len() > crate::MAX_STATE_MUTATIONS {
        return Err(KernelError::InvalidTransition);
    }
    let result = if spec.groups.is_empty() {
        let group = final_state
            .values()
            .next()
            .ok_or(KernelError::InvalidState)?;
        let row = result_row(spec, group)?;
        ResultDelta {
            node_id: spec.materialize_id,
            mutations: vec![ResultMutation::ReplaceScalar { row }],
        }
    } else {
        let batch = DeltaBatch {
            origin,
            layout_identity: spec.output_layout.identity,
            rows,
        };
        crate::materialize::materialize_with_budget(
            spec.materialize_id,
            &batch,
            &spec.output_layout,
            &spec.field_slots,
            &spec.output,
            budget,
        )
        .map_err(|_| KernelError::InvalidTransition)?
    };
    Ok((states, result))
}

fn output_row(spec: &AggregateSpec, group: &GroupState) -> Result<Option<TypedRow>, KernelError> {
    if !spec.groups.is_empty() && group.membership == 0 {
        return Ok(None);
    }
    let mut values = group.values.clone();
    values.extend(group.calls.iter().map(aggregate_state::output));
    if let Some(having) = &spec.having {
        let call_values = group
            .calls
            .iter()
            .map(aggregate_state::output)
            .collect::<Vec<_>>();
        if !matches!(
            having.evaluate(&call_values),
            Ok(crate::TypedValue::Bool(true))
        ) {
            return Ok(None);
        }
    }
    TypedRow::new(&spec.output_layout, values)
        .map(Some)
        .map_err(|_| KernelError::InvalidTransition)
}

fn result_row(spec: &AggregateSpec, group: &GroupState) -> Result<TypedResultRowV1, KernelError> {
    let row = output_row(spec, group)?.ok_or(KernelError::InvalidTransition)?;
    let values = spec
        .field_slots
        .iter()
        .map(|slot| row.value(&spec.output_layout, *slot).cloned())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| KernelError::InvalidTransition)?;
    TypedResultRowV1::new(&spec.output.schema, values)
        .map_err(|_| KernelError::OutputContractMismatch)
}
