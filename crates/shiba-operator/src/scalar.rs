use crate::{
    DeltaBatch, EncodedOperatorState, KernelError, NodeId, NodeInput, OperatorGraph,
    OperatorNodeKind, ResultDelta, StateDelta, StateEntry, StateKey, StateMutation, StateReadSet,
    StateSnapshot, TypedRow, TypedValue, ValueType,
};

const SCALAR_NAMESPACE: u16 = 0;

pub(crate) fn state_read_set(graph: &OperatorGraph) -> Result<StateReadSet, KernelError> {
    StateReadSet::canonical(
        graph
            .nodes
            .iter()
            .flat_map(|node| match node.kind {
                OperatorNodeKind::CountRows => vec![state_key(node.node_id)],
                OperatorNodeKind::SumInt8 { .. } => {
                    vec![state_key(node.node_id), non_null_key(node.node_id)]
                }
                _ => Vec::new(),
            })
            .collect(),
    )
    .map_err(|_| KernelError::InvalidState)
}

pub(crate) fn apply(
    graph: &OperatorGraph,
    snapshot: &StateSnapshot,
    batch: &DeltaBatch,
) -> Result<(Vec<StateDelta>, Vec<ResultDelta>), KernelError> {
    let mut state_deltas = Vec::new();
    let mut results = Vec::new();
    for node in &graph.nodes {
        let (states, value) = match node.kind {
            OperatorNodeKind::CountRows => {
                let value = apply_count(decode_count(snapshot, node.node_id)?, batch)?;
                (
                    vec![StateDelta {
                        key: state_key(node.node_id),
                        mutation: StateMutation::Upsert {
                            state: encode_int8(value),
                        },
                    }],
                    TypedValue::Int8(value),
                )
            }
            OperatorNodeKind::SumInt8 { input_slot } => {
                let value = apply_sum(decode_sum(snapshot, node.node_id)?, batch, input_slot)?;
                (
                    vec![
                        StateDelta {
                            key: state_key(node.node_id),
                            mutation: StateMutation::Upsert {
                                state: encode_int8(value.sum),
                            },
                        },
                        StateDelta {
                            key: non_null_key(node.node_id),
                            mutation: StateMutation::Upsert {
                                state: encode_int8(value.non_null_count),
                            },
                        },
                    ],
                    sum_result(value, graph, node.node_id)?,
                )
            }
            _ => continue,
        };
        state_deltas.extend(states);
        let terminal = graph
            .nodes
            .iter()
            .find(|candidate| candidate.input == NodeInput::Node(node.node_id))
            .ok_or(KernelError::InvalidGraph)?;
        if !matches!(
            terminal.kind,
            OperatorNodeKind::Materialize {
                output: crate::OutputContract::Scalar {
                    value_type: ValueType::Int8,
                    ..
                },
                ..
            }
        ) {
            return Err(KernelError::OutputContractMismatch);
        }
        results.push(ResultDelta::Scalar {
            node_id: terminal.node_id,
            value,
        });
    }
    Ok((state_deltas, results))
}

fn state_key(node_id: NodeId) -> StateKey {
    StateKey {
        node_id,
        namespace: SCALAR_NAMESPACE,
        partition_key: TypedValue::Bool(true),
        item_key: None,
    }
}

fn non_null_key(node_id: NodeId) -> StateKey {
    StateKey {
        node_id,
        namespace: SCALAR_NAMESPACE,
        partition_key: TypedValue::Bool(false),
        item_key: None,
    }
}

fn entry(snapshot: &StateSnapshot, node_id: NodeId) -> Result<&StateEntry, KernelError> {
    let entry = snapshot
        .entries
        .iter()
        .find(|entry| entry.key == state_key(node_id))
        .ok_or(KernelError::InvalidState)?;
    Ok(entry)
}

fn decode_count(snapshot: &StateSnapshot, node_id: NodeId) -> Result<i64, KernelError> {
    let entry = entry(snapshot, node_id)?;
    let Some(state) = &entry.state else {
        return Ok(0);
    };
    if state.codec_version != 1 {
        return Err(KernelError::InvalidState);
    }
    state
        .payload
        .as_slice()
        .try_into()
        .map(i64::from_be_bytes)
        .map_err(|_| KernelError::InvalidState)
}

fn encode_int8(value: i64) -> EncodedOperatorState {
    EncodedOperatorState {
        codec_version: 1,
        payload: value.to_be_bytes().to_vec(),
    }
}

#[derive(Clone, Copy)]
struct SumState {
    non_null_count: i64,
    sum: i64,
}

fn decode_sum(snapshot: &StateSnapshot, node_id: NodeId) -> Result<SumState, KernelError> {
    let sum = decode_count(snapshot, node_id)?;
    let count_entry = snapshot
        .entries
        .iter()
        .find(|entry| entry.key == non_null_key(node_id))
        .ok_or(KernelError::InvalidState)?;
    let non_null_count = decode_entry(count_entry)?;
    let decoded = SumState {
        non_null_count,
        sum,
    };
    if decoded.non_null_count < 0 {
        return Err(KernelError::InvalidState);
    }
    Ok(decoded)
}

fn decode_entry(entry: &StateEntry) -> Result<i64, KernelError> {
    let Some(state) = &entry.state else {
        return Ok(0);
    };
    if state.codec_version != 1 {
        return Err(KernelError::InvalidState);
    }
    state
        .payload
        .as_slice()
        .try_into()
        .map(i64::from_be_bytes)
        .map_err(|_| KernelError::InvalidState)
}

fn apply_count(mut value: i64, batch: &DeltaBatch) -> Result<i64, KernelError> {
    if value < 0 {
        return Err(KernelError::NegativeCount);
    }
    for delta in &batch.rows {
        value = match (&delta.before, &delta.after) {
            (None, Some(_)) => value.checked_add(1).ok_or(KernelError::Overflow)?,
            (Some(_), None) => value.checked_sub(1).ok_or(KernelError::Underflow)?,
            _ => value,
        };
        if value < 0 {
            return Err(KernelError::Underflow);
        }
    }
    Ok(value)
}

fn apply_sum(
    mut state: SumState,
    batch: &DeltaBatch,
    input_slot: u16,
) -> Result<SumState, KernelError> {
    for delta in &batch.rows {
        if let Some(before) = &delta.before {
            mutate_sum(&mut state, contribution(before, input_slot)?, false)?;
        }
        if let Some(after) = &delta.after {
            mutate_sum(&mut state, contribution(after, input_slot)?, true)?;
        }
    }
    Ok(state)
}

fn contribution(row: &TypedRow, slot: u16) -> Result<Option<i64>, KernelError> {
    match row.values.get(usize::from(slot)) {
        Some(TypedValue::Int8(value)) => Ok(Some(*value)),
        Some(TypedValue::Null(ValueType::Int8)) => Ok(None),
        Some(TypedValue::Absent) | None => Err(KernelError::AbsentInput),
        _ => Err(KernelError::WrongType),
    }
}

fn mutate_sum(
    state: &mut SumState,
    contribution: Option<i64>,
    add: bool,
) -> Result<(), KernelError> {
    let Some(value) = contribution else {
        return Ok(());
    };
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
    if state.non_null_count < 0 {
        return Err(KernelError::Underflow);
    }
    Ok(())
}

fn sum_result(
    state: SumState,
    graph: &OperatorGraph,
    node_id: NodeId,
) -> Result<TypedValue, KernelError> {
    let terminal = graph
        .nodes
        .iter()
        .find(|candidate| candidate.input == NodeInput::Node(node_id))
        .ok_or(KernelError::InvalidGraph)?;
    let OperatorNodeKind::Materialize {
        output: crate::OutputContract::Scalar { nullable, .. },
        ..
    } = terminal.kind
    else {
        return Err(KernelError::OutputContractMismatch);
    };
    if state.non_null_count == 0 && nullable {
        Ok(TypedValue::Null(ValueType::Int8))
    } else {
        Ok(TypedValue::Int8(state.sum))
    }
}
