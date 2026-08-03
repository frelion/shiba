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
            .filter(|node| {
                matches!(
                    node.kind,
                    OperatorNodeKind::CountRows | OperatorNodeKind::SumInt8 { .. }
                )
            })
            .map(|node| state_key(node.node_id))
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
        let value = match node.kind {
            OperatorNodeKind::CountRows => apply_count(decode(snapshot, node.node_id)?, batch)?,
            OperatorNodeKind::SumInt8 { input_slot } => {
                apply_sum(decode(snapshot, node.node_id)?, batch, input_slot)?
            }
            _ => continue,
        };
        state_deltas.push(StateDelta {
            key: state_key(node.node_id),
            mutation: StateMutation::Upsert {
                state: encode(value),
            },
        });
        let terminal = graph
            .nodes
            .iter()
            .find(|candidate| candidate.input == NodeInput::Node(node.node_id))
            .ok_or(KernelError::InvalidGraph)?;
        if !matches!(
            terminal.kind,
            OperatorNodeKind::Materialize {
                output: crate::OutputContract::Scalar {
                    value_type: ValueType::Int8
                },
                ..
            }
        ) {
            return Err(KernelError::OutputContractMismatch);
        }
        results.push(ResultDelta::Scalar {
            node_id: terminal.node_id,
            value: TypedValue::Int8(value),
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

fn decode(snapshot: &StateSnapshot, node_id: NodeId) -> Result<i64, KernelError> {
    let entry = snapshot
        .entries
        .iter()
        .find(|entry| entry.key == state_key(node_id))
        .ok_or(KernelError::InvalidState)?;
    decode_entry(entry)
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

fn encode(value: i64) -> EncodedOperatorState {
    EncodedOperatorState {
        codec_version: 1,
        payload: value.to_be_bytes().to_vec(),
    }
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

fn apply_sum(mut value: i64, batch: &DeltaBatch, input_slot: u16) -> Result<i64, KernelError> {
    for delta in &batch.rows {
        if let Some(before) = &delta.before {
            value = value
                .checked_sub(contribution(before, input_slot)?)
                .ok_or(KernelError::Overflow)?;
        }
        if let Some(after) = &delta.after {
            value = value
                .checked_add(contribution(after, input_slot)?)
                .ok_or(KernelError::Overflow)?;
        }
    }
    Ok(value)
}

fn contribution(row: &TypedRow, slot: u16) -> Result<i64, KernelError> {
    match row.values.get(usize::from(slot)) {
        Some(TypedValue::Int8(value)) => Ok(*value),
        Some(TypedValue::Null(ValueType::Int8)) => Ok(0),
        Some(TypedValue::Absent) | None => Err(KernelError::AbsentInput),
        _ => Err(KernelError::WrongType),
    }
}
