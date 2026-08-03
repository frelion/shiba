use crate::join_plan::{JoinSpec, join_spec};
use crate::join_transition::{
    affected_ids, apply_left, apply_right, decode_snapshot, index_left, joined_row, state_deltas,
};
use crate::{
    GraphTransition, KernelError, MultiInputBatch, OperatorGraph, StateKey, StatePartition,
    StateReadSet, StateSnapshot, TypedRow, TypedValue, ValueType,
};

pub(crate) const LEFT_NAMESPACE: u16 = 20;
pub(crate) const RIGHT_NAMESPACE: u16 = 21;
const MAX_JOIN_MUTATIONS: usize = 20_000;

pub(crate) fn state_read_set(
    graph: &OperatorGraph,
    batch: &MultiInputBatch,
) -> Result<Option<StateReadSet>, KernelError> {
    let Some(spec) = join_spec(graph)? else {
        return Ok(None);
    };
    let (left, right) = validate_batch(graph, &spec, batch)?;
    let mut keys = Vec::new();
    let mut partitions = Vec::new();
    for row in rows(left) {
        let id = int8(row, spec.left_id_slot)?;
        if let Some(key) = nullable_int8(row, spec.left_key_slot)? {
            keys.push(left_key(&spec, key.clone(), id));
            keys.push(right_key(&spec, key.clone()));
        }
    }
    for row in rows(right) {
        let key = TypedValue::Int8(int8(row, spec.right_id_slot)?);
        keys.push(right_key(&spec, key.clone()));
        partitions.push(StatePartition {
            node_id: spec.node_id,
            namespace: LEFT_NAMESPACE,
            partition_key: key,
        });
    }
    StateReadSet::with_partitions(keys, partitions)
        .map(Some)
        .map_err(|_| KernelError::InvalidState)
}

pub(crate) fn apply(
    graph: &OperatorGraph,
    snapshot: &StateSnapshot,
    batch: &MultiInputBatch,
) -> Result<Option<GraphTransition>, KernelError> {
    let Some(spec) = join_spec(graph)? else {
        return Ok(None);
    };
    let read_set = state_read_set(graph, batch)?.ok_or(KernelError::InvalidGraph)?;
    snapshot
        .validate_exact(&read_set)
        .map_err(|_| KernelError::InvalidState)?;
    let (left_batch, right_batch) = validate_batch(graph, &spec, batch)?;
    let (pre_left, pre_right) = decode_snapshot(&spec, snapshot)?;
    let mut post_left = pre_left.clone();
    let mut post_right = pre_right.clone();
    apply_left(&spec, left_batch, &mut post_left)?;
    apply_right(&spec, right_batch, &mut post_right)?;
    let affected = affected_ids(&spec, left_batch, right_batch, &pre_left, &post_left)?;
    let pre_left_by_id = index_left(&pre_left)?;
    let post_left_by_id = index_left(&post_left)?;
    let mut result_rows = Vec::new();
    for id in affected {
        let before = joined_row(&spec, id, &pre_left_by_id, &pre_right)?;
        let after = joined_row(&spec, id, &post_left_by_id, &post_right)?;
        if before != after {
            result_rows.push(crate::RowDelta { before, after });
            if result_rows.len() > MAX_JOIN_MUTATIONS {
                return Err(KernelError::OutputContractMismatch);
            }
        }
    }
    let state_deltas = state_deltas(&spec, &pre_left, &post_left, &pre_right, &post_right)?;
    let result = crate::materialize::materialize(
        spec.materialize_id,
        &crate::DeltaBatch {
            origin: batch.sources[0].delta.origin,
            layout_identity: spec.output_layout.identity,
            rows: result_rows,
        },
        &spec.output_layout,
        0,
        1,
        spec.key_nullable,
        spec.value_nullable,
    )
    .map_err(|_| KernelError::InvalidTransition)?;
    Ok(Some(GraphTransition {
        state_deltas,
        results: vec![result],
    }))
}

fn validate_batch<'a>(
    graph: &crate::OperatorGraph,
    spec: &JoinSpec,
    batch: &'a MultiInputBatch,
) -> Result<(&'a crate::DeltaBatch, &'a crate::DeltaBatch), KernelError> {
    if batch.sources.len() != 2
        || batch
            .sources
            .iter()
            .map(|source| source.source_id)
            .collect::<Vec<_>>()
            != graph
                .sources
                .iter()
                .map(|source| source.source_id)
                .collect::<Vec<_>>()
        || batch
            .sources
            .iter()
            .map(|source| source.delta.rows.len())
            .sum::<usize>()
            > 10_000
    {
        return Err(KernelError::InvalidGraph);
    }
    validate_origin(graph, batch)?;
    let left = batch
        .sources
        .iter()
        .find(|source| source.source_id == spec.left_source_id)
        .ok_or(KernelError::InvalidGraph)?;
    let right = batch
        .sources
        .iter()
        .find(|source| source.source_id == spec.right_source_id)
        .ok_or(KernelError::InvalidGraph)?;
    for source in &batch.sources {
        let port = graph
            .sources
            .iter()
            .find(|port| port.source_id == source.source_id)
            .ok_or(KernelError::InvalidGraph)?;
        let layout = crate::source_typed_layout(port.source_id, &port.layout)
            .map_err(|_| KernelError::InvalidGraph)?;
        if source.delta.layout_identity != layout.identity
            || source
                .delta
                .rows
                .iter()
                .flat_map(|delta| [delta.before.as_ref(), delta.after.as_ref()])
                .flatten()
                .any(|row| {
                    row.layout_identity != layout.identity
                        || row.values.len() != layout.value_types.len()
                })
        {
            return Err(KernelError::InvalidGraph);
        }
    }
    Ok((&left.delta, &right.delta))
}

fn validate_origin(
    graph: &crate::OperatorGraph,
    batch: &MultiInputBatch,
) -> Result<(), KernelError> {
    for source in &batch.sources {
        let valid = match (batch.origin, source.delta.origin) {
            (crate::GraphEffectOrigin::Wal(graph_tx), crate::EffectOrigin::Wal(source_tx)) => {
                graph_tx.graph_id == graph.graph_id
                    && source_tx.source_id == source.source_id
                    && graph_tx.slot_generation == source_tx.slot_generation
                    && graph_tx.commit_lsn == source_tx.commit_lsn
                    && graph_tx.ingress_transaction_id == source_tx.ingress_transaction_id
            }
            (
                crate::GraphEffectOrigin::Bootstrap(graph_batch),
                crate::EffectOrigin::Bootstrap(source_batch),
            ) => graph_batch == source_batch,
            _ => false,
        };
        if !valid {
            return Err(KernelError::InvalidGraph);
        }
    }
    Ok(())
}

pub(crate) fn rows(batch: &crate::DeltaBatch) -> impl Iterator<Item = &TypedRow> {
    batch
        .rows
        .iter()
        .flat_map(|delta| [delta.before.as_ref(), delta.after.as_ref()])
        .flatten()
}

pub(crate) fn int8(row: &TypedRow, slot: u16) -> Result<i64, KernelError> {
    match row.values.get(usize::from(slot)) {
        Some(TypedValue::Int8(value)) => Ok(*value),
        Some(TypedValue::Absent) | None => Err(KernelError::AbsentInput),
        _ => Err(KernelError::WrongType),
    }
}

pub(crate) fn nullable_int8(row: &TypedRow, slot: u16) -> Result<Option<TypedValue>, KernelError> {
    match row.values.get(usize::from(slot)) {
        Some(value @ TypedValue::Int8(_)) => Ok(Some(value.clone())),
        Some(TypedValue::Null(ValueType::Int8)) => Ok(None),
        Some(TypedValue::Absent) | None => Err(KernelError::AbsentInput),
        _ => Err(KernelError::WrongType),
    }
}

fn left_key(spec: &JoinSpec, partition_key: TypedValue, id: i64) -> StateKey {
    StateKey {
        node_id: spec.node_id,
        namespace: LEFT_NAMESPACE,
        partition_key,
        item_key: Some(TypedValue::Int8(id)),
    }
}

fn right_key(spec: &JoinSpec, partition_key: TypedValue) -> StateKey {
    StateKey {
        node_id: spec.node_id,
        namespace: RIGHT_NAMESPACE,
        partition_key,
        item_key: None,
    }
}
