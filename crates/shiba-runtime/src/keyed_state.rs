use std::collections::{BTreeMap, BTreeSet};

use postgres::{Row, Transaction};
use shiba_operator::{
    MultiInputBatch, OperatorGraph, StateDelta, StateKey, StateMutation, StatePartition,
    StateSnapshot, TypedValue, graph_state_read_set,
};

use crate::M2Error;

mod write;

use shiba_operator::MAX_STATE_KEYS;

type Coordinate = (i64, i32, Vec<u8>, Vec<u8>);
type PartitionCoordinate = (i64, i32, Vec<u8>);
type CoordinateArrays = (Vec<i64>, Vec<i32>, Vec<Vec<u8>>, Vec<Vec<u8>>);

pub(crate) struct LockedState {
    pub(crate) snapshot: StateSnapshot,
    present: BTreeSet<Coordinate>,
    exact: BTreeSet<Coordinate>,
    partitions: BTreeSet<PartitionCoordinate>,
}

pub(crate) fn load(
    transaction: &mut Transaction<'_>,
    graph_id: i64,
    graph: &OperatorGraph,
    batch: &MultiInputBatch,
) -> Result<LockedState, M2Error> {
    let read_set = graph_state_read_set(graph, batch)?;
    let exact = read_set
        .keys
        .iter()
        .map(coordinate)
        .collect::<Result<BTreeSet<_>, _>>()?;
    let partitions = read_set
        .partitions
        .iter()
        .map(partition_coordinate)
        .collect::<Result<BTreeSet<_>, _>>()?;
    let rows = query_rows(transaction, graph_id, &exact, &partitions)?;
    if rows.len() > MAX_STATE_KEYS {
        return Err(M2Error::TransactionLimitExceeded);
    }

    let requested_keys: BTreeMap<Coordinate, StateKey> = read_set
        .keys
        .iter()
        .cloned()
        .map(|key| Ok((coordinate(&key)?, key)))
        .collect::<Result<_, M2Error>>()?;
    let requested_partitions: BTreeMap<PartitionCoordinate, StatePartition> = read_set
        .partitions
        .iter()
        .cloned()
        .map(|partition| Ok((partition_coordinate(&partition)?, partition)))
        .collect::<Result<_, M2Error>>()?;
    let mut entries = Vec::with_capacity(rows.len().saturating_add(exact.len()));
    let mut present = BTreeSet::new();
    for row in rows {
        let coordinate = (row.get(0), row.get(1), row.get(2), row.get(3));
        let key = requested_keys.get(&coordinate).cloned().or_else(|| {
            let partition_coordinate = (coordinate.0, coordinate.1, coordinate.2.clone());
            requested_partitions
                .get(&partition_coordinate)
                .and_then(|partition| state_key(partition, &coordinate.3).ok())
        });
        let key = key.ok_or(M2Error::InvalidOperatorDefinition)?;
        if !present.insert(coordinate) {
            return Err(M2Error::InvalidOperatorDefinition);
        }
        entries.push(shiba_operator::StateEntry {
            key,
            state: Some(shiba_operator::EncodedOperatorState {
                codec_version: u32::try_from(row.get::<_, i32>(4))
                    .map_err(|_| M2Error::InvalidOperatorDefinition)?,
                payload: row.get(5),
            }),
        });
    }
    for (coordinate, key) in requested_keys {
        if !present.contains(&coordinate) {
            entries.push(shiba_operator::StateEntry { key, state: None });
        }
    }
    entries.sort_by(|left, right| left.key.cmp(&right.key));
    let snapshot =
        StateSnapshot::new(&read_set, entries).map_err(|_| M2Error::InvalidOperatorDefinition)?;
    Ok(LockedState {
        snapshot,
        present,
        exact,
        partitions,
    })
}

fn query_rows(
    transaction: &mut Transaction<'_>,
    graph_id: i64,
    exact: &BTreeSet<Coordinate>,
    partitions: &BTreeSet<PartitionCoordinate>,
) -> Result<Vec<Row>, M2Error> {
    let (exact_nodes, exact_namespaces, exact_partitions, exact_items) =
        coordinate_arrays(&exact.iter().cloned().collect::<Vec<_>>());
    let partition_nodes: Vec<i64> = partitions.iter().map(|value| value.0).collect();
    let partition_namespaces: Vec<i32> = partitions.iter().map(|value| value.1).collect();
    let partition_payloads: Vec<Vec<u8>> = partitions.iter().map(|value| value.2.clone()).collect();
    let limit =
        i64::try_from(MAX_STATE_KEYS + 1).map_err(|_| M2Error::InvalidOperatorDefinition)?;
    Ok(transaction.query(
        "WITH exact AS (
             SELECT * FROM unnest($2::bigint[], $3::integer[], $4::bytea[], $5::bytea[])
               AS value(node_id, namespace, partition_key_payload, item_key_payload)
         ), partitions AS (
             SELECT * FROM unnest($6::bigint[], $7::integer[], $8::bytea[])
               AS value(node_id, namespace, partition_key_payload)
         )
         SELECT state.node_id, state.namespace, state.partition_key_payload,
                state.item_key_payload, state.codec_version, state.state_payload
         FROM shiba_internal.graph_node_state AS state
         WHERE state.graph_id = $1 AND (
             EXISTS (SELECT 1 FROM exact WHERE exact.node_id = state.node_id
                 AND exact.namespace = state.namespace
                 AND exact.partition_key_payload = state.partition_key_payload
                 AND exact.item_key_payload = state.item_key_payload)
             OR EXISTS (SELECT 1 FROM partitions
                 WHERE partitions.node_id = state.node_id
                   AND partitions.namespace = state.namespace
                   AND partitions.partition_key_payload = state.partition_key_payload))
         ORDER BY state.node_id, state.namespace,
                  state.partition_key_payload, state.item_key_payload
         LIMIT $9 FOR UPDATE OF state",
        &[
            &graph_id,
            &exact_nodes,
            &exact_namespaces,
            &exact_partitions,
            &exact_items,
            &partition_nodes,
            &partition_namespaces,
            &partition_payloads,
            &limit,
        ],
    )?)
}

pub(crate) fn persist(
    transaction: &mut Transaction<'_>,
    graph_id: i64,
    locked: &LockedState,
    deltas: Vec<StateDelta>,
) -> Result<(), M2Error> {
    if deltas.len() > MAX_STATE_KEYS {
        return Err(M2Error::TransactionLimitExceeded);
    }
    let mut ordered = deltas
        .into_iter()
        .map(|delta| Ok((coordinate(&delta.key)?, delta)))
        .collect::<Result<Vec<_>, M2Error>>()?;
    ordered.sort_by(|left, right| left.0.cmp(&right.0));
    if ordered.windows(2).any(|pair| pair[0].0 == pair[1].0)
        || ordered.iter().any(|(coordinate, _)| {
            !locked.exact.contains(coordinate)
                && !locked
                    .partitions
                    .contains(&(coordinate.0, coordinate.1, coordinate.2.clone()))
        })
    {
        return Err(M2Error::InvalidOperatorDefinition);
    }
    let mut deletes = Vec::new();
    let mut upserts = Vec::new();
    for (coordinate, delta) in ordered {
        match delta.mutation {
            StateMutation::Delete => {
                if !locked.present.contains(&coordinate) {
                    return Err(M2Error::InvalidOperatorDefinition);
                }
                deletes.push(coordinate);
            }
            StateMutation::Upsert { state } => upserts.push((coordinate, state)),
        }
    }
    write::delete_states(transaction, graph_id, &deletes)?;
    write::upsert_states(transaction, graph_id, &upserts)
}

fn state_key(partition: &StatePartition, item: &[u8]) -> Result<StateKey, M2Error> {
    let item_key = if item == b"null" {
        None
    } else {
        Some(
            TypedValue::from_canonical_json(item)
                .map_err(|_| M2Error::InvalidOperatorDefinition)?,
        )
    };
    Ok(StateKey {
        node_id: partition.node_id,
        namespace: partition.namespace,
        partition_key: partition.partition_key.clone(),
        item_key,
    })
}

fn coordinate(key: &StateKey) -> Result<Coordinate, M2Error> {
    Ok((
        i64::from(key.node_id.get()),
        i32::from(key.namespace),
        canonical(&key.partition_key)?,
        key.item_key
            .as_ref()
            .map_or_else(|| Ok(b"null".to_vec()), canonical)?,
    ))
}

fn partition_coordinate(partition: &StatePartition) -> Result<PartitionCoordinate, M2Error> {
    Ok((
        i64::from(partition.node_id.get()),
        i32::from(partition.namespace),
        canonical(&partition.partition_key)?,
    ))
}

fn canonical(value: &TypedValue) -> Result<Vec<u8>, M2Error> {
    value
        .to_canonical_json()
        .map_err(|_| M2Error::InvalidOperatorDefinition)
}

fn coordinate_arrays(states: &[Coordinate]) -> CoordinateArrays {
    (
        states.iter().map(|state| state.0).collect(),
        states.iter().map(|state| state.1).collect(),
        states.iter().map(|state| state.2.clone()).collect(),
        states.iter().map(|state| state.3.clone()).collect(),
    )
}
