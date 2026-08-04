use std::collections::{BTreeMap, BTreeSet};

use postgres::{Row, Transaction};
use shiba_operator::{
    MAX_STATE_KEYS, MAX_STATE_PAYLOAD_BYTES, MultiInputBatch, OperatorGraph, StateDelta, StateKey,
    StateMutation, StatePartition, StateRange, StateRangeDirection, StateSnapshot, TypedValue,
    graph_state_read_set, state_item_order_key, validate_int8_order_key,
};

use crate::M2Error;

mod write;

type Coordinate = (i64, i32, Vec<u8>, Vec<u8>);
type PartitionCoordinate = (i64, i32, Vec<u8>);
type CoordinateArrays = (Vec<i64>, Vec<i32>, Vec<Vec<u8>>, Vec<Vec<u8>>);

pub(crate) struct LockedState {
    pub(crate) snapshot: StateSnapshot,
    present: BTreeSet<Coordinate>,
    exact: BTreeSet<Coordinate>,
    partitions: BTreeSet<PartitionCoordinate>,
    ranges: BTreeSet<PartitionCoordinate>,
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
    let ranges = read_set
        .ranges
        .iter()
        .map(range_coordinate)
        .collect::<Result<BTreeSet<_>, _>>()?;
    let rows = query_rows(transaction, graph_id, &exact, &partitions, &read_set.ranges)?;
    if rows.len() > MAX_STATE_KEYS {
        return Err(M2Error::TransactionLimitExceeded);
    }
    let payload_bytes = rows.iter().try_fold(0usize, |total, row| {
        total
            .checked_add(row.get::<_, Vec<u8>>(6).len())
            .ok_or(M2Error::TransactionLimitExceeded)
    })?;
    if payload_bytes > MAX_STATE_PAYLOAD_BYTES {
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
    let requested_ranges: BTreeMap<PartitionCoordinate, StateRange> = read_set
        .ranges
        .iter()
        .cloned()
        .map(|range| Ok((range_coordinate(&range)?, range)))
        .collect::<Result<_, M2Error>>()?;
    let mut entries = Vec::with_capacity(rows.len().saturating_add(exact.len()));
    let mut present = BTreeSet::new();
    for row in rows {
        let coordinate = (row.get(0), row.get(1), row.get(2), row.get(3));
        let key = requested_keys
            .get(&coordinate)
            .cloned()
            .or_else(|| {
                let partition_coordinate = (coordinate.0, coordinate.1, coordinate.2.clone());
                requested_partitions
                    .get(&partition_coordinate)
                    .and_then(|partition| state_key(partition, &coordinate.3).ok())
            })
            .or_else(|| {
                let partition_coordinate = (coordinate.0, coordinate.1, coordinate.2.clone());
                requested_ranges
                    .get(&partition_coordinate)
                    .and_then(|range| state_key_from_range(range, &coordinate.3).ok())
            });
        let key = key.ok_or(M2Error::InvalidOperatorDefinition)?;
        if !present.insert(coordinate) {
            continue;
        }
        validate_row_order_key(&key, row.get(4))?;
        entries.push(shiba_operator::StateEntry {
            key,
            state: Some(shiba_operator::EncodedOperatorState {
                codec_version: u32::try_from(row.get::<_, i32>(5))
                    .map_err(|_| M2Error::InvalidOperatorDefinition)?,
                payload: row.get(6),
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
        ranges,
    })
}

fn query_rows(
    transaction: &mut Transaction<'_>,
    graph_id: i64,
    exact: &BTreeSet<Coordinate>,
    partitions: &BTreeSet<PartitionCoordinate>,
    ranges: &[StateRange],
) -> Result<Vec<Row>, M2Error> {
    let (exact_nodes, exact_namespaces, exact_partitions, exact_items) =
        coordinate_arrays(&exact.iter().cloned().collect::<Vec<_>>());
    let partition_nodes: Vec<i64> = partitions.iter().map(|value| value.0).collect();
    let partition_namespaces: Vec<i32> = partitions.iter().map(|value| value.1).collect();
    let partition_payloads: Vec<Vec<u8>> = partitions.iter().map(|value| value.2.clone()).collect();
    let limit =
        i64::try_from(MAX_STATE_KEYS + 1).map_err(|_| M2Error::InvalidOperatorDefinition)?;
    let mut rows = transaction.query(
        "WITH exact AS (
             SELECT * FROM unnest($2::bigint[], $3::integer[], $4::bytea[], $5::bytea[])
               AS value(node_id, namespace, partition_key_payload, item_key_payload)
         ), partitions AS (
             SELECT * FROM unnest($6::bigint[], $7::integer[], $8::bytea[])
               AS value(node_id, namespace, partition_key_payload)
         )
         SELECT state.node_id, state.namespace, state.partition_key_payload,
                state.item_key_payload, state.item_order_key,
                state.codec_version, state.state_payload
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
    )?;
    rows.extend(query_range_rows(
        transaction,
        graph_id,
        ranges,
        StateRangeDirection::Ascending,
    )?);
    rows.extend(query_range_rows(
        transaction,
        graph_id,
        ranges,
        StateRangeDirection::Descending,
    )?);
    Ok(rows)
}

fn query_range_rows(
    transaction: &mut Transaction<'_>,
    graph_id: i64,
    ranges: &[StateRange],
    direction: StateRangeDirection,
) -> Result<Vec<Row>, M2Error> {
    let selected = ranges
        .iter()
        .filter(|range| range.direction == direction)
        .collect::<Vec<_>>();
    if selected.is_empty() {
        return Ok(Vec::new());
    }
    let nodes: Vec<i64> = selected
        .iter()
        .map(|range| i64::from(range.node_id.get()))
        .collect();
    let namespaces: Vec<i32> = selected
        .iter()
        .map(|range| i32::from(range.namespace))
        .collect();
    let partitions: Vec<Vec<u8>> = selected
        .iter()
        .map(|range| canonical(&range.partition_key))
        .collect::<Result<_, _>>()?;
    let limits: Vec<i64> = selected
        .iter()
        .map(|range| i64::from(range.limit))
        .collect();
    let order = match direction {
        StateRangeDirection::Ascending => "ASC",
        StateRangeDirection::Descending => "DESC",
    };
    let query = format!(
        "WITH ranges AS (\n             SELECT * FROM unnest($2::bigint[], $3::integer[], $4::bytea[], $5::bigint[])\n               AS input(node_id, namespace, partition_key_payload, range_limit)\n         )\n         SELECT state.node_id, state.namespace, state.partition_key_payload,\n                state.item_key_payload, state.item_order_key,\n                state.codec_version, state.state_payload\n         FROM ranges\n         CROSS JOIN LATERAL (\n             SELECT candidate.node_id, candidate.namespace,\n                    candidate.partition_key_payload, candidate.item_key_payload,\n                    candidate.item_order_key, candidate.codec_version, candidate.state_payload\n             FROM shiba_internal.graph_node_state AS candidate\n             WHERE candidate.graph_id = $1\n               AND candidate.node_id = ranges.node_id\n               AND candidate.namespace = ranges.namespace\n               AND candidate.partition_key_payload = ranges.partition_key_payload\n               AND candidate.item_order_key IS NOT NULL\n             ORDER BY candidate.item_order_key {order}\n             LIMIT ranges.range_limit\n             FOR UPDATE OF candidate\n         ) AS state"
    );
    Ok(transaction.query(
        &query,
        &[&graph_id, &nodes, &namespaces, &partitions, &limits],
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
                && !locked
                    .ranges
                    .contains(&(coordinate.0, coordinate.1, coordinate.2.clone()))
        })
    {
        return Err(M2Error::InvalidOperatorDefinition);
    }
    let mut deletes = Vec::new();
    let mut upserts = Vec::new();
    for (coordinate, delta) in ordered {
        let order_key =
            state_item_order_key(&delta.key).map_err(|_| M2Error::InvalidOperatorDefinition)?;
        match delta.mutation {
            StateMutation::Delete => {
                if !locked.present.contains(&coordinate) {
                    return Err(M2Error::InvalidOperatorDefinition);
                }
                deletes.push(coordinate);
            }
            StateMutation::Upsert { state } => upserts.push((coordinate, state, order_key)),
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

fn state_key_from_range(range: &StateRange, item: &[u8]) -> Result<StateKey, M2Error> {
    let item_key =
        TypedValue::from_canonical_json(item).map_err(|_| M2Error::InvalidOperatorDefinition)?;
    if !matches!(item_key, TypedValue::Int8(_)) {
        return Err(M2Error::InvalidOperatorDefinition);
    }
    Ok(StateKey {
        node_id: range.node_id,
        namespace: range.namespace,
        partition_key: range.partition_key.clone(),
        item_key: Some(item_key),
    })
}

fn validate_row_order_key(key: &StateKey, order_key: Option<Vec<u8>>) -> Result<(), M2Error> {
    match (&key.item_key, order_key) {
        (None, None) => Ok(()),
        (Some(value), Some(order_key)) => validate_int8_order_key(value, &order_key)
            .map_err(|_| M2Error::InvalidOperatorDefinition),
        _ => Err(M2Error::InvalidOperatorDefinition),
    }
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

fn range_coordinate(range: &StateRange) -> Result<PartitionCoordinate, M2Error> {
    Ok((
        i64::from(range.node_id.get()),
        i32::from(range.namespace),
        canonical(&range.partition_key)?,
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
