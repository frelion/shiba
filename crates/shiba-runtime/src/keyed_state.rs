use std::collections::{BTreeMap, BTreeSet};

use postgres::Transaction;
use shiba_operator::{
    MAX_STATE_KEYS, MAX_STATE_PAYLOAD_BYTES, MultiInputBatch, OperatorGraph, StateDelta, StateKey,
    StateMutation, StatePartition, StateRangeResult, StateSnapshot, graph_state_read_set,
    state_item_order_key,
};

use crate::M2Error;

#[path = "keyed_state/codec.rs"]
mod codec;
#[path = "keyed_state/range.rs"]
mod range;
mod write;
pub use range::build_ordered_range_query;

use codec::{
    coordinate, partition_coordinate, range_coordinate, state_entry, state_key,
    state_key_from_range, validate_row_order_key,
};

type Coordinate = (i64, i32, Vec<u8>, Vec<u8>);
type PartitionCoordinate = (i64, i32, Vec<u8>);

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
    let queried = range::query_rows(transaction, graph_id, &exact, &partitions, &read_set.ranges)?;
    let loaded_rows = queried.base.len()
        + queried
            .ranges
            .iter()
            .map(|result| result.rows.len())
            .sum::<usize>();
    if loaded_rows > MAX_STATE_KEYS {
        return Err(M2Error::TransactionLimitExceeded);
    }
    let payload_bytes = queried
        .base
        .iter()
        .chain(queried.ranges.iter().flat_map(|result| result.rows.iter()))
        .try_fold(0usize, |total, row| {
            total
                .checked_add(row.get::<_, Vec<u8>>(6).len())
                .ok_or(M2Error::TransactionLimitExceeded)
        })?;
    if payload_bytes > MAX_STATE_PAYLOAD_BYTES {
        return Err(M2Error::TransactionLimitExceeded);
    }

    let requested_keys = requested_keys(&read_set)?;
    let requested_partitions = requested_partitions(&read_set)?;
    let (snapshot, present) = materialize_snapshot(
        &read_set,
        queried,
        &exact,
        requested_keys,
        &requested_partitions,
        loaded_rows,
    )?;
    Ok(LockedState {
        snapshot,
        present,
        exact,
        partitions,
        ranges,
    })
}

fn requested_keys(
    read_set: &shiba_operator::StateReadSet,
) -> Result<BTreeMap<Coordinate, StateKey>, M2Error> {
    read_set
        .keys
        .iter()
        .cloned()
        .map(|key| Ok((coordinate(&key)?, key)))
        .collect::<Result<_, M2Error>>()
}

fn requested_partitions(
    read_set: &shiba_operator::StateReadSet,
) -> Result<BTreeMap<PartitionCoordinate, StatePartition>, M2Error> {
    read_set
        .partitions
        .iter()
        .cloned()
        .map(|partition| Ok((partition_coordinate(&partition)?, partition)))
        .collect::<Result<_, M2Error>>()
}

fn materialize_snapshot(
    read_set: &shiba_operator::StateReadSet,
    queried: range::QueriedRows,
    exact: &BTreeSet<Coordinate>,
    requested_keys: BTreeMap<Coordinate, StateKey>,
    requested_partitions: &BTreeMap<PartitionCoordinate, StatePartition>,
    loaded_rows: usize,
) -> Result<(StateSnapshot, BTreeSet<Coordinate>), M2Error> {
    let mut entries = Vec::with_capacity(loaded_rows.saturating_add(exact.len()));
    let mut present = BTreeSet::new();
    for row in queried.base {
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
        validate_row_order_key(&key, row.get(4))?;
        entries.push(state_entry(&key, &row)?);
    }
    for (coordinate, key) in requested_keys {
        if !present.contains(&coordinate) {
            entries.push(shiba_operator::StateEntry { key, state: None });
        }
    }
    let mut range_results = Vec::with_capacity(queried.ranges.len());
    for result in queried.ranges {
        let range = read_set
            .ranges
            .get(result.range_index)
            .ok_or(M2Error::InvalidOperatorDefinition)?;
        let mut range_entries = Vec::with_capacity(result.rows.len());
        for row in result.rows {
            let coordinate: Coordinate =
                (row.get(0), row.get(1), row.get(2), row.get::<_, Vec<u8>>(3));
            let key = state_key_from_range(range, &coordinate.3)?;
            validate_row_order_key(&key, row.get(4))?;
            let entry = state_entry(&key, &row)?;
            if exact.contains(&coordinate) {
                if !present.contains(&coordinate) {
                    return Err(M2Error::InvalidOperatorDefinition);
                }
            } else {
                if !present.insert(coordinate) {
                    return Err(M2Error::InvalidOperatorDefinition);
                }
                entries.push(entry.clone());
            }
            range_entries.push(entry);
        }
        range_results.push(StateRangeResult {
            range_index: result.range_index,
            entries: range_entries,
        });
    }
    entries.sort_by(|left, right| left.key.cmp(&right.key));
    let snapshot = StateSnapshot::new_with_ranges(read_set, entries, &range_results)
        .map_err(|_| M2Error::InvalidOperatorDefinition)?;
    Ok((snapshot, present))
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
