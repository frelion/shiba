use std::collections::BTreeMap;

use postgres::Transaction;
use shiba_operator::{
    CompiledPlan, DeltaBatch, StateDelta, StateKey, StateMutation, StateSnapshot, TypedValue,
    state_read_set,
};

use crate::M2Error;

type Coordinate = (i64, i32, Vec<u8>, Vec<u8>);
type CoordinateArrays = (Vec<i64>, Vec<i32>, Vec<Vec<u8>>, Vec<Vec<u8>>);

pub(crate) struct LockedState {
    pub(crate) snapshot: StateSnapshot,
    requested: BTreeMap<Coordinate, bool>,
}

pub(crate) fn load(
    transaction: &mut Transaction<'_>,
    operator_id: i64,
    plan: &CompiledPlan,
    batch: &DeltaBatch,
) -> Result<LockedState, M2Error> {
    let read_set = state_read_set(plan, batch)?;
    let mut requested = read_set
        .keys
        .iter()
        .cloned()
        .enumerate()
        .map(|(index, key)| Ok((coordinate(&key)?, index, key)))
        .collect::<Result<Vec<_>, M2Error>>()?;
    requested.sort_by(|left, right| left.0.cmp(&right.0));
    if requested.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(M2Error::InvalidOperatorDefinition);
    }
    let node_ids: Vec<i64> = requested.iter().map(|entry| entry.0.0).collect();
    let namespaces: Vec<i32> = requested.iter().map(|entry| entry.0.1).collect();
    let partitions: Vec<Vec<u8>> = requested.iter().map(|entry| entry.0.2.clone()).collect();
    let items: Vec<Vec<u8>> = requested.iter().map(|entry| entry.0.3.clone()).collect();
    let mut states = BTreeMap::<usize, _>::new();
    if !requested.is_empty() {
        for row in transaction.query(
            "SELECT request.ordinality::bigint, state.codec_version, state.state_payload
             FROM unnest($2::bigint[], $3::integer[], $4::bytea[], $5::bytea[])
                  WITH ORDINALITY AS request(
                      node_id, namespace, partition_key_payload,
                      item_key_payload, ordinality)
             JOIN shiba_internal.operator_node_state AS state
               ON state.operator_id = $1
              AND state.node_id = request.node_id
              AND state.namespace = request.namespace
              AND state.partition_key_payload = request.partition_key_payload
              AND state.item_key_payload = request.item_key_payload
             ORDER BY request.node_id, request.namespace,
                      request.partition_key_payload, request.item_key_payload
             FOR UPDATE OF state",
            &[&operator_id, &node_ids, &namespaces, &partitions, &items],
        )? {
            let ordinal = usize::try_from(row.get::<_, i64>(0))
                .ok()
                .and_then(|value| value.checked_sub(1))
                .filter(|value| *value < requested.len())
                .ok_or(M2Error::InvalidOperatorDefinition)?;
            let original_index = requested[ordinal].1;
            let codec_version = u32::try_from(row.get::<_, i32>(1))
                .map_err(|_| M2Error::InvalidOperatorDefinition)?;
            if states
                .insert(
                    original_index,
                    shiba_operator::EncodedOperatorState {
                        codec_version,
                        payload: row.get(2),
                    },
                )
                .is_some()
            {
                return Err(M2Error::InvalidOperatorDefinition);
            }
        }
    }
    let requested_presence = requested
        .iter()
        .map(|(coordinate, index, _)| (coordinate.clone(), states.contains_key(index)))
        .collect();
    let entries = read_set
        .keys
        .iter()
        .cloned()
        .enumerate()
        .map(|(index, key)| shiba_operator::StateEntry {
            key,
            state: states.remove(&index),
        })
        .collect();
    let snapshot =
        StateSnapshot::new(&read_set, entries).map_err(|_| M2Error::InvalidOperatorDefinition)?;
    Ok(LockedState {
        snapshot,
        requested: requested_presence,
    })
}

pub(crate) fn persist(
    transaction: &mut Transaction<'_>,
    operator_id: i64,
    locked: &LockedState,
    deltas: Vec<StateDelta>,
) -> Result<(), M2Error> {
    let mut ordered = deltas
        .into_iter()
        .map(|delta| Ok((coordinate(&delta.key)?, delta)))
        .collect::<Result<Vec<_>, M2Error>>()?;
    ordered.sort_by(|left, right| left.0.cmp(&right.0));
    if ordered.windows(2).any(|pair| pair[0].0 == pair[1].0)
        || ordered
            .iter()
            .any(|(coordinate, _)| !locked.requested.contains_key(coordinate))
    {
        return Err(M2Error::InvalidOperatorDefinition);
    }

    let mut deletes = Vec::new();
    let mut upserts = Vec::new();
    for (coordinate, delta) in ordered {
        match delta.mutation {
            StateMutation::Delete => {
                if locked.requested.get(&coordinate) != Some(&true) {
                    return Err(M2Error::InvalidOperatorDefinition);
                }
                deletes.push(coordinate);
            }
            StateMutation::Upsert { state } => upserts.push((coordinate, state)),
        }
    }
    delete_states(transaction, operator_id, &deletes)?;
    upsert_states(transaction, operator_id, &upserts)
}

pub(crate) fn clear(transaction: &mut Transaction<'_>, operator_id: i64) -> Result<(), M2Error> {
    transaction.execute(
        "DELETE FROM shiba_internal.operator_node_state WHERE operator_id = $1",
        &[&operator_id],
    )?;
    Ok(())
}

fn delete_states(
    transaction: &mut Transaction<'_>,
    operator_id: i64,
    states: &[Coordinate],
) -> Result<(), M2Error> {
    if states.is_empty() {
        return Ok(());
    }
    let (node_ids, namespaces, partitions, items) = coordinate_arrays(states);
    let changed = transaction.execute(
        "DELETE FROM shiba_internal.operator_node_state AS state
         USING unnest($2::bigint[], $3::integer[], $4::bytea[], $5::bytea[])
               AS input(node_id, namespace, partition_key_payload, item_key_payload)
         WHERE state.operator_id = $1 AND state.node_id = input.node_id
           AND state.namespace = input.namespace
           AND state.partition_key_payload = input.partition_key_payload
           AND state.item_key_payload = input.item_key_payload",
        &[&operator_id, &node_ids, &namespaces, &partitions, &items],
    )?;
    if usize::try_from(changed).ok() != Some(states.len()) {
        return Err(M2Error::InvalidOperatorDefinition);
    }
    Ok(())
}

fn upsert_states(
    transaction: &mut Transaction<'_>,
    operator_id: i64,
    states: &[(Coordinate, shiba_operator::EncodedOperatorState)],
) -> Result<(), M2Error> {
    if states.is_empty() {
        return Ok(());
    }
    let coordinates: Vec<_> = states.iter().map(|state| state.0.clone()).collect();
    let (node_ids, namespaces, partitions, items) = coordinate_arrays(&coordinates);
    let codecs = states
        .iter()
        .map(|state| {
            i32::try_from(state.1.codec_version).map_err(|_| M2Error::InvalidOperatorDefinition)
        })
        .collect::<Result<Vec<_>, M2Error>>()?;
    let payloads: Vec<Vec<u8>> = states.iter().map(|state| state.1.payload.clone()).collect();
    let changed = transaction.execute(
        "INSERT INTO shiba_internal.operator_node_state (
             operator_id, node_id, namespace, partition_key_payload,
             item_key_payload, codec_version, state_payload)
         SELECT $1, input.node_id, input.namespace, input.partition_key_payload,
                input.item_key_payload, input.codec_version, input.state_payload
         FROM unnest($2::bigint[], $3::integer[], $4::bytea[], $5::bytea[],
                     $6::integer[], $7::bytea[])
              AS input(node_id, namespace, partition_key_payload,
                       item_key_payload, codec_version, state_payload)
         ON CONFLICT (operator_id, node_id, namespace,
                      partition_key_payload, item_key_payload)
         DO UPDATE SET codec_version = EXCLUDED.codec_version,
                       state_payload = EXCLUDED.state_payload",
        &[
            &operator_id,
            &node_ids,
            &namespaces,
            &partitions,
            &items,
            &codecs,
            &payloads,
        ],
    )?;
    if usize::try_from(changed).ok() != Some(states.len()) {
        return Err(M2Error::InvalidOperatorDefinition);
    }
    Ok(())
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
