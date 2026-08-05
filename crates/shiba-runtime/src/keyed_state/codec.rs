use postgres::Row;
use shiba_operator::{
    EncodedOperatorState, StateEntry, StateKey, StatePartition, StateRange, TypedValue,
    validate_int8_order_key,
};

use super::{Coordinate, PartitionCoordinate};
use crate::M2Error;

type CoordinateArrays = (Vec<i64>, Vec<i32>, Vec<Vec<u8>>, Vec<Vec<u8>>);

pub(super) fn coordinate_arrays(states: &[Coordinate]) -> CoordinateArrays {
    (
        states.iter().map(|state| state.0).collect(),
        states.iter().map(|state| state.1).collect(),
        states.iter().map(|state| state.2.clone()).collect(),
        states.iter().map(|state| state.3.clone()).collect(),
    )
}

pub(super) fn state_key(partition: &StatePartition, item: &[u8]) -> Result<StateKey, M2Error> {
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

pub(super) fn state_key_from_range(range: &StateRange, item: &[u8]) -> Result<StateKey, M2Error> {
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

pub(super) fn state_entry(key: &StateKey, row: &Row) -> Result<StateEntry, M2Error> {
    Ok(StateEntry {
        key: key.clone(),
        state: Some(EncodedOperatorState {
            codec_version: u32::try_from(row.get::<_, i32>(5))
                .map_err(|_| M2Error::InvalidOperatorDefinition)?,
            payload: row.get(6),
        }),
    })
}

pub(super) fn validate_row_order_key(
    key: &StateKey,
    order_key: Option<Vec<u8>>,
) -> Result<(), M2Error> {
    match (&key.item_key, order_key) {
        (None, None) => Ok(()),
        (Some(value), Some(order_key)) => validate_int8_order_key(value, &order_key)
            .map_err(|_| M2Error::InvalidOperatorDefinition),
        _ => Err(M2Error::InvalidOperatorDefinition),
    }
}

pub(super) fn coordinate(key: &StateKey) -> Result<Coordinate, M2Error> {
    Ok((
        i64::from(key.node_id.get()),
        i32::from(key.namespace),
        canonical(&key.partition_key)?,
        key.item_key
            .as_ref()
            .map_or_else(|| Ok(b"null".to_vec()), canonical)?,
    ))
}

pub(super) fn partition_coordinate(
    partition: &StatePartition,
) -> Result<PartitionCoordinate, M2Error> {
    Ok((
        i64::from(partition.node_id.get()),
        i32::from(partition.namespace),
        canonical(&partition.partition_key)?,
    ))
}

pub(super) fn range_coordinate(range: &StateRange) -> Result<PartitionCoordinate, M2Error> {
    Ok((
        i64::from(range.node_id.get()),
        i32::from(range.namespace),
        canonical(&range.partition_key)?,
    ))
}

pub(super) fn canonical(value: &TypedValue) -> Result<Vec<u8>, M2Error> {
    value
        .to_canonical_json()
        .map_err(|_| M2Error::InvalidOperatorDefinition)
}
