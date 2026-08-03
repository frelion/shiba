use postgres::Transaction;

use super::{Coordinate, coordinate_arrays};
use crate::M2Error;

pub(super) fn delete_states(
    transaction: &mut Transaction<'_>,
    graph_id: i64,
    states: &[Coordinate],
) -> Result<(), M2Error> {
    if states.is_empty() {
        return Ok(());
    }
    let (nodes, namespaces, partitions, items) = coordinate_arrays(states);
    let changed = transaction.execute(
        "DELETE FROM shiba_internal.graph_node_state AS state
         USING unnest($2::bigint[], $3::integer[], $4::bytea[], $5::bytea[])
           AS input(node_id, namespace, partition_key_payload, item_key_payload)
         WHERE state.graph_id = $1 AND state.node_id = input.node_id
           AND state.namespace = input.namespace
           AND state.partition_key_payload = input.partition_key_payload
           AND state.item_key_payload = input.item_key_payload",
        &[&graph_id, &nodes, &namespaces, &partitions, &items],
    )?;
    if usize::try_from(changed).ok() != Some(states.len()) {
        return Err(M2Error::InvalidOperatorDefinition);
    }
    Ok(())
}

pub(super) fn upsert_states(
    transaction: &mut Transaction<'_>,
    graph_id: i64,
    states: &[(Coordinate, shiba_operator::EncodedOperatorState)],
) -> Result<(), M2Error> {
    if states.is_empty() {
        return Ok(());
    }
    let coordinates: Vec<_> = states.iter().map(|state| state.0.clone()).collect();
    let (nodes, namespaces, partitions, items) = coordinate_arrays(&coordinates);
    let codecs = states
        .iter()
        .map(|state| i32::try_from(state.1.codec_version))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| M2Error::InvalidOperatorDefinition)?;
    let payloads: Vec<Vec<u8>> = states.iter().map(|state| state.1.payload.clone()).collect();
    let changed = transaction.execute(
        "INSERT INTO shiba_internal.graph_node_state (
             graph_id, node_id, namespace, partition_key_payload,
             item_key_payload, codec_version, state_payload)
         SELECT $1, * FROM unnest($2::bigint[], $3::integer[], $4::bytea[],
                                  $5::bytea[], $6::integer[], $7::bytea[])
         ON CONFLICT (graph_id, node_id, namespace,
                      partition_key_payload, item_key_payload)
         DO UPDATE SET codec_version = EXCLUDED.codec_version,
                       state_payload = EXCLUDED.state_payload",
        &[
            &graph_id,
            &nodes,
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
