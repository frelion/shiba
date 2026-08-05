use std::collections::BTreeSet;

use postgres::{Row, Transaction};
use shiba_operator::{StateRange, StateRangeDirection};

use super::codec::coordinate_arrays;
use super::{Coordinate, PartitionCoordinate};
use crate::M2Error;

pub(super) struct QueriedRows {
    pub(super) base: Vec<Row>,
    pub(super) ranges: Vec<RangeRows>,
}

pub(super) struct RangeRows {
    pub(crate) range_index: usize,
    pub(crate) rows: Vec<Row>,
}

/// The production ordered-read template. Integration EXPLAIN tests use this
/// exact builder so the proof cannot drift from the query executed by Runtime.
#[must_use]
pub fn build_ordered_range_query(direction: StateRangeDirection) -> String {
    let order = match direction {
        StateRangeDirection::Ascending => "ASC",
        StateRangeDirection::Descending => "DESC",
    };
    format!(
        r"WITH ranges AS (
             SELECT * FROM unnest($2::bigint[], $3::integer[], $4::bytea[], $5::bigint[], $6::integer[])
               AS input(node_id, namespace, partition_key_payload, range_limit, range_index)
         )
         SELECT state.node_id, state.namespace, state.partition_key_payload,
                state.item_key_payload, state.item_order_key,
                state.codec_version, state.state_payload, ranges.range_index
         FROM ranges
         CROSS JOIN LATERAL (
             SELECT candidate.node_id, candidate.namespace,
                    candidate.partition_key_payload, candidate.item_key_payload,
                    candidate.item_order_key, candidate.codec_version, candidate.state_payload
             FROM shiba_internal.graph_node_state AS candidate
             WHERE candidate.graph_id = $1
               AND candidate.node_id = ranges.node_id
               AND candidate.namespace = ranges.namespace
               AND candidate.partition_key_payload = ranges.partition_key_payload
               AND candidate.item_order_key IS NOT NULL
             ORDER BY candidate.item_order_key {order}
             LIMIT ranges.range_limit
             FOR UPDATE OF candidate
         ) AS state
         ORDER BY ranges.range_index, state.item_order_key {order}"
    )
}

pub(super) fn query_rows(
    transaction: &mut Transaction<'_>,
    graph_id: i64,
    exact: &BTreeSet<Coordinate>,
    partitions: &BTreeSet<PartitionCoordinate>,
    ranges: &[StateRange],
) -> Result<QueriedRows, M2Error> {
    let (exact_nodes, exact_namespaces, exact_partitions, exact_items) =
        coordinate_arrays(&exact.iter().cloned().collect::<Vec<_>>());
    let partition_nodes: Vec<i64> = partitions.iter().map(|value| value.0).collect();
    let partition_namespaces: Vec<i32> = partitions.iter().map(|value| value.1).collect();
    let partition_payloads: Vec<Vec<u8>> = partitions.iter().map(|value| value.2.clone()).collect();
    let limit = i64::try_from(shiba_operator::MAX_STATE_KEYS + 1)
        .map_err(|_| M2Error::InvalidOperatorDefinition)?;
    let base = transaction.query(
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
    Ok(QueriedRows {
        base,
        ranges: [
            query_range_rows(
                transaction,
                graph_id,
                ranges,
                StateRangeDirection::Ascending,
            )?,
            query_range_rows(
                transaction,
                graph_id,
                ranges,
                StateRangeDirection::Descending,
            )?,
        ]
        .into_iter()
        .flatten()
        .collect(),
    })
}

pub(crate) fn query_range_rows(
    transaction: &mut Transaction<'_>,
    graph_id: i64,
    ranges: &[StateRange],
    direction: StateRangeDirection,
) -> Result<Vec<RangeRows>, M2Error> {
    let selected = ranges
        .iter()
        .enumerate()
        .filter(|(_, range)| range.direction == direction)
        .collect::<Vec<_>>();
    if selected.is_empty() {
        return Ok(Vec::new());
    }
    let indexes: Vec<i32> = selected
        .iter()
        .map(|(index, _)| i32::try_from(*index).map_err(|_| M2Error::InvalidOperatorDefinition))
        .collect::<Result<_, _>>()?;
    let nodes: Vec<i64> = selected
        .iter()
        .map(|(_, range)| i64::from(range.node_id.get()))
        .collect();
    let namespaces: Vec<i32> = selected
        .iter()
        .map(|(_, range)| i32::from(range.namespace))
        .collect();
    let partitions: Vec<Vec<u8>> = selected
        .iter()
        .map(|(_, range)| {
            range
                .partition_key
                .to_canonical_json()
                .map_err(|_| M2Error::InvalidOperatorDefinition)
        })
        .collect::<Result<_, _>>()?;
    let limits: Vec<i64> = selected
        .iter()
        .map(|(_, range)| i64::from(range.limit))
        .collect();
    let rows = transaction.query(
        &build_ordered_range_query(direction),
        &[
            &graph_id,
            &nodes,
            &namespaces,
            &partitions,
            &limits,
            &indexes,
        ],
    )?;
    let mut grouped = selected
        .iter()
        .map(|(index, _)| RangeRows {
            range_index: *index,
            rows: Vec::new(),
        })
        .collect::<Vec<_>>();
    for row in rows {
        let range_index = usize::try_from(row.get::<_, i32>(7))
            .map_err(|_| M2Error::InvalidOperatorDefinition)?;
        let slot = grouped
            .iter()
            .position(|result| result.range_index == range_index)
            .ok_or(M2Error::InvalidOperatorDefinition)?;
        grouped[slot].rows.push(row);
    }
    for result in &grouped {
        let range = ranges
            .get(result.range_index)
            .ok_or(M2Error::InvalidOperatorDefinition)?;
        if result.rows.len() > usize::try_from(range.limit).unwrap_or(usize::MAX) {
            return Err(M2Error::TransactionLimitExceeded);
        }
        for row in &result.rows {
            if row.get::<_, i64>(0) != i64::from(range.node_id.get())
                || row.get::<_, i32>(1) != i32::from(range.namespace)
            {
                return Err(M2Error::InvalidOperatorDefinition);
            }
            let partition: Vec<u8> = row.get(2);
            let expected = range
                .partition_key
                .to_canonical_json()
                .map_err(|_| M2Error::InvalidOperatorDefinition)?;
            if partition != expected || row.get::<_, Option<Vec<u8>>>(4).is_none() {
                return Err(M2Error::InvalidOperatorDefinition);
            }
        }
        for pair in result.rows.windows(2) {
            let left = pair[0]
                .get::<_, Option<Vec<u8>>>(4)
                .ok_or(M2Error::InvalidOperatorDefinition)?;
            let right = pair[1]
                .get::<_, Option<Vec<u8>>>(4)
                .ok_or(M2Error::InvalidOperatorDefinition)?;
            let ordered = match direction {
                StateRangeDirection::Ascending => left < right,
                StateRangeDirection::Descending => left > right,
            };
            if !ordered {
                return Err(M2Error::InvalidOperatorDefinition);
            }
        }
    }
    Ok(grouped)
}

#[cfg(test)]
mod tests {
    use super::build_ordered_range_query;
    use shiba_operator::StateRangeDirection;

    #[test]
    fn production_range_query_template_is_directional_and_bounded() {
        let ascending = build_ordered_range_query(StateRangeDirection::Ascending);
        assert!(ascending.contains("WITH ranges"));
        assert!(ascending.contains("CROSS JOIN LATERAL"));
        assert!(ascending.contains("ORDER BY candidate.item_order_key ASC"));
        assert!(ascending.contains("ORDER BY ranges.range_index, state.item_order_key ASC"));
        assert!(ascending.contains("LIMIT ranges.range_limit"));
        assert!(ascending.contains("FOR UPDATE OF candidate"));
        let descending = build_ordered_range_query(StateRangeDirection::Descending);
        assert!(descending.contains("ORDER BY candidate.item_order_key DESC"));
        assert!(descending.contains("ORDER BY ranges.range_index, state.item_order_key DESC"));
    }
}
