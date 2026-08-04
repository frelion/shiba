use postgres::Client;

use crate::support::keyed_int8_results;

#[derive(Debug, Eq, PartialEq)]
pub(super) struct Durable {
    pub(super) scalar: (i64, i64),
    pub(super) keyed: Vec<(i64, Option<i64>)>,
    pub(super) source: Vec<(i64, Option<i64>)>,
    pub(super) states: Vec<(i64, i32, String)>,
    pub(super) continuations: i64,
}

pub(super) fn durable(client: &mut Client) -> Durable {
    let scalar = client
        .query_one(
            "SELECT
                (SELECT (convert_from(row_payload, 'UTF8')::jsonb #>> '{values,0,value}')::bigint FROM shiba.graph_result_rows WHERE graph_id = 1 AND result_id = 4),
                (SELECT CASE
                    WHEN convert_from(row_payload, 'UTF8')::jsonb #>> '{values,0,type}' = 'null'
                    THEN 0
                    ELSE (convert_from(row_payload, 'UTF8')::jsonb #>> '{values,0,value}')::bigint
                 END FROM shiba.graph_result_rows WHERE graph_id = 1 AND result_id = 5)",
            &[],
        )
        .expect("query scalar results");
    Durable {
        scalar: (scalar.get(0), scalar.get(1)),
        keyed: keyed_int8_results(client, 1, 6)
            .into_iter()
            .map(|(key, value)| (key.expect("project key is non-null"), value))
            .collect(),
        source: pairs(
            client,
            "SELECT source_row_id, payload_int8
             FROM shiba_internal.source_row_state WHERE source_id = 1 ORDER BY 1",
        ),
        states: client
            .query(
                "SELECT node_id, namespace, encode(state_payload, 'hex')
                 FROM shiba_internal.graph_node_state
                 WHERE graph_id = 1
                 ORDER BY node_id, namespace, partition_key_payload, item_key_payload",
                &[],
            )
            .expect("query opaque states")
            .into_iter()
            .map(|row| (row.get(0), row.get(1), row.get(2)))
            .collect(),
        continuations: client
            .query_one(
                "SELECT count(*) FROM shiba_internal.graph_continuation",
                &[],
            )
            .expect("query continuation")
            .get(0),
    }
}

pub(super) fn pairs(client: &mut Client, sql: &str) -> Vec<(i64, Option<i64>)> {
    client
        .query(sql, &[])
        .expect("query keyed rows")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}
