use postgres::Client;
use shiba_operator::TypedValue;

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct DurableSnapshot {
    source_rows: String,
    scalar_states: String,
    node_states: String,
    results: String,
    continuations: String,
}

fn json(client: &mut Client, query: &str) -> String {
    client
        .query_one(query, &[])
        .expect("query canonical durable snapshot")
        .get(0)
}

pub(crate) fn durable_snapshot(client: &mut Client) -> DurableSnapshot {
    DurableSnapshot {
        source_rows: json(
            client,
            "SELECT COALESCE(jsonb_agg(to_jsonb(row_state)
                        ORDER BY source_row_id), '[]')::text
             FROM (SELECT source_id, source_row_id, payload_present,
                          payload_int8, payload_text
                   FROM shiba_internal.source_row_state) AS row_state",
        ),
        scalar_states: "[]".into(),
        node_states: json(
            client,
            "SELECT COALESCE(jsonb_agg(to_jsonb(state)
                        ORDER BY graph_id, node_id, namespace,
                                 partition_key, item_key), '[]')::text
             FROM (SELECT graph_id, node_id, namespace,
                          pg_catalog.encode(partition_key_payload, 'hex') AS partition_key,
                          pg_catalog.encode(item_key_payload, 'hex') AS item_key,
                          codec_version,
                          pg_catalog.encode(state_payload, 'hex') AS state_payload
                   FROM shiba_internal.graph_node_state) AS state",
        ),
        results: json(
            client,
            "SELECT jsonb_build_object(
                 'headers', (SELECT jsonb_agg(to_jsonb(header) ORDER BY graph_id, result_id)
                             FROM shiba.graph_result AS header),
                 'rows', (SELECT COALESCE(jsonb_agg(to_jsonb(result_row)
                                  ORDER BY graph_id, result_id, row_identity), '[]')
                          FROM (SELECT graph_id, result_id,
                                       pg_catalog.encode(schema_digest, 'hex') AS schema_digest,
                                       pg_catalog.encode(row_identity, 'hex') AS row_identity,
                                       pg_catalog.encode(row_payload, 'hex') AS row_payload
                                FROM shiba_internal.graph_result_row) AS result_row)
             )::text",
        ),
        continuations: json(
            client,
            "SELECT COALESCE(jsonb_agg(to_jsonb(continuation)
                        ORDER BY commit_lsn), '[]')::text
             FROM shiba_internal.graph_continuation AS continuation",
        ),
    }
}

type ResultRow = (i64, Option<i64>, Option<i64>, bool, bool);

fn result_rows(client: &mut Client, query: &str) -> Vec<ResultRow> {
    client
        .query(query, &[])
        .expect("query grouped result rows")
        .into_iter()
        .map(|row| (row.get(0), row.get(1), row.get(2), row.get(3), row.get(4)))
        .collect()
}

pub(crate) fn assert_sql_oracle(client: &mut Client) {
    let actual = result_rows(
        client,
        "SELECT result_id,
                CASE WHEN convert_from(row_payload, 'UTF8')::jsonb #>> '{values,0,type}' = 'null'
                     THEN NULL ELSE (convert_from(row_payload, 'UTF8')::jsonb
                         #>> '{values,0,value}')::bigint END,
                CASE WHEN convert_from(row_payload, 'UTF8')::jsonb #>> '{values,1,type}' = 'null'
                     THEN NULL ELSE (convert_from(row_payload, 'UTF8')::jsonb
                         #>> '{values,1,value}')::bigint END,
                convert_from(row_payload, 'UTF8')::jsonb #>> '{values,0,type}' = 'null',
                convert_from(row_payload, 'UTF8')::jsonb #>> '{values,1,type}' = 'null'
         FROM shiba.graph_result_rows WHERE graph_id = 1
         ORDER BY result_id, 4 DESC, 2 NULLS FIRST",
    );
    let expected = result_rows(
        client,
        "SELECT operator_id, key_value, aggregate_value,
                key_is_null, aggregate_is_null
         FROM (
             SELECT 7::bigint AS operator_id, payload AS key_value,
                    count(*)::bigint AS aggregate_value,
                    payload IS NULL AS key_is_null,
                    false AS aggregate_is_null
             FROM source.events GROUP BY payload
             UNION ALL
             SELECT 8, payload, sum(id)::bigint, payload IS NULL, false
             FROM source.events GROUP BY payload
             UNION ALL
             SELECT 9, id, sum(payload)::bigint, false,
                    sum(payload) IS NULL
             FROM source.events GROUP BY id
         ) AS oracle
         ORDER BY operator_id, key_is_null DESC,
                  key_value NULLS FIRST",
    );
    assert_eq!(actual, expected, "grouped results differ from SQL oracle");
}

pub(crate) fn node_state_payload(client: &mut Client, operator_id: i64, key: i64) -> Vec<u8> {
    let partition = TypedValue::Int8(key)
        .to_canonical_json()
        .expect("canonical grouped partition");
    client
        .query_one(
            "SELECT state.state_payload
             FROM shiba_internal.graph_node_state AS state
             WHERE state.graph_id = 1 AND state.node_id = $1
               AND state.partition_key_payload = $2",
            &[&operator_id, &partition],
        )
        .expect("query exact grouped node state")
        .get(0)
}

pub(crate) fn set_node_state_payload(
    client: &mut Client,
    operator_id: i64,
    key: i64,
    payload: &[u8],
) {
    let partition = TypedValue::Int8(key)
        .to_canonical_json()
        .expect("canonical grouped partition");
    assert_eq!(
        client
            .execute(
                "UPDATE shiba_internal.graph_node_state
                 SET state_payload = $3
                 WHERE graph_id = 1 AND node_id = $1
                   AND partition_key_payload = $2",
                &[&operator_id, &partition, &payload],
            )
            .expect("replace exact grouped node state"),
        1
    );
}
