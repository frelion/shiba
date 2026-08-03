use postgres::Client;

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
        scalar_states: json(
            client,
            "SELECT COALESCE(jsonb_agg(to_jsonb(state)
                        ORDER BY operator_id), '[]')::text
             FROM (SELECT operator_id, codec_version,
                          pg_catalog.encode(state_payload, 'hex') AS state_payload
                   FROM shiba_internal.operator_state) AS state",
        ),
        node_states: json(
            client,
            "SELECT COALESCE(jsonb_agg(to_jsonb(state)
                        ORDER BY operator_id, node_id, namespace,
                                 partition_key, item_key), '[]')::text
             FROM (SELECT operator_id, node_id, namespace,
                          pg_catalog.encode(partition_key_payload, 'hex') AS partition_key,
                          pg_catalog.encode(item_key_payload, 'hex') AS item_key,
                          codec_version,
                          pg_catalog.encode(state_payload, 'hex') AS state_payload
                   FROM shiba_internal.operator_node_state) AS state",
        ),
        results: json(
            client,
            "SELECT jsonb_build_object(
                 'headers', (SELECT jsonb_agg(to_jsonb(header) ORDER BY operator_id)
                             FROM shiba.operator_result AS header),
                 'rows', (SELECT COALESCE(jsonb_agg(to_jsonb(result_row)
                                  ORDER BY operator_id, key_payload), '[]')
                          FROM (SELECT operator_id,
                                       pg_catalog.encode(key_payload, 'hex') AS key_payload,
                                       result_key_is_null, result_key_bigint,
                                       result_value_is_null, result_value_bigint
                                FROM shiba_internal.operator_result_row) AS result_row)
             )::text",
        ),
        continuations: json(
            client,
            "SELECT COALESCE(jsonb_agg(to_jsonb(continuation)
                        ORDER BY commit_lsn), '[]')::text
             FROM shiba_internal.source_continuation AS continuation",
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
        "SELECT operator_id, result_key_bigint, result_value_bigint,
                result_key_is_null, result_value_is_null
         FROM shiba.operator_result_rows
         ORDER BY operator_id, result_key_is_null DESC,
                  result_key_bigint NULLS FIRST",
    );
    let expected = result_rows(
        client,
        "SELECT operator_id, result_key_bigint, result_value_bigint,
                result_key_is_null, result_value_is_null
         FROM (
             SELECT 1::bigint AS operator_id, payload AS result_key_bigint,
                    count(*)::bigint AS result_value_bigint,
                    payload IS NULL AS result_key_is_null,
                    false AS result_value_is_null
             FROM source.events GROUP BY payload
             UNION ALL
             SELECT 2, payload, sum(id)::bigint, payload IS NULL, false
             FROM source.events GROUP BY payload
             UNION ALL
             SELECT 3, id, sum(payload)::bigint, false,
                    sum(payload) IS NULL
             FROM source.events GROUP BY id
         ) AS oracle
         ORDER BY operator_id, result_key_is_null DESC,
                  result_key_bigint NULLS FIRST",
    );
    assert_eq!(actual, expected, "grouped results differ from SQL oracle");
}

pub(crate) fn node_state_payload(client: &mut Client, operator_id: i64, key: i64) -> Vec<u8> {
    client
        .query_one(
            "SELECT state.state_payload
             FROM shiba_internal.operator_node_state AS state
             JOIN shiba_internal.operator_result_row AS result
               ON result.operator_id = state.operator_id
              AND result.key_payload = state.partition_key_payload
             WHERE state.operator_id = $1
               AND result.result_key_bigint = $2
               AND NOT result.result_key_is_null",
            &[&operator_id, &key],
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
    assert_eq!(
        client
            .execute(
                "UPDATE shiba_internal.operator_node_state AS state
                 SET state_payload = $3
                 FROM shiba_internal.operator_result_row AS result
                 WHERE state.operator_id = $1
                   AND result.operator_id = state.operator_id
                   AND result.key_payload = state.partition_key_payload
                   AND result.result_key_bigint = $2
                   AND NOT result.result_key_is_null",
                &[&operator_id, &key, &payload],
            )
            .expect("replace exact grouped node state"),
        1
    );
}
