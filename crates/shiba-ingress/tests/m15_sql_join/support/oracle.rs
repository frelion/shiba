use postgres::Client;

pub(crate) fn assert_old_oracle(client: &mut Client) {
    assert_oracle(client, "left_source.events", "right_source.events");
}

pub(crate) fn assert_target_oracle(client: &mut Client) {
    assert_oracle(
        client,
        "join_target_left.events",
        "join_target_right.events",
    );
}

pub(crate) fn oracle(
    client: &mut Client,
    left: &str,
    right: &str,
) -> Vec<(i64, Option<i64>, bool)> {
    client
        .query(
            &format!(
                "SELECT left_row.id,right_row.payload,right_row.payload IS NULL
                 FROM {left} AS left_row INNER JOIN {right} AS right_row
                   ON left_row.right_key=right_row.id ORDER BY left_row.id"
            ),
            &[],
        )
        .expect("query complete PostgreSQL join oracle")
        .into_iter()
        .map(|row| {
            (
                row.get::<_, i64>(0),
                row.get::<_, Option<i64>>(1),
                row.get::<_, bool>(2),
            )
        })
        .collect()
}

fn assert_oracle(client: &mut Client, left: &str, right: &str) {
    let expected = oracle(client, left, right);
    let actual = client
        .query(
            "SELECT result_key_bigint,result_value_bigint,result_value_is_null
             FROM shiba.graph_result_rows WHERE graph_id=1 ORDER BY result_key_bigint",
            &[],
        )
        .expect("query complete materialized SQL join rows")
        .into_iter()
        .map(|row| {
            (
                row.get::<_, i64>(0),
                row.get::<_, Option<i64>>(1),
                row.get::<_, bool>(2),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
    let status: String = client
        .query_one(
            "SELECT result_status FROM shiba.graph_result WHERE graph_id=1",
            &[],
        )
        .expect("read active SQL join result")
        .get(0);
    assert_eq!(status, "active");
}
