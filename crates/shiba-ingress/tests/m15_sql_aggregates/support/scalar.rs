use postgres::Client;
use shiba_runtime::ProcessOutcome;

use super::{GraphFixture, assert_oracle, attach, wait_for_slot_lsn};

pub(crate) fn assert_count(client: &mut Client, fixture: &GraphFixture) {
    let expected: i64 = client
        .query_one(
            &format!("SELECT count(*)::bigint FROM {}.rows", fixture.schema),
            &[],
        )
        .expect("query count SQL oracle")
        .get(0);
    assert_eq!(scalar(client, fixture.graph), Some(expected));
}

pub(crate) fn assert_sum(client: &mut Client, fixture: &GraphFixture) {
    let expected: Option<i64> = client
        .query_one(
            &format!("SELECT sum(payload)::bigint FROM {}.rows", fixture.schema),
            &[],
        )
        .expect("query sum SQL oracle")
        .get(0);
    assert_eq!(scalar(client, fixture.graph), expected);
}

pub(crate) fn exercise_count(
    database_url: &str,
    replication_url: &str,
    client: &mut Client,
    fixture: &GraphFixture,
) {
    let mut session = attach(database_url, replication_url, fixture, 1);
    client
        .batch_execute(
            "BEGIN;
             INSERT INTO agg_count.rows VALUES (3,-5),(4,NULL);
             DELETE FROM agg_count.rows WHERE id=1;
             COMMIT;",
        )
        .expect("commit count I/D");
    let token = session
        .receive_and_apply_one()
        .expect("apply SQL Count transaction");
    assert_eq!(token.outcome(), ProcessOutcome::Applied);
    assert_oracle(client, fixture);
    session.acknowledge(&token).expect("ACK SQL Count");
    wait_for_slot_lsn(client, fixture.slot, token.end_lsn());
    session.detach().expect("detach SQL Count");
}

pub(crate) fn exercise_nullable_sum(
    database_url: &str,
    replication_url: &str,
    client: &mut Client,
    fixture: &GraphFixture,
) {
    assert_eq!(scalar(client, fixture.graph), None, "empty SUM is NULL");
    let mut session = attach(database_url, replication_url, fixture, 1);
    for (sql, expected) in [
        ("INSERT INTO agg_sum.rows VALUES (1,NULL)", None),
        ("INSERT INTO agg_sum.rows VALUES (2,5)", Some(5)),
        ("DELETE FROM agg_sum.rows WHERE id=2", None),
        ("DELETE FROM agg_sum.rows WHERE id=1", None),
    ] {
        client.batch_execute(sql).expect("commit SUM transition");
        let token = session
            .receive_and_apply_one()
            .expect("apply SQL SUM transition");
        assert_eq!(token.outcome(), ProcessOutcome::Applied);
        assert_eq!(scalar(client, fixture.graph), expected);
        assert_oracle(client, fixture);
        session.acknowledge(&token).expect("ACK SQL SUM");
        wait_for_slot_lsn(client, fixture.slot, token.end_lsn());
    }
    session.detach().expect("detach SQL SUM");
}

fn scalar(client: &mut Client, graph: u64) -> Option<i64> {
    client
        .query_one(
            "SELECT CASE WHEN convert_from(row_payload,'UTF8')::jsonb
                                  #>> '{values,0,type}' = 'null'
                         THEN NULL
                         ELSE (convert_from(row_payload,'UTF8')::jsonb
                                  #>> '{values,0,value}')::bigint END
             FROM shiba.graph_result_rows
             WHERE graph_id=$1",
            &[&i64::try_from(graph).expect("graph ID fits")],
        )
        .expect("query scalar aggregate result")
        .get(0)
}
