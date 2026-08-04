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

pub(crate) fn assert_multi_call(client: &mut Client, fixture: &GraphFixture) {
    let expected = client
        .query_one(
            &format!(
                "SELECT count(*)::bigint, count(payload)::bigint, sum(payload)::bigint
                 FROM {}.rows",
                fixture.schema
            ),
            &[],
        )
        .map(|row| {
            (
                row.get::<_, i64>(0),
                row.get::<_, i64>(1),
                row.get::<_, Option<i64>>(2),
            )
        })
        .expect("query multi-call SQL oracle");
    assert_eq!(multi_row(client, fixture.graph), expected);
}

pub(crate) fn assert_min_max(client: &mut Client, fixture: &GraphFixture) {
    let expected: (Option<i64>, Option<i64>) = client
        .query_one(
            &format!(
                "SELECT min(payload)::bigint, max(payload)::bigint FROM {}.rows",
                fixture.schema
            ),
            &[],
        )
        .map(|row| (row.get(0), row.get(1)))
        .expect("query MIN/MAX SQL oracle");
    assert_eq!(minmax_row(client, fixture.graph), expected);
}

pub(crate) fn exercise_multi_call(
    database_url: &str,
    replication_url: &str,
    client: &mut Client,
    fixture: &GraphFixture,
) {
    assert_multi_call(client, fixture);
    let mut session = attach(database_url, replication_url, fixture, 1);
    for (sql, expected) in [
        (
            "BEGIN;
             UPDATE agg_multi.rows SET payload=7 WHERE id=2;
             INSERT INTO agg_multi.rows VALUES (4,NULL);
             COMMIT;",
            (4_i64, 3_i64, Some(27_i64)),
        ),
        (
            "DELETE FROM agg_multi.rows WHERE id=1",
            (3_i64, 2_i64, Some(17_i64)),
        ),
    ] {
        client
            .batch_execute(sql)
            .expect("commit multi-call transition");
        let token = session
            .receive_and_apply_one()
            .expect("apply multi-call transition");
        assert_eq!(token.outcome(), ProcessOutcome::Applied);
        assert_eq!(multi_row(client, fixture.graph), expected);
        assert_oracle(client, fixture);
        session
            .acknowledge(&token)
            .expect("ACK multi-call transition");
        wait_for_slot_lsn(client, fixture.slot, token.end_lsn());
    }
    session.detach().expect("detach multi-call aggregate");
}

pub(crate) fn exercise_min_max(
    database_url: &str,
    replication_url: &str,
    client: &mut Client,
    fixture: &GraphFixture,
) {
    assert_min_max(client, fixture);
    let mut session = attach(database_url, replication_url, fixture, 1);
    for (sql, expected) in [
        (
            "INSERT INTO agg_extrema.rows VALUES (5,5)",
            (Some(5), Some(10)),
        ),
        (
            "DELETE FROM agg_extrema.rows WHERE id=3",
            (Some(5), Some(10)),
        ),
        (
            "DELETE FROM agg_extrema.rows WHERE id=5",
            (Some(10), Some(10)),
        ),
        (
            "UPDATE agg_extrema.rows SET payload=20 WHERE id=1",
            (Some(10), Some(20)),
        ),
        (
            "UPDATE agg_extrema.rows SET payload=NULL WHERE id=2",
            (Some(20), Some(20)),
        ),
    ] {
        client
            .batch_execute(sql)
            .expect("commit MIN/MAX transition");
        let token = session
            .receive_and_apply_one()
            .expect("apply MIN/MAX transition");
        assert_eq!(token.outcome(), ProcessOutcome::Applied);
        assert_eq!(minmax_row(client, fixture.graph), expected, "{sql}");
        assert_oracle(client, fixture);
        session.acknowledge(&token).expect("ACK MIN/MAX transition");
        wait_for_slot_lsn(client, fixture.slot, token.end_lsn());
    }
    session.detach().expect("detach MIN/MAX aggregate");
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

fn multi_row(client: &mut Client, graph: u64) -> (i64, i64, Option<i64>) {
    client
        .query_one(
            "SELECT
                (convert_from(row_payload,'UTF8')::jsonb #>> '{values,0,value}')::bigint,
                (convert_from(row_payload,'UTF8')::jsonb #>> '{values,1,value}')::bigint,
                CASE WHEN convert_from(row_payload,'UTF8')::jsonb
                              #>> '{values,2,type}' = 'null'
                     THEN NULL ELSE (convert_from(row_payload,'UTF8')::jsonb
                              #>> '{values,2,value}')::bigint END
             FROM shiba.graph_result_rows WHERE graph_id=$1",
            &[&i64::try_from(graph).expect("graph ID fits")],
        )
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .expect("query multi-call result row")
}

fn minmax_row(client: &mut Client, graph: u64) -> (Option<i64>, Option<i64>) {
    client
        .query_one(
            "SELECT
                CASE WHEN convert_from(row_payload,'UTF8')::jsonb
                              #>> '{values,0,type}' = 'null'
                     THEN NULL ELSE (convert_from(row_payload,'UTF8')::jsonb
                              #>> '{values,0,value}')::bigint END,
                CASE WHEN convert_from(row_payload,'UTF8')::jsonb
                              #>> '{values,1,type}' = 'null'
                     THEN NULL ELSE (convert_from(row_payload,'UTF8')::jsonb
                              #>> '{values,1,value}')::bigint END
             FROM shiba.graph_result_rows WHERE graph_id=$1",
            &[&i64::try_from(graph).expect("graph ID fits")],
        )
        .map(|row| (row.get(0), row.get(1)))
        .expect("query MIN/MAX result row")
}
