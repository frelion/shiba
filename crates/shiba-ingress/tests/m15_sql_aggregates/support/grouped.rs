use postgres::Client;
use shiba_runtime::ProcessOutcome;

use super::{GraphFixture, assert_oracle, attach, wait_for_slot_lsn};

type ResultRow = (Option<i64>, Option<i64>, bool, bool);
type HavingRow = (Option<i64>, i64, Option<i64>);

pub(crate) fn assert_count(client: &mut Client, fixture: &GraphFixture) {
    let expected = rows(
        client,
        "SELECT id,count(*)::bigint,false,false
         FROM agg_group_count.rows WHERE payload>0 GROUP BY id ORDER BY id",
    );
    assert_eq!(actual(client, fixture.graph), expected);
}

pub(crate) fn assert_sum(client: &mut Client, fixture: &GraphFixture) {
    let expected = rows(
        client,
        &format!(
            "SELECT payload,sum(id)::bigint,payload IS NULL,false
             FROM {}.rows GROUP BY payload
             ORDER BY payload IS NULL,payload",
            fixture.schema
        ),
    );
    assert_eq!(actual(client, fixture.graph), expected);
}

pub(crate) fn assert_having(client: &mut Client, fixture: &GraphFixture) {
    let expected: Vec<HavingRow> = client
        .query(
            "SELECT payload, count(*)::bigint, sum(payload)::bigint
             FROM agg_having.rows WHERE id > 0 GROUP BY payload
             HAVING count(*) > 1 ORDER BY payload NULLS LAST",
            &[],
        )
        .expect("query HAVING SQL oracle")
        .into_iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect();
    assert_eq!(having_actual(client, fixture.graph), expected);
}

pub(crate) fn exercise_having(
    database_url: &str,
    replication_url: &str,
    client: &mut Client,
    fixture: &GraphFixture,
) {
    assert_having(client, fixture);
    let mut session = attach(database_url, replication_url, fixture, 1);
    for sql in [
        "UPDATE agg_having.rows SET payload=5 WHERE id=1",
        "INSERT INTO agg_having.rows VALUES (6,5)",
        "DELETE FROM agg_having.rows WHERE id IN (3,6)",
        "UPDATE agg_having.rows SET payload=NULL WHERE id=2",
    ] {
        client.batch_execute(sql).expect("commit HAVING transition");
        let token = session
            .receive_and_apply_one()
            .expect("apply HAVING transition");
        assert_eq!(token.outcome(), ProcessOutcome::Applied);
        assert_having(client, fixture);
        session.acknowledge(&token).expect("ACK HAVING transition");
        wait_for_slot_lsn(client, fixture.slot, token.end_lsn());
    }
    session.detach().expect("detach HAVING aggregate");
}

pub(crate) fn exercise_filtered_count(
    database_url: &str,
    replication_url: &str,
    client: &mut Client,
    fixture: &GraphFixture,
) {
    let mut session = attach(database_url, replication_url, fixture, 1);
    client
        .batch_execute(
            "BEGIN;
             UPDATE agg_group_count.rows SET payload=2 WHERE id=2;
             UPDATE agg_group_count.rows SET payload=3 WHERE id=3;
             UPDATE agg_group_count.rows SET id=10 WHERE id=1;
             COMMIT;",
        )
        .expect("commit false/NULL/true and key-change transitions");
    apply_and_ack(&mut session, client, fixture);
    client
        .batch_execute(
            "BEGIN;
             UPDATE agg_group_count.rows SET payload=0 WHERE id=2;
             DELETE FROM agg_group_count.rows WHERE id=10;
             COMMIT;",
        )
        .expect("commit group deletion transitions");
    apply_and_ack(&mut session, client, fixture);
    session.detach().expect("detach grouped Count");
}

pub(crate) fn exercise_sum_and_recovery(
    database_url: &str,
    replication_url: &str,
    client: &mut Client,
    fixture: &GraphFixture,
) {
    let mut session = attach(database_url, replication_url, fixture, 1);
    client
        .batch_execute(
            "BEGIN;
             UPDATE agg_group_sum.rows SET payload=20 WHERE id=2;
             DELETE FROM agg_group_sum.rows WHERE id=1;
             INSERT INTO agg_group_sum.rows VALUES (4,NULL);
             COMMIT;",
        )
        .expect("commit group create/delete/key-change and NULL transitions");
    apply_and_ack(&mut session, client, fixture);
    session
        .detach()
        .expect("detach grouped Sum before overflow");

    let state = client
        .query_one(
            "SELECT node_id,state_payload
             FROM shiba_internal.graph_node_state
             WHERE graph_id=4 AND namespace=1
               AND (convert_from(partition_key_payload,'UTF8')::jsonb
                    #>> '{value}')::bigint=20
               AND convert_from(partition_key_payload,'UTF8')::jsonb
                    #>> '{type}' = 'int8'",
            &[],
        )
        .expect("read grouped SUM partition state");
    let node: i64 = state.get(0);
    let original: Vec<u8> = state.get(1);
    let mut overflow = 1_i64.to_be_bytes().to_vec();
    overflow.extend_from_slice(&i64::MAX.to_be_bytes());
    client
        .execute(
            "UPDATE shiba_internal.graph_node_state SET state_payload=$2
             WHERE graph_id=4 AND node_id=$1 AND namespace=1
               AND (convert_from(partition_key_payload,'UTF8')::jsonb
                    #>> '{value}')::bigint=20
               AND convert_from(partition_key_payload,'UTF8')::jsonb
                    #>> '{type}' = 'int8'",
            &[&node, &overflow],
        )
        .expect("inject checked-overflow state");
    let before = durable(client, fixture);
    let feedback = slot_lsn(client, fixture.slot);
    client
        .batch_execute("INSERT INTO agg_group_sum.rows VALUES (5,20)")
        .expect("commit overflow input");
    let mut failed = attach(database_url, replication_url, fixture, 1);
    assert!(failed.receive_and_apply_one().is_err());
    drop(failed);
    assert_eq!(durable(client, fixture), before);
    assert_eq!(slot_lsn(client, fixture.slot), feedback);

    client
        .execute(
            "UPDATE shiba_internal.graph_node_state SET state_payload=$2
             WHERE graph_id=4 AND node_id=$1 AND namespace=1
               AND (convert_from(partition_key_payload,'UTF8')::jsonb
                    #>> '{value}')::bigint=20
               AND convert_from(partition_key_payload,'UTF8')::jsonb
                    #>> '{type}' = 'int8'",
            &[&node, &original],
        )
        .expect("restore valid grouped SUM state");
    let mut retry = attach(database_url, replication_url, fixture, 1);
    let applied = retry
        .receive_and_apply_one()
        .expect("retry overflow-rolled-back transaction");
    assert_eq!(applied.outcome(), ProcessOutcome::Applied);
    assert_oracle(client, fixture);
    let end_lsn = applied.end_lsn();
    drop(retry);
    let mut replay = attach(database_url, replication_url, fixture, 1);
    let replayed = replay
        .receive_and_apply_one()
        .expect("replay committed grouped SUM transaction");
    assert_eq!(replayed.outcome(), ProcessOutcome::AlreadyApplied);
    assert_eq!(replayed.end_lsn(), end_lsn);
    replay.acknowledge(&replayed).expect("ACK exact replay");
    wait_for_slot_lsn(client, fixture.slot, end_lsn);
    replay.detach().expect("detach grouped SUM replay");
}

fn apply_and_ack(
    session: &mut shiba_ingress::GovernedGraphSession,
    client: &mut Client,
    fixture: &GraphFixture,
) {
    let token = session
        .receive_and_apply_one()
        .expect("apply grouped aggregate transaction");
    assert_eq!(token.outcome(), ProcessOutcome::Applied);
    assert_oracle(client, fixture);
    session.acknowledge(&token).expect("ACK grouped aggregate");
    wait_for_slot_lsn(client, fixture.slot, token.end_lsn());
}

fn actual(client: &mut Client, graph: u64) -> Vec<ResultRow> {
    rows(
        client,
        &format!(
            "SELECT
                    CASE WHEN convert_from(row_payload,'UTF8')::jsonb
                                   #>> '{{values,0,type}}' = 'null'
                         THEN NULL ELSE (convert_from(row_payload,'UTF8')::jsonb
                                   #>> '{{values,0,value}}')::bigint END,
                    CASE WHEN convert_from(row_payload,'UTF8')::jsonb
                                   #>> '{{values,1,type}}' = 'null'
                         THEN NULL ELSE (convert_from(row_payload,'UTF8')::jsonb
                                   #>> '{{values,1,value}}')::bigint END,
                    convert_from(row_payload,'UTF8')::jsonb #>> '{{values,0,type}}' = 'null',
                    convert_from(row_payload,'UTF8')::jsonb #>> '{{values,1,type}}' = 'null'
             FROM shiba.graph_result_rows WHERE graph_id={graph}
             ORDER BY 3,1"
        ),
    )
}

fn having_actual(client: &mut Client, graph: u64) -> Vec<HavingRow> {
    client
        .query(
            "SELECT
                CASE WHEN convert_from(row_payload,'UTF8')::jsonb #>> '{values,0,type}' = 'null'
                     THEN NULL ELSE (convert_from(row_payload,'UTF8')::jsonb #>> '{values,0,value}')::bigint END,
                (convert_from(row_payload,'UTF8')::jsonb #>> '{values,1,value}')::bigint,
                CASE WHEN convert_from(row_payload,'UTF8')::jsonb #>> '{values,2,type}' = 'null'
                     THEN NULL ELSE (convert_from(row_payload,'UTF8')::jsonb #>> '{values,2,value}')::bigint END
             FROM shiba.graph_result_rows WHERE graph_id=$1 ORDER BY 1 NULLS LAST",
            &[&i64::try_from(graph).expect("graph ID fits")],
        )
        .expect("query HAVING result rows")
        .into_iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect()
}

fn rows(client: &mut Client, sql: &str) -> Vec<ResultRow> {
    client
        .query(sql, &[])
        .expect("query complete aggregate rows")
        .into_iter()
        .map(|row| (row.get(0), row.get(1), row.get(2), row.get(3)))
        .collect()
}

fn durable(client: &mut Client, fixture: &GraphFixture) -> String {
    client
        .query_one(
            "SELECT jsonb_build_object(
                 'source',(SELECT COALESCE(jsonb_agg(to_jsonb(s) ORDER BY source_row_id),'[]')
                           FROM shiba_internal.source_row_state s WHERE source_id=4),
                 'state',(SELECT COALESCE(jsonb_agg(to_jsonb(s) ORDER BY node_id,partition_key_payload),'[]')
                          FROM shiba_internal.graph_node_state s WHERE graph_id=4),
                 'result',(SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY row_identity),'[]')
                           FROM shiba_internal.graph_result_row r WHERE graph_id=4),
                 'continuation',(SELECT COALESCE(jsonb_agg(to_jsonb(c) ORDER BY commit_lsn),'[]')
                                 FROM shiba_internal.graph_continuation c WHERE graph_id=4)
             )::text",
            &[],
        )
        .unwrap_or_else(|_| panic!("query graph {} durable snapshot", fixture.graph))
        .get(0)
}

fn slot_lsn(client: &mut Client, slot: &str) -> String {
    client
        .query_one(
            "SELECT confirmed_flush_lsn::text FROM pg_catalog.pg_replication_slots
             WHERE slot_name=$1",
            &[&slot],
        )
        .expect("read aggregate slot LSN")
        .get(0)
}
