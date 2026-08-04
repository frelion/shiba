use postgres::{Client, NoTls};
use shiba_operator::TypedValue;
use shiba_protocol::{SlotGeneration, SourceId};
use shiba_runtime::{M2Error, PgoutputSource, ProcessOutcome, decode_committed_changes, process};

mod support;

use support::{PgoutputCapture, set_scalar_int8_result};

const CAPTURE: PgoutputCapture = PgoutputCapture {
    script: "scripts/test-m9-count-sum.sh",
    env_prefix: "SHIBA_M9_COUNT_SUM",
    slot: "shiba_m9_count_sum_slot",
    publication: "shiba_m9_count_sum_pub",
};

fn capture(
    client: &mut Client,
    source: PgoutputSource,
    sql: &str,
    name: &str,
) -> shiba_runtime::GraphTransaction {
    client
        .batch_execute(sql)
        .expect("commit source transaction");
    let wire = CAPTURE.capture(client, name);
    decode_committed_changes(&wire, &support::singleton_graph(1, source))
        .unwrap_or_else(|error| panic!("decode {name}: {error:?}"))
}

fn values(client: &mut Client) -> (i64, i64, i64) {
    let row = client
        .query_one(
            "SELECT
                (SELECT (convert_from(row_payload, 'UTF8')::jsonb #>> '{values,0,value}')::bigint FROM shiba.graph_result_rows WHERE graph_id = 1 AND result_id = 3),
                (SELECT CASE
                    WHEN convert_from(row_payload, 'UTF8')::jsonb #>> '{values,0,type}' = 'null'
                    THEN 0
                    ELSE (convert_from(row_payload, 'UTF8')::jsonb #>> '{values,0,value}')::bigint
                 END FROM shiba.graph_result_rows WHERE graph_id = 1 AND result_id = 4),
                (SELECT count(*) FROM shiba_internal.graph_continuation)",
            &[],
        )
        .expect("query results and continuation");
    let public = (row.get(0), row.get(1), row.get(2));
    let scalar_partition = TypedValue::Bool(true)
        .to_canonical_json()
        .expect("canonical scalar state partition");
    let private = client
        .query(
            "SELECT state.state_payload
             FROM shiba.graph_result AS result
             LEFT JOIN shiba_internal.graph_node_state AS state
              ON state.graph_id = result.graph_id
              AND state.node_id = result.result_id - 2
              AND state.namespace = 1
              AND state.partition_key_payload = $1
              AND state.item_key_payload = $2
             WHERE result.graph_id = 1 ORDER BY result.result_id",
            &[&scalar_partition, &b"null".as_slice()],
        )
        .expect("query private states")
        .into_iter()
        .map(|row| {
            row.get::<_, Option<Vec<u8>>>(0).map_or(0, |payload| {
                let offset = payload.len().saturating_sub(8);
                i64::from_be_bytes(payload[offset..].try_into().expect("aggregate int8 state"))
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(private, vec![public.0, public.1]);
    public
}

fn rows(client: &mut Client) -> Vec<(i64, Option<i64>)> {
    client
        .query(
            "SELECT source_row_id, payload_int8
             FROM shiba_internal.source_row_state ORDER BY source_row_id",
            &[],
        )
        .expect("query current source-row state")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

fn install_crash_after_count(client: &mut Client) {
    client
        .batch_execute(
            "CREATE SCHEMA m9_count_sum_test;
             CREATE FUNCTION m9_count_sum_test.crash_after_count()
             RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
                 IF NEW.result_id = 3 THEN
                     PERFORM pg_terminate_backend(pg_backend_pid());
                 END IF;
                 RETURN NEW;
             END
             $$;
             CREATE TRIGGER m9_crash_after_count
             AFTER UPDATE ON shiba_internal.graph_result_row
             FOR EACH ROW EXECUTE FUNCTION m9_count_sum_test.crash_after_count();",
        )
        .expect("install crash after first operator result");
}

fn install_replay_trap(client: &mut Client) {
    client
        .batch_execute(
            "CREATE FUNCTION m9_count_sum_test.reject_operator_replay()
             RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
                 RAISE EXCEPTION 'operator executed during exact replay';
             END
             $$;
             CREATE TRIGGER m9_reject_operator_replay
             BEFORE UPDATE ON shiba_internal.graph_node_state
             FOR EACH ROW EXECUTE FUNCTION m9_count_sum_test.reject_operator_replay();",
        )
        .expect("install exact replay operator trap");
}

#[test]
#[ignore = "requires the isolated PostgreSQL cluster from scripts/test-m9-count-sum.sh"]
#[allow(clippy::too_many_lines, reason = "one ordered crash/retry proof")]
fn m9_count_and_sum_share_one_atomic_effect_batch() {
    let connection = CAPTURE.required("DATABASE_URL");
    let mut client = Client::connect(&connection, NoTls).expect("connect to PostgreSQL");
    client
        .batch_execute(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source;
             CREATE TABLE source.events (id bigint PRIMARY KEY, payload bigint NULL);
             CREATE PUBLICATION shiba_m9_count_sum_pub FOR TABLE source.events;
             SELECT shiba_internal.register_source(1, 'source.events'::regclass);",
        )
        .expect("install source and binding");
    support::register_count_sum_graph(&mut client, 1);
    let relation_oid = client
        .query_one("SELECT 'source.events'::regclass::oid::bigint", &[])
        .expect("read relation oid")
        .get::<_, i64>(0);
    let source = PgoutputSource::with_nullable_int8_payload(
        SourceId::new(1).expect("source id"),
        SlotGeneration::new(1).expect("generation"),
        u32::try_from(relation_oid).expect("oid fits u32"),
    );
    CAPTURE.create_slot();
    support::configure_graph_ingress(&mut client, 1, CAPTURE.publication, CAPTURE.slot);

    let _unapplied = capture(
        &mut client,
        source,
        "INSERT INTO source.events VALUES (999, 1)",
        "missing-insert.pgoutput",
    );
    let missing = capture(
        &mut client,
        source,
        "DELETE FROM source.events WHERE id = 999",
        "missing-delete.pgoutput",
    );
    assert!(matches!(
        process(&mut client, &missing),
        Err(M2Error::MissingSourceRow)
    ));
    assert_eq!(values(&mut client), (0, 0, 0));

    let insert = capture(
        &mut client,
        source,
        "INSERT INTO source.events VALUES (1, 10), (2, NULL)",
        "insert.pgoutput",
    );
    assert_eq!(
        process(&mut client, &insert).unwrap(),
        ProcessOutcome::Applied
    );
    assert_eq!(values(&mut client), (2, 10, 1));
    assert_eq!(rows(&mut client), vec![(1, Some(10)), (2, None)]);

    let update_null = capture(
        &mut client,
        source,
        "UPDATE source.events SET payload = 7 WHERE id = 2",
        "update-null.pgoutput",
    );
    assert_eq!(
        process(&mut client, &update_null).unwrap(),
        ProcessOutcome::Applied
    );
    assert_eq!(values(&mut client), (2, 17, 2));
    let update_value = capture(
        &mut client,
        source,
        "UPDATE source.events SET payload = NULL WHERE id = 1",
        "update-value.pgoutput",
    );
    assert_eq!(
        process(&mut client, &update_value).unwrap(),
        ProcessOutcome::Applied
    );
    assert_eq!(values(&mut client), (2, 7, 3));
    let delete = capture(
        &mut client,
        source,
        "DELETE FROM source.events WHERE id = 2",
        "delete.pgoutput",
    );
    assert_eq!(
        process(&mut client, &delete).unwrap(),
        ProcessOutcome::Applied
    );
    assert_eq!(values(&mut client), (1, 0, 4));
    assert_eq!(rows(&mut client), vec![(1, None)]);

    let overflow = capture(
        &mut client,
        source,
        "INSERT INTO source.events VALUES (3, 1)",
        "overflow.pgoutput",
    );
    client
        .batch_execute(
            "UPDATE shiba_internal.graph_node_state
                SET state_payload = decode('00000000000000017fffffffffffffff', 'hex')
               WHERE graph_id = 1 AND node_id = 2 AND namespace = 1;
             ",
        )
        .expect("inject sum overflow boundary");
    set_scalar_int8_result(&mut client, 1, 4, Some(i64::MAX));
    assert!(matches!(
        process(&mut client, &overflow),
        Err(M2Error::Kernel(_))
    ));
    assert_eq!(values(&mut client), (1, i64::MAX, 4));
    assert_eq!(rows(&mut client), vec![(1, None)]);
    client
        .batch_execute(
            "UPDATE shiba_internal.graph_node_state
                SET state_payload = decode('00000000000000000000000000000000', 'hex')
              WHERE graph_id = 1 AND node_id = 2 AND namespace = 1;
             ",
        )
        .expect("remove overflow injection");
    set_scalar_int8_result(&mut client, 1, 4, Some(0));
    assert_eq!(
        process(&mut client, &overflow).unwrap(),
        ProcessOutcome::Applied
    );
    assert_eq!(values(&mut client), (2, 1, 5));

    let crash = capture(
        &mut client,
        source,
        "UPDATE source.events SET payload = 2 WHERE id = 3",
        "crash.pgoutput",
    );
    install_crash_after_count(&mut client);
    assert!(process(&mut client, &crash).is_err());
    let mut client = Client::connect(&connection, NoTls).expect("reconnect after crash");
    assert_eq!(values(&mut client), (2, 1, 5));
    assert_eq!(rows(&mut client), vec![(1, None), (3, Some(1))]);
    client
        .batch_execute("DROP SCHEMA m9_count_sum_test CASCADE")
        .expect("remove crash injection");
    assert_eq!(
        process(&mut client, &crash).unwrap(),
        ProcessOutcome::Applied
    );
    assert_eq!(values(&mut client), (2, 2, 6));
    install_crash_after_count(&mut client);
    install_replay_trap(&mut client);
    assert_eq!(
        process(&mut client, &crash).unwrap(),
        ProcessOutcome::AlreadyApplied
    );
    assert_eq!(values(&mut client), (2, 2, 6));
    client
        .batch_execute("DROP SCHEMA m9_count_sum_test CASCADE")
        .expect("remove replay trap");

    let invalidated = capture(
        &mut client,
        source,
        "INSERT INTO source.events VALUES (4, 4)",
        "invalidated.pgoutput",
    );
    client
        .batch_execute("ALTER TABLE source.events RENAME COLUMN payload TO renamed_payload")
        .expect("invalidate bound payload column");
    assert!(matches!(
        process(&mut client, &invalidated),
        Err(M2Error::SourceInvalidated)
    ));
    assert_eq!(values(&mut client), (2, 2, 6));
    assert_eq!(rows(&mut client), vec![(1, None), (3, Some(2))]);
}
