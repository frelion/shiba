use postgres::{Client, NoTls};
use shiba_operator::KernelError;
use shiba_protocol::{SlotGeneration, SourceId};
use shiba_runtime::{
    GraphTransaction, M2Error, PgoutputSource, ProcessOutcome, decode_committed_changes, process,
};

mod support;

use support::{PgoutputCapture, message_end, read_u16, register_source, set_scalar_int8_result};

const CAPTURE: PgoutputCapture = PgoutputCapture {
    script: "scripts/test-m4-delete.sh",
    env_prefix: "SHIBA_M4_DELETE",
    slot: "shiba_m4_delete_slot",
    publication: "shiba_m4_delete_pub",
};

fn durable_state(client: &mut Client) -> (i64, i64, i64, i64) {
    let row = client
        .query_one(
            "SELECT
                (SELECT (convert_from(row_payload, 'UTF8')::jsonb #>> '{values,0,value}')::bigint FROM shiba.graph_result_rows WHERE graph_id = 1 AND result_id = 2),
                (SELECT state_payload FROM shiba_internal.graph_node_state WHERE graph_id = 1 AND node_id = 1 AND namespace = 0),
                (SELECT count(*) FROM shiba_internal.source_row_state),
                (SELECT count(*) FROM shiba_internal.graph_continuation)",
            &[],
        )
        .expect("query durable state");
    (
        row.get(0),
        support::decode_optional_scalar_state(row.get::<_, Option<Vec<u8>>>(1).as_deref()),
        row.get(2),
        row.get(3),
    )
}

fn delete_key_tag(wire: &[u8]) -> usize {
    assert_eq!(wire[0], b'B');
    let relation = message_end(wire, 0);
    assert_eq!(wire[relation], b'R');
    let delete = message_end(wire, relation);
    assert_eq!(wire[delete], b'D');
    assert_eq!(
        wire[delete + 5],
        b'K',
        "default replica identity must use K"
    );
    assert_eq!(read_u16(wire, delete + 6), 1);
    delete + 8
}

fn install_crash_trigger(client: &mut Client) {
    client
        .batch_execute(
            "CREATE SCHEMA m4_delete_test;
             CREATE FUNCTION m4_delete_test.crash_after_continuation()
             RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
                 PERFORM pg_terminate_backend(pg_backend_pid());
                 RETURN NEW;
             END
             $$;
             CREATE TRIGGER m4_delete_crash
             AFTER INSERT ON shiba_internal.graph_continuation
             FOR EACH ROW EXECUTE FUNCTION m4_delete_test.crash_after_continuation();",
        )
        .expect("install continuation crash point");
}

fn assert_apply_row(client: &mut Client, row_id: i64) {
    let count = client
        .query_one(
            "SELECT count(*) FROM shiba_internal.source_row_state WHERE source_row_id = $1",
            &[&row_id],
        )
        .expect("query unaffected Apply row")
        .get::<_, i64>(0);
    assert_eq!(count, 1);
}

fn prove_count_underflow(client: &mut Client, delete: &GraphTransaction) {
    client
        .batch_execute(
            "CREATE TEMP TABLE m4_delete_state_backup AS
                 SELECT * FROM shiba_internal.graph_node_state
                 WHERE graph_id = 1 AND node_id = 1;
             DELETE FROM shiba_internal.graph_node_state
                 WHERE graph_id = 1 AND node_id = 1;",
        )
        .expect("install count underflow precondition");
    set_scalar_int8_result(client, 1, 2, Some(0));
    let error = process(client, delete).expect_err("count underflow must fail");
    assert!(matches!(error, M2Error::Kernel(KernelError::Underflow)));
    assert_eq!(durable_state(client), (0, 0, 2, 1));
    assert_apply_row(client, 401);
    client
        .batch_execute(
            "INSERT INTO shiba_internal.graph_node_state
                 SELECT * FROM m4_delete_state_backup;
             DROP TABLE m4_delete_state_backup;",
        )
        .expect("restore count after underflow proof");
    set_scalar_int8_result(client, 1, 2, Some(2));
}

fn prove_missing_row(client: &mut Client, source: PgoutputSource) {
    client
        .batch_execute("INSERT INTO source_m4_delete.events VALUES (499)")
        .expect("commit deliberately unapplied source row");
    let _ = CAPTURE.capture(client, "unapplied-insert.pgoutput");
    client
        .batch_execute("DELETE FROM source_m4_delete.events WHERE id = 499")
        .expect("commit delete whose Apply row is missing");
    let wire = CAPTURE.capture(client, "missing-delete.pgoutput");
    let missing = decode_committed_changes(&wire, &support::singleton_graph(1, source))
        .expect("decode missing-row delete");
    assert!(process(client, &missing).is_err());
    assert_eq!(durable_state(client), (1, 1, 1, 2));
    assert_apply_row(client, 402);
}

#[test]
#[ignore = "requires the isolated logical PostgreSQL cluster from scripts/test-m4-delete.sh"]
fn m4_real_pgoutput_delete_replay_decode_failure_and_crash() {
    let connection = CAPTURE.required("DATABASE_URL");
    let mut client = Client::connect(&connection, NoTls).expect("connect to temporary PostgreSQL");
    client
        .batch_execute(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source_m4_delete;
             CREATE TABLE source_m4_delete.events (id bigint PRIMARY KEY);
             CREATE PUBLICATION shiba_m4_delete_pub
                 FOR TABLE source_m4_delete.events;",
        )
        .expect("install delete source objects");
    let relation_id = u32::try_from(
        client
            .query_one(
                "SELECT 'source_m4_delete.events'::regclass::oid::bigint",
                &[],
            )
            .expect("read source relation OID")
            .get::<_, i64>(0),
    )
    .expect("relation OID fits u32");
    let source = PgoutputSource::new(
        SourceId::new(1).expect("non-zero source"),
        SlotGeneration::new(1).expect("non-zero generation"),
        relation_id,
    );
    register_source(&mut client, "source_m4_delete.events");
    CAPTURE.create_slot();
    support::configure_graph_ingress(&mut client, 1, CAPTURE.publication, CAPTURE.slot);

    client
        .batch_execute("INSERT INTO source_m4_delete.events VALUES (401), (402)")
        .expect("commit source insert");
    let insert_wire = CAPTURE.capture(&mut client, "insert.pgoutput");
    let insert = decode_committed_changes(&insert_wire, &support::singleton_graph(1, source))
        .expect("decode insert");
    assert_eq!(
        process(&mut client, &insert).expect("apply insert"),
        ProcessOutcome::Applied
    );
    assert_eq!(durable_state(&mut client), (2, 2, 2, 1));

    client
        .batch_execute("DELETE FROM source_m4_delete.events WHERE id = 401")
        .expect("commit source delete");
    let delete_wire = CAPTURE.capture(&mut client, "delete.pgoutput");
    let mut bad_delete = delete_wire.clone();
    let key_tag = delete_key_tag(&bad_delete);
    assert_eq!(bad_delete[key_tag], b't');
    bad_delete[key_tag] = b'n';
    assert!(decode_committed_changes(&bad_delete, &support::singleton_graph(1, source)).is_err());
    assert_eq!(durable_state(&mut client), (2, 2, 2, 1));

    let delete = decode_committed_changes(&delete_wire, &support::singleton_graph(1, source))
        .expect("decode delete");
    let wrong_relation = PgoutputSource::new(
        SourceId::new(1).expect("non-zero source"),
        SlotGeneration::new(1).expect("non-zero generation"),
        relation_id.checked_add(1).expect("different relation OID"),
    );
    assert!(
        decode_committed_changes(&delete_wire, &support::singleton_graph(1, wrong_relation))
            .is_err()
    );
    assert_eq!(durable_state(&mut client), (2, 2, 2, 1));

    prove_count_underflow(&mut client, &delete);

    install_crash_trigger(&mut client);
    assert!(process(&mut client, &delete).is_err());
    let mut client = Client::connect(&connection, NoTls).expect("reconnect after crash");
    assert_eq!(durable_state(&mut client), (2, 2, 2, 1));
    assert_apply_row(&mut client, 402);
    client
        .batch_execute("DROP SCHEMA m4_delete_test CASCADE")
        .expect("remove crash point");

    assert_eq!(
        process(&mut client, &delete).expect("apply delete"),
        ProcessOutcome::Applied
    );
    assert_eq!(durable_state(&mut client), (1, 1, 1, 2));
    assert_eq!(
        process(&mut client, &delete).expect("exact delete replay"),
        ProcessOutcome::AlreadyApplied
    );
    assert_eq!(durable_state(&mut client), (1, 1, 1, 2));

    prove_missing_row(&mut client, source);
}
