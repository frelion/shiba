use postgres::{Client, NoTls};
use shiba_protocol::{SlotGeneration, SourceId};
use shiba_runtime::{M2Error, PgoutputSource, ProcessOutcome, decode_committed_changes, process};

mod support;

use support::{PgoutputCapture, message_end, read_u16, read_u32, register_source};

const CAPTURE: PgoutputCapture = PgoutputCapture {
    script: "scripts/test-m5-composite-delete.sh",
    env_prefix: "SHIBA_M5_COMPOSITE_DELETE",
    slot: "shiba_m5_composite_delete_slot",
    publication: "shiba_m5_composite_delete_pub",
};

fn durable_state(client: &mut Client) -> (i64, i64, i64, i64) {
    let row = client
        .query_one(
            "SELECT
                (SELECT value_bigint FROM shiba.operator_result WHERE operator_id = 1),
                (SELECT value_bigint FROM shiba_internal.operator_state WHERE operator_id = 1),
                (SELECT count(*) FROM shiba_internal.applied_insert),
                (SELECT count(*) FROM shiba_internal.source_continuation)",
            &[],
        )
        .expect("query durable state");
    (row.get(0), row.get(1), row.get(2), row.get(3))
}

fn assert_pair(client: &mut Client, first: i64, second: i64, expected: i64) {
    let count = client
        .query_one(
            "SELECT count(*) FROM shiba_internal.applied_insert
             WHERE source_row_id = $1 AND source_row_sub_id = $2",
            &[&first, &second],
        )
        .expect("query composite Apply row")
        .get::<_, i64>(0);
    assert_eq!(count, expected);
}

fn second_delete_key_tag(wire: &[u8]) -> usize {
    assert_eq!(wire[0], b'B');
    let relation = message_end(wire, 0);
    assert_eq!(wire[relation], b'R');
    let delete = message_end(wire, relation);
    assert_eq!(wire[delete], b'D');
    assert_eq!(wire[delete + 5], b'K');
    assert_eq!(read_u16(wire, delete + 6), 2);
    let first_tag = delete + 8;
    assert_eq!(wire[first_tag], b't');
    let first_length = usize::try_from(read_u32(wire, first_tag + 1)).expect("first key length");
    assert_eq!(&wire[first_tag + 5..first_tag + 5 + first_length], b"801");
    let second_tag = first_tag + 5 + first_length;
    assert_eq!(wire[second_tag], b't');
    let second_length = usize::try_from(read_u32(wire, second_tag + 1)).expect("second key length");
    assert_eq!(&wire[second_tag + 5..second_tag + 5 + second_length], b"1");
    second_tag
}

fn install_crash_trigger(client: &mut Client) {
    client
        .batch_execute(
            "CREATE SCHEMA m5_composite_delete_test;
             CREATE FUNCTION m5_composite_delete_test.crash_after_continuation()
             RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
                 PERFORM pg_terminate_backend(pg_backend_pid());
                 RETURN NEW;
             END
             $$;
             CREATE TRIGGER m5_composite_delete_crash
             AFTER INSERT ON shiba_internal.source_continuation
             FOR EACH ROW EXECUTE FUNCTION
                 m5_composite_delete_test.crash_after_continuation();",
        )
        .expect("install continuation crash point");
}

fn prove_missing_pair(client: &mut Client, source: PgoutputSource) {
    client
        .batch_execute("INSERT INTO source_m5_composite_delete.events VALUES (899, 9)")
        .expect("commit deliberately unapplied composite row");
    let _ = CAPTURE.capture(client, "unapplied-insert.pgoutput");
    client
        .batch_execute(
            "DELETE FROM source_m5_composite_delete.events
             WHERE key1 = 899 AND key2 = 9",
        )
        .expect("commit missing composite delete");
    let wire = CAPTURE.capture(client, "missing-delete.pgoutput");
    let missing = decode_committed_changes(&wire, source).expect("decode missing composite delete");
    assert!(matches!(
        process(client, &missing),
        Err(M2Error::MissingSourceRow)
    ));
    assert_eq!(durable_state(client), (1, 1, 1, 2));
    assert_pair(client, 801, 2, 1);
}

#[test]
#[ignore = "requires the isolated logical PostgreSQL cluster from scripts/test-m5-composite-delete.sh"]
fn m5_real_composite_delete_crash_retry_replay_and_missing_row() {
    let connection = CAPTURE.required("DATABASE_URL");
    let mut client = Client::connect(&connection, NoTls).expect("connect to temporary PostgreSQL");
    client
        .batch_execute(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source_m5_composite_delete;
             CREATE TABLE source_m5_composite_delete.events (
                 key1 bigint,
                 key2 bigint,
                 PRIMARY KEY (key1, key2)
             );
             CREATE PUBLICATION shiba_m5_composite_delete_pub
                 FOR TABLE source_m5_composite_delete.events;",
        )
        .expect("install composite DELETE source objects");
    let relation_id = u32::try_from(
        client
            .query_one(
                "SELECT 'source_m5_composite_delete.events'::regclass::oid::bigint",
                &[],
            )
            .expect("read source relation OID")
            .get::<_, i64>(0),
    )
    .expect("relation OID fits u32");
    let source = PgoutputSource::composite_int8(
        SourceId::new(1).expect("non-zero source"),
        SlotGeneration::new(1).expect("non-zero generation"),
        relation_id,
    );
    register_source(&mut client, "source_m5_composite_delete.events");
    CAPTURE.create_slot();

    client
        .batch_execute(
            "INSERT INTO source_m5_composite_delete.events
             VALUES (801, 1), (801, 2)",
        )
        .expect("commit composite inserts");
    let insert_wire = CAPTURE.capture(&mut client, "composite-insert.pgoutput");
    let insert = decode_committed_changes(&insert_wire, source).expect("decode composite inserts");
    assert_eq!(
        process(&mut client, &insert).expect("apply composite inserts"),
        ProcessOutcome::Applied
    );
    assert_eq!(durable_state(&mut client), (2, 2, 2, 1));

    client
        .batch_execute(
            "DELETE FROM source_m5_composite_delete.events
             WHERE key1 = 801 AND key2 = 1",
        )
        .expect("commit composite delete");
    let delete_wire = CAPTURE.capture(&mut client, "composite-delete.pgoutput");
    let second_tag = second_delete_key_tag(&delete_wire);
    let mut corrupt = delete_wire.clone();
    corrupt[second_tag] = b'n';
    assert!(decode_committed_changes(&corrupt, source).is_err());
    assert_eq!(durable_state(&mut client), (2, 2, 2, 1));

    let delete = decode_committed_changes(&delete_wire, source).expect("decode composite delete");
    install_crash_trigger(&mut client);
    assert!(process(&mut client, &delete).is_err());
    let mut client = Client::connect(&connection, NoTls).expect("reconnect after crash");
    assert_eq!(durable_state(&mut client), (2, 2, 2, 1));
    assert_pair(&mut client, 801, 1, 1);
    assert_pair(&mut client, 801, 2, 1);
    client
        .batch_execute("DROP SCHEMA m5_composite_delete_test CASCADE")
        .expect("remove crash point");

    assert_eq!(
        process(&mut client, &delete).expect("retry composite delete"),
        ProcessOutcome::Applied
    );
    assert_eq!(durable_state(&mut client), (1, 1, 1, 2));
    assert_pair(&mut client, 801, 1, 0);
    assert_pair(&mut client, 801, 2, 1);
    assert_eq!(
        process(&mut client, &delete).expect("replay composite delete"),
        ProcessOutcome::AlreadyApplied
    );
    assert_eq!(durable_state(&mut client), (1, 1, 1, 2));
    prove_missing_pair(&mut client, source);
}
