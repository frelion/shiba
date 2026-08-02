use postgres::{Client, NoTls};
use shiba_protocol::{SlotGeneration, SourceId};
use shiba_runtime::{PgoutputSource, ProcessOutcome, decode_committed_changes, process};

mod support;

use support::{PgoutputCapture, message_end, read_u16, register_source};

const CAPTURE: PgoutputCapture = PgoutputCapture {
    script: "scripts/test-m4-update.sh",
    env_prefix: "SHIBA_M4_UPDATE",
    slot: "shiba_m4_update_slot",
    publication: "shiba_m4_update_pub",
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

fn payload(client: &mut Client) -> (bool, Option<i64>) {
    let row = client
        .query_one(
            "SELECT payload_present, payload_int8
             FROM shiba_internal.applied_insert WHERE source_row_id = 301",
            &[],
        )
        .expect("query applied payload");
    (row.get(0), row.get(1))
}

fn update_key_tag(wire: &[u8]) -> usize {
    assert_eq!(wire[0], b'B');
    let relation = message_end(wire, 0);
    assert_eq!(wire[relation], b'R');
    let update = message_end(wire, relation);
    assert_eq!(wire[update], b'U');
    assert_eq!(wire[update + 5], b'N', "UPDATE must have only a new tuple");
    assert_eq!(read_u16(wire, update + 6), 2);
    update + 8
}

fn install_crash_trigger(client: &mut Client) {
    client
        .batch_execute(
            "CREATE SCHEMA m4_update_test;
             CREATE FUNCTION m4_update_test.crash_after_continuation()
             RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
                 PERFORM pg_terminate_backend(pg_backend_pid());
                 RETURN NEW;
             END
             $$;
             CREATE TRIGGER m4_update_crash
             AFTER INSERT ON shiba_internal.source_continuation
             FOR EACH ROW EXECUTE FUNCTION m4_update_test.crash_after_continuation();",
        )
        .expect("install continuation crash point");
}

#[test]
#[ignore = "requires the isolated logical PostgreSQL cluster from scripts/test-m4-update.sh"]
fn m4_real_pgoutput_update_replay_decode_failure_and_crash() {
    let connection = CAPTURE.required("DATABASE_URL");
    let mut client = Client::connect(&connection, NoTls).expect("connect to temporary PostgreSQL");
    client
        .batch_execute(
            "CREATE EXTENSION shiba_catalog;
             CREATE SCHEMA source_m4_update;
             CREATE TABLE source_m4_update.events (
                 id bigint PRIMARY KEY,
                 payload bigint NULL
             );
             CREATE PUBLICATION shiba_m4_update_pub
                 FOR TABLE source_m4_update.events;",
        )
        .expect("install update source objects");
    let relation_id = u32::try_from(
        client
            .query_one(
                "SELECT 'source_m4_update.events'::regclass::oid::bigint",
                &[],
            )
            .expect("read source relation OID")
            .get::<_, i64>(0),
    )
    .expect("relation OID fits u32");
    let source = PgoutputSource::with_nullable_int8_payload(
        SourceId::new(1).expect("non-zero source"),
        SlotGeneration::new(1).expect("non-zero generation"),
        relation_id,
    );
    register_source(&mut client, "source_m4_update.events");
    CAPTURE.create_slot();

    client
        .batch_execute("INSERT INTO source_m4_update.events VALUES (301, 10)")
        .expect("commit source insert");
    let insert_wire = CAPTURE.capture(&mut client, "insert.pgoutput");
    let insert = decode_committed_changes(&insert_wire, source).expect("decode insert");
    assert_eq!(
        process(&mut client, &insert).expect("apply insert"),
        ProcessOutcome::Applied
    );
    assert_eq!(durable_state(&mut client), (1, 1, 1, 1));
    assert_eq!(payload(&mut client), (true, Some(10)));

    client
        .batch_execute("UPDATE source_m4_update.events SET payload = NULL WHERE id = 301")
        .expect("commit source update");
    let update_wire = CAPTURE.capture(&mut client, "update.pgoutput");
    let mut bad_update = update_wire.clone();
    let key_tag = update_key_tag(&bad_update);
    assert_eq!(bad_update[key_tag], b't');
    bad_update[key_tag] = b'n';
    assert!(decode_committed_changes(&bad_update, source).is_err());
    assert_eq!(durable_state(&mut client), (1, 1, 1, 1));
    assert_eq!(payload(&mut client), (true, Some(10)));

    let update = decode_committed_changes(&update_wire, source).expect("decode update");
    install_crash_trigger(&mut client);
    assert!(process(&mut client, &update).is_err());
    let mut client = Client::connect(&connection, NoTls).expect("reconnect after crash");
    assert_eq!(durable_state(&mut client), (1, 1, 1, 1));
    assert_eq!(payload(&mut client), (true, Some(10)));
    client
        .batch_execute("DROP SCHEMA m4_update_test CASCADE")
        .expect("remove crash point");

    assert_eq!(
        process(&mut client, &update).expect("apply update"),
        ProcessOutcome::Applied
    );
    assert_eq!(durable_state(&mut client), (1, 1, 1, 2));
    assert_eq!(payload(&mut client), (true, None));
    assert_eq!(
        process(&mut client, &update).expect("exact update replay"),
        ProcessOutcome::AlreadyApplied
    );
    assert_eq!(durable_state(&mut client), (1, 1, 1, 2));
    assert_eq!(payload(&mut client), (true, None));

    client
        .batch_execute("UPDATE source_m4_update.events SET payload = 77 WHERE id = 301")
        .expect("commit non-null source update");
    let non_null_wire = CAPTURE.capture(&mut client, "non-null-update.pgoutput");
    let non_null =
        decode_committed_changes(&non_null_wire, source).expect("decode text payload update");
    assert_eq!(
        process(&mut client, &non_null).expect("apply non-null update"),
        ProcessOutcome::Applied
    );
    assert_eq!(durable_state(&mut client), (1, 1, 1, 3));
    assert_eq!(payload(&mut client), (true, Some(77)));

    client
        .batch_execute("INSERT INTO source_m4_update.events VALUES (302, 1)")
        .expect("commit deliberately unapplied source row");
    let _ = CAPTURE.capture(&mut client, "unapplied-insert.pgoutput");
    client
        .batch_execute("UPDATE source_m4_update.events SET payload = 2 WHERE id = 302")
        .expect("commit update whose Apply row is missing");
    let missing_wire = CAPTURE.capture(&mut client, "missing-update.pgoutput");
    let missing =
        decode_committed_changes(&missing_wire, source).expect("decode missing-row update");
    assert!(process(&mut client, &missing).is_err());
    assert_eq!(durable_state(&mut client), (1, 1, 1, 3));
    assert_eq!(payload(&mut client), (true, Some(77)));
}
